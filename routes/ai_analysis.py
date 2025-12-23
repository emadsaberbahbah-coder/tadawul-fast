# routes/ai_analysis.py
"""
AI & QUANT ANALYSIS ROUTES – GOOGLE SHEETS FRIENDLY (v3.4.0)

Key goals
- Engine-driven only: core.data_engine_v2.DataEngine (no direct provider calls).
- Prefer main.py lifespan engine (request.app.state.engine), fallback to singleton.
- Defensive batching: chunking + timeout + bounded concurrency.
- Sheets-safe: /sheet-rows ALWAYS returns HTTP 200 with {headers, rows, status}.
- Canonical headers: core.schemas.get_headers_for_sheet (59 columns).
- Token guard via X-APP-TOKEN (APP_TOKEN / BACKUP_APP_TOKEN). If no token is set => open.

Notes
- This module is intentionally self-contained (auth + engine resolution) to avoid circular imports.
- If EnrichedQuote model is not importable for any reason, we still return 59-col rows via a
  robust fallback mapping (never crash, never break Sheets).
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field

from core.config import get_settings
from core.data_engine_v2 import DataEngine, UnifiedQuote, normalize_symbol

# Canonical headers (59 cols) when available
try:
    from core.schemas import get_headers_for_sheet  # type: ignore
except Exception:  # pragma: no cover
    get_headers_for_sheet = None  # type: ignore

# Prefer EnrichedQuote if your project exposes it (best formatting & mapping)
EnrichedQuote = None  # type: ignore
try:
    from core.enriched_quote import EnrichedQuote as _EQ  # type: ignore

    EnrichedQuote = _EQ  # type: ignore
except Exception:  # pragma: no cover
    try:
        from routes.enriched_quote import EnrichedQuote as _EQ2  # type: ignore

        EnrichedQuote = _EQ2  # type: ignore
    except Exception:
        EnrichedQuote = None  # type: ignore


logger = logging.getLogger("routes.ai_analysis")

AI_ANALYSIS_VERSION = "3.4.0"
router = APIRouter(prefix="/v1/analysis", tags=["AI & Analysis"])


# =============================================================================
# Auth (X-APP-TOKEN) — same contract as routes/enriched_quote.py
# =============================================================================
@lru_cache(maxsize=1)
def _allowed_tokens() -> List[str]:
    tokens: List[str] = []

    # settings first
    try:
        s = get_settings()
        for attr in ("app_token", "backup_app_token", "APP_TOKEN", "BACKUP_APP_TOKEN"):
            v = getattr(s, attr, None)
            if isinstance(v, str) and v.strip():
                tokens.append(v.strip())
    except Exception:
        pass

    # env.py exports if present
    try:
        import env as env_mod  # type: ignore

        for attr in ("APP_TOKEN", "BACKUP_APP_TOKEN"):
            v = getattr(env_mod, attr, None)
            if isinstance(v, str) and v.strip():
                tokens.append(v.strip())
    except Exception:
        pass

    # environment variables last resort
    for k in ("APP_TOKEN", "BACKUP_APP_TOKEN"):
        v = os.getenv(k)
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
        logger.warning("[analysis] No APP_TOKEN configured -> endpoints are OPEN (no auth).")
    return out


def _require_token_or_401(x_app_token: Optional[str]) -> None:
    allowed = _allowed_tokens()
    if not allowed:
        return  # open mode
    if not x_app_token or x_app_token.strip() not in allowed:
        raise HTTPException(status_code=401, detail="Unauthorized (invalid or missing X-APP-TOKEN).")


# =============================================================================
# Engine resolution (prefer app.state.engine; else singleton)
# =============================================================================
_ENGINE: Optional[DataEngine] = None
_ENGINE_LOCK = asyncio.Lock()


def _get_app_engine(request: Optional[Request]) -> Optional[DataEngine]:
    try:
        if request is None:
            return None
        st = getattr(getattr(request, "app", None), "state", None)
        if not st:
            return None
        for attr in ("engine", "data_engine", "data_engine_v2"):
            eng = getattr(st, attr, None)
            if isinstance(eng, DataEngine):
                return eng
        return None
    except Exception:
        return None


async def _get_singleton_engine() -> Optional[DataEngine]:
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE

    async with _ENGINE_LOCK:
        if _ENGINE is None:
            try:
                _ENGINE = DataEngine()
                logger.info("[analysis] DataEngine initialized (fallback singleton).")
            except Exception as exc:
                logger.exception("[analysis] Failed to init DataEngine: %s", exc)
                _ENGINE = None
    return _ENGINE


async def _resolve_engine(request: Optional[Request]) -> Optional[DataEngine]:
    eng = _get_app_engine(request)
    if eng is not None:
        return eng
    return await _get_singleton_engine()


# =============================================================================
# Settings / defaults
# =============================================================================
def _safe_int(x: Any, default: int) -> int:
    try:
        v = int(str(x).strip())
        return v if v > 0 else default
    except Exception:
        return default


def _safe_float(x: Any, default: float) -> float:
    try:
        v = float(str(x).strip())
        return v if v > 0 else default
    except Exception:
        return default


def _cfg() -> Dict[str, Any]:
    s = None
    try:
        s = get_settings()
    except Exception:
        s = None

    batch_size = _safe_int(getattr(s, "ai_batch_size", None), 20)
    timeout_sec = _safe_float(getattr(s, "ai_batch_timeout_sec", None), 45.0)
    concurrency = _safe_int(getattr(s, "ai_batch_concurrency", None), 5)
    max_tickers = _safe_int(getattr(s, "ai_max_tickers", None), 500)

    # env overrides
    batch_size = _safe_int(os.getenv("AI_BATCH_SIZE", batch_size), batch_size)
    timeout_sec = _safe_float(os.getenv("AI_BATCH_TIMEOUT_SEC", timeout_sec), timeout_sec)
    concurrency = _safe_int(os.getenv("AI_BATCH_CONCURRENCY", concurrency), concurrency)
    max_tickers = _safe_int(os.getenv("AI_MAX_TICKERS", max_tickers), max_tickers)

    # sanity clamps
    batch_size = max(5, min(200, batch_size))
    timeout_sec = max(5.0, min(180.0, timeout_sec))
    concurrency = max(1, min(25, concurrency))
    max_tickers = max(10, min(2000, max_tickers))

    return {
        "batch_size": batch_size,
        "timeout_sec": timeout_sec,
        "concurrency": concurrency,
        "max_tickers": max_tickers,
    }


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# =============================================================================
# Models
# =============================================================================
class _ExtraIgnoreBase(BaseModel):
    model_config = ConfigDict(extra="ignore")


class SingleAnalysisResponse(_ExtraIgnoreBase):
    symbol: str
    name: Optional[str] = None
    market_region: Optional[str] = None

    price: Optional[float] = None
    change_pct: Optional[float] = None
    market_cap: Optional[float] = None
    pe_ttm: Optional[float] = None
    pb: Optional[float] = None
    dividend_yield: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None

    data_quality: str = "MISSING"

    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    risk_score: Optional[float] = None
    overall_score: Optional[float] = None
    recommendation: Optional[str] = None

    fair_value: Optional[float] = None
    upside_percent: Optional[float] = None
    valuation_label: Optional[str] = None

    last_updated_utc: Optional[str] = None
    sources: List[str] = Field(default_factory=list)
    error: Optional[str] = None


class BatchAnalysisRequest(_ExtraIgnoreBase):
    # accept both to keep clients stable
    tickers: List[str] = Field(default_factory=list)
    symbols: List[str] = Field(default_factory=list)
    sheet_name: Optional[str] = None


class BatchAnalysisResponse(_ExtraIgnoreBase):
    results: List[SingleAnalysisResponse] = Field(default_factory=list)


class SheetAnalysisResponse(_ExtraIgnoreBase):
    headers: List[str] = Field(default_factory=list)
    rows: List[List[Any]] = Field(default_factory=list)
    status: str = "success"  # success | partial | error | skipped
    error: Optional[str] = None


# =============================================================================
# Helpers
# =============================================================================
def _clean_symbols(items: Sequence[Any]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in (items or []):
        if x is None:
            continue
        s = normalize_symbol(str(x).strip())
        if not s:
            continue
        su = s.upper()
        if su in seen:
            continue
        seen.add(su)
        out.append(su)
    return out


def _chunk(items: List[str], size: int) -> List[List[str]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]


def _select_headers(sheet_name: Optional[str]) -> List[str]:
    if get_headers_for_sheet:
        try:
            h = get_headers_for_sheet(sheet_name)
            if isinstance(h, list) and h and any(str(x).strip().lower() == "symbol" for x in h):
                return [str(x) for x in h]
        except Exception:
            pass
    # last-resort fallback (Sheets will still render)
    return ["Symbol", "Error"]


def _idx(headers: List[str], name: str) -> Optional[int]:
    k = name.strip().lower()
    for i, h in enumerate(headers):
        if str(h).strip().lower() == k:
            return i
    return None


def _error_row(symbol: str, headers: List[str], err: str) -> List[Any]:
    row = [None] * len(headers)
    i_sym = _idx(headers, "Symbol")
    if i_sym is not None:
        row[i_sym] = symbol
    i_err = _idx(headers, "Error")
    if i_err is not None:
        row[i_err] = err
    return row


def _extract_sources_from_any(x: Any) -> List[str]:
    if not x:
        return []
    if isinstance(x, list):
        return [str(a).strip() for a in x if str(a).strip()]
    s = str(x).strip()
    if not s:
        return []
    if "," in s:
        parts = [p.strip() for p in s.split(",") if p.strip()]
        return parts or [s]
    return [s]


def _ratio_to_percent(v: Optional[float]) -> Optional[float]:
    """
    Normalize common ratios that may come as decimals to percent scale:
    If -1..1 => *100, else return as-is.
    """
    if v is None:
        return None
    try:
        return (float(v) * 100.0) if -1.0 <= float(v) <= 1.0 else float(v)
    except Exception:
        return v


def _status_from_rows(headers: List[str], rows: List[List[Any]]) -> str:
    i_err = _idx(headers, "Error")
    if i_err is None:
        return "success"
    for r in rows:
        if len(r) > i_err and (r[i_err] not in (None, "", "null", "None")):
            return "partial"
    return "success"


def _safe_get(obj: Any, *names: str) -> Any:
    for n in names:
        try:
            v = getattr(obj, n, None)
            if v is not None:
                return v
        except Exception:
            pass
    return None


def _compute_52w_position_pct(cp: Any, low_52w: Any, high_52w: Any) -> Optional[float]:
    try:
        cp_f = float(cp)
        lo = float(low_52w)
        hi = float(high_52w)
        if hi == lo:
            return None
        return round(((cp_f - lo) / (hi - lo)) * 100.0, 2)
    except Exception:
        return None


def _to_riyadh_iso(utc_any: Any) -> Optional[str]:
    """
    Best-effort conversion to Asia/Riyadh.
    If parsing fails, returns None.
    """
    if not utc_any:
        return None
    try:
        from zoneinfo import ZoneInfo  # py3.9+

        tz = ZoneInfo("Asia/Riyadh")
        if isinstance(utc_any, datetime):
            dt = utc_any
        else:
            dt = datetime.fromisoformat(str(utc_any))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(tz).isoformat()
    except Exception:
        return None


# =============================================================================
# Engine calls (defensive batching)
# =============================================================================
async def _engine_get_quotes(engine: DataEngine, syms: List[str]) -> List[UnifiedQuote]:
    """
    Compatibility shim:
    - Prefer engine.get_enriched_quotes if present
    - Else engine.get_quotes
    - Else per-symbol engine.get_quote
    """
    if hasattr(engine, "get_enriched_quotes"):
        return await engine.get_enriched_quotes(syms)  # type: ignore
    if hasattr(engine, "get_quotes"):
        return await engine.get_quotes(syms)  # type: ignore

    out: List[UnifiedQuote] = []
    for s in syms:
        out.append(await engine.get_quote(s))  # type: ignore
    return out


async def _get_quotes_chunked(
    request: Optional[Request],
    symbols: List[str],
    *,
    batch_size: int,
    timeout_sec: float,
    max_concurrency: int,
) -> Dict[str, UnifiedQuote]:
    clean = _clean_symbols(symbols)
    if not clean:
        return {}

    engine = await _resolve_engine(request)
    if engine is None:
        msg = "Engine unavailable"
        return {s: UnifiedQuote(symbol=s, data_quality="MISSING", error=msg).finalize() for s in clean}

    chunks = _chunk(clean, batch_size)
    sem = asyncio.Semaphore(max(1, max_concurrency))

    async def _run_chunk(chunk_syms: List[str]) -> Tuple[List[str], List[UnifiedQuote] | Exception]:
        async with sem:
            try:
                res = await asyncio.wait_for(_engine_get_quotes(engine, chunk_syms), timeout=timeout_sec)
                return chunk_syms, res
            except Exception as e:
                return chunk_syms, e

    results = await asyncio.gather(*[_run_chunk(c) for c in chunks])

    out: Dict[str, UnifiedQuote] = {}
    for chunk_syms, res in results:
        if isinstance(res, Exception):
            msg = "Engine batch timeout" if isinstance(res, asyncio.TimeoutError) else f"Engine batch error: {res}"
            logger.warning("[analysis] %s for chunk(size=%d)", msg, len(chunk_syms))
            for s in chunk_syms:
                out[s.upper()] = UnifiedQuote(symbol=s.upper(), data_quality="MISSING", error=msg).finalize()
            continue

        returned = list(res or [])
        chunk_map = {q.symbol.upper(): q for q in returned if q and getattr(q, "symbol", None)}

        for s in chunk_syms:
            s_up = s.upper()
            out[s_up] = chunk_map.get(s_up) or UnifiedQuote(symbol=s_up, data_quality="MISSING", error="No data returned").finalize()

    return out


# =============================================================================
# Transform
# =============================================================================
def _quote_to_analysis(requested_symbol: str, uq: UnifiedQuote) -> SingleAnalysisResponse:
    # best-effort scoring on UnifiedQuote itself (some engines already attach scores)
    try:
        from core.scoring_engine import enrich_with_scores  # type: ignore

        uq = enrich_with_scores(uq)  # type: ignore
    except Exception:
        pass

    sym = (getattr(uq, "symbol", None) or normalize_symbol(requested_symbol) or requested_symbol or "").upper()

    return SingleAnalysisResponse(
        symbol=sym,
        name=_safe_get(uq, "name", "company_name"),
        market_region=_safe_get(uq, "market", "market_region"),

        price=_safe_get(uq, "current_price", "last_price", "price"),
        change_pct=_safe_get(uq, "percent_change", "change_pct"),
        market_cap=_safe_get(uq, "market_cap"),
        pe_ttm=_safe_get(uq, "pe_ttm"),
        pb=_safe_get(uq, "pb"),

        dividend_yield=_ratio_to_percent(_safe_get(uq, "dividend_yield")),
        roe=_ratio_to_percent(_safe_get(uq, "roe")),
        roa=_ratio_to_percent(_safe_get(uq, "roa")),

        data_quality=str(_safe_get(uq, "data_quality") or "MISSING"),

        value_score=_safe_get(uq, "value_score"),
        quality_score=_safe_get(uq, "quality_score"),
        momentum_score=_safe_get(uq, "momentum_score"),
        opportunity_score=_safe_get(uq, "opportunity_score"),
        risk_score=_safe_get(uq, "risk_score"),
        overall_score=_safe_get(uq, "overall_score"),
        recommendation=_safe_get(uq, "recommendation"),

        fair_value=_safe_get(uq, "fair_value"),
        upside_percent=_safe_get(uq, "upside_percent"),
        valuation_label=_safe_get(uq, "valuation_label"),

        last_updated_utc=_safe_get(uq, "last_updated_utc"),
        sources=_extract_sources_from_any(_safe_get(uq, "data_source", "source")),

        error=_safe_get(uq, "error"),
    )


def _fallback_row_59(headers: List[str], uq: UnifiedQuote) -> List[Any]:
    """
    59-col fallback row builder when EnrichedQuote is unavailable.
    Uses header names to pull values from UnifiedQuote with sensible conversions.
    """
    # attempt to compute 52W position if missing
    cp = _safe_get(uq, "current_price", "last_price", "price")
    low_52w = _safe_get(uq, "low_52w", "week_52_low")
    high_52w = _safe_get(uq, "high_52w", "week_52_high")
    pos_52w = _safe_get(uq, "position_52w", "position_52w_percent", "pos_52w")
    if pos_52w is None and cp is not None and low_52w is not None and high_52w is not None:
        pos_52w = _compute_52w_position_pct(cp, low_52w, high_52w)

    last_utc = _safe_get(uq, "last_updated_utc", "as_of_utc")
    last_riy = _safe_get(uq, "last_updated_riyadh")
    if last_riy is None:
        last_riy = _to_riyadh_iso(last_utc)

    # mapping by header label
    m: Dict[str, Any] = {
        # Identity
        "Symbol": _safe_get(uq, "symbol"),
        "Company Name": _safe_get(uq, "name", "company_name"),
        "Sector": _safe_get(uq, "sector"),
        "Sub-Sector": _safe_get(uq, "sub_sector", "subsector"),
        "Market": _safe_get(uq, "market", "market_region"),
        "Currency": _safe_get(uq, "currency"),
        "Listing Date": _safe_get(uq, "listing_date"),

        # Prices
        "Last Price": cp,
        "Previous Close": _safe_get(uq, "previous_close"),
        "Price Change": _safe_get(uq, "price_change"),
        "Percent Change": _safe_get(uq, "percent_change"),
        "Day High": _safe_get(uq, "day_high"),
        "Day Low": _safe_get(uq, "day_low"),
        "52W High": high_52w,
        "52W Low": low_52w,
        "52W Position %": pos_52w,

        # Volume / Liquidity
        "Volume": _safe_get(uq, "volume"),
        "Avg Volume (30D)": _safe_get(uq, "avg_volume_30d", "avg_volume"),
        "Value Traded": _safe_get(uq, "value_traded"),
        "Turnover %": _ratio_to_percent(_safe_get(uq, "turnover_percent", "turnover")),

        # Shares / Cap
        "Shares Outstanding": _safe_get(uq, "shares_outstanding"),
        "Free Float %": _ratio_to_percent(_safe_get(uq, "free_float_percent", "free_float")),
        "Market Cap": _safe_get(uq, "market_cap"),
        "Free Float Market Cap": _safe_get(uq, "free_float_market_cap"),
        "Liquidity Score": _safe_get(uq, "liquidity_score"),

        # Fundamentals
        "EPS (TTM)": _safe_get(uq, "eps_ttm", "eps"),
        "Forward EPS": _safe_get(uq, "forward_eps"),
        "P/E (TTM)": _safe_get(uq, "pe_ttm"),
        "Forward P/E": _safe_get(uq, "forward_pe"),
        "P/B": _safe_get(uq, "pb"),
        "P/S": _safe_get(uq, "ps"),
        "EV/EBITDA": _safe_get(uq, "ev_ebitda"),
        "Dividend Yield %": _ratio_to_percent(_safe_get(uq, "dividend_yield")),
        "Dividend Rate": _safe_get(uq, "dividend_rate"),
        "Payout Ratio %": _ratio_to_percent(_safe_get(uq, "payout_ratio")),
        "ROE %": _ratio_to_percent(_safe_get(uq, "roe")),
        "ROA %": _ratio_to_percent(_safe_get(uq, "roa")),
        "Net Margin %": _ratio_to_percent(_safe_get(uq, "net_margin")),
        "EBITDA Margin %": _ratio_to_percent(_safe_get(uq, "ebitda_margin")),
        "Revenue Growth %": _ratio_to_percent(_safe_get(uq, "revenue_growth")),
        "Net Income Growth %": _ratio_to_percent(_safe_get(uq, "net_income_growth")),
        "Beta": _safe_get(uq, "beta"),

        # Technicals
        "Volatility (30D)": _ratio_to_percent(_safe_get(uq, "volatility_30d")),
        "RSI (14)": _safe_get(uq, "rsi_14"),

        # Valuation / Targets
        "Fair Value": _safe_get(uq, "fair_value"),
        "Upside %": _ratio_to_percent(_safe_get(uq, "upside_percent")),
        "Valuation Label": _safe_get(uq, "valuation_label"),

        # Scores / Recommendation
        "Value Score": _safe_get(uq, "value_score"),
        "Quality Score": _safe_get(uq, "quality_score"),
        "Momentum Score": _safe_get(uq, "momentum_score"),
        "Opportunity Score": _safe_get(uq, "opportunity_score"),
        "Risk Score": _safe_get(uq, "risk_score"),
        "Overall Score": _safe_get(uq, "overall_score"),
        "Error": _safe_get(uq, "error") or "",
        "Recommendation": _safe_get(uq, "recommendation") or "",

        # Meta
        "Data Source": _safe_get(uq, "data_source"),
        "Data Quality": _safe_get(uq, "data_quality"),
        "Last Updated (UTC)": last_utc,
        "Last Updated (Riyadh)": last_riy,
    }

    row = [m.get(h, None) for h in headers]
    if len(row) < len(headers):
        row += [None] * (len(headers) - len(row))
    return row[: len(headers)]


# =============================================================================
# Routes
# =============================================================================
@router.get("/health")
@router.get("/ping")
async def analysis_health(request: Request) -> Dict[str, Any]:
    cfg = _cfg()
    eng = await _resolve_engine(request)
    return {
        "status": "ok",
        "module": "routes.ai_analysis",
        "version": AI_ANALYSIS_VERSION,
        "engine": "DataEngineV2",
        "providers": list(getattr(eng, "enabled_providers", []) or []) if eng else [],
        "limits": {
            "batch_size": cfg["batch_size"],
            "batch_timeout_sec": cfg["timeout_sec"],
            "batch_concurrency": cfg["concurrency"],
            "max_tickers": cfg["max_tickers"],
        },
        "auth": "open" if not _allowed_tokens() else "token",
        "timestamp_utc": _now_utc_iso(),
    }


@router.get("/quote", response_model=SingleAnalysisResponse)
async def analyze_single_quote(
    request: Request,
    symbol: str = Query(..., description="Ticker symbol (e.g., 1120.SR, AAPL, ^GSPC)."),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> SingleAnalysisResponse:
    _require_token_or_401(x_app_token)

    t = (symbol or "").strip()
    if not t:
        return SingleAnalysisResponse(symbol="", data_quality="MISSING", error="Symbol is required")

    cfg = _cfg()
    m = await _get_quotes_chunked(
        request,
        [t],
        batch_size=1,
        timeout_sec=cfg["timeout_sec"],
        max_concurrency=1,
    )

    key = (normalize_symbol(t) or t).upper()
    uq = m.get(key) or UnifiedQuote(symbol=key, data_quality="MISSING", error="No data returned").finalize()
    return _quote_to_analysis(t, uq)


@router.post("/quotes", response_model=BatchAnalysisResponse)
async def analyze_batch_quotes(
    request: Request,
    body: BatchAnalysisRequest = Body(...),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> BatchAnalysisResponse:
    _require_token_or_401(x_app_token)

    tickers = _clean_symbols((body.tickers or []) + (body.symbols or []))
    if not tickers:
        return BatchAnalysisResponse(results=[])

    cfg = _cfg()
    if len(tickers) > cfg["max_tickers"]:
        tickers = tickers[: cfg["max_tickers"]]

    m = await _get_quotes_chunked(
        request,
        tickers,
        batch_size=cfg["batch_size"],
        timeout_sec=cfg["timeout_sec"],
        max_concurrency=cfg["concurrency"],
    )

    results: List[SingleAnalysisResponse] = []
    for t in tickers:
        uq = m.get(t) or UnifiedQuote(symbol=t, data_quality="MISSING", error="No data returned").finalize()
        results.append(_quote_to_analysis(t, uq))

    return BatchAnalysisResponse(results=results)


@router.post("/sheet-rows", response_model=SheetAnalysisResponse)
async def analyze_for_sheet(
    request: Request,
    body: BatchAnalysisRequest = Body(...),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> SheetAnalysisResponse:
    """
    Sheets-safe: ALWAYS returns HTTP 200 with {headers, rows, status}.
    If auth fails -> returns error rows (no exception).
    """
    headers = _select_headers(body.sheet_name)
    tickers = _clean_symbols((body.tickers or []) + (body.symbols or []))

    if not tickers:
        return SheetAnalysisResponse(headers=headers, rows=[], status="skipped", error="No tickers provided")

    # Sheets-safe auth (never raise out)
    try:
        _require_token_or_401(x_app_token)
    except Exception as e:
        rows = [_error_row(t, headers, str(e)) for t in tickers]
        return SheetAnalysisResponse(headers=headers, rows=rows, status="error", error=str(e))

    cfg = _cfg()
    if len(tickers) > cfg["max_tickers"]:
        tickers = tickers[: cfg["max_tickers"]]

    try:
        m = await _get_quotes_chunked(
            request,
            tickers,
            batch_size=cfg["batch_size"],
            timeout_sec=cfg["timeout_sec"],
            max_concurrency=cfg["concurrency"],
        )

        rows: List[List[Any]] = []
        for t in tickers:
            uq = m.get(t) or UnifiedQuote(symbol=t, data_quality="MISSING", error="No data returned").finalize()

            # Best: EnrichedQuote row mapping if available
            if EnrichedQuote is not None:
                try:
                    eq = EnrichedQuote.from_unified(uq)  # type: ignore
                    rows.append(eq.to_row(headers))  # type: ignore
                    continue
                except Exception:
                    # fallback to local mapping
                    pass

            rows.append(_fallback_row_59(headers, uq))

        status = _status_from_rows(headers, rows)
        return SheetAnalysisResponse(headers=headers, rows=rows, status=status)

    except Exception as exc:
        logger.exception("[analysis] exception in /sheet-rows: %s", exc)
        rows = [_error_row(t, headers, str(exc)) for t in tickers]
        return SheetAnalysisResponse(headers=headers, rows=rows, status="error", error=str(exc))


__all__ = [
    "router",
    "SingleAnalysisResponse",
    "BatchAnalysisRequest",
    "BatchAnalysisResponse",
    "SheetAnalysisResponse",
]
