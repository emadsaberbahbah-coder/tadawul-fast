"""
routes/ai_analysis.py
------------------------------------------------------------
AI & QUANT ANALYSIS ROUTES – GOOGLE SHEETS FRIENDLY (v2.7.0)

Preferred backend:
  - core.data_engine_v2.DataEngine (class-based unified engine)

Fallback backend:
  - core.data_engine (module-level async functions)

Last-resort:
  - In-process stub engine that returns MISSING placeholders (never crashes Sheets)

Provides:
  - GET  /v1/analysis/health  (alias: /ping)
  - GET  /v1/analysis/quote?symbol=...
  - POST /v1/analysis/quotes
  - POST /v1/analysis/sheet-rows   (headers + rows for Google Sheets)

Design goals:
  - KSA-safe: router never calls providers directly.
  - Extremely defensive: bounded batch size, chunking, timeout, concurrency.
  - Never throws 502 for Sheets endpoints; returns headers + rows (maybe empty).

v2.7.0 Improvements:
  - Accept optional sheet_name; ignore unknown body fields (prevents 422).
  - Preserve input order (map results by symbol, not by zip order).
  - Always pad rows to header length for Sheets safety.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

logger = logging.getLogger("routes.ai_analysis")

AI_ANALYSIS_VERSION = "2.7.0"

# ----------------------------------------------------------------------
# Optional env.py integration (safe)
# ----------------------------------------------------------------------
try:  # pragma: no cover
    import env as _env  # type: ignore
except Exception:
    _env = None  # type: ignore

# ----------------------------------------------------------------------
# Runtime limits (env overrides)
# ----------------------------------------------------------------------
DEFAULT_BATCH_SIZE = int(os.getenv("AI_BATCH_SIZE", "50"))
DEFAULT_BATCH_TIMEOUT = float(os.getenv("AI_BATCH_TIMEOUT_SEC", "30"))
DEFAULT_CONCURRENCY = int(os.getenv("AI_BATCH_CONCURRENCY", "3"))
DEFAULT_MAX_TICKERS = int(os.getenv("AI_MAX_TICKERS", "500"))

# ----------------------------------------------------------------------
# ENGINE DETECTION & FALLBACKS
# ----------------------------------------------------------------------
_ENGINE_MODE: str = "stub"  # "v2" | "v1_module" | "stub"
_ENGINE_IS_STUB: bool = True

try:
    from core.data_engine_v2 import DataEngine as _V2DataEngine  # type: ignore

    def _build_v2_kwargs() -> Dict[str, Any]:
        if _env is None:
            return {}

        candidates: Dict[str, Any] = {}

        if hasattr(_env, "ENGINE_CACHE_TTL_SECONDS"):
            candidates["cache_ttl"] = getattr(_env, "ENGINE_CACHE_TTL_SECONDS")
        if hasattr(_env, "ENGINE_PROVIDER_TIMEOUT_SECONDS"):
            candidates["provider_timeout"] = getattr(_env, "ENGINE_PROVIDER_TIMEOUT_SECONDS")
        elif hasattr(_env, "HTTP_TIMEOUT"):
            candidates["provider_timeout"] = getattr(_env, "HTTP_TIMEOUT")
        if hasattr(_env, "ENABLED_PROVIDERS"):
            candidates["enabled_providers"] = getattr(_env, "ENABLED_PROVIDERS")
        if hasattr(_env, "ENGINE_ENABLE_ADVANCED_ANALYSIS"):
            candidates["enable_advanced_analysis"] = getattr(_env, "ENGINE_ENABLE_ADVANCED_ANALYSIS")

        try:
            sig = inspect.signature(_V2DataEngine.__init__)
            allowed = set(sig.parameters.keys())
            allowed.discard("self")
            return {k: v for k, v in candidates.items() if k in allowed and v is not None}
        except Exception:
            return {}

    @lru_cache(maxsize=1)
    def _get_engine_singleton() -> Any:
        kwargs = _build_v2_kwargs()
        try:
            return _V2DataEngine(**kwargs) if kwargs else _V2DataEngine()
        except TypeError:
            return _V2DataEngine()

    _ENGINE_MODE = "v2"
    _ENGINE_IS_STUB = False
    logger.info("routes.ai_analysis: using DataEngine v2 (singleton)")

except Exception as exc_v2:  # pragma: no cover
    logger.exception("routes.ai_analysis: failed to import DataEngine v2: %s", exc_v2)

    try:
        from core import data_engine as _data_engine_module  # type: ignore

        @lru_cache(maxsize=1)
        def _get_engine_singleton() -> Any:
            return _data_engine_module

        _ENGINE_MODE = "v1_module"
        _ENGINE_IS_STUB = False
        logger.warning("routes.ai_analysis: falling back to core.data_engine module-level API")

    except Exception as exc_v1:  # pragma: no cover
        logger.exception("routes.ai_analysis: failed to import v1 fallback: %s", exc_v1)

        class _StubEngine:
            async def get_enriched_quote(self, symbol: str) -> Dict[str, Any]:
                sym = (symbol or "").strip().upper() or "UNKNOWN"
                return {
                    "symbol": sym,
                    "name": None,
                    "market_region": "UNKNOWN",
                    "market": None,
                    "currency": None,
                    "price": None,
                    "last_price": None,
                    "change_pct": None,
                    "change_percent": None,
                    "market_cap": None,
                    "pe_ttm": None,
                    "pe_ratio": None,
                    "pb": None,
                    "pb_ratio": None,
                    "dividend_yield": None,
                    "dividend_yield_percent": None,
                    "roe": None,
                    "roe_percent": None,
                    "roa": None,
                    "data_quality": "MISSING",
                    "value_score": None,
                    "quality_score": None,
                    "momentum_score": None,
                    "opportunity_score": None,
                    "overall_score": None,
                    "recommendation": "MISSING",
                    "fair_value": None,
                    "upside_percent": None,
                    "valuation_label": None,
                    "as_of_utc": None,
                    "last_updated_utc": None,
                    "sources": [],
                    "notes": None,
                    "error": "STUB_ENGINE: core.data_engine_v2/core.data_engine unavailable.",
                }

            async def get_enriched_quotes(self, symbols: List[str]) -> List[Dict[str, Any]]:
                return [await self.get_enriched_quote(s) for s in (symbols or [])]

        @lru_cache(maxsize=1)
        def _get_engine_singleton() -> Any:
            return _StubEngine()

        _ENGINE_MODE = "stub"
        _ENGINE_IS_STUB = True
        logger.error("routes.ai_analysis: using STUB engine (MISSING placeholders).")


# ----------------------------------------------------------------------
# ROUTER
# ----------------------------------------------------------------------
router = APIRouter(prefix="/v1/analysis", tags=["AI & Analysis"])


# ----------------------------------------------------------------------
# MODELS (ignore extra fields to prevent 422 from Sheets client)
# ----------------------------------------------------------------------
class _ExtraIgnoreBase(BaseModel):
    class Config:
        extra = "ignore"


class SingleAnalysisResponse(_ExtraIgnoreBase):
    symbol: str
    name: Optional[str] = None
    market_region: Optional[str] = None

    price: Optional[float] = None
    change_pct: Optional[float] = None
    market_cap: Optional[float] = None
    pe_ttm: Optional[float] = None
    pb: Optional[float] = None

    dividend_yield: Optional[float] = None  # fraction 0–1
    roe: Optional[float] = None             # fraction 0–1
    roa: Optional[float] = None             # fraction 0–1

    data_quality: str = "MISSING"

    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    overall_score: Optional[float] = None
    recommendation: Optional[str] = None

    fair_value: Optional[float] = None
    upside_percent: Optional[float] = None
    valuation_label: Optional[str] = None

    last_updated_utc: Optional[datetime] = None
    sources: List[str] = Field(default_factory=list)

    notes: Optional[str] = None
    error: Optional[str] = None


class BatchAnalysisRequest(_ExtraIgnoreBase):
    tickers: List[str] = Field(default_factory=list)
    sheet_name: Optional[str] = None  # optional, safe for Sheets client


class BatchAnalysisResponse(_ExtraIgnoreBase):
    results: List[SingleAnalysisResponse]


class SheetAnalysisResponse(_ExtraIgnoreBase):
    headers: List[str]
    rows: List[List[Any]]


# ----------------------------------------------------------------------
# INTERNAL HELPERS
# ----------------------------------------------------------------------
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None

def _clamp_0_100(x: Any) -> Optional[float]:
    v = _safe_float(x)
    if v is None:
        return None
    return max(0.0, min(100.0, v))

def _normalize_percent_like_to_fraction(x: Any) -> Optional[float]:
    v = _safe_float(x)
    if v is None:
        return None
    if 0.0 <= v <= 1.0:
        return v
    if 1.0 < v <= 100.0:
        return v / 100.0
    return v

def _model_to_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "model_dump"):
        try:
            return obj.model_dump()  # type: ignore
        except Exception:
            pass
    return getattr(obj, "__dict__", {}) or {}

def _clean_tickers(items: Sequence[str]) -> List[str]:
    raw = [(x or "").strip() for x in (items or [])]
    raw = [x for x in raw if x]
    seen = set()
    out: List[str] = []
    for x in raw:
        ux = x.upper()
        if ux in seen:
            continue
        seen.add(ux)
        out.append(ux)
    return out

def _chunk(items: List[str], size: int) -> List[List[str]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]

def _valuation_label_from_upside(upside_percent: Optional[float]) -> Optional[str]:
    if upside_percent is None:
        return None
    if upside_percent >= 25:
        return "UNDERVALUED"
    if upside_percent <= -15:
        return "OVERVALUED"
    return "FAIR"

def _compute_scores_fallback(data: Dict[str, Any]) -> Dict[str, Optional[float]]:
    dq = str(data.get("data_quality") or "").upper()
    if dq in {"MISSING", "UNKNOWN", ""}:
        return {"value_score": None, "quality_score": None, "momentum_score": None}

    pe = _safe_float(data.get("pe_ttm") or data.get("pe_ratio") or data.get("pe"))
    pb = _safe_float(data.get("pb") or data.get("pb_ratio"))
    dy = _normalize_percent_like_to_fraction(data.get("dividend_yield") or data.get("dividend_yield_percent"))
    roe = _normalize_percent_like_to_fraction(data.get("roe") or data.get("roe_percent"))
    change_pct = _safe_float(data.get("change_pct") or data.get("change_percent"))

    v = 50.0
    if pe is not None:
        if 0 < pe < 12:
            v += 20
        elif 12 <= pe <= 20:
            v += 10
        elif pe > 35:
            v -= 15
    if pb is not None:
        if 0 < pb < 1:
            v += 10
        elif pb > 4:
            v -= 10
    if dy is not None:
        if 0.02 <= dy <= 0.06:
            v += 10
        elif dy > 0.08:
            v -= 5
    value_score = max(0.0, min(100.0, v))

    q = 50.0
    if roe is not None:
        if roe >= 0.20:
            q += 25
        elif 0.10 <= roe < 0.20:
            q += 10
        elif roe < 0:
            q -= 15
    quality_score = max(0.0, min(100.0, q))

    m = 50.0
    if change_pct is not None:
        if change_pct > 0:
            m += min(change_pct, 10) * 2
        elif change_pct < 0:
            m += max(change_pct, -10) * 2
    momentum_score = max(0.0, min(100.0, m))

    return {
        "value_score": round(value_score, 2),
        "quality_score": round(quality_score, 2),
        "momentum_score": round(momentum_score, 2),
    }

def _compute_opportunity_score(data: Dict[str, Any], scores: Dict[str, Optional[float]]) -> Optional[float]:
    existing = _clamp_0_100(data.get("opportunity_score"))
    if existing is not None:
        return existing
    v = scores.get("value_score")
    q = scores.get("quality_score")
    m = scores.get("momentum_score")
    if isinstance(v, (int, float)) and isinstance(q, (int, float)) and isinstance(m, (int, float)):
        return round(v * 0.4 + q * 0.3 + m * 0.3, 2)
    return None

def _compute_overall_and_reco(
    opportunity: Optional[float],
    value: Optional[float],
    quality: Optional[float],
    momentum: Optional[float],
    existing_overall: Optional[float] = None,
    existing_reco: Optional[str] = None,
) -> Tuple[Optional[float], Optional[str]]:
    if existing_overall is not None:
        overall = max(0.0, min(100.0, existing_overall))
    else:
        parts = [x for x in (opportunity, value, quality, momentum) if isinstance(x, (int, float))]
        overall = round(sum(parts) / len(parts), 2) if parts else None

    if existing_reco:
        return overall, str(existing_reco)

    if overall is None:
        return None, None

    if overall >= 80:
        return overall, "STRONG_BUY"
    if overall >= 65:
        return overall, "BUY"
    if overall >= 45:
        return overall, "HOLD"
    if overall >= 30:
        return overall, "REDUCE"
    return overall, "SELL"

def _parse_dt(x: Any) -> Optional[datetime]:
    if x is None:
        return None
    if isinstance(x, datetime):
        return x if x.tzinfo else x.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(x))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None

def _extract_sources(raw_sources: Any) -> List[str]:
    if not raw_sources:
        return []
    out: List[str] = []
    try:
        for s in raw_sources:
            if isinstance(s, str):
                out.append(s)
            elif isinstance(s, dict) and "provider" in s:
                out.append(str(s["provider"]))
            elif hasattr(s, "provider"):
                out.append(str(getattr(s, "provider")))
            else:
                out.append(str(s))
    except Exception:
        return []
    seen = set()
    cleaned: List[str] = []
    for x in out:
        if x in seen:
            continue
        seen.add(x)
        cleaned.append(x)
    return cleaned


# ----------------------------------------------------------------------
# ENGINE CALLS
# ----------------------------------------------------------------------
async def _engine_get_many(symbols: List[str]) -> List[Any]:
    eng = _get_engine_singleton()

    # object method
    if hasattr(eng, "get_enriched_quotes"):
        return await eng.get_enriched_quotes(symbols)  # type: ignore[attr-defined]

    # v1 module fallback: function or coroutine attribute
    if hasattr(eng, "get_enriched_quotes"):
        fn = getattr(eng, "get_enriched_quotes")
        return await fn(symbols)

    # per ticker fallback
    out: List[Any] = []
    for s in symbols:
        if hasattr(eng, "get_enriched_quote"):
            out.append(await eng.get_enriched_quote(s))  # type: ignore[attr-defined]
        else:
            out.append({"symbol": s.upper(), "data_quality": "MISSING", "error": "Engine missing methods"})
    return out

async def _get_quotes_chunked(
    symbols: List[str],
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    timeout_sec: float = DEFAULT_BATCH_TIMEOUT,
    max_concurrency: int = DEFAULT_CONCURRENCY,
) -> Dict[str, Any]:
    clean = _clean_tickers(symbols)
    if not clean:
        return {}

    chunks = _chunk(clean, batch_size)
    sem = asyncio.Semaphore(max(1, max_concurrency))

    async def _run_chunk(chunk_syms: List[str]) -> Tuple[List[str], Any]:
        async with sem:
            try:
                res = await asyncio.wait_for(_engine_get_many(chunk_syms), timeout=timeout_sec)
                return chunk_syms, res
            except Exception as e:
                return chunk_syms, e

    results = await asyncio.gather(*[_run_chunk(c) for c in chunks])

    out: Dict[str, Any] = {}
    for chunk_syms, res in results:
        if isinstance(res, Exception):
            msg = "Engine batch timeout" if isinstance(res, asyncio.TimeoutError) else f"Engine batch error: {res}"
            logger.warning("ai_analysis: %s for chunk(size=%d): %s", msg, len(chunk_syms), chunk_syms)
            for s in chunk_syms:
                out[s] = {"symbol": s, "data_quality": "MISSING", "error": msg}
            continue

        returned = list(res or [])

        # map by returned symbol (order-independent)
        by_symbol: Dict[str, Any] = {}
        for q in returned:
            d = _model_to_dict(q)
            sym = (d.get("symbol") or d.get("ticker") or "").strip().upper()
            if sym:
                by_symbol[sym] = q

        # for each requested symbol, pick by_symbol, else fallback to positional if safe
        for idx, s in enumerate(chunk_syms):
            picked = by_symbol.get(s.upper())
            if picked is None and idx < len(returned):
                picked = returned[idx]
            if picked is None:
                picked = {"symbol": s, "data_quality": "MISSING", "error": "No data returned"}
            out[s] = picked

    return out


# ----------------------------------------------------------------------
# TRANSFORM: quote -> SingleAnalysisResponse
# ----------------------------------------------------------------------
def _quote_to_analysis(raw_symbol: str, quote: Any) -> SingleAnalysisResponse:
    symbol_in = (raw_symbol or "").strip().upper()
    data = _model_to_dict(quote)

    symbol = str(data.get("symbol") or data.get("ticker") or symbol_in or "").upper() or (symbol_in or "UNKNOWN")
    dq = str(data.get("data_quality") or "MISSING")
    name = data.get("name") or data.get("company_name")
    market_region = data.get("market_region") or data.get("market") or data.get("exchange")

    price = _safe_float(data.get("price") or data.get("last_price") or data.get("close") or data.get("last"))
    change_pct = _safe_float(data.get("change_pct") or data.get("change_percent") or data.get("changePercent"))
    market_cap = _safe_float(data.get("market_cap") or data.get("marketCap"))
    pe_ttm = _safe_float(data.get("pe_ttm") or data.get("pe_ratio") or data.get("pe"))
    pb = _safe_float(data.get("pb") or data.get("pb_ratio") or data.get("priceToBook"))

    dividend_yield = _normalize_percent_like_to_fraction(
        data.get("dividend_yield") or data.get("dividend_yield_percent") or data.get("dividendYield")
    )
    roe = _normalize_percent_like_to_fraction(
        data.get("roe") or data.get("roe_percent") or data.get("returnOnEquity")
    )
    roa = _normalize_percent_like_to_fraction(
        data.get("roa") or data.get("roa_percent") or data.get("returnOnAssets")
    )

    value_score = _clamp_0_100(data.get("value_score"))
    quality_score = _clamp_0_100(data.get("quality_score"))
    momentum_score = _clamp_0_100(data.get("momentum_score"))

    if value_score is None or quality_score is None or momentum_score is None:
        fb = _compute_scores_fallback(data)
        value_score = value_score if value_score is not None else fb["value_score"]
        quality_score = quality_score if quality_score is not None else fb["quality_score"]
        momentum_score = momentum_score if momentum_score is not None else fb["momentum_score"]

    opportunity_score = _compute_opportunity_score(
        data, {"value_score": value_score, "quality_score": quality_score, "momentum_score": momentum_score}
    )

    existing_overall = _clamp_0_100(data.get("overall_score"))
    existing_reco = data.get("recommendation") or data.get("recommendation_label") or data.get("rating")
    overall_score, recommendation = _compute_overall_and_reco(
        opportunity=opportunity_score,
        value=value_score,
        quality=quality_score,
        momentum=momentum_score,
        existing_overall=existing_overall,
        existing_reco=str(existing_reco) if existing_reco is not None else None,
    )

    fair_value = _safe_float(data.get("fair_value") or data.get("target_price") or data.get("intrinsic_value"))
    upside_percent = _safe_float(data.get("upside_percent") or data.get("upside") or data.get("upside_pct"))
    if upside_percent is None and fair_value is not None and price is not None and price != 0:
        try:
            upside_percent = round((fair_value - price) / price * 100.0, 2)
        except Exception:
            upside_percent = None

    valuation_label = data.get("valuation_label") or _valuation_label_from_upside(upside_percent)

    last_updated_utc = _parse_dt(data.get("last_updated_utc") or data.get("as_of_utc") or data.get("timestamp_utc"))
    sources = _extract_sources(data.get("sources") or data.get("providers") or [])

    notes = data.get("notes")
    error = data.get("error")
    if str(dq).upper() == "MISSING" and not error:
        error = "No data available from providers"

    return SingleAnalysisResponse(
        symbol=symbol,
        name=name,
        market_region=market_region,
        price=price,
        change_pct=change_pct,
        market_cap=market_cap,
        pe_ttm=pe_ttm,
        pb=pb,
        dividend_yield=dividend_yield,
        roe=roe,
        roa=roa,
        data_quality=str(dq),
        value_score=value_score,
        quality_score=quality_score,
        momentum_score=momentum_score,
        opportunity_score=opportunity_score,
        overall_score=overall_score,
        recommendation=recommendation,
        fair_value=fair_value,
        upside_percent=upside_percent,
        valuation_label=valuation_label,
        last_updated_utc=last_updated_utc,
        sources=sources,
        notes=notes,
        error=error,
    )


# ----------------------------------------------------------------------
# SHEET HELPERS
# ----------------------------------------------------------------------
def _build_sheet_headers() -> List[str]:
    return [
        "Symbol",
        "Company Name",
        "Market / Region",
        "Price",
        "Change %",
        "Market Cap",
        "P/E (TTM)",
        "P/B",
        "Dividend Yield %",
        "ROE %",
        "Data Quality",
        "Value Score",
        "Quality Score",
        "Momentum Score",
        "Opportunity Score",
        "Overall Score",
        "Recommendation",
        "Last Updated (UTC)",
        "Sources",
        "Fair Value",
        "Upside %",
        "Valuation Label",
        "Error",
    ]

def _to_iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    try:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return None

def _pad_row(row: List[Any], length: int) -> List[Any]:
    if len(row) == length:
        return row
    if len(row) < length:
        return row + [None] * (length - len(row))
    return row[:length]

def _analysis_to_sheet_row(a: SingleAnalysisResponse, header_len: int) -> List[Any]:
    row = [
        a.symbol,
        a.name or "",
        a.market_region or "",
        a.price,
        a.change_pct,
        a.market_cap,
        a.pe_ttm,
        a.pb,
        (a.dividend_yield * 100.0) if a.dividend_yield is not None else None,
        (a.roe * 100.0) if a.roe is not None else None,
        a.data_quality,
        a.value_score,
        a.quality_score,
        a.momentum_score,
        a.opportunity_score,
        a.overall_score,
        a.recommendation,
        _to_iso(a.last_updated_utc),
        ", ".join(a.sources) if a.sources else "",
        a.fair_value,
        a.upside_percent,
        a.valuation_label or "",
        a.error or "",
    ]
    return _pad_row(row, header_len)


# ----------------------------------------------------------------------
# ROUTES
# ----------------------------------------------------------------------
@router.get("/health")
@router.get("/ping")
async def analysis_health() -> Dict[str, Any]:
    env_name = getattr(_env, "APP_ENV", None) if _env is not None else None
    app_version = getattr(_env, "APP_VERSION", None) if _env is not None else None

    return {
        "status": "ok",
        "module": "routes.ai_analysis",
        "version": AI_ANALYSIS_VERSION,
        "engine_mode": _ENGINE_MODE,
        "engine_is_stub": _ENGINE_IS_STUB,
        "environment": env_name or "unknown",
        "app_version": app_version or "unknown",
        "batch_size": DEFAULT_BATCH_SIZE,
        "batch_timeout_sec": DEFAULT_BATCH_TIMEOUT,
        "batch_concurrency": DEFAULT_CONCURRENCY,
        "max_tickers": DEFAULT_MAX_TICKERS,
        "timestamp_utc": _now_utc().isoformat(),
    }

@router.get("/quote", response_model=SingleAnalysisResponse)
async def analyze_single_quote(symbol: str = Query(..., alias="symbol")) -> SingleAnalysisResponse:
    t = (symbol or "").strip()
    if not t:
        return SingleAnalysisResponse(symbol="", data_quality="MISSING", error="Symbol is required")

    try:
        m = await _get_quotes_chunked([t], batch_size=1)
        q = m.get(t.upper()) or {"symbol": t.upper(), "data_quality": "MISSING", "error": "No data returned"}
        return _quote_to_analysis(t, q)
    except Exception as exc:
        logger.exception("ai_analysis: exception in /quote for %s", t)
        return SingleAnalysisResponse(symbol=t.upper(), data_quality="MISSING", error=f"Exception in analysis: {exc}")

@router.post("/quotes", response_model=BatchAnalysisResponse)
async def analyze_batch_quotes(body: BatchAnalysisRequest) -> BatchAnalysisResponse:
    tickers = _clean_tickers(body.tickers or [])
    if not tickers:
        return BatchAnalysisResponse(results=[])

    if len(tickers) > DEFAULT_MAX_TICKERS:
        tickers = tickers[:DEFAULT_MAX_TICKERS]

    try:
        m = await _get_quotes_chunked(tickers)
    except Exception as exc:
        logger.exception("ai_analysis: batch failure: %s", exc)
        return BatchAnalysisResponse(
            results=[SingleAnalysisResponse(symbol=t, data_quality="MISSING", error=f"Batch failure: {exc}") for t in tickers]
        )

    results: List[SingleAnalysisResponse] = []
    for t in tickers:
        q = m.get(t.upper()) or {"symbol": t, "data_quality": "MISSING", "error": "No data returned"}
        try:
            results.append(_quote_to_analysis(t, q))
        except Exception as exc:
            logger.exception("ai_analysis: transform failure for %s: %s", t, exc)
            results.append(SingleAnalysisResponse(symbol=t, data_quality="MISSING", error=f"Transform failure: {exc}"))

    return BatchAnalysisResponse(results=results)

@router.post("/sheet-rows", response_model=SheetAnalysisResponse)
async def analyze_for_sheet(body: BatchAnalysisRequest) -> SheetAnalysisResponse:
    headers = _build_sheet_headers()
    header_len = len(headers)

    batch = await analyze_batch_quotes(body)
    rows = [_analysis_to_sheet_row(r, header_len) for r in batch.results]

    return SheetAnalysisResponse(headers=headers, rows=rows)

__all__ = ["router", "SingleAnalysisResponse", "BatchAnalysisRequest", "BatchAnalysisResponse", "SheetAnalysisResponse"]
