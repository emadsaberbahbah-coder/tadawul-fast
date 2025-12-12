"""
routes/advanced_analysis.py
================================================
TADAWUL FAST BRIDGE – ADVANCED ANALYSIS ROUTES (v2.4.0)

Purpose
-------
- Expose "advanced analysis" and "opportunity ranking" endpoints
  on top of the unified engine (core.data_engine_v2 preferred).
- Google Sheets–friendly: provides /sheet-rows (headers + rows).

Endpoints
---------
- GET  /v1/advanced/health        (alias: /ping)
- GET  /v1/advanced/scoreboard?tickers=AAPL,MSFT,1120.SR&top_n=50
- POST /v1/advanced/sheet-rows    (Sheets-friendly scoreboard)

Design notes
------------
- This router NEVER calls providers directly. It only talks to the engine.
- Defensive + stable for Sheets:
    • Dedupes tickers (preserves order)
    • Chunked batch requests + bounded concurrency + timeout per chunk
    • Returns empty items/rows (200) instead of throwing 502 on provider failure
- Singleton engine per process to avoid repeated initialization.

Author: Emad Bahbah (with GPT-5.2 Thinking)
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

logger = logging.getLogger("routes.advanced_analysis")

ADVANCED_ANALYSIS_VERSION = "2.4.0"

# =============================================================================
# Runtime config (env overrides)
# =============================================================================

DEFAULT_BATCH_SIZE = int(os.getenv("ADV_BATCH_SIZE", "50"))
DEFAULT_BATCH_TIMEOUT = float(os.getenv("ADV_BATCH_TIMEOUT_SEC", "30"))
DEFAULT_MAX_TICKERS = int(os.getenv("ADV_MAX_TICKERS", "500"))
DEFAULT_CONCURRENCY = int(os.getenv("ADV_BATCH_CONCURRENCY", "3"))

# =============================================================================
# Optional env.py integration (purely metadata)
# =============================================================================

try:  # pragma: no cover
    import env as _env  # type: ignore
except Exception:
    _env = None  # type: ignore


# =============================================================================
# Engine import & fallbacks (v2 preferred -> v1 module -> stub)
# =============================================================================

_ENGINE_MODE: str = "stub"       # v2 | v1_module | stub
_ENGINE_IS_STUB: bool = True

try:
    from core.data_engine_v2 import DataEngine as _V2DataEngine  # type: ignore
    from core.data_engine_v2 import UnifiedQuote  # type: ignore

    @lru_cache(maxsize=1)
    def _get_engine_singleton() -> Any:
        return _V2DataEngine()

    _ENGINE_MODE = "v2"
    _ENGINE_IS_STUB = False
    logger.info("AdvancedAnalysis: using DataEngine v2 (singleton)")

    _data_engine_module = None  # type: ignore

except Exception as exc_v2:  # pragma: no cover
    logger.exception("AdvancedAnalysis: failed to import DataEngine v2: %s", exc_v2)

    try:
        from core import data_engine as _data_engine_module  # type: ignore

        class UnifiedQuote(BaseModel):  # type: ignore[no-redef]
            symbol: str
            data_quality: Optional[str] = None
            error: Optional[str] = None

        @lru_cache(maxsize=1)
        def _get_engine_singleton() -> Any:
            # v1 is module-level; singleton returns the module itself
            return _data_engine_module

        _ENGINE_MODE = "v1_module"
        _ENGINE_IS_STUB = False
        logger.warning("AdvancedAnalysis: falling back to core.data_engine module-level API")

    except Exception as exc_v1:  # pragma: no cover
        logger.exception("AdvancedAnalysis: failed to import v1 fallback: %s", exc_v1)

        class UnifiedQuote(BaseModel):  # type: ignore[no-redef]
            symbol: str
            data_quality: str = "MISSING"
            error: Optional[str] = None

        class _StubEngine:
            async def get_enriched_quote(self, symbol: str) -> Dict[str, Any]:
                sym = (symbol or "").strip().upper() or "UNKNOWN"
                return {
                    "symbol": sym,
                    "name": None,
                    "market": None,
                    "sector": None,
                    "currency": None,
                    "last_price": None,
                    "fair_value": None,
                    "upside_percent": None,
                    "value_score": 0.0,
                    "quality_score": 0.0,
                    "momentum_score": 0.0,
                    "opportunity_score": 0.0,
                    "overall_score": 0.0,
                    "recommendation": "MISSING",
                    "risk_label": "UNKNOWN",
                    "provider": None,
                    "data_quality": "MISSING",
                    "as_of_utc": None,
                    "error": "STUB_ENGINE: DataEngine could not be imported on backend.",
                }

            async def get_enriched_quotes(self, symbols: List[str]) -> List[Dict[str, Any]]:
                return [await self.get_enriched_quote(s) for s in (symbols or [])]

        @lru_cache(maxsize=1)
        def _get_engine_singleton() -> Any:
            return _StubEngine()

        _ENGINE_MODE = "stub"
        _ENGINE_IS_STUB = True
        _data_engine_module = None  # type: ignore
        logger.error("AdvancedAnalysis: using STUB engine (all values will be MISSING).")


# =============================================================================
# Utilities
# =============================================================================

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _clean_tickers(items: Sequence[str]) -> List[str]:
    raw = [(x or "").strip() for x in (items or [])]
    raw = [x for x in raw if x]
    seen = set()
    out: List[str] = []
    for x in raw:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out

def _parse_tickers_csv(s: str) -> List[str]:
    if not s:
        return []
    parts = [p.strip() for p in s.split(",")]
    return _clean_tickers(parts)

def _chunk(items: List[str], size: int) -> List[List[str]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]

def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None

def _model_to_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "model_dump"):
        try:
            return obj.model_dump()  # type: ignore[call-arg]
        except Exception:
            pass
    return getattr(obj, "__dict__", {}) or {}

def _map_data_quality_to_score(label: Optional[str]) -> Optional[float]:
    if not label:
        return None
    dq = str(label).strip().upper()
    mapping = {
        "EXCELLENT": 90.0,
        "GOOD": 80.0,
        "OK": 75.0,
        "FAIR": 55.0,
        "PARTIAL": 50.0,
        "STALE": 40.0,
        "POOR": 30.0,
        "MISSING": 20.0,
        "UNKNOWN": 30.0,
    }
    return mapping.get(dq, 30.0)

def _extract_data_quality_score(data: Dict[str, Any]) -> Optional[float]:
    # 1) explicit numeric
    for k in ("data_quality_score", "dq_score", "quality_score_numeric", "confidence_score"):
        x = _safe_float(data.get(k))
        if x is not None:
            return x
    # 2) string label
    return _map_data_quality_to_score(data.get("data_quality") or data.get("data_quality_level"))

def _compute_risk_bucket(opportunity: Optional[float], dq: Optional[float]) -> str:
    opp = opportunity or 0.0
    conf = dq or 0.0

    if conf < 40:
        return "LOW_CONFIDENCE"

    if opp >= 75 and conf >= 70:
        return "HIGH_OPP_HIGH_CONF"
    if 55 <= opp < 75 and conf >= 60:
        return "MED_OPP_HIGH_CONF"
    if opp >= 55 and 40 <= conf < 60:
        return "OPP_WITH_MED_CONF"
    if opp < 35 and conf >= 60:
        return "LOW_OPP_HIGH_CONF"

    return "NEUTRAL"

def _data_age_minutes(as_of_utc: Any) -> Optional[float]:
    if not as_of_utc:
        return None
    try:
        if isinstance(as_of_utc, datetime):
            ts = as_of_utc
        else:
            ts = datetime.fromisoformat(str(as_of_utc))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        diff = datetime.now(timezone.utc) - ts
        return round(diff.total_seconds() / 60.0, 2)
    except Exception:
        return None


# =============================================================================
# Engine wrappers (supports v2 object OR v1 module OR stub)
# =============================================================================

async def _engine_get_many(symbols: List[str]) -> List[Any]:
    eng = _get_engine_singleton()

    # v2/stub object
    if hasattr(eng, "get_enriched_quotes"):
        return await eng.get_enriched_quotes(symbols)  # type: ignore[attr-defined]

    # v1 module-level
    if hasattr(eng, "get_enriched_quotes"):
        return await eng.get_enriched_quotes(symbols)  # type: ignore[attr-defined]

    # fallback: per symbol if only single method exists
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

    async def _run_chunk(chunk_syms: List[str]) -> Tuple[List[str], List[Any] | Exception]:
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
            logger.warning("AdvancedAnalysis: %s for chunk(size=%d): %s", msg, len(chunk_syms), chunk_syms)
            for s in chunk_syms:
                out[s] = {"symbol": s.upper(), "data_quality": "MISSING", "error": msg}
            continue

        # map returned by symbol if possible, else order zip
        returned = list(res or [])
        by_sym: Dict[str, Any] = {}
        for q in returned:
            d = _model_to_dict(q)
            sym = (d.get("symbol") or d.get("ticker") or "").strip().upper()
            if sym:
                by_sym[sym] = q

        for s, q in zip(chunk_syms, returned):
            out.setdefault(s, q)

        # fill missing by symbol lookup
        for s in chunk_syms:
            if s not in out:
                out[s] = by_sym.get(s.upper()) or {"symbol": s.upper(), "data_quality": "MISSING", "error": "No data returned"}

    return out


# =============================================================================
# Pydantic models
# =============================================================================

class AdvancedItem(BaseModel):
    symbol: str
    name: Optional[str] = None
    market: Optional[str] = None
    sector: Optional[str] = None
    currency: Optional[str] = None

    last_price: Optional[float] = None
    fair_value: Optional[float] = None
    upside_percent: Optional[float] = None

    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    overall_score: Optional[float] = None

    data_quality: Optional[str] = None
    data_quality_score: Optional[float] = None

    recommendation: Optional[str] = None
    risk_label: Optional[str] = None
    risk_bucket: Optional[str] = None

    provider: Optional[str] = None
    as_of_utc: Optional[str] = None
    data_age_minutes: Optional[float] = None
    error: Optional[str] = None


class AdvancedScoreboardResponse(BaseModel):
    generated_at_utc: str
    version: str
    engine_mode: str
    engine_is_stub: bool

    total_requested: int
    total_returned: int
    top_n_applied: bool

    tickers: List[str]
    items: List[AdvancedItem]


class AdvancedSheetRequest(BaseModel):
    tickers: List[str] = Field(default_factory=list)
    top_n: Optional[int] = Field(default=50, ge=1, le=500)


class AdvancedSheetResponse(BaseModel):
    headers: List[str]
    rows: List[List[Any]]


# =============================================================================
# Transform logic
# =============================================================================

def _to_advanced_item(raw_symbol: str, payload: Any) -> AdvancedItem:
    d = _model_to_dict(payload)

    symbol = (d.get("symbol") or d.get("ticker") or raw_symbol or "").strip() or raw_symbol
    symbol = symbol.upper()

    last_price = _safe_float(d.get("last_price") or d.get("price") or d.get("close") or d.get("last"))
    fair_value = _safe_float(d.get("fair_value") or d.get("intrinsic_value") or d.get("target_price"))
    upside = _safe_float(d.get("upside_percent") or d.get("upside") or d.get("upside_pct"))

    value_score = _safe_float(d.get("value_score"))
    quality_score = _safe_float(d.get("quality_score"))
    momentum_score = _safe_float(d.get("momentum_score"))
    opportunity_score = _safe_float(d.get("opportunity_score"))
    overall_score = _safe_float(d.get("overall_score"))

    data_quality = d.get("data_quality") or d.get("data_quality_level")
    dq_score = _extract_data_quality_score(d)

    as_of_utc_raw = d.get("as_of_utc") or d.get("last_updated_utc") or d.get("timestamp_utc")
    if isinstance(as_of_utc_raw, datetime):
        as_of_utc = as_of_utc_raw.isoformat()
    else:
        as_of_utc = str(as_of_utc_raw) if as_of_utc_raw else None

    age_min = _data_age_minutes(as_of_utc)

    rec = d.get("recommendation") or d.get("recommendation_label") or d.get("rating")
    risk_label = d.get("risk_label")
    risk_bucket = _compute_risk_bucket(opportunity_score, dq_score)

    provider = d.get("primary_provider") or d.get("provider") or d.get("data_source")
    error = d.get("error")

    return AdvancedItem(
        symbol=symbol,
        name=d.get("name") or d.get("company_name"),
        market=d.get("market") or d.get("exchange"),
        sector=d.get("sector"),
        currency=d.get("currency"),
        last_price=last_price,
        fair_value=fair_value,
        upside_percent=upside,
        value_score=value_score,
        quality_score=quality_score,
        momentum_score=momentum_score,
        opportunity_score=opportunity_score,
        overall_score=overall_score,
        data_quality=str(data_quality) if data_quality is not None else None,
        data_quality_score=dq_score,
        recommendation=str(rec) if rec is not None else None,
        risk_label=str(risk_label) if risk_label is not None else None,
        risk_bucket=risk_bucket,
        provider=str(provider) if provider is not None else None,
        as_of_utc=as_of_utc,
        data_age_minutes=age_min,
        error=str(error) if error is not None else None,
    )


def _sort_key(it: AdvancedItem) -> float:
    opp = it.opportunity_score or 0.0
    dq = it.data_quality_score or 0.0
    up = it.upside_percent or 0.0
    return opp * 1_000_000 + dq * 1_000 + up


def _sheet_headers() -> List[str]:
    return [
        "Symbol",
        "Company Name",
        "Market",
        "Sector",
        "Currency",
        "Last Price",
        "Fair Value",
        "Upside %",
        "Opportunity Score",
        "Value Score",
        "Quality Score",
        "Momentum Score",
        "Overall Score",
        "Data Quality",
        "Data Quality Score",
        "Recommendation",
        "Risk Label",
        "Risk Bucket",
        "Provider",
        "As Of (UTC)",
        "Data Age (Minutes)",
        "Error",
    ]


def _item_to_row(it: AdvancedItem) -> List[Any]:
    return [
        it.symbol,
        it.name,
        it.market,
        it.sector,
        it.currency,
        it.last_price,
        it.fair_value,
        it.upside_percent,
        it.opportunity_score,
        it.value_score,
        it.quality_score,
        it.momentum_score,
        it.overall_score,
        it.data_quality,
        it.data_quality_score,
        it.recommendation,
        it.risk_label,
        it.risk_bucket,
        it.provider,
        it.as_of_utc,
        it.data_age_minutes,
        it.error,
    ]


# =============================================================================
# Router
# =============================================================================

router = APIRouter(prefix="/v1/advanced", tags=["Advanced Analysis"])


@router.get("/health")
@router.get("/ping")
async def advanced_health() -> Dict[str, Any]:
    env_name = getattr(_env, "APP_ENV", None) if _env is not None else None
    app_version = getattr(_env, "APP_VERSION", None) if _env is not None else None

    return {
        "status": "ok",
        "module": "routes.advanced_analysis",
        "version": ADVANCED_ANALYSIS_VERSION,
        "engine_mode": _ENGINE_MODE,
        "engine_is_stub": _ENGINE_IS_STUB,
        "environment": env_name or "unknown",
        "app_version": app_version or "unknown",
        "batch_size": DEFAULT_BATCH_SIZE,
        "batch_timeout_sec": DEFAULT_BATCH_TIMEOUT,
        "batch_concurrency": DEFAULT_CONCURRENCY,
        "max_tickers": DEFAULT_MAX_TICKERS,
        "timestamp_utc": _now_utc_iso(),
    }


@router.get("/scoreboard", response_model=AdvancedScoreboardResponse)
async def advanced_scoreboard(
    tickers: str = Query(..., description="Comma-separated tickers e.g. 'AAPL,MSFT,1120.SR'"),
    top_n: int = Query(50, ge=1, le=500, description="Max rows returned after sorting"),
) -> AdvancedScoreboardResponse:
    requested = _parse_tickers_csv(tickers)
    if not requested:
        return AdvancedScoreboardResponse(
            generated_at_utc=_now_utc_iso(),
            version=ADVANCED_ANALYSIS_VERSION,
            engine_mode=_ENGINE_MODE,
            engine_is_stub=_ENGINE_IS_STUB,
            total_requested=0,
            total_returned=0,
            top_n_applied=False,
            tickers=[],
            items=[],
        )

    if len(requested) > DEFAULT_MAX_TICKERS:
        requested = requested[:DEFAULT_MAX_TICKERS]

    unified_map = await _get_quotes_chunked(requested)

    items: List[AdvancedItem] = []
    for s in requested:
        payload = unified_map.get(s) or {"symbol": s.upper(), "data_quality": "MISSING", "error": "No data returned"}
        items.append(_to_advanced_item(s, payload))

    # sort & truncate
    items_sorted = sorted(items, key=_sort_key, reverse=True)
    top_applied = False
    if len(items_sorted) > top_n:
        items_sorted = items_sorted[:top_n]
        top_applied = True

    return AdvancedScoreboardResponse(
        generated_at_utc=_now_utc_iso(),
        version=ADVANCED_ANALYSIS_VERSION,
        engine_mode=_ENGINE_MODE,
        engine_is_stub=_ENGINE_IS_STUB,
        total_requested=len(requested),
        total_returned=len(items_sorted),
        top_n_applied=top_applied,
        tickers=requested,
        items=items_sorted,
    )


@router.post("/sheet-rows", response_model=AdvancedSheetResponse)
async def advanced_sheet_rows(body: AdvancedSheetRequest) -> AdvancedSheetResponse:
    requested = _clean_tickers(body.tickers or [])
    headers = _sheet_headers()

    if not requested:
        return AdvancedSheetResponse(headers=headers, rows=[])

    if len(requested) > DEFAULT_MAX_TICKERS:
        requested = requested[:DEFAULT_MAX_TICKERS]

    unified_map = await _get_quotes_chunked(requested)

    items: List[AdvancedItem] = []
    for s in requested:
        payload = unified_map.get(s) or {"symbol": s.upper(), "data_quality": "MISSING", "error": "No data returned"}
        items.append(_to_advanced_item(s, payload))

    items_sorted = sorted(items, key=_sort_key, reverse=True)
    top_n = body.top_n or 50
    if len(items_sorted) > top_n:
        items_sorted = items_sorted[:top_n]

    rows = [_item_to_row(it) for it in items_sorted]
    return AdvancedSheetResponse(headers=headers, rows=rows)


__all__ = ["router"]
