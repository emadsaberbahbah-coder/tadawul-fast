"""
routes/ai_analysis.py
------------------------------------------------------------
AI & QUANT ANALYSIS ROUTES – GOOGLE SHEETS FRIENDLY (v3.2.0)

Key goals (aligned with your backend architecture)
- Uses core.data_engine_v2.DataEngine only (no direct provider calls).
- Defensive batching: chunking + timeout + bounded concurrency.
- Sheets-safe: /sheet-rows ALWAYS returns 200 with {headers, rows, status}.
- Symbol normalization uses core.data_engine_v2.normalize_symbol (single source).
- Header selection uses core.schemas.get_headers_for_sheet (canonical 59 columns).
- Token guard via X-APP-TOKEN (APP_TOKEN / BACKUP_APP_TOKEN). If no token is set => open.

Notes on “repetition”
- Auth guard + singleton patterns are repeated intentionally per-route to keep
  each router self-contained and production-safe (no circular imports).
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, Query
from pydantic import BaseModel, Field, ConfigDict

from core.config import get_settings
from core.data_engine_v2 import DataEngine, UnifiedQuote, normalize_symbol
from core.enriched_quote import EnrichedQuote

# Preferred headers helper (59 columns)
try:
    from core.schemas import get_headers_for_sheet  # type: ignore
except Exception:  # pragma: no cover
    get_headers_for_sheet = None  # type: ignore

logger = logging.getLogger("routes.ai_analysis")
AI_ANALYSIS_VERSION = "3.2.0"

router = APIRouter(prefix="/v1/analysis", tags=["AI & Analysis"])


# =============================================================================
# Auth (X-APP-TOKEN) — same contract as routes/enriched_quote.py
# =============================================================================

@lru_cache(maxsize=1)
def _allowed_tokens() -> List[str]:
    tokens: List[str] = []
    try:
        s = get_settings()
        for attr in ("app_token", "backup_app_token", "APP_TOKEN", "BACKUP_APP_TOKEN"):
            v = getattr(s, attr, None)
            if isinstance(v, str) and v.strip():
                tokens.append(v.strip())
    except Exception:
        pass

    # Also support env.py exports if present
    try:
        import env as env_mod  # type: ignore
        for attr in ("APP_TOKEN", "BACKUP_APP_TOKEN"):
            v = getattr(env_mod, attr, None)
            if isinstance(v, str) and v.strip():
                tokens.append(v.strip())
    except Exception:
        pass

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


def _require_token(x_app_token: Optional[str]) -> None:
    allowed = _allowed_tokens()
    if not allowed:
        return  # open mode if no token configured
    if not x_app_token or x_app_token.strip() not in allowed:
        # For /sheet-rows we will never raise (Sheets-safe),
        # but for normal endpoints we keep strict auth.
        raise Exception("Unauthorized (invalid or missing X-APP-TOKEN).")


# =============================================================================
# Engine singleton (async-safe)
# =============================================================================

_ENGINE: Optional[DataEngine] = None
_ENGINE_LOCK = asyncio.Lock()


async def _get_engine() -> Optional[DataEngine]:
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE
    async with _ENGINE_LOCK:
        if _ENGINE is None:
            try:
                _ENGINE = DataEngine()
                logger.info("[analysis] DataEngine initialized (singleton).")
            except Exception as exc:
                logger.exception("[analysis] Failed to init DataEngine: %s", exc)
                _ENGINE = None
    return _ENGINE


# =============================================================================
# Settings / defaults
# =============================================================================

def _safe_int(x: Any, default: int) -> int:
    try:
        v = int(x)
        return v if v > 0 else default
    except Exception:
        return default


def _safe_float(x: Any, default: float) -> float:
    try:
        v = float(x)
        return v if v > 0 else default
    except Exception:
        return default


def _get_cfg() -> Dict[str, Any]:
    s = None
    try:
        s = get_settings()
    except Exception:
        s = None

    batch_size = _safe_int(getattr(s, "ai_batch_size", None), 20)
    timeout_sec = _safe_float(getattr(s, "ai_batch_timeout_sec", None), 45.0)
    concurrency = _safe_int(getattr(s, "ai_batch_concurrency", None), 5)
    max_tickers = _safe_int(getattr(s, "ai_max_tickers", None), 500)

    # env overrides (optional)
    batch_size = _safe_int(os.getenv("AI_BATCH_SIZE", batch_size), batch_size)
    timeout_sec = _safe_float(os.getenv("AI_BATCH_TIMEOUT_SEC", timeout_sec), timeout_sec)
    concurrency = _safe_int(os.getenv("AI_BATCH_CONCURRENCY", concurrency), concurrency)
    max_tickers = _safe_int(os.getenv("AI_MAX_TICKERS", max_tickers), max_tickers)

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
    # support both keys to be robust with clients
    tickers: List[str] = Field(default_factory=list)
    symbols: List[str] = Field(default_factory=list)
    sheet_name: Optional[str] = None


class BatchAnalysisResponse(_ExtraIgnoreBase):
    results: List[SingleAnalysisResponse] = Field(default_factory=list)


class SheetAnalysisResponse(_ExtraIgnoreBase):
    headers: List[str] = Field(default_factory=list)
    rows: List[List[Any]] = Field(default_factory=list)
    status: str = "success"
    error: Optional[str] = None


# =============================================================================
# Helpers
# =============================================================================

def _clean_symbols(items: Sequence[str]) -> List[str]:
    raw = [normalize_symbol(x) for x in (items or [])]
    raw = [x for x in raw if x]
    seen = set()
    out: List[str] = []
    for x in raw:
        xu = x.upper()
        if xu in seen:
            continue
        seen.add(xu)
        out.append(xu)
    return out


def _chunk(items: List[str], size: int) -> List[List[str]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]


def _select_headers(sheet_name: Optional[str]) -> List[str]:
    # Canonical: use core.schemas (59 cols)
    if get_headers_for_sheet:
        try:
            h = get_headers_for_sheet(sheet_name)
            if isinstance(h, list) and h and any(str(x).strip().lower() == "symbol" for x in h):
                return [str(x) for x in h]
        except Exception:
            pass

    # Ultimate fallback (minimal but safe)
    return ["Symbol", "Error"]


def _error_row(symbol: str, headers: List[str], err: str) -> List[Any]:
    row = [None] * len(headers)
    # put Symbol
    try:
        sym_idx = next(i for i, h in enumerate(headers) if str(h).strip().lower() == "symbol")
        row[sym_idx] = symbol
    except Exception:
        pass
    # put Error if exists
    try:
        err_idx = next(i for i, h in enumerate(headers) if str(h).strip().lower() == "error")
        row[err_idx] = err
    except Exception:
        # if no Error column, append nothing (still fixed length)
        pass
    return row


def _extract_sources(eq: EnrichedQuote) -> List[str]:
    ds = getattr(eq, "data_source", None)
    return [ds] if ds else []


def _ratio_to_percent(v: Optional[float]) -> Optional[float]:
    if v is None:
        return None
    try:
        return (v * 100.0) if -1.0 <= v <= 1.0 else v
    except Exception:
        return v


# =============================================================================
# Engine calls (defensive batching)
# =============================================================================

async def _get_quotes_chunked(
    symbols: List[str],
    *,
    batch_size: int,
    timeout_sec: float,
    max_concurrency: int,
) -> Dict[str, UnifiedQuote]:
    clean = _clean_symbols(symbols)
    if not clean:
        return {}

    engine = await _get_engine()
    if engine is None:
        msg = "Engine unavailable"
        return {s: UnifiedQuote(symbol=s, data_quality="MISSING", error=msg).finalize() for s in clean}

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
            logger.warning("[analysis] %s for chunk(size=%d)", msg, len(chunk_syms))
            for s in chunk_syms:
                out[s.upper()] = UnifiedQuote(symbol=s.upper(), data_quality="MISSING", error=msg).finalize()
            continue

        returned = list(res or [])
        chunk_map = {q.symbol.upper(): q for q in returned}

        for s in chunk_syms:
            s_up = s.upper()
            out[s_up] = chunk_map.get(s_up) or UnifiedQuote(
                symbol=s_up, data_quality="MISSING", error="No data returned"
            ).finalize()

    return out


# =============================================================================
# Transform
# =============================================================================

def _quote_to_analysis(requested_symbol: str, uq: UnifiedQuote) -> SingleAnalysisResponse:
    # DataEngine already attempts scoring; still try again if needed (best-effort)
    try:
        missing_scores = any(getattr(uq, k, None) is None for k in ("value_score", "quality_score", "momentum_score", "opportunity_score"))
        if missing_scores:
            from core.scoring_engine import enrich_with_scores  # type: ignore
            uq = enrich_with_scores(uq)  # type: ignore
    except Exception:
        pass

    eq = EnrichedQuote.from_unified(uq)

    sym = (getattr(eq, "symbol", None) or normalize_symbol(requested_symbol) or requested_symbol or "").upper()

    return SingleAnalysisResponse(
        symbol=sym,
        name=getattr(eq, "name", None),
        market_region=getattr(eq, "market", None),

        price=getattr(eq, "current_price", None),
        change_pct=getattr(eq, "percent_change", None),
        market_cap=getattr(eq, "market_cap", None),
        pe_ttm=getattr(eq, "pe_ttm", None),
        pb=getattr(eq, "pb", None),

        dividend_yield=_ratio_to_percent(getattr(eq, "dividend_yield", None)),
        roe=_ratio_to_percent(getattr(eq, "roe", None)),
        roa=_ratio_to_percent(getattr(eq, "roa", None)),

        data_quality=getattr(eq, "data_quality", "MISSING") or "MISSING",

        value_score=getattr(eq, "value_score", None),
        quality_score=getattr(eq, "quality_score", None),
        momentum_score=getattr(eq, "momentum_score", None),
        opportunity_score=getattr(eq, "opportunity_score", None),
        risk_score=getattr(eq, "risk_score", None),
        overall_score=getattr(eq, "overall_score", None),
        recommendation=getattr(eq, "recommendation", None),

        fair_value=getattr(eq, "fair_value", None),
        upside_percent=getattr(eq, "upside_percent", None),
        valuation_label=getattr(eq, "valuation_label", None),

        last_updated_utc=getattr(eq, "last_updated_utc", None),
        sources=_extract_sources(eq),

        error=getattr(eq, "error", None),
    )


# =============================================================================
# Routes
# =============================================================================

@router.get("/health")
@router.get("/ping")
async def analysis_health() -> Dict[str, Any]:
    cfg = _get_cfg()
    eng = await _get_engine()
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
    symbol: str = Query(..., description="Ticker symbol (e.g., 1120.SR, AAPL, ^GSPC)."),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> SingleAnalysisResponse:
    # Strict auth here
    try:
        _require_token(x_app_token)
    except Exception as e:
        return SingleAnalysisResponse(symbol=(symbol or "").strip().upper(), data_quality="MISSING", error=str(e))

    t = (symbol or "").strip()
    if not t:
        return SingleAnalysisResponse(symbol="", data_quality="MISSING", error="Symbol is required")

    cfg = _get_cfg()
    try:
        m = await _get_quotes_chunked([t], batch_size=1, timeout_sec=cfg["timeout_sec"], max_concurrency=1)
        key = normalize_symbol(t).upper() if normalize_symbol(t) else t.upper()
        q = m.get(key) or UnifiedQuote(symbol=key, data_quality="MISSING", error="No data returned").finalize()
        return _quote_to_analysis(t, q)
    except Exception as exc:
        logger.exception("[analysis] exception in /quote for %s", t)
        sym = normalize_symbol(t).upper() if normalize_symbol(t) else t.upper()
        return SingleAnalysisResponse(symbol=sym, data_quality="MISSING", error=f"Exception in analysis: {exc}")


@router.post("/quotes", response_model=BatchAnalysisResponse)
async def analyze_batch_quotes(
    body: BatchAnalysisRequest = Body(...),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> BatchAnalysisResponse:
    # Strict auth here
    try:
        _require_token(x_app_token)
    except Exception as e:
        syms = _clean_symbols((body.tickers or []) + (body.symbols or []))
        return BatchAnalysisResponse(
            results=[SingleAnalysisResponse(symbol=s, data_quality="MISSING", error=str(e)) for s in syms]
        )

    tickers = _clean_symbols((body.tickers or []) + (body.symbols or []))
    if not tickers:
        return BatchAnalysisResponse(results=[])

    cfg = _get_cfg()
    if len(tickers) > cfg["max_tickers"]:
        tickers = tickers[: cfg["max_tickers"]]

    try:
        m = await _get_quotes_chunked(
            tickers,
            batch_size=cfg["batch_size"],
            timeout_sec=cfg["timeout_sec"],
            max_concurrency=cfg["concurrency"],
        )
    except Exception as exc:
        logger.exception("[analysis] batch failure: %s", exc)
        return BatchAnalysisResponse(
            results=[SingleAnalysisResponse(symbol=t, data_quality="MISSING", error=f"Batch failure: {exc}") for t in tickers]
        )

    results: List[SingleAnalysisResponse] = []
    for t in tickers:
        q = m.get(t) or UnifiedQuote(symbol=t, data_quality="MISSING", error="No data returned").finalize()
        try:
            results.append(_quote_to_analysis(t, q))
        except Exception as exc:
            logger.exception("[analysis] transform failure for %s: %s", t, exc)
            results.append(SingleAnalysisResponse(symbol=t, data_quality="MISSING", error=f"Transform failure: {exc}"))

    return BatchAnalysisResponse(results=results)


@router.post("/sheet-rows", response_model=SheetAnalysisResponse)
async def analyze_for_sheet(
    body: BatchAnalysisRequest = Body(...),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> SheetAnalysisResponse:
    """
    Sheets-safe: ALWAYS returns {headers, rows, status} with HTTP 200.
    Even if auth fails, it returns error rows (so Apps Script never breaks).
    """
    headers = _select_headers(body.sheet_name)

    # Sheets-safe auth behavior: never raise
    try:
        _require_token(x_app_token)
    except Exception as e:
        tickers = _clean_symbols((body.tickers or []) + (body.symbols or []))
        rows = [_error_row(t, headers, str(e)) for t in tickers]
        return SheetAnalysisResponse(headers=headers, rows=rows, status="error", error=str(e))

    tickers = _clean_symbols((body.tickers or []) + (body.symbols or []))
    if not tickers:
        return SheetAnalysisResponse(headers=headers, rows=[], status="skipped", error="No tickers provided")

    cfg = _get_cfg()
    if len(tickers) > cfg["max_tickers"]:
        tickers = tickers[: cfg["max_tickers"]]

    try:
        m = await _get_quotes_chunked(
            tickers,
            batch_size=cfg["batch_size"],
            timeout_sec=cfg["timeout_sec"],
            max_concurrency=cfg["concurrency"],
        )

        rows: List[List[Any]] = []
        for t in tickers:
            uq = m.get(t) or UnifiedQuote(symbol=t, data_quality="MISSING", error="No data returned").finalize()

            # Ensure scores best-effort
            try:
                missing_scores = any(getattr(uq, k, None) is None for k in ("value_score", "quality_score", "momentum_score", "opportunity_score"))
                if missing_scores:
                    from core.scoring_engine import enrich_with_scores  # type: ignore
                    uq = enrich_with_scores(uq)  # type: ignore
            except Exception:
                pass

            eq = EnrichedQuote.from_unified(uq)
            rows.append(eq.to_row(headers))

        # If any row has error, mark partial
        status = "success"
        try:
            err_idx = next(i for i, h in enumerate(headers) if str(h).strip().lower() == "error")
            if any((r[err_idx] not in (None, "", "null")) for r in rows if len(r) > err_idx):
                status = "partial"
        except Exception:
            pass

        return SheetAnalysisResponse(headers=headers, rows=rows, status=status)

    except Exception as exc:
        logger.exception("[analysis] exception in /sheet-rows")
        rows = [_error_row(t, headers, str(exc)) for t in tickers]
        return SheetAnalysisResponse(headers=headers, rows=rows, status="error", error=str(exc))


__all__ = [
    "router",
    "SingleAnalysisResponse",
    "BatchAnalysisRequest",
    "BatchAnalysisResponse",
    "SheetAnalysisResponse",
]
