"""
routes/enriched_quote.py
===========================================================
Enriched Quotes Router (v3.4.0)

Unified on top of:
  - core.data_engine_v2.DataEngine (async, multi-provider, KSA-safe)
  - core.enriched_quote.EnrichedQuote (sheet-friendly model)
  - core.schemas.get_headers_for_sheet (preferred; multi-sheet schema)

Responsibilities
----------------
- GET  /v1/enriched/health       -> router + engine diagnostics
- GET  /v1/enriched/headers      -> headers only (debug / Apps Script setup)
- GET  /v1/enriched/quote        -> single EnrichedQuote JSON
- POST /v1/enriched/quotes       -> batch EnrichedQuote JSON objects
- POST /v1/enriched/sheet-rows   -> headers + rows for Google Sheets

Design notes
------------
- Router NEVER talks directly to market providers.
- Very defensive: chunked calls + concurrency limit + timeout per chunk.
- Singleton engine per process to avoid repeated DataEngine init.
- Preserves input order in sheet-rows and batch results.

Author: Emad Bahbah (with GPT-5.2 Thinking)
"""

from __future__ import annotations

import asyncio
import logging
import os
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

logger = logging.getLogger("routes.enriched_quote")

API_VERSION = "3.4.0"

# =============================================================================
# Config (env override)
# =============================================================================

DEFAULT_BATCH_SIZE = int(os.getenv("ENRICHED_BATCH_SIZE", "40") or 40)
DEFAULT_BATCH_TIMEOUT = float(os.getenv("ENRICHED_BATCH_TIMEOUT_SEC", "30") or 30)
DEFAULT_MAX_TICKERS = int(os.getenv("ENRICHED_MAX_TICKERS", "250") or 250)
DEFAULT_MAX_CONCURRENCY = int(os.getenv("ENRICHED_BATCH_CONCURRENCY", "3") or 3)

# =============================================================================
# Optional env.py integration (safe)
# =============================================================================

try:  # pragma: no cover
    import env as _env_mod  # type: ignore
except Exception:  # pragma: no cover
    _env_mod = None  # type: ignore


def _get_env_attr(name: str, default: Any = None) -> Any:
    """
    Read from env.py first (if present), then OS env vars.
    """
    if _env_mod is not None and hasattr(_env_mod, name):
        try:
            return getattr(_env_mod, name)
        except Exception:
            return default
    return os.getenv(name, default)


# =============================================================================
# Symbol normalization & list helpers
# =============================================================================

def _normalize_symbol(symbol: str) -> str:
    """
    Normalization rules (aligned with legacy_service):
      - 1120 -> 1120.SR
      - TADAWUL:1120 -> 1120.SR
      - 1120.TADAWUL -> 1120.SR
      - Trim + upper
    """
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


def _dedupe_preserve_order(items: Sequence[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in items or []:
        if not x:
            continue
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _clean_tickers(tickers: Sequence[str]) -> List[str]:
    clean = [_normalize_symbol(t) for t in (tickers or [])]
    clean = [t for t in clean if t]
    return _dedupe_preserve_order(clean)


def _chunk(items: List[str], size: int) -> List[List[str]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]


# =============================================================================
# Engine + models (real preferred, stub fallback)
# =============================================================================

_ENGINE_MODE: str = "unknown"
_ENGINE_IS_STUB: bool = False

try:
    from core.data_engine_v2 import DataEngine, UnifiedQuote  # type: ignore
    from core.enriched_quote import EnrichedQuote  # type: ignore

    def _build_engine_kwargs() -> Dict[str, Any]:
        """
        Initialize DataEngine with optional tuning if available.
        Works across versions by passing only known/likely kwargs; falls back on TypeError.
        """
        kwargs: Dict[str, Any] = {}

        # providers
        enabled = _get_env_attr("ENABLED_PROVIDERS", None)
        if isinstance(enabled, (list, tuple)) and enabled:
            kwargs["enabled_providers"] = list(enabled)
        elif isinstance(enabled, str) and enabled.strip():
            kwargs["enabled_providers"] = [p.strip() for p in enabled.split(",") if p.strip()]

        # cache ttl
        cache_ttl = _get_env_attr("ENGINE_CACHE_TTL_SECONDS", None)
        if cache_ttl is None:
            cache_ttl = _get_env_attr("DATAENGINE_CACHE_TTL", None)
        if cache_ttl is not None:
            kwargs["cache_ttl"] = cache_ttl

        # provider timeout
        provider_timeout = _get_env_attr("ENGINE_PROVIDER_TIMEOUT_SECONDS", None)
        if provider_timeout is None:
            provider_timeout = _get_env_attr("DATAENGINE_TIMEOUT", None)
        if provider_timeout is not None:
            kwargs["provider_timeout"] = provider_timeout

        # advanced analysis toggle (optional)
        enable_adv = _get_env_attr("ENGINE_ENABLE_ADVANCED_ANALYSIS", None)
        if enable_adv is None:
            enable_adv = _get_env_attr("ENABLE_ADVANCED_ANALYSIS", None)
        if enable_adv is not None:
            kwargs["enable_advanced_analysis"] = enable_adv

        return kwargs

    @lru_cache(maxsize=1)
    def _get_engine_singleton() -> Any:
        """
        Ensure we only initialize DataEngine once per process.
        """
        kwargs = _build_engine_kwargs()
        try:
            return DataEngine(**kwargs) if kwargs else DataEngine()
        except TypeError:
            # Signature mismatch across versions -> safe fallback
            return DataEngine()

    _ENGINE_MODE = "v2"
    _ENGINE_IS_STUB = False
    logger.info("routes.enriched_quote: Using core.data_engine_v2.DataEngine (singleton)")

except Exception as exc:  # pragma: no cover
    logger.exception(
        "routes.enriched_quote: Failed to import DataEngine/EnrichedQuote; "
        "falling back to stub engine: %s",
        exc,
    )

    class UnifiedQuote(BaseModel):  # type: ignore[no-redef]
        symbol: str
        data_quality: str = "MISSING"
        error: Optional[str] = None

    class EnrichedQuote(BaseModel):  # type: ignore[no-redef]
        symbol: str
        data_quality: str = "MISSING"
        error: Optional[str] = None

        @classmethod
        def from_unified(cls, uq: "UnifiedQuote | Dict[str, Any]") -> "EnrichedQuote":
            if isinstance(uq, dict):
                sym = str(uq.get("symbol") or uq.get("ticker") or "").upper()
                return cls(symbol=sym or "UNKNOWN", data_quality=uq.get("data_quality", "MISSING"), error=uq.get("error"))
            return cls(symbol=(uq.symbol or "").upper(), data_quality=uq.data_quality, error=uq.error)

        def to_row(self, headers: Sequence[str]) -> List[Any]:
            hdrs = list(headers or [])
            if not hdrs:
                return [self.symbol]
            # first column assumed to be Symbol
            return [self.symbol] + [None] * (len(hdrs) - 1)

    class _StubEngine:
        async def get_enriched_quote(self, symbol: str) -> UnifiedQuote:
            sym = _normalize_symbol(symbol) or "UNKNOWN"
            return UnifiedQuote(
                symbol=sym,
                data_quality="MISSING",
                error="DataEngine v2 unavailable; using stub engine",
            )

        async def get_enriched_quotes(self, symbols: List[str]) -> List[UnifiedQuote]:
            return [await self.get_enriched_quote(s) for s in (symbols or [])]

    @lru_cache(maxsize=1)
    def _get_engine_singleton() -> Any:
        return _StubEngine()

    _ENGINE_MODE = "stub"
    _ENGINE_IS_STUB = True


# Backwards-compatible alias for legacy imports
EnrichedQuoteResponse = EnrichedQuote  # type: ignore[misc]


# =============================================================================
# Header helper (core.schemas) with robust fallbacks
# =============================================================================

def _fallback_headers() -> List[str]:
    # Last-resort minimal stable header set
    return ["Symbol", "Data Quality", "Error"]

try:
    from core.schemas import get_headers_for_sheet as _get_headers_for_sheet  # type: ignore
except Exception as exc:  # pragma: no cover
    logger.warning(
        "routes.enriched_quote: core.schemas.get_headers_for_sheet not available; "
        "using EnrichedQuote-based fallback headers. (%s)",
        exc,
    )

    def _get_headers_for_sheet(sheet_name: Optional[str]) -> List[str]:
        # Prefer model-provided headers if present
        try:
            if hasattr(EnrichedQuote, "get_headers"):
                hdrs = EnrichedQuote.get_headers(sheet_name)  # type: ignore[attr-defined]
                if isinstance(hdrs, list) and hdrs:
                    return [str(x) for x in hdrs]
            if hasattr(EnrichedQuote, "headers"):
                hdrs = getattr(EnrichedQuote, "headers")  # type: ignore[attr-defined]
                if isinstance(hdrs, list) and hdrs:
                    return [str(x) for x in hdrs]
        except Exception:
            pass
        return _fallback_headers()


# =============================================================================
# Engine call wrappers (support alternate method names safely)
# =============================================================================

async def _engine_get_one(symbol: str) -> Any:
    eng = _get_engine_singleton()
    if hasattr(eng, "get_enriched_quote"):
        return await eng.get_enriched_quote(symbol)  # type: ignore[attr-defined]
    if hasattr(eng, "get_quote"):
        return await eng.get_quote(symbol)  # type: ignore[attr-defined]
    raise RuntimeError("Engine missing get_enriched_quote/get_quote")


async def _engine_get_many(symbols: List[str]) -> List[Any]:
    eng = _get_engine_singleton()
    if hasattr(eng, "get_enriched_quotes"):
        return await eng.get_enriched_quotes(symbols)  # type: ignore[attr-defined]
    if hasattr(eng, "get_quotes"):
        return await eng.get_quotes(symbols)  # type: ignore[attr-defined]
    # fallback: per-symbol
    out: List[Any] = []
    for s in symbols:
        out.append(await _engine_get_one(s))
    return out


def _make_missing_unified(symbol: str, message: str) -> Any:
    sym = _normalize_symbol(symbol) or (symbol or "").strip().upper() or "UNKNOWN"
    try:
        return UnifiedQuote(symbol=sym, data_quality="MISSING", error=message)
    except Exception:
        return {"symbol": sym, "data_quality": "MISSING", "error": message}


async def _get_unified_quotes_chunked(
    symbols: List[str],
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    timeout_sec: float = DEFAULT_BATCH_TIMEOUT,
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
) -> Dict[str, Any]:
    """
    Chunked + concurrency-limited batch fetch.
    Returns dict keyed by the (normalized) input symbol string.
    """
    clean = _clean_tickers(symbols)
    if not clean:
        return {}

    chunks = _chunk(clean, batch_size)
    sem = asyncio.Semaphore(max(1, max_concurrency))

    logger.info(
        "Enriched batch: %d symbols in %d chunk(s) (engine=%s, batch=%d, timeout=%.1fs, conc=%d)",
        len(clean),
        len(chunks),
        _ENGINE_MODE,
        batch_size,
        timeout_sec,
        max(1, max_concurrency),
    )

    async def _run_chunk(chunk_syms: List[str]) -> Tuple[List[str], List[Any] | Exception]:
        async with sem:
            try:
                result = await asyncio.wait_for(_engine_get_many(chunk_syms), timeout=timeout_sec)
                return chunk_syms, result
            except Exception as e:
                return chunk_syms, e

    results = await asyncio.gather(*[_run_chunk(c) for c in chunks])

    out: Dict[str, Any] = {}
    for chunk_syms, res in results:
        if isinstance(res, Exception):
            msg = "Engine batch timeout" if isinstance(res, asyncio.TimeoutError) else f"Engine batch error: {res}"
            logger.warning("%s for chunk(size=%d): %s", msg, len(chunk_syms), chunk_syms)
            for s in chunk_syms:
                out[s] = _make_missing_unified(s, msg)
            continue

        # Build a symbol->quote map from returned payload
        by_sym: Dict[str, Any] = {}
        for q in (res or []):
            try:
                sym = getattr(q, "symbol", None) if not isinstance(q, dict) else q.get("symbol")
                sym = _normalize_symbol(str(sym or ""))
                if sym:
                    by_sym[sym] = q
            except Exception:
                continue

        # Preserve order and ensure each input has an output
        for s in chunk_syms:
            out[s] = by_sym.get(s) or _make_missing_unified(s, "No data returned from engine")

    return out


def _to_enriched(uq: Any, fallback_symbol: str) -> Any:
    sym = _normalize_symbol(fallback_symbol) or (fallback_symbol or "").strip().upper() or "UNKNOWN"
    try:
        return EnrichedQuote.from_unified(uq)
    except Exception as exc:
        logger.exception("UnifiedQuote -> EnrichedQuote conversion failed for %s", sym, exc_info=exc)
        try:
            return EnrichedQuote(symbol=sym, data_quality="MISSING", error=f"Conversion error: {exc}")
        except Exception:
            return EnrichedQuote.from_unified({"symbol": sym, "data_quality": "MISSING", "error": f"Conversion error: {exc}"})


async def _build_sheet_rows(
    symbols: List[str],
    sheet_name: Optional[str],
) -> Tuple[List[str], List[List[Any]]]:
    headers = _get_headers_for_sheet(sheet_name)
    if not headers:
        headers = _fallback_headers()

    clean = _clean_tickers(symbols)
    if not clean:
        return headers, []

    unified_map = await _get_unified_quotes_chunked(clean)
    rows: List[List[Any]] = []

    for s in clean:
        uq = unified_map.get(s) or _make_missing_unified(s, "No data returned from engine")
        enriched = _to_enriched(uq, s)

        try:
            row = enriched.to_row(headers)  # type: ignore[attr-defined]
            if not isinstance(row, list):
                raise ValueError("to_row did not return a list")
            # Ensure row length matches headers
            if len(row) < len(headers):
                row = row + [None] * (len(headers) - len(row))
            elif len(row) > len(headers):
                row = row[: len(headers)]
        except Exception as exc:
            logger.exception("enriched.to_row failed for %s", s, exc_info=exc)
            row = [getattr(enriched, "symbol", s)] + [None] * (len(headers) - 1)

        rows.append(row)

    return headers, rows


# =============================================================================
# FastAPI router & request/response models
# =============================================================================

router = APIRouter(prefix="/v1/enriched", tags=["Enriched Quotes"])


class BatchEnrichedRequest(BaseModel):
    tickers: List[str] = Field(default_factory=list, description="Symbols, e.g. ['AAPL','MSFT','1120.SR']")
    sheet_name: Optional[str] = Field(default=None, description="Optional sheet name for header selection")


class BatchEnrichedResponse(BaseModel):
    results: List[EnrichedQuote]


class SheetEnrichedResponse(BaseModel):
    headers: List[str]
    rows: List[List[Any]]


# =============================================================================
# Routes
# =============================================================================

@router.get("/health")
async def enriched_health() -> Dict[str, Any]:
    eng = _get_engine_singleton()
    providers = getattr(eng, "enabled_providers", None) or getattr(eng, "providers", None)
    primary = getattr(eng, "primary_provider", None) or getattr(eng, "primary", None)
    return {
        "status": "ok",
        "module": "enriched_quote",
        "version": API_VERSION,
        "engine_mode": _ENGINE_MODE,
        "engine_is_stub": _ENGINE_IS_STUB,
        "engine_providers": providers,
        "engine_primary": primary,
        "batch_size": DEFAULT_BATCH_SIZE,
        "batch_timeout_sec": DEFAULT_BATCH_TIMEOUT,
        "max_tickers": DEFAULT_MAX_TICKERS,
        "batch_concurrency": DEFAULT_MAX_CONCURRENCY,
    }


@router.get("/headers")
async def get_headers(
    sheet_name: Optional[str] = Query(default=None, description="Sheet name, e.g. 'Global_Markets'"),
) -> Dict[str, Any]:
    headers = _get_headers_for_sheet(sheet_name)
    if not headers:
        headers = _fallback_headers()
    return {
        "sheet_name": sheet_name,
        "count": len(headers),
        "headers": headers,
        "version": API_VERSION,
    }


@router.get("/quote", response_model=EnrichedQuote, response_model_exclude_none=True)
async def get_enriched_quote_route(
    symbol: str = Query(..., alias="symbol"),
) -> EnrichedQuote:
    ticker = _normalize_symbol(symbol)
    if not ticker:
        raise HTTPException(status_code=400, detail="Symbol is required")

    try:
        uq = await _engine_get_one(ticker)
    except Exception as exc:
        logger.exception("engine.get_enriched_quote failed for %s", ticker, exc_info=exc)
        return _to_enriched(_make_missing_unified(ticker, f"Engine error: {exc}"), ticker)

    return _to_enriched(uq, ticker)


@router.post("/quotes", response_model=BatchEnrichedResponse, response_model_exclude_none=True)
async def get_enriched_quotes_route(body: BatchEnrichedRequest) -> BatchEnrichedResponse:
    tickers = _clean_tickers(body.tickers)

    if not tickers:
        raise HTTPException(status_code=400, detail="At least one symbol is required")

    if len(tickers) > DEFAULT_MAX_TICKERS:
        raise HTTPException(
            status_code=400,
            detail=f"Too many symbols ({len(tickers)}). Max allowed is {DEFAULT_MAX_TICKERS}.",
        )

    unified_map = await _get_unified_quotes_chunked(tickers)
    results: List[EnrichedQuote] = []

    for s in tickers:
        uq = unified_map.get(s) or _make_missing_unified(s, "No data returned from engine")
        results.append(_to_enriched(uq, s))

    return BatchEnrichedResponse(results=results)


@router.post("/sheet-rows", response_model=SheetEnrichedResponse)
async def get_enriched_sheet_rows(body: BatchEnrichedRequest) -> SheetEnrichedResponse:
    tickers = _clean_tickers(body.tickers)

    # For Sheets workflows, allow empty tickers and still return headers
    if len(tickers) > DEFAULT_MAX_TICKERS:
        raise HTTPException(
            status_code=400,
            detail=f"Too many symbols ({len(tickers)}). Max allowed is {DEFAULT_MAX_TICKERS}.",
        )

    headers, rows = await _build_sheet_rows(tickers, body.sheet_name)
    return SheetEnrichedResponse(headers=headers, rows=rows)


__all__ = ["router", "EnrichedQuoteResponse"]
