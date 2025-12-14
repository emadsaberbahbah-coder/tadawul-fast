# routes/enriched_quote.py
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

try:
    from config import get_settings
except Exception:  # pragma: no cover
    get_settings = None  # type: ignore


router = APIRouter(prefix="/v1/enriched", tags=["enriched"])


def _settings():
    if get_settings:
        return get_settings()
    class _S:
        ENRICHED_MAX_TICKERS = 250
        ENRICHED_BATCH_CONCURRENCY = 5
    return _S()


settings = _settings()


class QuotesBatchRequest(BaseModel):
    symbols: List[str] = Field(..., min_length=1)


def _normalize_symbol(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    # Keep case stable (most providers accept upper; .SR remains)
    return s.upper()


def _as_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    # Pydantic v2
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    # Pydantic v1
    if hasattr(obj, "dict"):
        return obj.dict()
    # last resort
    return {"value": obj}


async def _engine_get_quote(symbol: str) -> Dict[str, Any]:
    """
    Tries v2 engine first, then legacy v1 engine.
    Always returns a dict and NEVER raises to the caller.
    """
    # 1) v2 engine
    try:
        from core.data_engine_v2 import DataEngine  # type: ignore
        eng = DataEngine()
        res = await eng.get_enriched_quote(symbol)
        d = _as_dict(res)
        if d:
            return d
    except Exception:
        pass

    # 2) legacy v1 engine (module-level or class wrapper)
    try:
        from core import data_engine as de  # type: ignore

        if hasattr(de, "get_enriched_quote"):
            res = await de.get_enriched_quote(symbol)
            d = _as_dict(res)
            if d:
                return d

        if hasattr(de, "DataEngine"):
            eng = de.DataEngine()
            if hasattr(eng, "get_enriched_quote"):
                res = await eng.get_enriched_quote(symbol)
                d = _as_dict(res)
                if d:
                    return d
    except Exception:
        pass

    # Safe fallback
    return {
        "symbol": symbol,
        "market": "KSA" if symbol.endswith(".SR") or symbol.isdigit() else None,
        "error": "MISSING: quote engine returned no data",
        "data_quality": "MISSING",
        "last_updated_utc": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/health")
async def enriched_health():
    return {
        "status": "ok",
        "time_utc": datetime.now(timezone.utc).isoformat(),
        "max_tickers": int(getattr(settings, "ENRICHED_MAX_TICKERS", 250)),
        "batch_concurrency": int(getattr(settings, "ENRICHED_BATCH_CONCURRENCY", 5)),
    }


@router.get("/quote")
async def enriched_quote(symbol: str = Query(..., min_length=1, description="Ticker symbol, e.g. 1120.SR or AAPL")):
    sym = _normalize_symbol(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="Invalid symbol")
    return await _engine_get_quote(sym)


@router.get("/quotes")
async def enriched_quotes_get(
    symbols: str = Query(..., description="Comma-separated symbols, e.g. 1120.SR,AAPL,MSFT"),
):
    raw = [x.strip() for x in (symbols or "").split(",")]
    syms = [_normalize_symbol(x) for x in raw if _normalize_symbol(x)]
    return await _batch_fetch(syms)


@router.post("/quotes")
async def enriched_quotes_post(body: QuotesBatchRequest):
    syms = [_normalize_symbol(x) for x in body.symbols if _normalize_symbol(x)]
    return await _batch_fetch(syms)


async def _batch_fetch(symbols: List[str]) -> Dict[str, Any]:
    if not symbols:
        raise HTTPException(status_code=400, detail="No symbols provided")

    max_tickers = int(getattr(settings, "ENRICHED_MAX_TICKERS", 250))
    if len(symbols) > max_tickers:
        raise HTTPException(status_code=400, detail=f"Too many symbols. Max={max_tickers}")

    concurrency = max(1, int(getattr(settings, "ENRICHED_BATCH_CONCURRENCY", 5)))
    sem = asyncio.Semaphore(concurrency)

    async def _one(sym: str) -> Dict[str, Any]:
        async with sem:
            return await _engine_get_quote(sym)

    results = await asyncio.gather(*[_one(s) for s in symbols], return_exceptions=True)

    out: List[Dict[str, Any]] = []
    for sym, r in zip(symbols, results):
        if isinstance(r, Exception):
            out.append(
                {
                    "symbol": sym,
                    "error": f"ERROR: {type(r).__name__}",
                    "data_quality": "MISSING",
                    "last_updated_utc": datetime.now(timezone.utc).isoformat(),
                }
            )
        else:
            out.append(r)

    return {
        "count": len(out),
        "symbols": symbols,
        "items": out,
        "time_utc": datetime.now(timezone.utc).isoformat(),
    }
