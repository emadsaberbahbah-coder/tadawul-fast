"""
legacy_service.py
------------------------------------------------------------
LEGACY SERVICE BRIDGE – v3.9.0 (Engine v2 Compatible)

GOAL
- Provide a stable "legacy" API layer so old clients continue to work:
    • GET  /v1/quote?symbol=...
    • POST /v1/quotes   { "tickers": [...] }
- Never call providers directly. Always delegates to DataEngine.
- Adds AI scores (value/quality/momentum/opportunity) when possible.

KEY PRINCIPLES
- Defensive: never crash due to missing engine methods or missing fields.
- KSA-safe: normalization supports numeric Tadawul codes -> .SR.
- Backward compatible JSON schema for old PowerShell / early Sheets scripts.

Author: Emad Bahbah (with GPT-5.2 Thinking)
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence

from fastapi import APIRouter, Body, Query
from pydantic import BaseModel

# ----------------------------------------------------------------------
# Safe imports (work across repo layout changes)
# ----------------------------------------------------------------------
try:
    # Preferred (current)
    from core.data_engine_v2 import DataEngine, UnifiedQuote  # type: ignore
except Exception as e:  # pragma: no cover
    raise ImportError("core.data_engine_v2 is required for legacy_service") from e

try:
    from core.enriched_quote import EnrichedQuote  # type: ignore
except Exception as e:  # pragma: no cover
    raise ImportError("core.enriched_quote is required for legacy_service") from e

try:
    from core.scoring_engine import enrich_with_scores  # type: ignore
except Exception as e:  # pragma: no cover
    raise ImportError("core.scoring_engine is required for legacy_service") from e


logger = logging.getLogger("routes.legacy_service")

LEGACY_VERSION = "3.9.0"

# ----------------------------------------------------------------------
# Configuration (supports both env.settings and core.config)
# ----------------------------------------------------------------------
def _get_settings_obj():
    # Legacy env.py style
    try:
        from env import settings as _settings  # type: ignore
        return _settings
    except Exception:
        pass

    # core.config style
    try:
        from core.config import get_settings  # type: ignore
        return get_settings()
    except Exception:
        pass

    # fallback
    class _S:
        enriched_batch_size = int(os.getenv("LEGACY_BATCH_SIZE", "25"))
        enriched_batch_timeout_sec = float(os.getenv("LEGACY_BATCH_TIMEOUT_SEC", "45"))
        enriched_batch_concurrency = int(os.getenv("LEGACY_BATCH_CONCURRENCY", "5"))
    return _S()

settings = _get_settings_obj()

BATCH_SIZE = int(getattr(settings, "enriched_batch_size", 25) or 25)
BATCH_TIMEOUT = float(getattr(settings, "enriched_batch_timeout_sec", 45) or 45)
MAX_CONCURRENCY = int(getattr(settings, "enriched_batch_concurrency", 5) or 5)

# ----------------------------------------------------------------------
# Legacy Data Models (Schema expected by old clients)
# ----------------------------------------------------------------------
class LegacyQuoteModel(BaseModel):
    # Identity
    symbol: str
    ticker: str
    name: Optional[str] = None
    exchange: Optional[str] = None
    market: Optional[str] = None
    currency: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None

    # Price
    price: Optional[float] = None
    previous_close: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    change: Optional[float] = None
    change_percent: Optional[float] = None

    # Liquidity
    volume: Optional[float] = None
    avg_volume: Optional[float] = None
    market_cap: Optional[float] = None
    shares_outstanding: Optional[float] = None

    # 52 Week
    fifty_two_week_high: Optional[float] = None
    fifty_two_week_low: Optional[float] = None
    fifty_two_week_position_pct: Optional[float] = None

    # Fundamentals
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None
    eps: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None
    profit_margin: Optional[float] = None
    debt_to_equity: Optional[float] = None

    # Scores (safe additions; old clients ignore extra fields)
    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    overall_score: Optional[float] = None
    recommendation: Optional[str] = None

    # Meta
    data_quality: str = "MISSING"
    data_source: Optional[str] = None
    last_updated_utc: Optional[str] = None
    error: Optional[str] = None


class LegacyResponse(BaseModel):
    quotes: List[LegacyQuoteModel]
    meta: Dict[str, Any]


# ----------------------------------------------------------------------
# Engine Singleton
# ----------------------------------------------------------------------
@lru_cache(maxsize=1)
def _get_engine() -> DataEngine:
    return DataEngine()


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def _normalize_symbol(symbol: str) -> str:
    """
    Normalizes many client formats into engine-ready symbols:
      - '1120' -> '1120.SR'
      - 'TADAWUL:1120' -> '1120.SR'
      - '1120.TADAWUL' -> '1120.SR'
      - keeps 'AAPL' or 'AAPL.US' as-is (uppercased)
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

def _chunk(items: List[str], size: int) -> List[List[str]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]

def _safe_get(obj: Any, name: str, default=None):
    try:
        return getattr(obj, name)
    except Exception:
        return default

def _map_unified_to_legacy(uq: UnifiedQuote) -> LegacyQuoteModel:
    """
    Maps UnifiedQuote -> EnrichedQuote (with Scores) -> LegacyQuoteModel.
    Fully defensive (missing fields don't break mapping).
    """
    eq = EnrichedQuote.from_unified(uq)

    # scores (never raise)
    try:
        eq = enrich_with_scores(eq)
    except Exception:
        pass

    # price helpers
    price = _safe_get(eq, "current_price", None) or _safe_get(eq, "last_price", None)
    change = _safe_get(eq, "price_change", None) or _safe_get(eq, "change", None)
    chg_pct = _safe_get(eq, "percent_change", None) or _safe_get(eq, "change_percent", None)

    return LegacyQuoteModel(
        symbol=_safe_get(eq, "symbol", uq.symbol),
        ticker=_safe_get(eq, "symbol", uq.symbol),

        name=_safe_get(eq, "name", None),
        exchange=_safe_get(eq, "exchange", None),
        market=_safe_get(eq, "market", None) or _safe_get(eq, "market_region", None),
        currency=_safe_get(eq, "currency", None),
        sector=_safe_get(eq, "sector", None),
        industry=_safe_get(eq, "industry", None),

        price=price,
        previous_close=_safe_get(eq, "previous_close", None),
        open=_safe_get(eq, "open", None),
        high=_safe_get(eq, "day_high", None) or _safe_get(eq, "high", None),
        low=_safe_get(eq, "day_low", None) or _safe_get(eq, "low", None),
        change=change,
        change_percent=chg_pct,

        volume=_safe_get(eq, "volume", None),
        avg_volume=_safe_get(eq, "avg_volume_30d", None) or _safe_get(eq, "avg_volume", None),
        market_cap=_safe_get(eq, "market_cap", None),
        shares_outstanding=_safe_get(eq, "shares_outstanding", None),

        fifty_two_week_high=_safe_get(eq, "high_52w", None),
        fifty_two_week_low=_safe_get(eq, "low_52w", None),
        fifty_two_week_position_pct=_safe_get(eq, "position_52w_percent", None),

        pe_ratio=_safe_get(eq, "pe_ttm", None),
        pb_ratio=_safe_get(eq, "pb", None),
        dividend_yield=_safe_get(eq, "dividend_yield", None),
        eps=_safe_get(eq, "eps_ttm", None),
        roe=_safe_get(eq, "roe", None),
        roa=_safe_get(eq, "roa", None),
        profit_margin=_safe_get(eq, "net_margin", None),
        debt_to_equity=_safe_get(eq, "debt_to_equity", None),

        value_score=_safe_get(eq, "value_score", None),
        quality_score=_safe_get(eq, "quality_score", None),
        momentum_score=_safe_get(eq, "momentum_score", None),
        opportunity_score=_safe_get(eq, "opportunity_score", None),
        overall_score=_safe_get(eq, "overall_score", None),
        recommendation=_safe_get(eq, "recommendation", None),

        data_quality=_safe_get(eq, "data_quality", "MISSING") or "MISSING",
        data_source=_safe_get(eq, "data_source", None) or _safe_get(eq, "primary_provider", None),
        last_updated_utc=_safe_get(eq, "last_updated_utc", None),
        error=_safe_get(eq, "error", None),
    )

async def _engine_get_quotes(engine: Any, symbols: List[str]) -> List[Any]:
    """
    Compatibility wrapper:
    - Prefer engine.get_enriched_quotes(symbols)
    - Else engine.get_quotes(symbols)
    - Else gather engine.get_quote(s) / get_enriched_quote(s)
    """
    if hasattr(engine, "get_enriched_quotes"):
        return await engine.get_enriched_quotes(symbols)
    if hasattr(engine, "get_quotes"):
        return await engine.get_quotes(symbols)

    # last resort: per-symbol
    out = []
    for s in symbols:
        if hasattr(engine, "get_enriched_quote"):
            out.append(await engine.get_enriched_quote(s))
        elif hasattr(engine, "get_quote"):
            out.append(await engine.get_quote(s))
        else:
            out.append(UnifiedQuote(symbol=s, data_quality="MISSING", error="Engine missing quote methods"))
    return out

async def _fetch_legacy_quotes(tickers: Sequence[str]) -> List[LegacyQuoteModel]:
    clean = [_normalize_symbol(t) for t in (tickers or []) if str(t).strip()]
    # dedupe preserve order
    clean = list(dict.fromkeys(clean))

    if not clean:
        return []

    engine = _get_engine()
    chunks = _chunk(clean, BATCH_SIZE)
    sem = asyncio.Semaphore(max(1, MAX_CONCURRENCY))

    results_map: Dict[str, LegacyQuoteModel] = {}

    async def process_chunk(chunk_syms: List[str]) -> None:
        async with sem:
            try:
                uqs = await asyncio.wait_for(_engine_get_quotes(engine, chunk_syms), timeout=BATCH_TIMEOUT)
                uq_map = {getattr(uq, "symbol", "").upper(): uq for uq in (uqs or [])}

                for s in chunk_syms:
                    uq = uq_map.get(s.upper()) or UnifiedQuote(symbol=s, data_quality="MISSING", error="No data returned")
                    # finalize if the model supports it
                    if hasattr(uq, "finalize"):
                        try:
                            uq = uq.finalize()
                        except Exception:
                            pass
                    results_map[s.upper()] = _map_unified_to_legacy(uq)

            except Exception as e:
                logger.error("Legacy batch error: %s", e)
                for s in chunk_syms:
                    results_map[s.upper()] = LegacyQuoteModel(
                        symbol=s, ticker=s, data_quality="MISSING", error=str(e)
                    )

    await asyncio.gather(*[process_chunk(c) for c in chunks])

    # Return in original order
    final_list: List[LegacyQuoteModel] = []
    for s in clean:
        final_list.append(results_map.get(s.upper()) or LegacyQuoteModel(symbol=s, ticker=s, data_quality="MISSING", error="No data returned"))
    return final_list


# ----------------------------------------------------------------------
# Router
# ----------------------------------------------------------------------
router = APIRouter(tags=["Legacy Service"])

@router.get("/v1/legacy/health")
async def legacy_health():
    engine = _get_engine()
    return {
        "status": "ok",
        "module": "routes.legacy_service",
        "version": LEGACY_VERSION,
        "engine": "DataEngineV2",
        "providers": getattr(engine, "enabled_providers", None),
        "batch": {"size": BATCH_SIZE, "timeout_sec": BATCH_TIMEOUT, "concurrency": MAX_CONCURRENCY},
    }

@router.get("/v1/quote", response_model=LegacyResponse)
async def get_legacy_quote(symbol: str = Query(..., alias="symbol")):
    """Legacy single quote endpoint."""
    quotes = await _fetch_legacy_quotes([symbol])
    return LegacyResponse(
        quotes=quotes,
        meta={
            "count": len(quotes),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "note": "Powered by Tadawul Fast Bridge V2",
        },
    )

@router.post("/v1/quotes", response_model=LegacyResponse)
async def get_legacy_quotes_batch(
    symbols: List[str] = Body(..., alias="tickers", embed=True)
    # Accepts JSON: { "tickers": ["AAPL", "1120.SR"] }
):
    """Legacy batch quotes endpoint."""
    quotes = await _fetch_legacy_quotes(symbols)
    return LegacyResponse(
        quotes=quotes,
        meta={
            "count": len(quotes),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "note": "Powered by Tadawul Fast Bridge V2",
        },
    )

# Public Export (for main.py or direct usage)
get_legacy_quotes = _fetch_legacy_quotes

__all__ = ["router", "get_legacy_quotes"]
