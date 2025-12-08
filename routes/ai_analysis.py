"""
routes/ai_analysis.py
------------------------------------------------------------
AI & QUANT ANALYSIS ROUTES – GOOGLE SHEETS FRIENDLY (v2.3)

- Preferred backend:
    • core.data_engine_v2.DataEngine (class-based unified engine)

- Fallback backend:
    • core.data_engine (module-level async functions:
        get_enriched_quote / get_enriched_quotes)

- Last-resort:
    • In-process stub engine that always returns MISSING data so that
      the API and Google Sheets never crash.

- Computes:
    • Value / Quality / Momentum / Opportunity / Overall / Recommendation

- Designed for:
    • 9-page Google Sheets dashboard
    • KSA-safe routing (KSA handled by the unified engine / Argaam,
      no direct EODHD calls for .SR from here)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

logger = logging.getLogger("routes.ai_analysis")

# ----------------------------------------------------------------------
# Optional env.py integration (for engine configuration / diagnostics)
# ----------------------------------------------------------------------

try:  # pragma: no cover - optional
    import env as _env  # type: ignore
except Exception:
    _env = None  # type: ignore

# ----------------------------------------------------------------------
# ENGINE DETECTION & FALLBACKS (aligned with advanced_analysis)
# ----------------------------------------------------------------------

_ENGINE_MODE: str = "stub"  # "v2", "v1_module", or "stub"
_ENGINE_IS_STUB: bool = True
_engine: Any = None
_data_engine_module: Any = None

# Preferred: new unified engine (class-based)
try:  # pragma: no cover - defensive
    from core.data_engine_v2 import DataEngine as _V2DataEngine  # type: ignore

    engine_kwargs: Dict[str, Any] = {}

    if _env is not None:
        cache_ttl = getattr(_env, "ENGINE_CACHE_TTL_SECONDS", None)
        if cache_ttl is not None:
            engine_kwargs["cache_ttl"] = cache_ttl

        provider_timeout = getattr(_env, "ENGINE_PROVIDER_TIMEOUT_SECONDS", None)
        if provider_timeout is not None:
            engine_kwargs["provider_timeout"] = provider_timeout

        enabled_providers = getattr(_env, "ENABLED_PROVIDERS", None)
        if enabled_providers:
            engine_kwargs["enabled_providers"] = enabled_providers

        enable_adv = getattr(_env, "ENGINE_ENABLE_ADVANCED_ANALYSIS", True)
        engine_kwargs["enable_advanced_analysis"] = enable_adv

    try:
        if engine_kwargs:
            _engine = _V2DataEngine(**engine_kwargs)
        else:
            _engine = _V2DataEngine()
    except TypeError:
        # Signature mismatch -> fall back to default constructor
        _engine = _V2DataEngine()

    _ENGINE_MODE = "v2"
    _ENGINE_IS_STUB = False
    logger.info(
        "ai_analysis: Using DataEngine v2 from core.data_engine_v2 (kwargs=%s)",
        list(engine_kwargs.keys()),
    )

except Exception as e_v2:  # pragma: no cover - defensive
    logger.exception(
        "ai_analysis: Failed to import/use core.data_engine_v2.DataEngine: %s",
        e_v2,
    )
    # Fallback: legacy data engine module with async functions
    try:
        from core import data_engine as _data_engine_module  # type: ignore

        _ENGINE_MODE = "v1_module"
        _ENGINE_IS_STUB = False
        logger.warning(
            "ai_analysis: Falling back to core.data_engine module-level API"
        )
    except Exception as e_v1:  # pragma: no cover - defensive
        logger.exception(
            "ai_analysis: Failed to import core.data_engine as fallback: %s",
            e_v1,
        )

        class _StubEngine:
            """
            Safe stub engine when no real engine is available.

            All responses have data_quality='MISSING', but the API
            stays up and Google Sheets never see 500 errors.
            """

            async def get_enriched_quote(self, symbol: str) -> Dict[str, Any]:
                sym = (symbol or "").strip().upper()
                return {
                    "symbol": sym,
                    "name": None,
                    "market_region": "UNKNOWN",
                    "price": None,
                    "change_pct": None,
                    "market_cap": None,
                    "pe_ttm": None,
                    "pb": None,
                    "dividend_yield": None,
                    "roe": None,
                    "roa": None,
                    "data_quality": "MISSING",
                    "opportunity_score": None,
                    "as_of_utc": None,
                    "sources": [],
                    "notes": None,
                    "error": (
                        "Data engine modules (core.data_engine_v2/core.data_engine) "
                        "are not available or failed to import."
                    ),
                }

            async def get_enriched_quotes(
                self, symbols: List[str]
            ) -> List[Dict[str, Any]]:
                out: List[Dict[str, Any]] = []
                for s in symbols:
                    out.append(await self.get_enriched_quote(s))
                return out

        _engine = _StubEngine()
        _ENGINE_MODE = "stub"
        _ENGINE_IS_STUB = True
        logger.error("ai_analysis: Using stub DataEngine with MISSING data.")

# ----------------------------------------------------------------------
# ROUTER
# ----------------------------------------------------------------------

router = APIRouter(
    prefix="/v1/analysis",
    tags=["AI & Analysis"],
)

# ----------------------------------------------------------------------
# MODELS
# ----------------------------------------------------------------------


class SingleAnalysisResponse(BaseModel):
    """One ticker full analysis – safe for Sheets consumption."""

    symbol: str
    name: Optional[str] = None
    market_region: Optional[str] = None

    price: Optional[float] = None
    change_pct: Optional[float] = None
    market_cap: Optional[float] = None
    pe_ttm: Optional[float] = None
    pb: Optional[float] = None
    dividend_yield: Optional[float] = None  # stored as fraction (0–1)
    roe: Optional[float] = None             # stored as fraction (0–1)
    roa: Optional[float] = None             # optional, currently not in sheet

    data_quality: str = "MISSING"

    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    overall_score: Optional[float] = None
    recommendation: Optional[str] = None

    last_updated_utc: Optional[datetime] = None
    sources: List[str] = Field(default_factory=list)

    notes: Optional[str] = None
    error: Optional[str] = None  # text error instead of 500


class BatchAnalysisRequest(BaseModel):
    tickers: List[str]


class BatchAnalysisResponse(BaseModel):
    results: List[SingleAnalysisResponse]


class SheetAnalysisResponse(BaseModel):
    headers: List[str]
    rows: List[List[Any]]


# ----------------------------------------------------------------------
# INTERNAL HELPERS – ENGINE WRAPPERS
# ----------------------------------------------------------------------


async def _engine_get_quote(symbol: str) -> Any:
    """
    Unified wrapper for a single quote, hiding engine differences.
    """
    sym = (symbol or "").strip()
    if not sym:
        return None

    if _ENGINE_MODE == "v2":
        return await _engine.get_enriched_quote(sym)
    if _ENGINE_MODE == "v1_module" and _data_engine_module is not None:
        return await _data_engine_module.get_enriched_quote(sym)  # type: ignore[attr-defined]
    # Stub
    return await _engine.get_enriched_quote(sym)


async def _engine_get_quotes(symbols: List[str]) -> List[Any]:
    """
    Unified wrapper for multiple quotes.
    """
    clean = [s.strip() for s in (symbols or []) if s and s.strip()]
    if not clean:
        return []

    if _ENGINE_MODE == "v2":
        return await _engine.get_enriched_quotes(clean)
    if _ENGINE_MODE == "v1_module" and _data_engine_module is not None:
        return await _data_engine_module.get_enriched_quotes(clean)  # type: ignore[attr-defined]
    # Stub
    return await _engine.get_enriched_quotes(clean)


def _qget(obj: Any, *names: str) -> Any:
    """
    Safely get a field from a UnifiedQuote-like object or dict.

    Tries attributes first, then dict keys, returns None if not found.
    """
    if obj is None:
        return None
    for name in names:
        if hasattr(obj, name):
            try:
                val = getattr(obj, name)
                if val is not None:
                    return val
            except Exception:
                continue
        if isinstance(obj, dict) and name in obj and obj[name] is not None:
            return obj[name]
    return None


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _normalize_percent_like(x: Any) -> Optional[float]:
    """
    Convert any percent-like value into a fraction (0–1) for internal use.

    - If x is None -> None
    - If 0 <= x <= 1  -> treated as fraction already (e.g. 0.15 = 15%)
    - If 1 < x <= 100 -> treated as percent, converted to fraction (e.g. 15 -> 0.15)
    - If x > 100      -> left as-is (defensive; will be clamped by scoring)
    """
    v = _safe_float(x)
    if v is None:
        return None
    if 0.0 <= v <= 1.0:
        return v
    if 1.0 < v <= 100.0:
        return v / 100.0
    return v


def _compute_scores(quote: Any) -> Dict[str, Optional[float]]:
    """
    Basic AI-like scoring layer. 0–100 scale for each score.
    Uses fields from the unified engine (v2 or v1).
    """
    pe = _safe_float(_qget(quote, "pe_ttm", "pe_ratio", "pe"))
    pb = _safe_float(_qget(quote, "pb", "pb_ratio", "priceToBook"))
    dy = _normalize_percent_like(
        _qget(quote, "dividend_yield", "dividend_yield_percent", "dividendYield")
    )
    roe = _normalize_percent_like(_qget(quote, "roe", "roe_percent", "returnOnEquity"))
    profit_margin = _normalize_percent_like(
        _qget(quote, "profit_margin", "net_margin_percent", "profitMargins")
    )
    change_pct = _safe_float(
        _qget(quote, "change_pct", "change_percent", "changePercent")
    )

    # ------------------------
    # Value score (P/E, P/B, Dividend Yield)
    # ------------------------
    vs = 50.0
    if pe is not None:
        if 0 < pe < 12:
            vs += 20
        elif 12 <= pe <= 20:
            vs += 10
        elif pe > 35:
            vs -= 15
    if pb is not None:
        if 0 < pb < 1:
            vs += 10
        elif pb > 4:
            vs -= 10
    if dy is not None:
        # dy is fraction here (0–1)
        if 0.02 <= dy <= 0.06:  # 2–6%
            vs += 10
        elif dy > 0.08:  # >8%
            vs -= 5
    value_score: float = max(0.0, min(100.0, vs))

    # ------------------------
    # Quality score (ROE, profit margin)
    # ------------------------
    qs = 50.0
    if roe is not None:
        if roe >= 0.20:  # ≥20%
            qs += 25
        elif 0.10 <= roe < 0.20:
            qs += 10
        elif roe < 0.0:
            qs -= 15
    if profit_margin is not None:
        if profit_margin >= 0.20:
            qs += 20
        elif 0.10 <= profit_margin < 0.20:
            qs += 10
        elif profit_margin < 0.0:
            qs -= 10
    quality_score: float = max(0.0, min(100.0, qs))

    # ------------------------
    # Momentum score (daily change %)
    # ------------------------
    ms = 50.0
    if change_pct is not None:
        if change_pct > 0:
            ms += min(change_pct, 10) * 2  # cap upside contribution
        elif change_pct < 0:
            ms += max(change_pct, -10) * 2  # penalties for deep red
    momentum_score: float = max(0.0, min(100.0, ms))

    return {
        "value_score": value_score,
        "quality_score": quality_score,
        "momentum_score": momentum_score,
    }


def _compute_overall_and_reco(
    opportunity_score: Optional[float],
    value_score: Optional[float],
    quality_score: Optional[float],
    momentum_score: Optional[float],
) -> Dict[str, Optional[float]]:
    scores = [
        s
        for s in [opportunity_score, value_score, quality_score, momentum_score]
        if isinstance(s, (int, float))
    ]
    if not scores:
        return {"overall_score": None, "recommendation": None}

    overall = sum(scores) / len(scores)

    if overall >= 80:
        reco = "STRONG_BUY"
    elif overall >= 65:
        reco = "BUY"
    elif overall >= 45:
        reco = "HOLD"
    elif overall >= 30:
        reco = "REDUCE"
    else:
        reco = "SELL"

    return {"overall_score": overall, "recommendation": reco}


def _quote_to_analysis(quote: Any) -> SingleAnalysisResponse:
    """
    Convert a UnifiedQuote-like object to SingleAnalysisResponse.

    Works with:
    - core.data_engine_v2.UnifiedQuote
    - core.data_engine.UnifiedQuote
    - dict payloads
    """
    if quote is None:
        return SingleAnalysisResponse(
            symbol="",
            data_quality="MISSING",
            error="No quote data returned from engine",
        )

    base_scores = _compute_scores(quote)

    opportunity_score = _safe_float(_qget(quote, "opportunity_score"))
    more = _compute_overall_and_reco(
        opportunity_score=opportunity_score,
        value_score=base_scores["value_score"],
        quality_score=base_scores["quality_score"],
        momentum_score=base_scores["momentum_score"],
    )

    symbol = str(_qget(quote, "symbol", "ticker") or "").upper()
    name = _qget(quote, "name", "company_name", "longName", "shortName")
    market_region = _qget(
        quote,
        "market_region",
        "market",
        "exchange_short_name",
        "exchange",
    )

    price = _safe_float(_qget(quote, "price", "last_price"))
    change_pct = _safe_float(
        _qget(quote, "change_pct", "change_percent", "changePercent")
    )
    market_cap = _safe_float(_qget(quote, "market_cap", "marketCap"))
    pe_ttm = _safe_float(_qget(quote, "pe_ttm", "pe_ratio", "pe"))
    pb = _safe_float(_qget(quote, "pb", "pb_ratio", "priceToBook"))
    dividend_yield = _normalize_percent_like(
        _qget(quote, "dividend_yield", "dividend_yield_percent", "dividendYield")
    )
    roe = _normalize_percent_like(_qget(quote, "roe", "roe_percent", "returnOnEquity"))
    roa = _normalize_percent_like(_qget(quote, "roa", "roa_percent", "returnOnAssets"))
    data_quality = str(_qget(quote, "data_quality") or "MISSING")

    last_updated_raw = _qget(quote, "last_updated_utc", "as_of_utc")
    # Pydantic will parse this; it's fine if it's str or datetime
    last_updated_utc: Optional[datetime]
    if isinstance(last_updated_raw, datetime):
        last_updated_utc = last_updated_raw
    elif isinstance(last_updated_raw, str):
        try:
            last_updated_utc = datetime.fromisoformat(last_updated_raw)
        except Exception:
            last_updated_utc = None
    else:
        last_updated_utc = None

    # Sources can be objects, dicts, or strings
    raw_sources = _qget(quote, "sources") or []
    sources: List[str] = []
    try:
        for s in raw_sources:
            if hasattr(s, "provider"):
                sources.append(str(getattr(s, "provider")))
            elif isinstance(s, dict) and "provider" in s:
                sources.append(str(s["provider"]))
            else:
                sources.append(str(s))
    except Exception:
        pass

    notes = _qget(quote, "notes")
    error = _qget(quote, "error")

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
        data_quality=data_quality,
        value_score=base_scores["value_score"],
        quality_score=base_scores["quality_score"],
        momentum_score=base_scores["momentum_score"],
        opportunity_score=opportunity_score,
        overall_score=more["overall_score"],
        recommendation=more["recommendation"],
        last_updated_utc=last_updated_utc,
        sources=sources,
        notes=notes,
        error=error,
    )


def _build_sheet_headers() -> List[str]:
    """
    Headers to use directly in Google Sheets for the AI/Insights layer.
    This is mainly for the Insights_Analysis page, but can be reused for
    any scoring/AI overlay on top of the other 8 pages.
    """
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


def _analysis_to_sheet_row(a: SingleAnalysisResponse) -> List[Any]:
    """
    Flatten one analysis result to a row (all primitives),
    so Apps Script can use setValues() safely.
    """
    return [
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
    ]


# ----------------------------------------------------------------------
# ROUTES
# ----------------------------------------------------------------------


@router.get("/health")
@router.get("/ping", summary="Quick health-check for AI analysis")
async def analysis_health() -> Dict[str, Any]:
    """
    Simple health check for this module.
    """
    return {
        "status": "ok",
        "module": "ai_analysis",
        "version": "2.3",
        "engine_mode": _ENGINE_MODE,
        "engine_is_stub": _ENGINE_IS_STUB,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/quote", response_model=SingleAnalysisResponse)
async def analyze_single_quote(
    ticker: str = Query(..., alias="symbol"),
) -> SingleAnalysisResponse:
    """
    High-level AI/quant analysis for a single ticker.
    Designed to be consumed by Google Sheets / Apps Script.

    Example:
        GET /v1/analysis/quote?symbol=AAPL
        GET /v1/analysis/quote?symbol=1120.SR
    """
    ticker = (ticker or "").strip()
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker is required")

    try:
        quote = await _engine_get_quote(ticker)
        analysis = _quote_to_analysis(quote)

        # If we truly have no data from providers, mark an explicit error
        if analysis.data_quality == "MISSING" and not analysis.error:
            analysis.error = "No data available from providers"
        return analysis
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Exception in analyze_single_quote for %s", ticker)
        # NEVER crash Google Sheets – always return a valid body
        return SingleAnalysisResponse(
            symbol=ticker.upper(),
            data_quality="MISSING",
            error=f"Exception in analysis: {exc}",
        )


@router.post("/quotes", response_model=BatchAnalysisResponse)
async def analyze_batch_quotes(body: BatchAnalysisRequest) -> BatchAnalysisResponse:
    """
    Batch AI/quant analysis for multiple tickers.

    Body:
        {
          "tickers": ["AAPL", "MSFT", "1120.SR"]
        }
    """
    tickers = [t.strip() for t in (body.tickers or []) if t and t.strip()]

    if not tickers:
        raise HTTPException(status_code=400, detail="At least one ticker is required")

    try:
        unified_quotes = await _engine_get_quotes(tickers)
    except Exception as exc:
        logger.exception("Batch analysis failed for tickers=%s", tickers)
        # Total failure – return placeholder rows for all tickers
        placeholder_results: List[SingleAnalysisResponse] = [
            SingleAnalysisResponse(
                symbol=t.upper(),
                data_quality="MISSING",
                error=f"Batch analysis failed: {exc}",
            )
            for t in tickers
        ]
        return BatchAnalysisResponse(results=placeholder_results)

    # Defensive: ensure we always iterate len(tickers) times
    if unified_quotes is None:
        unified_quotes = []
    if not isinstance(unified_quotes, list):
        unified_quotes = [unified_quotes]

    # Pad or truncate to match number of tickers
    if len(unified_quotes) < len(tickers):
        unified_quotes.extend([None] * (len(tickers) - len(unified_quotes)))
    elif len(unified_quotes) > len(tickers):
        unified_quotes = unified_quotes[: len(tickers)]

    results: List[SingleAnalysisResponse] = []
    for t, q in zip(tickers, unified_quotes):
        try:
            analysis = _quote_to_analysis(q)
            if not analysis.symbol:
                analysis.symbol = t.upper()
            if analysis.data_quality == "MISSING" and not analysis.error:
                analysis.error = "No data available from providers"
            results.append(analysis)
        except Exception as exc:
            logger.exception(
                "Exception building analysis for %s in batch", t, exc_info=exc
            )
            results.append(
                SingleAnalysisResponse(
                    symbol=t.upper(),
                    data_quality="MISSING",
                    error=f"Exception building analysis: {exc}",
                )
            )

    return BatchAnalysisResponse(results=results)


@router.post("/sheet-rows", response_model=SheetAnalysisResponse)
async def analyze_for_sheet(body: BatchAnalysisRequest) -> SheetAnalysisResponse:
    """
    Google Sheets-friendly endpoint.

    Returns:
        {
          "headers": [...],
          "rows": [ [row for t1], [row for t2], ... ]
        }

    Apps Script can simply setValues() using rows with the returned headers.

    This is designed for:
        - Insights_Analysis sheet
        - Any scoring/AI layer on top of KSA_Tadawul, Global_Markets,
          Mutual_Funds, Commodities_FX, and My_Portfolio pages.
    """
    # Reuse the batch endpoint to guarantee identical logic
    batch = await analyze_batch_quotes(body)
    headers = _build_sheet_headers()

    rows: List[List[Any]] = []
    for res in batch.results:
        rows.append(_analysis_to_sheet_row(res))

    return SheetAnalysisResponse(headers=headers, rows=rows)
