# routes/advanced_analysis.py
"""
TADAWUL FAST BRIDGE – ADVANCED ANALYSIS ROUTES (v3.4.0) – PROD SAFE

Design goals
- 100% engine-driven (core.data_engine_v2.DataEngine). No direct provider calls.
- Uses core.data_engine_v2.normalize_symbol as the single normalization source.
- Google Sheets–friendly:
    • /sheet-rows never raises for normal usage (always returns headers + rows + status).
- Defensive batching:
    • chunking + timeout + bounded concurrency + placeholders on failures.
- Engine resolution:
    • prefer request.app.state.engine (created by main.py lifespan), else fallback singleton.
- Token guard via X-APP-TOKEN (APP_TOKEN / BACKUP_APP_TOKEN). If no token is set => open mode.

Notes
- This router intentionally contains its own auth + engine fallback logic to avoid circular imports.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, Query, Request
from pydantic import BaseModel, ConfigDict, Field

from core.config import get_settings
from core.data_engine_v2 import DataEngine, UnifiedQuote, normalize_symbol

# Prefer schema helper (if available)
try:
    from core.schemas import get_headers_for_sheet  # type: ignore
except Exception:  # pragma: no cover
    get_headers_for_sheet = None  # type: ignore

# EnrichedQuote is used only when returning canonical 59-column quote rows
try:
    from core.enriched_quote import EnrichedQuote  # type: ignore
except Exception:  # pragma: no cover
    EnrichedQuote = None  # type: ignore


logger = logging.getLogger("routes.advanced_analysis")

ADVANCED_ANALYSIS_VERSION = "3.4.0"
router = APIRouter(prefix="/v1/advanced", tags=["Advanced Analysis"])


# =============================================================================
# Auth (X-APP-TOKEN)
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
        logger.warning("[advanced] No APP_TOKEN configured -> endpoints are OPEN (no auth).")
    return out


def _auth_ok(x_app_token: Optional[str]) -> bool:
    allowed = _allowed_tokens()
    if not allowed:
        return True  # open mode
    return bool(x_app_token and x_app_token.strip() in allowed)


# =============================================================================
# Engine resolution (prefer app.state.engine; else module singleton)
# =============================================================================
_ENGINE: Optional[DataEngine] = None
_ENGINE_LOCK = asyncio.Lock()


def _get_app_engine(request: Request) -> Optional[DataEngine]:
    """
    Prefer engine created in main.py lifespan:
      request.app.state.engine
    Also accept common aliases to be robust.
    """
    try:
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
                logger.info("[advanced] DataEngine initialized (fallback singleton).")
            except Exception as exc:
                logger.exception("[advanced] Failed to init DataEngine singleton: %s", exc)
                _ENGINE = None
    return _ENGINE


async def _resolve_engine(request: Request) -> Optional[DataEngine]:
    eng = _get_app_engine(request)
    if eng is not None:
        return eng
    return await _get_singleton_engine()


# =============================================================================
# Config helpers
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
    """
    Router-level limits, with optional env overrides.
    """
    s = None
    try:
        s = get_settings()
    except Exception:
        s = None

    # settings (if present) else defaults
    batch_size = _safe_int(getattr(s, "adv_batch_size", None), 25)
    timeout_sec = _safe_float(getattr(s, "adv_batch_timeout_sec", None), 45.0)
    max_tickers = _safe_int(getattr(s, "adv_max_tickers", None), 500)
    concurrency = _safe_int(getattr(s, "adv_batch_concurrency", None), 6)

    # env overrides
    batch_size = _safe_int(os.getenv("ADV_BATCH_SIZE", batch_size), batch_size)
    timeout_sec = _safe_float(os.getenv("ADV_BATCH_TIMEOUT_SEC", timeout_sec), timeout_sec)
    max_tickers = _safe_int(os.getenv("ADV_MAX_TICKERS", max_tickers), max_tickers)
    concurrency = _safe_int(os.getenv("ADV_BATCH_CONCURRENCY", concurrency), concurrency)

    # sanity clamps
    batch_size = max(5, min(200, batch_size))
    timeout_sec = max(5.0, min(180.0, timeout_sec))
    max_tickers = max(10, min(2000, max_tickers))
    concurrency = max(1, min(25, concurrency))

    return {
        "batch_size": batch_size,
        "timeout_sec": timeout_sec,
        "max_tickers": max_tickers,
        "concurrency": concurrency,
    }


# =============================================================================
# Utilities
# =============================================================================
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean_tickers(items: Sequence[Any]) -> List[str]:
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


def _parse_tickers_csv(s: str) -> List[str]:
    if not s:
        return []
    parts = [p.strip() for p in s.split(",") if p.strip()]
    return _clean_tickers(parts)


def _chunk(items: List[str], size: int) -> List[List[str]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]


def _dq_score(label: Optional[str]) -> float:
    dq = (label or "").strip().upper()
    mapping = {
        "FULL": 95.0,
        "EXCELLENT": 90.0,
        "GOOD": 80.0,
        "OK": 75.0,
        "FAIR": 55.0,
        "PARTIAL": 50.0,
        "STALE": 40.0,
        "POOR": 30.0,
        "MISSING": 0.0,
    }
    return float(mapping.get(dq, 30.0))


def _risk_bucket(opportunity: Optional[float], dq_score: float) -> str:
    opp = float(opportunity or 0.0)
    conf = float(dq_score or 0.0)

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
# Engine calls (compat shim + chunking)
# =============================================================================
async def _engine_get_quotes(engine: DataEngine, syms: List[str]) -> List[UnifiedQuote]:
    """
    Compatibility shim:
    - Prefer engine.get_enriched_quotes
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
    engine: Optional[DataEngine],
    symbols: List[str],
    *,
    batch_size: int,
    timeout_sec: float,
    max_concurrency: int,
) -> Dict[str, UnifiedQuote]:
    """
    Returns map keyed by UPPERCASE symbol.
    Defensive: never raises; failures become placeholder UnifiedQuote items.
    """
    clean = _clean_tickers(symbols)
    if not clean:
        return {}

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
            logger.warning("[advanced] %s for chunk(size=%d)", msg, len(chunk_syms))
            for s in chunk_syms:
                out[s.upper()] = UnifiedQuote(symbol=s.upper(), data_quality="MISSING", error=msg).finalize()
            continue

        returned = list(res or [])
        chunk_map = {q.symbol.upper(): q for q in returned if q and getattr(q, "symbol", None)}

        for s in chunk_syms:
            k = s.upper()
            out[k] = chunk_map.get(k) or UnifiedQuote(symbol=k, data_quality="MISSING", error="No data returned").finalize()

    return out


# =============================================================================
# Response Models
# =============================================================================
class _ExtraIgnore(BaseModel):
    model_config = ConfigDict(extra="ignore")


class AdvancedItem(_ExtraIgnore):
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
    risk_score: Optional[float] = None
    overall_score: Optional[float] = None

    data_quality: Optional[str] = None
    data_quality_score: Optional[float] = None

    recommendation: Optional[str] = None
    valuation_label: Optional[str] = None
    risk_bucket: Optional[str] = None

    provider: Optional[str] = None
    as_of_utc: Optional[str] = None
    data_age_minutes: Optional[float] = None
    error: Optional[str] = None


class AdvancedScoreboardResponse(_ExtraIgnore):
    status: str = "success"
    error: Optional[str] = None

    generated_at_utc: str
    version: str
    engine_mode: str = "v2"

    total_requested: int
    total_returned: int
    top_n_applied: bool

    tickers: List[str]
    items: List[AdvancedItem]


class AdvancedSheetRequest(_ExtraIgnore):
    # Support both "tickers" and "symbols" (Apps Script clients vary)
    tickers: List[str] = Field(default_factory=list)
    symbols: List[str] = Field(default_factory=list)
    top_n: Optional[int] = Field(default=50, ge=1, le=500)
    sheet_name: Optional[str] = None


class AdvancedSheetResponse(_ExtraIgnore):
    status: str = "success"
    error: Optional[str] = None
    headers: List[str] = Field(default_factory=list)
    rows: List[List[Any]] = Field(default_factory=list)


# =============================================================================
# Transform + mapping
# =============================================================================
def _to_advanced_item(raw_symbol: str, uq: UnifiedQuote) -> AdvancedItem:
    # scoring best-effort (engine may already do it)
    try:
        from core.scoring_engine import enrich_with_scores  # type: ignore

        uq = enrich_with_scores(uq)  # type: ignore
    except Exception:
        pass

    symbol = (getattr(uq, "symbol", None) or normalize_symbol(raw_symbol) or raw_symbol or "").strip().upper()

    dq = getattr(uq, "data_quality", None)
    dq_s = _dq_score(dq)

    as_of_utc = getattr(uq, "last_updated_utc", None)
    age_min = _data_age_minutes(as_of_utc)

    opp = getattr(uq, "opportunity_score", None)
    bucket = _risk_bucket(opp, dq_s)

    return AdvancedItem(
        symbol=symbol,
        name=getattr(uq, "name", None),
        market=getattr(uq, "market", None),
        sector=getattr(uq, "sector", None),
        currency=getattr(uq, "currency", None),
        last_price=getattr(uq, "current_price", None),
        fair_value=getattr(uq, "fair_value", None),
        upside_percent=getattr(uq, "upside_percent", None),
        value_score=getattr(uq, "value_score", None),
        quality_score=getattr(uq, "quality_score", None),
        momentum_score=getattr(uq, "momentum_score", None),
        opportunity_score=opp,
        risk_score=getattr(uq, "risk_score", None),
        overall_score=getattr(uq, "overall_score", None),
        data_quality=dq,
        data_quality_score=dq_s,
        recommendation=getattr(uq, "recommendation", None),
        valuation_label=getattr(uq, "valuation_label", None),
        risk_bucket=bucket,
        provider=getattr(uq, "data_source", None),
        as_of_utc=as_of_utc,
        data_age_minutes=age_min,
        error=getattr(uq, "error", None),
    )


def _sort_key(it: AdvancedItem) -> float:
    def f(x: Any) -> float:
        try:
            return float(x or 0.0)
        except Exception:
            return 0.0

    opp = f(it.opportunity_score)
    conf = f(it.data_quality_score)
    up = f(it.upside_percent)
    ov = f(it.overall_score)

    # prioritize opportunity, then confidence, then upside, then overall
    return (opp * 1_000_000.0) + (conf * 1_000.0) + (up * 10.0) + ov


def _default_advanced_headers() -> List[str]:
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
        "Valuation Label",
        "Risk Bucket",
        "Provider",
        "As Of (UTC)",
        "Data Age (Minutes)",
        "Error",
    ]


def _select_headers(sheet_name: Optional[str]) -> Tuple[List[str], str]:
    """
    Returns (headers, mode):
      - "quote_59": return EnrichedQuote.to_row(headers) sorted by opportunity
      - "advanced": return AdvancedItem mapped rows

    Heuristic:
      - If sheet_name suggests "opportunity/advisor/advanced" => advanced mode
      - Else if schemas provides headers => quote_59 mode (canonical 59)
      - Else => advanced mode
    """
    nm = (sheet_name or "").strip().lower()
    if any(k in nm for k in ("advanced", "opportunity", "advisor", "best")):
        return _default_advanced_headers(), "advanced"

    if sheet_name and get_headers_for_sheet:
        try:
            h = get_headers_for_sheet(sheet_name)
            if isinstance(h, list) and h and any(str(x).strip().lower() == "symbol" for x in h):
                return [str(x) for x in h], "quote_59"
        except Exception:
            pass

    return _default_advanced_headers(), "advanced"


def _item_value_map(it: AdvancedItem) -> Dict[str, Any]:
    return {
        "Symbol": it.symbol,
        "Company Name": it.name or "",
        "Market": it.market or "",
        "Sector": it.sector or "",
        "Currency": it.currency or "",
        "Last Price": it.last_price,
        "Fair Value": it.fair_value,
        "Upside %": it.upside_percent,
        "Opportunity Score": it.opportunity_score,
        "Value Score": it.value_score,
        "Quality Score": it.quality_score,
        "Momentum Score": it.momentum_score,
        "Overall Score": it.overall_score,
        "Data Quality": it.data_quality or "",
        "Data Quality Score": it.data_quality_score,
        "Recommendation": it.recommendation or "",
        "Valuation Label": it.valuation_label or "",
        "Risk Bucket": it.risk_bucket or "",
        "Provider": it.provider or "",
        "As Of (UTC)": it.as_of_utc or "",
        "Data Age (Minutes)": it.data_age_minutes,
        "Error": it.error or "",
    }


def _row_for_headers(it: AdvancedItem, headers: List[str]) -> List[Any]:
    m = _item_value_map(it)
    row = [m.get(h, None) for h in headers]
    if len(row) < len(headers):
        row += [None] * (len(headers) - len(row))
    return row[: len(headers)]


def _status_from_rows(headers: List[str], rows: List[List[Any]]) -> str:
    """
    Returns 'partial' if Error column has any content, else 'success'.
    """
    try:
        err_idx = next(i for i, h in enumerate(headers) if str(h).strip().lower() == "error")
        for r in rows:
            if len(r) > err_idx and (r[err_idx] not in (None, "", "null", "None")):
                return "partial"
    except Exception:
        pass
    return "success"


# =============================================================================
# Routes
# =============================================================================
@router.get("/health")
@router.get("/ping")
async def advanced_health(request: Request) -> Dict[str, Any]:
    cfg = _cfg()
    eng = _get_app_engine(request) or await _get_singleton_engine()
    return {
        "status": "ok",
        "module": "routes.advanced_analysis",
        "version": ADVANCED_ANALYSIS_VERSION,
        "engine_mode": "v2",
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


@router.get("/scoreboard", response_model=AdvancedScoreboardResponse)
async def advanced_scoreboard(
    request: Request,
    tickers: str = Query(..., description="Comma-separated tickers e.g. 'AAPL,MSFT,1120.SR'"),
    top_n: int = Query(50, ge=1, le=500, description="Max rows returned after sorting"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> AdvancedScoreboardResponse:
    if not _auth_ok(x_app_token):
        return AdvancedScoreboardResponse(
            status="error",
            error="Unauthorized (invalid or missing X-APP-TOKEN).",
            generated_at_utc=_now_utc_iso(),
            version=ADVANCED_ANALYSIS_VERSION,
            engine_mode="v2",
            total_requested=0,
            total_returned=0,
            top_n_applied=False,
            tickers=[],
            items=[],
        )

    requested = _parse_tickers_csv(tickers)
    if not requested:
        return AdvancedScoreboardResponse(
            generated_at_utc=_now_utc_iso(),
            version=ADVANCED_ANALYSIS_VERSION,
            engine_mode="v2",
            total_requested=0,
            total_returned=0,
            top_n_applied=False,
            tickers=[],
            items=[],
        )

    cfg = _cfg()
    if len(requested) > cfg["max_tickers"]:
        requested = requested[: cfg["max_tickers"]]

    engine = await _resolve_engine(request)

    unified_map = await _get_quotes_chunked(
        engine,
        requested,
        batch_size=cfg["batch_size"],
        timeout_sec=cfg["timeout_sec"],
        max_concurrency=cfg["concurrency"],
    )

    items: List[AdvancedItem] = []
    for s in requested:
        uq = unified_map.get(s.upper()) or UnifiedQuote(symbol=s.upper(), data_quality="MISSING", error="No data returned").finalize()
        items.append(_to_advanced_item(s, uq))

    items_sorted = sorted(items, key=_sort_key, reverse=True)
    top_applied = False
    if len(items_sorted) > top_n:
        items_sorted = items_sorted[:top_n]
        top_applied = True

    return AdvancedScoreboardResponse(
        generated_at_utc=_now_utc_iso(),
        version=ADVANCED_ANALYSIS_VERSION,
        engine_mode="v2",
        total_requested=len(requested),
        total_returned=len(items_sorted),
        top_n_applied=top_applied,
        tickers=requested,
        items=items_sorted,
    )


@router.post("/sheet-rows", response_model=AdvancedSheetResponse)
async def advanced_sheet_rows(
    request: Request,
    body: AdvancedSheetRequest = Body(...),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> AdvancedSheetResponse:
    """
    Sheets-safe: ALWAYS returns {status, headers, rows, error?}.
    """
    headers, mode = _select_headers(body.sheet_name)

    # Auth is sheets-safe: never raise
    if not _auth_ok(x_app_token):
        requested = _clean_tickers((body.tickers or []) + (body.symbols or []))
        rows = []
        for s in requested:
            it = AdvancedItem(
                symbol=s.upper(),
                data_quality="MISSING",
                data_quality_score=0.0,
                risk_bucket="LOW_CONFIDENCE",
                provider="none",
                as_of_utc=_now_utc_iso(),
                error="Unauthorized (invalid or missing X-APP-TOKEN).",
            )
            rows.append(_row_for_headers(it, headers))
        return AdvancedSheetResponse(
            status="error",
            error="Unauthorized (invalid or missing X-APP-TOKEN).",
            headers=headers,
            rows=rows,
        )

    requested = _clean_tickers((body.tickers or []) + (body.symbols or []))
    if not requested:
        return AdvancedSheetResponse(status="skipped", error="No tickers provided", headers=headers, rows=[])

    cfg = _cfg()
    if len(requested) > cfg["max_tickers"]:
        requested = requested[: cfg["max_tickers"]]

    top_n = _safe_int(body.top_n or 50, 50)
    top_n = max(1, min(500, top_n))

    try:
        engine = await _resolve_engine(request)

        unified_map = await _get_quotes_chunked(
            engine,
            requested,
            batch_size=cfg["batch_size"],
            timeout_sec=cfg["timeout_sec"],
            max_concurrency=cfg["concurrency"],
        )

        # Build items + sort
        items: List[AdvancedItem] = []
        for s in requested:
            uq = unified_map.get(s.upper()) or UnifiedQuote(symbol=s.upper(), data_quality="MISSING", error="No data returned").finalize()
            items.append(_to_advanced_item(s, uq))

        items_sorted = sorted(items, key=_sort_key, reverse=True)
        if len(items_sorted) > top_n:
            items_sorted = items_sorted[:top_n]

        rows: List[List[Any]] = []

        if mode == "quote_59":
            # If EnrichedQuote is missing, fallback to advanced rows (never crash)
            if EnrichedQuote is None:
                logger.warning("[advanced] EnrichedQuote not importable -> falling back to advanced rows.")
                rows = [_row_for_headers(it, headers) for it in items_sorted]
            else:
                for it in items_sorted:
                    uq = unified_map.get(it.symbol) or UnifiedQuote(symbol=it.symbol, data_quality="MISSING", error="No data returned").finalize()
                    eq = EnrichedQuote.from_unified(uq)  # type: ignore
                    rows.append(eq.to_row(headers))  # type: ignore
        else:
            rows = [_row_for_headers(it, headers) for it in items_sorted]

        status = _status_from_rows(headers, rows)
        return AdvancedSheetResponse(status=status, headers=headers, rows=rows)

    except Exception as exc:
        logger.exception("[advanced] exception in /sheet-rows: %s", exc)

        rows: List[List[Any]] = []
        for s in requested[:top_n]:
            it = AdvancedItem(
                symbol=s.upper(),
                data_quality="MISSING",
                data_quality_score=0.0,
                risk_bucket="LOW_CONFIDENCE",
                provider="none",
                as_of_utc=_now_utc_iso(),
                error=str(exc),
            )
            rows.append(_row_for_headers(it, headers))

        return AdvancedSheetResponse(status="error", error=str(exc), headers=headers, rows=rows)


__all__ = [
    "router",
    "AdvancedItem",
    "AdvancedScoreboardResponse",
    "AdvancedSheetRequest",
    "AdvancedSheetResponse",
]
