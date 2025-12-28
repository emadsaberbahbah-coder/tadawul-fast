# routes/advanced_analysis.py  (FULL REPLACEMENT)
"""
TADAWUL FAST BRIDGE – ADVANCED ANALYSIS ROUTES (v3.7.1) – PROD SAFE (ALIGNED)

Design goals
- 100% engine-driven (prefer app.state.engine; fallback singleton).
- PROD SAFE: no hard dependency on core.data_engine_v2 at import-time (lazy + guarded).
- Google Sheets–friendly:
    • /sheet-rows never raises for normal usage (always returns headers + rows + status).
- Defensive batching:
    • chunking + timeout + bounded concurrency + placeholders on failures.
- Token guard via X-APP-TOKEN (APP_TOKEN / BACKUP_APP_TOKEN). If no token is set => open mode.

Modes for /sheet-rows:
- "advanced": returns Advanced headers (opportunity scoreboard style)
- "quote_59": returns 59-col quote rows (sorted by opportunity score), using:
    • EnrichedQuote.to_row(headers) if available and works
    • else robust header-driven mapping fallback (computed 52W position, Riyadh timestamp fill)

Compatibility:
- Engines may return list[quote] OR dict[symbol->quote] for batch calls.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from fastapi import APIRouter, Body, Header, Query, Request
from pydantic import BaseModel, ConfigDict, Field

# Settings shim (safe)
try:
    from core.config import get_settings  # type: ignore
except Exception:  # pragma: no cover
    def get_settings():  # type: ignore
        return None


# Prefer schema helper (if available)
try:
    from core.schemas import get_headers_for_sheet  # type: ignore
except Exception:  # pragma: no cover
    get_headers_for_sheet = None  # type: ignore

# EnrichedQuote is used only for canonical 59-column quote rows (optional)
try:
    from core.enriched_quote import EnrichedQuote  # type: ignore
except Exception:  # pragma: no cover
    EnrichedQuote = None  # type: ignore


logger = logging.getLogger("routes.advanced_analysis")

ADVANCED_ANALYSIS_VERSION = "3.7.1"
router = APIRouter(prefix="/v1/advanced", tags=["Advanced Analysis"])


# =============================================================================
# Lazy imports: engine + models (PROD SAFE)
# =============================================================================
def _import_v2() -> Tuple[Any, Any, Any]:
    """
    Returns (DataEngine, UnifiedQuote, normalize_symbol) or (None,None,fallback_normalize).
    """
    def _fallback_normalize(raw: str) -> str:
        s = (raw or "").strip().upper()
        if not s:
            return ""
        if s.startswith("TADAWUL:"):
            s = s.split(":", 1)[1].strip()
        if s.endswith(".TADAWUL"):
            s = s.replace(".TADAWUL", "")
        if any(ch in s for ch in ("^", "=")):
            return s
        if s.isdigit():
            return f"{s}.SR"
        if "." in s:
            return s
        return f"{s}.US"

    try:
        from core.data_engine_v2 import DataEngine as _DE  # type: ignore
        from core.data_engine_v2 import UnifiedQuote as _UQ  # type: ignore
        from core.data_engine_v2 import normalize_symbol as _NS  # type: ignore
        return _DE, _UQ, _NS
    except Exception:
        return None, None, _fallback_normalize


DataEngine, UnifiedQuote, normalize_symbol = _import_v2()


# =============================================================================
# Auth (X-APP-TOKEN)
# =============================================================================
def _read_token_attr(obj: Any, attr: str) -> Optional[str]:
    try:
        v = getattr(obj, attr, None)
        if isinstance(v, str) and v.strip():
            return v.strip()
    except Exception:
        pass
    return None


@lru_cache(maxsize=1)
def _allowed_tokens() -> List[str]:
    tokens: List[str] = []

    # 1) settings
    try:
        s = get_settings()
        for attr in ("app_token", "backup_app_token"):
            v = _read_token_attr(s, attr)
            if v:
                tokens.append(v)
    except Exception:
        pass

    # 2) env.py exports `settings` (common in your project)
    try:
        from env import settings as env_settings  # type: ignore
        for attr in ("app_token", "backup_app_token"):
            v = _read_token_attr(env_settings, attr)
            if v:
                tokens.append(v)
    except Exception:
        pass

    # 3) environment variables
    for k in ("APP_TOKEN", "BACKUP_APP_TOKEN"):
        v = (os.getenv(k) or "").strip()
        if v:
            tokens.append(v)

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
# Engine resolution (prefer app.state.engine; else singleton)
# =============================================================================
_ENGINE: Optional[Any] = None
_ENGINE_LOCK = asyncio.Lock()


def _engine_capable(obj: Any) -> bool:
    if obj is None:
        return False
    for fn in ("get_enriched_quote", "get_quote", "get_enriched_quotes", "get_quotes"):
        if callable(getattr(obj, fn, None)):
            return True
    return False


def _get_app_engine(request: Request) -> Optional[Any]:
    """
    Prefer engine created in main.py lifespan:
      request.app.state.engine
    Also accept common aliases.
    """
    try:
        st = getattr(getattr(request, "app", None), "state", None)
        if not st:
            return None
        for attr in ("engine", "data_engine", "data_engine_v2"):
            eng = getattr(st, attr, None)
            if _engine_capable(eng):
                return eng
        return None
    except Exception:
        return None


async def _get_singleton_engine() -> Optional[Any]:
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE

    async with _ENGINE_LOCK:
        if _ENGINE is None:
            try:
                DE, _, _ = _import_v2()
                if DE is None:
                    _ENGINE = None
                else:
                    _ENGINE = DE()
                    logger.info("[advanced] DataEngine initialized (fallback singleton).")
            except Exception as exc:
                logger.exception("[advanced] Failed to init DataEngine singleton: %s", exc)
                _ENGINE = None
    return _ENGINE


async def _resolve_engine(request: Request) -> Optional[Any]:
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
    s = None
    try:
        s = get_settings()
    except Exception:
        s = None

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
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _now_utc_iso() -> str:
    return _now_utc().isoformat()


def _iso_or_none(x: Any) -> Optional[str]:
    if x is None or x == "":
        return None
    try:
        if isinstance(x, datetime):
            dt = x
        else:
            dt = datetime.fromisoformat(str(x))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        try:
            return str(x)
        except Exception:
            return None


def _safe_get(obj: Any, *names: str) -> Any:
    if obj is None:
        return None
    if isinstance(obj, dict):
        for n in names:
            if n in obj and obj[n] is not None:
                return obj[n]
        return None
    for n in names:
        try:
            v = getattr(obj, n, None)
            if v is not None:
                return v
        except Exception:
            pass
    return None


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
    if not items:
        return []
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
        "ERROR": 0.0,
    }
    return float(mapping.get(dq, 30.0))


def _risk_bucket(opportunity: Optional[float], dq_score_value: float) -> str:
    opp = float(opportunity or 0.0)
    conf = float(dq_score_value or 0.0)

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
        diff = _now_utc() - ts
        return round(diff.total_seconds() / 60.0, 2)
    except Exception:
        return None


def _finalize_quote(uq: Any) -> Any:
    try:
        fn = getattr(uq, "finalize", None)
        if callable(fn):
            return fn()
    except Exception:
        pass
    return uq


def _make_placeholder(symbol: str, *, dq: str = "MISSING", err: str = "No data") -> Any:
    sym = (symbol or "").strip().upper() or "UNKNOWN"
    if UnifiedQuote is not None:
        try:
            uq = UnifiedQuote(symbol=sym, data_quality=dq, error=err, status="error")  # type: ignore
            return _finalize_quote(uq)
        except Exception:
            pass
    return {
        "symbol": sym,
        "data_quality": dq,
        "data_source": "none",
        "error": err,
        "status": "error",
        "last_updated_utc": _now_utc_iso(),
    }


def _ratio_to_percent(v: Any) -> Any:
    if v is None:
        return None
    try:
        f = float(v)
        return (f * 100.0) if -1.0 <= f <= 1.0 else f
    except Exception:
        return v


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


def _index_keys_for_quote(q: Any) -> List[str]:
    """
    Builds multiple keys to match returned objects robustly:
    symbol / symbol_normalized / normalize(symbol_input)
    """
    keys: List[str] = []
    sym = (_safe_get(q, "symbol") or "").strip().upper()
    if sym:
        keys.append(sym)

    sn = (_safe_get(q, "symbol_normalized") or "").strip().upper()
    if sn:
        keys.append(sn)

    si = (_safe_get(q, "symbol_input") or "").strip()
    if si:
        try:
            keys.append(normalize_symbol(si).upper())
        except Exception:
            keys.append(si.strip().upper())

    out: List[str] = []
    seen = set()
    for k in keys:
        if k and k not in seen:
            seen.add(k)
            out.append(k)
    return out


# =============================================================================
# Engine calls (compat shim + chunking)
# =============================================================================
async def _maybe_await(x: Any) -> Any:
    if asyncio.iscoroutine(x):
        return await x
    return x


async def _engine_get_quotes(engine: Any, syms: List[str]) -> List[Any]:
    """
    - Prefer engine.get_enriched_quotes
    - Else engine.get_quotes
    - Else per-symbol methods
    Accept list OR dict returns.
    """
    fn = getattr(engine, "get_enriched_quotes", None)
    if callable(fn):
        res = await _maybe_await(fn(syms))
        if isinstance(res, dict):
            return list(res.values())
        return list(res or [])

    fn2 = getattr(engine, "get_quotes", None)
    if callable(fn2):
        res = await _maybe_await(fn2(syms))
        if isinstance(res, dict):
            return list(res.values())
        return list(res or [])

    out: List[Any] = []
    for s in syms:
        fn3 = getattr(engine, "get_enriched_quote", None)
        if callable(fn3):
            out.append(await _maybe_await(fn3(s)))
            continue
        fn4 = getattr(engine, "get_quote", None)
        if callable(fn4):
            out.append(await _maybe_await(fn4(s)))
            continue
        out.append(_make_placeholder(s, dq="MISSING", err="Engine missing quote methods"))
    return out


async def _get_quotes_chunked(
    engine: Optional[Any],
    symbols: List[str],
    *,
    batch_size: int,
    timeout_sec: float,
    max_concurrency: int,
) -> Dict[str, Any]:
    clean = _clean_tickers(symbols)
    if not clean:
        return {}

    if engine is None:
        return {s: _make_placeholder(s, dq="MISSING", err="Engine unavailable") for s in clean}

    chunks = _chunk(clean, batch_size)
    sem = asyncio.Semaphore(max(1, max_concurrency))

    async def _run_chunk(chunk_syms: List[str]) -> Tuple[List[str], Union[List[Any], Exception]]:
        async with sem:
            try:
                res = await asyncio.wait_for(_engine_get_quotes(engine, chunk_syms), timeout=timeout_sec)
                return chunk_syms, res
            except Exception as e:
                return chunk_syms, e

    results = await asyncio.gather(*[_run_chunk(c) for c in chunks])

    out: Dict[str, Any] = {}
    for chunk_syms, res in results:
        if isinstance(res, Exception):
            msg = "Engine batch timeout" if isinstance(res, asyncio.TimeoutError) else f"Engine batch error: {res}"
            logger.warning("[advanced] %s for chunk(size=%d)", msg, len(chunk_syms))
            for s in chunk_syms:
                out[s.upper()] = _make_placeholder(s, dq="MISSING", err=msg)
            continue

        returned = list(res or [])
        chunk_map: Dict[str, Any] = {}

        for q in returned:
            q2 = _finalize_quote(q)
            for k in _index_keys_for_quote(q2):
                chunk_map.setdefault(k, q2)

        for s in chunk_syms:
            k = s.upper()
            out[k] = chunk_map.get(k) or _make_placeholder(k, dq="MISSING", err="No data returned")

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
# Transform
# =============================================================================
def _to_advanced_item(raw_symbol: str, uq: Any) -> AdvancedItem:
    # scoring best-effort (engine may already do it)
    try:
        from core.scoring_engine import enrich_with_scores  # type: ignore
        uq = enrich_with_scores(uq)  # type: ignore
    except Exception:
        pass

    symbol = (_safe_get(uq, "symbol") or normalize_symbol(raw_symbol) or raw_symbol or "").strip().upper()
    dq = _safe_get(uq, "data_quality")
    dq_s = _dq_score(dq)

    as_of = _safe_get(uq, "last_updated_utc", "as_of_utc")
    as_of_iso = _iso_or_none(as_of)
    age_min = _data_age_minutes(as_of)

    opp = _safe_get(uq, "opportunity_score")
    bucket = _risk_bucket(opp, dq_s)

    last_price = _safe_get(uq, "current_price", "last_price", "price")

    return AdvancedItem(
        symbol=symbol,
        name=_safe_get(uq, "name", "company_name"),
        market=_safe_get(uq, "market", "market_region"),
        sector=_safe_get(uq, "sector"),
        currency=_safe_get(uq, "currency"),
        last_price=last_price,
        fair_value=_safe_get(uq, "fair_value"),
        upside_percent=_safe_get(uq, "upside_percent"),
        value_score=_safe_get(uq, "value_score"),
        quality_score=_safe_get(uq, "quality_score"),
        momentum_score=_safe_get(uq, "momentum_score"),
        opportunity_score=opp,
        risk_score=_safe_get(uq, "risk_score"),
        overall_score=_safe_get(uq, "overall_score"),
        data_quality=dq,
        data_quality_score=dq_s,
        recommendation=_safe_get(uq, "recommendation"),
        valuation_label=_safe_get(uq, "valuation_label"),
        risk_bucket=bucket,
        provider=_safe_get(uq, "data_source", "provider", "source"),
        as_of_utc=as_of_iso,
        data_age_minutes=age_min,
        error=_safe_get(uq, "error"),
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

    return (opp * 1_000_000.0) + (conf * 1_000.0) + (up * 10.0) + ov


# =============================================================================
# Headers / modes
# =============================================================================
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
    nm = (sheet_name or "").strip().lower()

    # explicit advanced-ish keywords -> advanced scoreboard
    if any(k in nm for k in ("advanced", "opportunity", "advisor", "best")):
        return _default_advanced_headers(), "advanced"

    # if schemas provides a header list (your 59-col sheet layout), use it
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
    try:
        err_idx = next(i for i, h in enumerate(headers) if str(h).strip().lower() == "error")
        for r in rows:
            if len(r) > err_idx and (r[err_idx] not in (None, "", "null", "None")):
                return "partial"
    except Exception:
        pass
    return "success"


# =============================================================================
# Quote-59 fallback mapping (when EnrichedQuote is missing or fails)
# =============================================================================
def _hkey(h: str) -> str:
    s = str(h or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def _snake_guess(header: str) -> str:
    s = str(header or "").strip().lower()
    s = s.replace("%", " percent ")
    s = re.sub(r"[^\w]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def _value_for_header(header: str, uq: Any) -> Any:
    hk = _hkey(header)

    # computed 52w position
    if hk in ("52w position %", "52w position"):
        v = _safe_get(uq, "position_52w_percent", "position_52w")
        if v is not None:
            return v
        cp = _safe_get(uq, "current_price", "last_price", "price")
        lo = _safe_get(uq, "low_52w")
        hi = _safe_get(uq, "high_52w")
        return _compute_52w_position_pct(cp, lo, hi)

    # ensure Riyadh timestamp if sheet asks
    if hk == "last updated (riyadh)":
        v = _safe_get(uq, "last_updated_riyadh")
        if not v:
            last_utc = _safe_get(uq, "last_updated_utc", "as_of_utc")
            v = _to_riyadh_iso(last_utc)
            try:
                if isinstance(uq, dict):
                    uq["last_updated_riyadh"] = v
                else:
                    setattr(uq, "last_updated_riyadh", v)
            except Exception:
                pass
        return _iso_or_none(v) or v

    # percent normalization for common percent headers
    if hk in ("dividend yield %", "roe %", "roa %", "turnover %", "free float %", "volatility (30d)"):
        guess = _snake_guess(header)
        return _ratio_to_percent(_safe_get(uq, guess, guess.replace("_percent", "")))

    # direct snake guess
    guess = _snake_guess(header)
    v = _safe_get(uq, guess)
    if hk == "last updated (utc)":
        return _iso_or_none(v) or v
    return v


def _row_59_from_headers(headers: List[str], uq: Any) -> List[Any]:
    row = [_value_for_header(h, uq) for h in headers]
    if len(row) < len(headers):
        row += [None] * (len(headers) - len(row))
    return row[: len(headers)]


# =============================================================================
# Routes
# =============================================================================
@router.get("/health")
@router.get("/ping")
async def advanced_health(request: Request) -> Dict[str, Any]:
    cfg = _cfg()
    eng = await _resolve_engine(request)

    providers: List[str] = []
    try:
        providers = list(getattr(eng, "enabled_providers", []) or []) if eng else []
    except Exception:
        providers = []

    return {
        "status": "ok",
        "module": "routes.advanced_analysis",
        "version": ADVANCED_ANALYSIS_VERSION,
        "engine_mode": "v2",
        "providers": providers,
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
        uq = unified_map.get(s.upper()) or _make_placeholder(s, dq="MISSING", err="No data returned")
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

    requested = _clean_tickers((body.tickers or []) + (body.symbols or []))
    top_n = _safe_int(body.top_n or 50, 50)
    top_n = max(1, min(500, top_n))

    # Auth is sheets-safe: never raise
    if not _auth_ok(x_app_token):
        rows: List[List[Any]] = []
        for s in requested[:top_n]:
            if mode == "advanced":
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
            else:
                rows.append(_row_59_from_headers(headers, _make_placeholder(s, err="Unauthorized")))
        return AdvancedSheetResponse(
            status="error",
            error="Unauthorized (invalid or missing X-APP-TOKEN).",
            headers=headers,
            rows=rows,
        )

    if not requested:
        return AdvancedSheetResponse(status="skipped", error="No tickers provided", headers=headers, rows=[])

    cfg = _cfg()
    if len(requested) > cfg["max_tickers"]:
        requested = requested[: cfg["max_tickers"]]

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
            uq = unified_map.get(s.upper()) or _make_placeholder(s, dq="MISSING", err="No data returned")
            items.append(_to_advanced_item(s, uq))

        items_sorted = sorted(items, key=_sort_key, reverse=True)
        if len(items_sorted) > top_n:
            items_sorted = items_sorted[:top_n]

        rows: List[List[Any]] = []

        if mode == "quote_59":
            # Return quote rows sorted by opportunity
            for it in items_sorted:
                uq = unified_map.get(it.symbol) or _make_placeholder(it.symbol, dq="MISSING", err="No data returned")

                if EnrichedQuote is not None:
                    try:
                        eq = EnrichedQuote.from_unified(uq)  # type: ignore
                        row = eq.to_row(headers)  # type: ignore
                        if not isinstance(row, list):
                            raise ValueError("EnrichedQuote.to_row did not return a list")
                        if len(row) < len(headers):
                            row += [None] * (len(headers) - len(row))
                        rows.append(row[: len(headers)])
                        continue
                    except Exception as exc:
                        # fallback mapping
                        try:
                            if isinstance(uq, dict):
                                uq["error"] = f"Row mapping failed: {exc}"
                            else:
                                setattr(uq, "error", f"Row mapping failed: {exc}")
                        except Exception:
                            pass
                        rows.append(_row_59_from_headers(headers, uq))
                        continue

                # If EnrichedQuote missing: header-driven fallback
                rows.append(_row_59_from_headers(headers, uq))

        else:
            # advanced headers
            rows = [_row_for_headers(it, headers) for it in items_sorted]

        status = _status_from_rows(headers, rows)
        return AdvancedSheetResponse(status=status, headers=headers, rows=rows)

    except Exception as exc:
        logger.exception("[advanced] exception in /sheet-rows: %s", exc)

        rows: List[List[Any]] = []
        for s in requested[:top_n]:
            if mode == "advanced":
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
            else:
                rows.append(_row_59_from_headers(headers, _make_placeholder(s, err=str(exc))))

        return AdvancedSheetResponse(status="error", error=str(exc), headers=headers, rows=rows)


__all__ = [
    "router",
    "AdvancedItem",
    "AdvancedScoreboardResponse",
    "AdvancedSheetRequest",
    "AdvancedSheetResponse",
]
