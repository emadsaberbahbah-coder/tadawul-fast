# routes/ai_analysis.py
"""
TADAWUL FAST BRIDGE — AI ANALYSIS ROUTES (v6.2.0) — PROD SAFE (ENHANCED)
------------------------------------------------------------

Provides
- GET  /v1/analysis/health
- GET  /v1/analysis/quote?symbol=...
- POST /v1/analysis/quote              (single JSON: {symbol} OR {ticker})
- GET  /v1/analysis/quotes?tickers=...  (batch via query string)
- POST /v1/analysis/quotes             (batch JSON)
- POST /v1/analysis/sheet-rows         (dual mode: PUSH cache OR COMPUTE grid)
- GET  /v1/analysis/scoreboard         (quick ranking table)

Key guarantees (Sheets + Backend safety)
- ✅ Canonical ROI keys:
    expected_roi_1m / expected_roi_3m / expected_roi_12m
- ✅ Canonical forecast keys:
    forecast_price_1m / forecast_price_3m / forecast_price_12m
- ✅ Canonical timestamps (UTC + Riyadh):
    last_updated_utc / last_updated_riyadh
    forecast_updated_utc / forecast_updated_riyadh
- ✅ Robust input parsing: tickers may arrive as list, string, or nested dict payloads
- ✅ Engine-aware: uses request.app.state.engine if present; else get_engine fallback
- ✅ Method probing: supports multiple engine versions (single + batch)
- ✅ Chunked + concurrency-limited + timeout protected
- ✅ Push-mode supports sync/async snapshot setters (multiple method names, optional meta)
- ✅ Always returns stable non-empty headers for Sheets writers
- ✅ Optional auth guard (X-APP-TOKEN / Bearer / ?token when ALLOW_QUERY_TOKEN=1)
- ✅ Output rows are always padded/truncated to headers length

Environment
- AI_BATCH_SIZE (default 25)
- AI_BATCH_TIMEOUT_SEC (default 60)
- AI_BATCH_CONCURRENCY (default 6)
- AI_MAX_TICKERS (default 1200)
- AI_ROUTE_TIMEOUT_SEC (default 120)  # total soft deadline for heavy endpoints
- ALLOW_QUERY_TOKEN=1 to allow query token
- APP_TOKEN / BACKUP_APP_TOKEN / TFB_APP_TOKEN for auth (if set, auth is enforced)

Notes
- No heavy work at import time. Engines/providers are resolved lazily.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import time
from dataclasses import is_dataclass
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from fastapi import APIRouter, Body, Header, Query, Request

# Pydantic v2 preferred, v1 fallback
try:
    from pydantic import BaseModel, ConfigDict, Field  # type: ignore

    _PYDANTIC_V2 = True
except Exception:  # pragma: no cover
    from pydantic import BaseModel, Field  # type: ignore

    ConfigDict = None  # type: ignore
    _PYDANTIC_V2 = False

logger = logging.getLogger("routes.ai_analysis")

AI_ANALYSIS_VERSION = "6.2.0"
router = APIRouter(prefix="/v1/analysis", tags=["AI & Analysis"])


# =============================================================================
# Small env helpers
# =============================================================================
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disable", "disabled"}


def _env_str(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _env_bool(name: str, default: bool = False) -> bool:
    v = _env_str(name, "")
    if not v:
        return default
    vv = v.lower()
    if vv in _TRUTHY:
        return True
    if vv in _FALSY:
        return False
    return default


def _env_int(name: str, default: int, *, lo: int = -10**9, hi: int = 10**9) -> int:
    v = _env_str(name, "")
    if not v:
        return int(default)
    try:
        x = int(float(v))
        return max(lo, min(hi, x))
    except Exception:
        return int(default)


def _env_float(name: str, default: float, *, lo: float = -1e18, hi: float = 1e18) -> float:
    v = _env_str(name, "")
    if not v:
        return float(default)
    try:
        x = float(v)
        return max(lo, min(hi, x))
    except Exception:
        return float(default)


def _cfg() -> Dict[str, Any]:
    batch_size = _env_int("AI_BATCH_SIZE", 25, lo=5, hi=250)
    timeout = _env_float("AI_BATCH_TIMEOUT_SEC", 60.0, lo=5.0, hi=240.0)
    concurrency = _env_int("AI_BATCH_CONCURRENCY", 6, lo=1, hi=40)
    max_tickers = _env_int("AI_MAX_TICKERS", 1200, lo=10, hi=5000)
    route_timeout = _env_float("AI_ROUTE_TIMEOUT_SEC", 120.0, lo=10.0, hi=300.0)

    return {
        "batch_size": batch_size,
        "timeout_sec": timeout,
        "concurrency": concurrency,
        "max_tickers": max_tickers,
        "route_timeout_sec": route_timeout,
    }


# =============================================================================
# Time helpers (Riyadh via ZoneInfo preferred)
# =============================================================================
@lru_cache(maxsize=1)
def _riyadh_tz():
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo("Asia/Riyadh")
    except Exception:
        # fallback fixed offset (+03:00)
        from datetime import timedelta

        return timezone(timedelta(hours=3))


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _riyadh_iso() -> str:
    return datetime.now(_riyadh_tz()).isoformat(timespec="seconds")


def _to_riyadh_iso(utc_iso: Optional[str]) -> str:
    if not utc_iso:
        return ""
    try:
        s = str(utc_iso).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(_riyadh_tz()).isoformat(timespec="seconds")
    except Exception:
        return ""


# =============================================================================
# Auth (optional)
# =============================================================================
_ALLOW_QUERY_TOKEN = _env_bool("ALLOW_QUERY_TOKEN", False)


@lru_cache(maxsize=1)
def _allowed_tokens() -> List[str]:
    toks: List[str] = []
    for k in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
        v = _env_str(k, "")
        if v:
            toks.append(v)
    out: List[str] = []
    seen = set()
    for t in toks:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _extract_bearer(authorization: Optional[str]) -> str:
    a = (authorization or "").strip()
    if not a:
        return ""
    parts = a.split()
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1].strip()
    return ""


def _auth_ok(x_app_token: Optional[str], authorization: Optional[str], query_token: Optional[str]) -> bool:
    allowed = _allowed_tokens()
    if not allowed:
        return True  # open mode

    tok = (x_app_token or "").strip()
    if tok and tok in allowed:
        return True

    btok = _extract_bearer(authorization)
    if btok and btok in allowed:
        return True

    if _ALLOW_QUERY_TOKEN:
        qtok = (query_token or "").strip()
        if qtok and qtok in allowed:
            return True

    return False


# =============================================================================
# Lazy imports (safe)
# =============================================================================
@lru_cache(maxsize=1)
def _try_import_scoring_engine():
    try:
        from core.scoring_engine import enrich_with_scores  # type: ignore

        return enrich_with_scores
    except Exception:
        return None


@lru_cache(maxsize=1)
def _try_import_enriched_quote():
    try:
        from core.enriched_quote import EnrichedQuote  # type: ignore

        return EnrichedQuote
    except Exception:
        return None


@lru_cache(maxsize=1)
def _try_import_schemas():
    try:
        import core.schemas as schemas  # type: ignore

        return schemas
    except Exception:
        return None


@lru_cache(maxsize=1)
def _try_import_normalizer():
    try:
        from core.symbols.normalize import normalize_symbol  # type: ignore

        if callable(normalize_symbol):
            return normalize_symbol
    except Exception:
        pass

    def _fallback(raw: Any) -> str:
        s = ("" if raw is None else str(raw)).strip().upper()
        if not s:
            return ""
        s = s.replace(" ", "")
        if s.startswith("TADAWUL:"):
            s = s.split(":", 1)[1].strip()
        if s.endswith(".TADAWUL"):
            s = s[: -len(".TADAWUL")].strip()
        if s.endswith(".SA"):
            s = s[:-3] + ".SR"
        if s.isdigit() and 3 <= len(s) <= 6:
            return f"{s}.SR"
        return s

    return _fallback


def _enrich_scores_best_effort(uq: Any) -> Any:
    enricher = _try_import_scoring_engine()
    if callable(enricher):
        try:
            return enricher(uq)
        except Exception:
            return uq
    return uq


# =============================================================================
# Models
# =============================================================================
class _ExtraIgnoreBase(BaseModel):
    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")
    else:  # pragma: no cover
        class Config:  # type: ignore
            extra = "ignore"


class SingleAnalysisResponse(_ExtraIgnoreBase):
    symbol: str
    name: Optional[str] = None
    market_region: Optional[str] = None
    price: Optional[float] = None
    change_pct: Optional[float] = None

    market_cap: Optional[float] = None
    pe_ttm: Optional[float] = None
    dividend_yield: Optional[float] = None

    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    risk_score: Optional[float] = None
    overall_score: Optional[float] = None
    recommendation: Optional[str] = "HOLD"
    rec_badge: Optional[str] = None

    expected_roi_1m: Optional[float] = None
    expected_roi_3m: Optional[float] = None
    expected_roi_12m: Optional[float] = None

    forecast_price_1m: Optional[float] = None
    forecast_price_3m: Optional[float] = None
    forecast_price_12m: Optional[float] = None

    forecast_confidence: Optional[float] = None
    forecast_updated_utc: Optional[str] = None
    forecast_updated_riyadh: Optional[str] = None

    data_quality: str = "MISSING"
    last_updated_utc: Optional[str] = None
    last_updated_riyadh: Optional[str] = None
    error: Optional[str] = None


class BatchAnalysisRequest(_ExtraIgnoreBase):
    tickers: Any = Field(default_factory=list)
    symbols: Any = Field(default_factory=list)
    sheet_name: Optional[str] = None
    top_n: Optional[int] = None


class BatchAnalysisResponse(_ExtraIgnoreBase):
    results: List[SingleAnalysisResponse] = Field(default_factory=list)
    status: str = "success"
    error: Optional[str] = None
    version: str = AI_ANALYSIS_VERSION
    meta: Dict[str, Any] = Field(default_factory=dict)


class PushSheetItem(_ExtraIgnoreBase):
    sheet: str
    headers: List[str] = Field(default_factory=list)
    rows: List[List[Any]] = Field(default_factory=list)


class PushSheetRowsRequest(_ExtraIgnoreBase):
    items: List[PushSheetItem] = Field(default_factory=list)
    universe_rows: Optional[int] = None
    source: Optional[str] = None


class SheetAnalysisResponse(_ExtraIgnoreBase):
    headers: List[str] = Field(default_factory=list)
    rows: List[List[Any]] = Field(default_factory=list)
    status: str = "success"
    error: Optional[str] = None
    version: str = AI_ANALYSIS_VERSION
    meta: Dict[str, Any] = Field(default_factory=dict)


# =============================================================================
# Engine & async helpers
# =============================================================================
async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


async def _resolve_engine(request: Request) -> Any:
    # 1) app.state.engine
    try:
        st = getattr(request.app, "state", None)
        if st is not None:
            eng = getattr(st, "engine", None)
            if eng is not None:
                return eng
    except Exception:
        pass

    # 2) get_engine fallback
    try:
        from core.data_engine_v2 import get_engine  # type: ignore

        return await _maybe_await(get_engine())
    except Exception:
        return None


async def _wait_for(coro, timeout: float):
    return await asyncio.wait_for(coro, timeout=timeout)


# =============================================================================
# Input normalization
# =============================================================================
def _split_symbols(s: str) -> List[str]:
    raw = (s or "").replace(",", " ").replace("|", " ").replace(";", " ")
    return [p.strip() for p in raw.split() if p.strip()]


def _extract_symbols_from_any(obj: Any) -> List[str]:
    """
    Accepts:
    - list[str]
    - str
    - dict with keys: tickers/symbols/all_tickers/ksa_tickers/global_tickers
    - pydantic-like objects
    """
    if obj is None:
        return []

    if isinstance(obj, str):
        return _split_symbols(obj)

    if isinstance(obj, (list, tuple)):
        return [str(x) for x in obj if str(x or "").strip()]

    if isinstance(obj, dict):
        acc: List[str] = []
        for k in ("tickers", "symbols", "all_tickers", "ksa_tickers", "global_tickers"):
            v = obj.get(k)
            if isinstance(v, str):
                acc.extend(_split_symbols(v))
            elif isinstance(v, (list, tuple)):
                acc.extend([str(x) for x in v if str(x or "").strip()])
        return acc

    try:
        md = getattr(obj, "model_dump", None)
        if callable(md):
            return _extract_symbols_from_any(md())
        dd = getattr(obj, "dict", None)
        if callable(dd):
            return _extract_symbols_from_any(dd())
    except Exception:
        pass

    return []


def _normalize_symbols(raw: Sequence[Any]) -> List[str]:
    norm = _try_import_normalizer()
    seen = set()
    out: List[str] = []
    for x in raw or []:
        s0 = str(x or "").strip()
        if not s0:
            continue
        try:
            s = (norm(s0) or "").strip().upper()
        except Exception:
            s = s0.strip().upper()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _clean_tickers_from_request(req: BatchAnalysisRequest, *, body: Optional[Dict[str, Any]] = None) -> List[str]:
    raw: List[str] = []
    raw.extend(_extract_symbols_from_any(getattr(req, "tickers", None)))
    raw.extend(_extract_symbols_from_any(getattr(req, "symbols", None)))
    if body:
        raw.extend(_extract_symbols_from_any(body))
    return _normalize_symbols(raw)


# =============================================================================
# Quote shaping (placeholders + canonicalization)
# =============================================================================
def _coerce_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        s = str(x).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def _make_placeholder(symbol: str, dq: str = "MISSING", err: str = "No Data") -> Dict[str, Any]:
    utc = _now_utc_iso()
    sym = str(symbol or "").strip().upper() or "UNKNOWN"
    return {
        "symbol": sym,
        "symbol_normalized": sym,
        "name": None,
        "market_region": None,
        "price": None,
        "change_pct": None,
        "recommendation": "HOLD",
        "data_quality": dq,
        "error": err,
        "last_updated_utc": utc,
        "last_updated_riyadh": _to_riyadh_iso(utc),
        "forecast_updated_utc": None,
        "forecast_updated_riyadh": None,
    }


def _canonicalize_quote(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Bring various provider keys into canonical keys used by Sheets.
    Never deletes values, only fills missing.
    """
    if not isinstance(d, dict):
        return {}

    out = dict(d)

    # Identity
    if out.get("symbol") is None:
        out["symbol"] = out.get("ticker") or out.get("symbol_normalized")

    # Price
    if out.get("price") is None:
        out["price"] = out.get("current_price") or out.get("last_price")

    # Change %
    if out.get("change_pct") is None:
        out["change_pct"] = out.get("percent_change") or out.get("change_percent")

    # ROI aliases (wide net)
    roi_aliases = {
        "expected_roi_1m": [
            "expected_roi_1m",
            "expected_roi_pct_1m",
            "expected_roi_percent_1m",
            "expectedroi1m",
            "expected_roi_30d",
            "expected_roi_pct_30d",
            "roi_1m",
            "roi1m",
        ],
        "expected_roi_3m": [
            "expected_roi_3m",
            "expected_roi_pct_3m",
            "expected_roi_percent_3m",
            "expectedroi3m",
            "expected_roi_90d",
            "expected_roi_pct_90d",
            "roi_3m",
            "roi3m",
        ],
        "expected_roi_12m": [
            "expected_roi_12m",
            "expected_roi_pct_12m",
            "expected_roi_percent_12m",
            "expectedroi12m",
            "expected_roi_1y",
            "expected_roi_pct_1y",
            "roi_12m",
            "roi12m",
        ],
    }
    for canon, keys in roi_aliases.items():
        if out.get(canon) is None:
            for k in keys:
                if k in out and out.get(k) not in (None, ""):
                    out[canon] = out.get(k)
                    break

    # Forecast price aliases
    px_aliases = {
        "forecast_price_1m": ["forecast_price_1m", "forecast_1m", "forecastprice1m", "target_price_1m", "target1m"],
        "forecast_price_3m": ["forecast_price_3m", "forecast_3m", "forecastprice3m", "target_price_3m", "target3m"],
        "forecast_price_12m": ["forecast_price_12m", "forecast_12m", "forecastprice12m", "target_price_12m", "target12m"],
    }
    for canon, keys in px_aliases.items():
        if out.get(canon) is None:
            for k in keys:
                if k in out and out.get(k) not in (None, ""):
                    out[canon] = out.get(k)
                    break

    # Forecast updated timestamps
    if out.get("forecast_updated_utc") is None:
        out["forecast_updated_utc"] = out.get("forecast_updated") or out.get("forecast_ts_utc")

    if out.get("forecast_updated_riyadh") is None:
        out["forecast_updated_riyadh"] = _to_riyadh_iso(out.get("forecast_updated_utc"))

    # last_updated timestamps
    if out.get("last_updated_utc") is None:
        out["last_updated_utc"] = out.get("updated_at") or out.get("last_updated") or _now_utc_iso()

    if out.get("last_updated_riyadh") is None:
        out["last_updated_riyadh"] = _to_riyadh_iso(out.get("last_updated_utc"))

    # Normalize recommendation
    if not out.get("recommendation"):
        out["recommendation"] = "HOLD"

    return out


def _quote_to_response(q: Any) -> SingleAnalysisResponse:
    d = q if isinstance(q, dict) else (q.__dict__ if hasattr(q, "__dict__") else {})
    if not isinstance(d, dict):
        d = {}

    d = _enrich_scores_best_effort(d)
    d = _canonicalize_quote(d)

    sym = str(d.get("symbol") or d.get("symbol_normalized") or "UNKNOWN").strip().upper()

    last_utc = d.get("last_updated_utc") or _now_utc_iso()
    last_riy = d.get("last_updated_riyadh") or _to_riyadh_iso(last_utc)

    f_utc = d.get("forecast_updated_utc")
    f_riy = d.get("forecast_updated_riyadh") or (_to_riyadh_iso(f_utc) if f_utc else None)

    return SingleAnalysisResponse(
        symbol=sym,
        name=d.get("name"),
        market_region=d.get("market_region") or d.get("market"),
        price=_coerce_float(d.get("price")),
        change_pct=_coerce_float(d.get("change_pct")),
        market_cap=_coerce_float(d.get("market_cap")),
        pe_ttm=_coerce_float(d.get("pe_ttm")),
        dividend_yield=_coerce_float(d.get("dividend_yield")),
        value_score=_coerce_float(d.get("value_score")),
        quality_score=_coerce_float(d.get("quality_score")),
        momentum_score=_coerce_float(d.get("momentum_score")),
        risk_score=_coerce_float(d.get("risk_score")),
        overall_score=_coerce_float(d.get("overall_score")),
        recommendation=(d.get("recommendation") or "HOLD"),
        rec_badge=d.get("rec_badge") or d.get("recBadge"),
        expected_roi_1m=_coerce_float(d.get("expected_roi_1m")),
        expected_roi_3m=_coerce_float(d.get("expected_roi_3m")),
        expected_roi_12m=_coerce_float(d.get("expected_roi_12m")),
        forecast_price_1m=_coerce_float(d.get("forecast_price_1m")),
        forecast_price_3m=_coerce_float(d.get("forecast_price_3m")),
        forecast_price_12m=_coerce_float(d.get("forecast_price_12m")),
        forecast_confidence=_coerce_float(d.get("forecast_confidence")),
        forecast_updated_utc=f_utc,
        forecast_updated_riyadh=f_riy,
        data_quality=str(d.get("data_quality") or "OK"),
        last_updated_utc=last_utc,
        last_updated_riyadh=last_riy,
        error=d.get("error"),
    )


# =============================================================================
# Engine method probing (single + batch)
# =============================================================================
async def _engine_get_one(engine: Any, symbol: str) -> Any:
    if not engine:
        return _make_placeholder(symbol, err="No Engine")

    for name in (
        "get_enriched_quote",
        "fetch_enriched_quote",
        "get_quote_unified",
        "get_unified_quote",
        "get_quote",
        "fetch_quote",
    ):
        fn = getattr(engine, name, None)
        if callable(fn):
            try:
                return await _maybe_await(fn(symbol))
            except Exception:
                raise

    raise RuntimeError("Engine has no supported single-quote method")


async def _engine_get_batch(engine: Any, symbols: List[str]) -> Optional[Any]:
    if not engine:
        return None

    for name in (
        "get_enriched_quotes_batch",
        "get_enriched_quotes",
        "get_quotes_batch",
        "fetch_quotes_batch",
        "get_unified_quotes",
    ):
        fn = getattr(engine, name, None)
        if callable(fn):
            try:
                return await _maybe_await(fn(symbols))
            except Exception:
                return None
    return None


def _align_batch_result(symbols: List[str], out: Any) -> Dict[str, Any]:
    """
    Normalize batch output into {SYMBOL: quote_obj_or_dict}.
    Supports dict map OR list of dict/objects that include symbol fields.
    """
    res: Dict[str, Any] = {}

    if isinstance(out, dict):
        for k, v in out.items():
            kk = str(k or "").strip().upper()
            if kk:
                res[kk] = v

    elif isinstance(out, list):
        for item in out:
            if isinstance(item, dict):
                sym = str(item.get("symbol") or item.get("symbol_normalized") or item.get("ticker") or "").strip().upper()
                if sym:
                    res[sym] = item
            else:
                sym = ""
                try:
                    sym = str(getattr(item, "symbol", "") or getattr(item, "symbol_normalized", "") or "").strip().upper()
                except Exception:
                    sym = ""
                if sym:
                    res[sym] = item

    # ensure every requested symbol exists
    for s in symbols:
        ss = str(s).strip().upper()
        if ss and ss not in res:
            res[ss] = _make_placeholder(ss, err="Missing from engine batch response")

    return res


# =============================================================================
# Batch execution (chunked, concurrency-limited, timeout-protected)
# =============================================================================
async def _get_quotes_chunked(engine: Any, symbols: List[str], cfg: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Returns (quotes_map, stats)
    quotes_map ensures every requested symbol exists.
    """
    stats: Dict[str, Any] = {
        "requested": len(symbols),
        "chunks": 0,
        "timeouts": 0,
        "chunk_failures": 0,
        "single_failures": 0,
        "used_batch_api": False,
    }

    if not symbols:
        return {}, stats

    if not engine:
        return {s: _make_placeholder(s, err="No Engine") for s in symbols}, stats

    batch_size = int(cfg["batch_size"])
    concurrency = int(cfg["concurrency"])
    timeout_sec = float(cfg["timeout_sec"])

    chunks = [symbols[i : i + batch_size] for i in range(0, len(symbols), batch_size)]
    stats["chunks"] = len(chunks)

    sem = asyncio.Semaphore(concurrency)
    results: Dict[str, Any] = {}

    async def _run_chunk(chunk_syms: List[str]) -> None:
        nonlocal results
        async with sem:
            # 1) try engine batch method for the chunk
            try:
                out = await _wait_for(_engine_get_batch(engine, chunk_syms), timeout=timeout_sec)
                if out is not None:
                    stats["used_batch_api"] = True
                    aligned = _align_batch_result(chunk_syms, out)
                    results.update(aligned)
                    return
            except asyncio.TimeoutError:
                stats["timeouts"] += 1
            except Exception:
                pass

            # 2) fallback: per-symbol single fetch with same timeout
            for s in chunk_syms:
                try:
                    q = await _wait_for(_engine_get_one(engine, s), timeout=timeout_sec)
                    results[str(s).upper()] = q
                except asyncio.TimeoutError:
                    stats["single_failures"] += 1
                    results[str(s).upper()] = _make_placeholder(s, err=f"Timeout>{timeout_sec}s")
                except Exception as e:
                    stats["single_failures"] += 1
                    results[str(s).upper()] = _make_placeholder(s, err=str(e))

    await asyncio.gather(*[_run_chunk(c) for c in chunks])

    # Ensure all requested symbols exist
    for s in symbols:
        key = str(s).strip().upper()
        if key and key not in results:
            results[key] = _make_placeholder(key, err="Missing after processing")

    return results, stats


# =============================================================================
# Sheet helpers (headers + row mapping)
# =============================================================================
def _fallback_sheet_headers(sheet_name: str) -> List[str]:
    return [
        "Symbol",
        "Name",
        "Price",
        "Change %",
        "Overall Score",
        "Recommendation",
        "Rec Badge",
        "Expected ROI % (1M)",
        "Forecast Price (1M)",
        "Expected ROI % (3M)",
        "Forecast Price (3M)",
        "Expected ROI % (12M)",
        "Forecast Price (12M)",
        "Forecast Confidence",
        "Data Quality",
        "Error",
        "Last Updated (UTC)",
        "Last Updated (Riyadh)",
        "Forecast Updated (UTC)",
        "Forecast Updated (Riyadh)",
    ]


def _get_headers_for_sheet(sheet_name: str) -> List[str]:
    schemas = _try_import_schemas()
    if schemas and hasattr(schemas, "get_headers_for_sheet"):
        try:
            hh = schemas.get_headers_for_sheet(sheet_name)  # type: ignore
            if isinstance(hh, list) and hh:
                out = [str(x).strip() for x in hh if str(x).strip()]
                if out:
                    return out
        except Exception:
            pass
    return _fallback_sheet_headers(sheet_name)


def _pad_row(row: List[Any], n: int) -> List[Any]:
    if len(row) == n:
        return row
    if len(row) < n:
        return row + [None] * (n - len(row))
    return row[:n]


def _map_rows_for_sheet(headers: List[str], quotes_in_order: List[Dict[str, Any]]) -> List[List[Any]]:
    headers = headers or ["Symbol", "Error"]
    EnrichedQuote = _try_import_enriched_quote()
    rows: List[List[Any]] = []

    # Prefer project mapper if available
    if EnrichedQuote:
        for q in quotes_in_order:
            try:
                eq = EnrichedQuote.from_unified(q)  # type: ignore
                to_row = getattr(eq, "to_row", None)
                if callable(to_row):
                    r = list(to_row(headers))  # type: ignore
                else:
                    r = [q.get("symbol")] + [None] * (len(headers) - 1)
                rows.append(_pad_row(r, len(headers)))
            except Exception as e:
                sym = str(q.get("symbol") or "ERROR").upper()
                r = [None] * len(headers)
                r[0] = sym
                r[-1] = f"Mapping error: {e}"
                rows.append(r)
        return rows

    # Lightweight fallback mapping by header label
    def _hkey(s: str) -> str:
        return "".join(ch.lower() for ch in (s or "") if ch.isalnum())

    hmap = {_hkey(h): i for i, h in enumerate(headers)}

    def _set(row: List[Any], header_label: str, val: Any):
        idx = hmap.get(_hkey(header_label))
        if idx is not None and 0 <= idx < len(row):
            row[idx] = val

    for q in quotes_in_order:
        d = _canonicalize_quote(_enrich_scores_best_effort(q))
        row = [None] * len(headers)

        sym = str(d.get("symbol") or "UNKNOWN").upper()
        _set(row, "Symbol", sym)
        _set(row, "Name", d.get("name"))
        _set(row, "Price", d.get("price"))
        _set(row, "Change %", d.get("change_pct"))
        _set(row, "Overall Score", d.get("overall_score"))
        _set(row, "Recommendation", d.get("recommendation") or "HOLD")
        _set(row, "Rec Badge", d.get("rec_badge"))
        _set(row, "Expected ROI % (1M)", d.get("expected_roi_1m"))
        _set(row, "Forecast Price (1M)", d.get("forecast_price_1m"))
        _set(row, "Expected ROI % (3M)", d.get("expected_roi_3m"))
        _set(row, "Forecast Price (3M)", d.get("forecast_price_3m"))
        _set(row, "Expected ROI % (12M)", d.get("expected_roi_12m"))
        _set(row, "Forecast Price (12M)", d.get("forecast_price_12m"))
        _set(row, "Forecast Confidence", d.get("forecast_confidence"))
        _set(row, "Data Quality", d.get("data_quality"))
        _set(row, "Error", d.get("error"))
        _set(row, "Last Updated (UTC)", d.get("last_updated_utc"))
        _set(row, "Last Updated (Riyadh)", d.get("last_updated_riyadh"))
        _set(row, "Forecast Updated (UTC)", d.get("forecast_updated_utc"))
        _set(row, "Forecast Updated (Riyadh)", d.get("forecast_updated_riyadh"))

        rows.append(row)

    return rows


# =============================================================================
# Snapshot push support
# =============================================================================
async def _engine_set_snapshot(engine: Any, sheet: str, headers: List[str], rows: List[List[Any]], meta: Dict[str, Any]) -> None:
    """
    Probe multiple setter names and signatures (sync/async, meta optional).
    """
    if not engine:
        raise RuntimeError("Engine missing")

    candidates = [
        getattr(engine, "set_cached_sheet_snapshot", None),
        getattr(engine, "set_sheet_snapshot", None),
        getattr(engine, "cache_sheet_snapshot", None),
    ]
    setter = next((fn for fn in candidates if callable(fn)), None)
    if not callable(setter):
        raise RuntimeError("Engine does not support snapshot caching")

    # Try with meta kwarg first, then without
    try:
        await _maybe_await(setter(sheet, headers, rows, meta=meta))  # type: ignore
        return
    except TypeError:
        await _maybe_await(setter(sheet, headers, rows))  # type: ignore


# =============================================================================
# Routes
# =============================================================================
@router.get("/health")
async def health(request: Request):
    eng = await _resolve_engine(request)
    cfg = _cfg()
    return {
        "status": "ok",
        "version": AI_ANALYSIS_VERSION,
        "engine": type(eng).__name__ if eng else "none",
        "time_utc": _now_utc_iso(),
        "time_riyadh": _riyadh_iso(),
        "auth_enabled": True if _allowed_tokens() else False,
        "allow_query_token": bool(_ALLOW_QUERY_TOKEN),
        "cfg": cfg,
    }


@router.get("/quote", response_model=SingleAnalysisResponse)
async def get_quote(
    request: Request,
    symbol: str = Query(..., description="Ticker/Symbol"),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(None, alias="Authorization"),
):
    if not _auth_ok(x_app_token, authorization, token):
        return SingleAnalysisResponse(symbol=(symbol or "UNKNOWN").upper(), data_quality="MISSING", error="Unauthorized")

    norm = _try_import_normalizer()
    s = (norm(symbol) or "").strip().upper() or str(symbol).strip().upper() or "UNKNOWN"

    engine = await _resolve_engine(request)
    if not engine:
        return _quote_to_response(_make_placeholder(s, err="No Engine"))

    try:
        q = await _maybe_await(_engine_get_one(engine, s))
        if not q:
            q = _make_placeholder(s, err="Empty quote")
        return _quote_to_response(q)
    except Exception as e:
        return _quote_to_response(_make_placeholder(s, err=str(e)))


@router.post("/quote", response_model=SingleAnalysisResponse)
async def post_quote(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(None, alias="Authorization"),
):
    """
    POST single quote:
      { "symbol": "1120.SR" } OR { "ticker": "AAPL" }
    """
    if not _auth_ok(x_app_token, authorization, token):
        sym = str(body.get("symbol") or body.get("ticker") or "UNKNOWN").upper()
        return SingleAnalysisResponse(symbol=sym, data_quality="MISSING", error="Unauthorized")

    sym_in = body.get("symbol") or body.get("ticker") or body.get("Symbol") or body.get("Ticker")
    norm = _try_import_normalizer()
    s = (norm(sym_in) or "").strip().upper() if sym_in is not None else ""
    if not s:
        return SingleAnalysisResponse(symbol="UNKNOWN", data_quality="MISSING", error="No symbol provided")

    engine = await _resolve_engine(request)
    if not engine:
        return _quote_to_response(_make_placeholder(s, err="No Engine"))

    try:
        q = await _maybe_await(_engine_get_one(engine, s))
        if not q:
            q = _make_placeholder(s, err="Empty quote")
        return _quote_to_response(q)
    except Exception as e:
        return _quote_to_response(_make_placeholder(s, err=str(e)))


@router.get("/quotes", response_model=BatchAnalysisResponse)
async def get_quotes(
    request: Request,
    tickers: str = Query(default="", description="Tickers: 'AAPL,MSFT 1120.SR'"),
    top_n: int = Query(default=0, ge=0, le=5000),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(None, alias="Authorization"),
):
    if not _auth_ok(x_app_token, authorization, token):
        return BatchAnalysisResponse(status="error", error="Unauthorized", results=[], meta={"ok": False, "version": AI_ANALYSIS_VERSION})

    syms = _normalize_symbols(_split_symbols(tickers))
    if not syms:
        return BatchAnalysisResponse(status="skipped", results=[], meta={"ok": True, "reason": "No tickers", "version": AI_ANALYSIS_VERSION})

    cfg = _cfg()
    cap = cfg["max_tickers"]
    if top_n and top_n > 0:
        cap = min(cap, int(top_n))
    if len(syms) > cap:
        syms = syms[:cap]

    t0 = time.perf_counter()
    engine = await _resolve_engine(request)
    m, stats = await _get_quotes_chunked(engine, syms, cfg)

    results = [_quote_to_response(m.get(s) or _make_placeholder(s)) for s in syms]

    return BatchAnalysisResponse(
        status="success",
        results=results,
        version=AI_ANALYSIS_VERSION,
        meta={
            "ok": True,
            "tickers_requested": len(syms),
            "engine": type(engine).__name__ if engine else "none",
            "time_utc": _now_utc_iso(),
            "time_riyadh": _riyadh_iso(),
            "processing_time_ms": round((time.perf_counter() - t0) * 1000.0, 2),
            "batch_stats": stats,
            "cfg": cfg,
        },
    )


@router.post("/quotes", response_model=BatchAnalysisResponse)
async def batch_quotes(
    req: BatchAnalysisRequest,
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(None, alias="Authorization"),
):
    """
    Batch analysis endpoint (JSON).
    """
    if not _auth_ok(x_app_token, authorization, token):
        return BatchAnalysisResponse(status="error", error="Unauthorized", results=[], meta={"ok": False, "version": AI_ANALYSIS_VERSION})

    tickers = _clean_tickers_from_request(req, body=body)
    if not tickers:
        return BatchAnalysisResponse(status="skipped", results=[], meta={"ok": True, "reason": "No tickers", "version": AI_ANALYSIS_VERSION})

    cfg = _cfg()
    cap = cfg["max_tickers"]

    # request-level cap (top_n) if provided
    if req.top_n is not None:
        try:
            cap = min(cap, max(1, int(req.top_n)))
        except Exception:
            pass

    if len(tickers) > cap:
        tickers = tickers[:cap]

    t0 = time.perf_counter()
    engine = await _resolve_engine(request)

    # Soft deadline (route level)
    try:
        m, stats = await _wait_for(_get_quotes_chunked(engine, tickers, cfg), timeout=float(cfg["route_timeout_sec"]))
    except asyncio.TimeoutError:
        # Return placeholders but keep stable response for Sheets
        m = {s: _make_placeholder(s, err=f"Route timeout>{cfg['route_timeout_sec']}s") for s in tickers}
        stats = {"requested": len(tickers), "route_timeout": True}

    results: List[SingleAnalysisResponse] = []
    for t in tickers:
        q = m.get(t) or _make_placeholder(t)
        results.append(_quote_to_response(q))

    return BatchAnalysisResponse(
        status="success",
        results=results,
        version=AI_ANALYSIS_VERSION,
        meta={
            "ok": True,
            "tickers_requested": len(tickers),
            "engine": type(engine).__name__ if engine else "none",
            "time_utc": _now_utc_iso(),
            "time_riyadh": _riyadh_iso(),
            "processing_time_ms": round((time.perf_counter() - t0) * 1000.0, 2),
            "batch_stats": stats,
            "cfg": cfg,
        },
    )


@router.post("/sheet-rows")
async def sheet_rows(
    request: Request,
    body: Dict[str, Any] = Body(...),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(None, alias="Authorization"),
) -> Union[SheetAnalysisResponse, Dict[str, Any]]:
    """
    Dual-Mode Endpoint:
    1) PUSH MODE: {"items":[{sheet,headers,rows},...]} -> cache snapshots inside engine
    2) COMPUTE MODE: {"tickers":[...], "sheet_name":"Market_Leaders"} -> return {headers, rows}
    """
    if not _auth_ok(x_app_token, authorization, token):
        return {"status": "error", "error": "Unauthorized", "version": AI_ANALYSIS_VERSION}

    cfg = _cfg()

    # -----------------------
    # 1) PUSH MODE
    # -----------------------
    if "items" in body and isinstance(body.get("items"), list):
        try:
            req = PushSheetRowsRequest.model_validate(body) if _PYDANTIC_V2 else PushSheetRowsRequest.parse_obj(body)  # type: ignore
            engine = await _resolve_engine(request)

            written: List[Dict[str, Any]] = []
            for item in req.items:
                try:
                    meta = {
                        "source": str(req.source or "push_api"),
                        "universe_rows": req.universe_rows,
                        "ts_utc": _now_utc_iso(),
                        "ts_riyadh": _riyadh_iso(),
                        "route": "ai_analysis",
                        "version": AI_ANALYSIS_VERSION,
                    }
                    await _engine_set_snapshot(engine, item.sheet, item.headers, item.rows, meta=meta)
                    written.append({"sheet": item.sheet, "rows": len(item.rows), "status": "cached"})
                except Exception as e:
                    written.append({"sheet": item.sheet, "rows": len(item.rows), "status": "error", "error": str(e)})

            st = "success" if not any(w.get("status") == "error" for w in written) else "partial"
            return {"status": st, "mode": "push", "written": written, "version": AI_ANALYSIS_VERSION}

        except Exception as e:
            return {"status": "error", "error": str(e), "version": AI_ANALYSIS_VERSION}

    # -----------------------
    # 2) COMPUTE MODE
    # -----------------------
    sheet_name = str(body.get("sheet_name") or body.get("sheetName") or body.get("sheet") or "Analysis").strip() or "Analysis"

    req_like = BatchAnalysisRequest(tickers=body.get("tickers", []), symbols=body.get("symbols", []), sheet_name=sheet_name)  # type: ignore
    tickers = _clean_tickers_from_request(req_like, body=body)

    headers = _get_headers_for_sheet(sheet_name)
    if not headers:
        headers = _fallback_sheet_headers(sheet_name)

    if not tickers:
        return SheetAnalysisResponse(
            status="skipped",
            error="No tickers",
            headers=headers,
            rows=[],
            version=AI_ANALYSIS_VERSION,
            meta={"ok": True, "sheet_name": sheet_name, "time_utc": _now_utc_iso(), "time_riyadh": _riyadh_iso()},
        )

    if len(tickers) > cfg["max_tickers"]:
        tickers = tickers[: cfg["max_tickers"]]

    t0 = time.perf_counter()
    engine = await _resolve_engine(request)

    try:
        m, stats = await _wait_for(_get_quotes_chunked(engine, tickers, cfg), timeout=float(cfg["route_timeout_sec"]))
    except asyncio.TimeoutError:
        m = {s: _make_placeholder(s, err=f"Route timeout>{cfg['route_timeout_sec']}s") for s in tickers}
        stats = {"requested": len(tickers), "route_timeout": True}

    # Ensure scoring + canonical keys + timestamps BEFORE mapping
    ordered_quotes: List[Dict[str, Any]] = []
    for t in tickers:
        q = m.get(t) or _make_placeholder(t)
        d = q if isinstance(q, dict) else (q.__dict__ if hasattr(q, "__dict__") else _make_placeholder(t, err="Invalid quote type"))
        if not isinstance(d, dict):
            d = _make_placeholder(t, err="Invalid quote type")
        d = _enrich_scores_best_effort(d)
        d = _canonicalize_quote(d)
        # If forecast Riyadh missing, set now (route-level standard)
        if not d.get("forecast_updated_riyadh"):
            d["forecast_updated_riyadh"] = _riyadh_iso()
        ordered_quotes.append(d)

    rows = _map_rows_for_sheet(headers, ordered_quotes)
    # pad/truncate rows to headers length
    rows = [_pad_row(list(r), len(headers)) for r in rows]

    return SheetAnalysisResponse(
        status="success",
        headers=headers,
        rows=rows,
        version=AI_ANALYSIS_VERSION,
        meta={
            "ok": True,
            "sheet_name": sheet_name,
            "tickers_count": len(tickers),
            "engine": type(engine).__name__ if engine else "none",
            "time_utc": _now_utc_iso(),
            "time_riyadh": _riyadh_iso(),
            "processing_time_ms": round((time.perf_counter() - t0) * 1000.0, 2),
            "batch_stats": stats,
            "cfg": cfg,
        },
    )


@router.get("/scoreboard")
async def scoreboard(
    request: Request,
    tickers: str = Query(default="", description="Tickers: 'AAPL,MSFT 1120.SR'"),
    top_n: int = Query(default=20, ge=1, le=200),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(None, alias="Authorization"),
):
    if not _auth_ok(x_app_token, authorization, token):
        return {"status": "error", "error": "Unauthorized", "version": AI_ANALYSIS_VERSION}

    syms = _normalize_symbols(_split_symbols(tickers))
    if not syms:
        return {"status": "skipped", "headers": ["Symbol", "Overall Score", "Recommendation"], "rows": [], "meta": {"ok": True}}

    cfg = _cfg()
    syms = syms[: min(len(syms), cfg["max_tickers"], top_n)]

    t0 = time.perf_counter()
    engine = await _resolve_engine(request)
    m, stats = await _get_quotes_chunked(engine, syms, cfg)

    items: List[Dict[str, Any]] = []
    for s in syms:
        q = m.get(s) or _make_placeholder(s)
        d = q if isinstance(q, dict) else (q.__dict__ if hasattr(q, "__dict__") else _make_placeholder(s, err="Invalid quote type"))
        d = _canonicalize_quote(_enrich_scores_best_effort(d))
        items.append(d)

    def _score(x: Dict[str, Any]) -> float:
        v = _coerce_float(x.get("overall_score"))
        return v if v is not None else -1e18

    items.sort(key=_score, reverse=True)

    headers = ["Rank", "Symbol", "Name", "Price", "Overall Score", "Risk Score", "Recommendation", "Expected ROI 1M", "Expected ROI 3M", "Expected ROI 12M"]
    rows: List[List[Any]] = []
    for i, d in enumerate(items[:top_n], start=1):
        rows.append(
            [
                i,
                str(d.get("symbol") or "").upper(),
                d.get("name"),
                d.get("price"),
                d.get("overall_score"),
                d.get("risk_score"),
                d.get("recommendation") or "HOLD",
                d.get("expected_roi_1m"),
                d.get("expected_roi_3m"),
                d.get("expected_roi_12m"),
            ]
        )

    return {
        "status": "success",
        "headers": headers,
        "rows": rows,
        "version": AI_ANALYSIS_VERSION,
        "meta": {
            "ok": True,
            "tickers_count": len(syms),
            "top_n": top_n,
            "engine": type(engine).__name__ if engine else "none",
            "time_utc": _now_utc_iso(),
            "time_riyadh": _riyadh_iso(),
            "processing_time_ms": round((time.perf_counter() - t0) * 1000.0, 2),
            "batch_stats": stats,
            "cfg": cfg,
        },
    }


__all__ = ["router"]
