# routes/ai_analysis.py
"""
TADAWUL FAST BRIDGE – AI ANALYSIS ROUTES (v5.3.0) – PROD SAFE
------------------------------------------------------------

What this router provides
- /v1/analysis/health
- /v1/analysis/quotes           (batch JSON)
- /v1/analysis/quote            (single JSON)
- /v1/analysis/sheet-rows       (dual mode: PUSH cache OR COMPUTE grid)
- /v1/analysis/scoreboard       (quick ranking table for dashboards)

Key guarantees (Sheets + Backend safety)
- ✅ ROI canonical keys: expected_roi_1m / expected_roi_3m / expected_roi_12m
- ✅ Price canonical keys: forecast_price_1m / forecast_price_3m / forecast_price_12m
- ✅ Riyadh timestamps injected: last_updated_riyadh / forecast_updated_riyadh
- ✅ Robust input parsing: tickers may arrive as list, string, or nested dict payloads
- ✅ Engine-aware: uses request.app.state.engine if present; else get_engine fallback
- ✅ Concurrency-controlled chunked calls with timeout protection
- ✅ Push-mode supports sync/async set_cached_sheet_snapshot
- ✅ Always returns stable non-empty headers to Google Sheets writers
- ✅ Optional auth guard (X-APP-TOKEN / Bearer / ?token when ALLOW_QUERY_TOKEN=1)

Environment
- AI_BATCH_SIZE (default 25)
- AI_BATCH_TIMEOUT_SEC (default 60)
- AI_BATCH_CONCURRENCY (default 6)
- AI_MAX_TICKERS (default 1200)
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
from datetime import datetime, timezone, timedelta
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from fastapi import APIRouter, Body, Header, Query, Request

# Pydantic v2 preferred
try:
    from pydantic import BaseModel, ConfigDict, Field  # type: ignore

    _PYDANTIC_V2 = True
except Exception:  # pragma: no cover
    from pydantic import BaseModel, Field  # type: ignore

    ConfigDict = None  # type: ignore
    _PYDANTIC_V2 = False

logger = logging.getLogger("routes.ai_analysis")

AI_ANALYSIS_VERSION = "5.3.0"
router = APIRouter(prefix="/v1/analysis", tags=["AI & Analysis"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_RIYADH_TZ = timezone(timedelta(hours=3))


# =============================================================================
# Settings Shim
# =============================================================================
try:
    from core.config import get_settings  # type: ignore
except Exception:  # pragma: no cover
    def get_settings():  # type: ignore
        return None


# =============================================================================
# Small config helpers
# =============================================================================
def _safe_int(x: Any, default: int) -> int:
    try:
        return int(float(str(x).strip())) if x is not None and str(x).strip() != "" else default
    except Exception:
        return default


def _safe_float(x: Any, default: float) -> float:
    try:
        return float(str(x).strip()) if x is not None and str(x).strip() != "" else default
    except Exception:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if not v:
        return default
    return v in _TRUTHY


def _cfg() -> Dict[str, Any]:
    # Sensible defaults
    batch_size = _safe_int(os.getenv("AI_BATCH_SIZE"), 25)
    timeout = _safe_float(os.getenv("AI_BATCH_TIMEOUT_SEC"), 60.0)
    concurrency = _safe_int(os.getenv("AI_BATCH_CONCURRENCY"), 6)
    max_tickers = _safe_int(os.getenv("AI_MAX_TICKERS"), 1200)

    return {
        "batch_size": max(5, min(250, batch_size)),
        "timeout_sec": max(5.0, min(240.0, timeout)),
        "max_tickers": max(10, min(5000, max_tickers)),
        "concurrency": max(1, min(40, concurrency)),
    }


# =============================================================================
# Date/time helpers
# =============================================================================
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _riyadh_iso() -> str:
    return datetime.now(_RIYADH_TZ).isoformat()


def _to_riyadh_iso(utc_iso: Optional[str]) -> str:
    if not utc_iso:
        return ""
    try:
        dt = datetime.fromisoformat(str(utc_iso).replace("Z", "+00:00"))
        return dt.astimezone(_RIYADH_TZ).isoformat()
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
        v = (os.getenv(k) or "").strip()
        if v:
            toks.append(v)
    # stable de-dupe
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


def _auth_ok(
    x_app_token: Optional[str],
    authorization: Optional[str],
    query_token: Optional[str],
) -> bool:
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

    # Fundamental
    market_cap: Optional[float] = None
    pe_ttm: Optional[float] = None
    dividend_yield: Optional[float] = None

    # Scores
    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    risk_score: Optional[float] = None
    overall_score: Optional[float] = None
    recommendation: Optional[str] = "HOLD"
    rec_badge: Optional[str] = None

    # Forecast (Canonical)
    expected_roi_1m: Optional[float] = None
    expected_roi_3m: Optional[float] = None
    expected_roi_12m: Optional[float] = None

    forecast_price_1m: Optional[float] = None
    forecast_price_3m: Optional[float] = None
    forecast_price_12m: Optional[float] = None

    forecast_confidence: Optional[float] = None
    forecast_updated_utc: Optional[str] = None
    forecast_updated_riyadh: Optional[str] = None

    # Meta
    data_quality: str = "MISSING"
    last_updated_utc: Optional[str] = None
    last_updated_riyadh: Optional[str] = None
    error: Optional[str] = None


class BatchAnalysisRequest(_ExtraIgnoreBase):
    tickers: Any = Field(default_factory=list)  # allow list/str/dict (we normalize)
    symbols: Any = Field(default_factory=list)
    sheet_name: Optional[str] = None
    top_n: Optional[int] = None  # optional soft cap


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


# =============================================================================
# Input normalization
# =============================================================================
def _split_string_symbols(s: str) -> List[str]:
    raw = (s or "").replace(",", " ").replace("|", " ").replace(";", " ")
    return [p.strip() for p in raw.split() if p.strip()]


def _extract_symbols_from_any(obj: Any) -> List[str]:
    """
    Accepts:
    - list[str]
    - str
    - dict with keys: tickers/symbols/all_tickers/ksa_tickers/global_tickers
    """
    if obj is None:
        return []

    if isinstance(obj, str):
        return _split_string_symbols(obj)

    if isinstance(obj, (list, tuple)):
        return [str(x) for x in obj if str(x or "").strip()]

    if isinstance(obj, dict):
        acc: List[str] = []
        for k in ("tickers", "symbols", "all_tickers", "ksa_tickers", "global_tickers"):
            v = obj.get(k)
            if isinstance(v, str):
                acc.extend(_split_string_symbols(v))
            elif isinstance(v, (list, tuple)):
                acc.extend([str(x) for x in v if str(x or "").strip()])
        return acc

    # pydantic objects
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


def _normalize_symbols(inputs: Sequence[Any]) -> List[str]:
    norm = _try_import_normalizer()
    seen = set()
    out: List[str] = []
    for x in inputs or []:
        raw = str(x or "").strip()
        if not raw:
            continue
        try:
            s = (norm(raw) or "").strip().upper()
        except Exception:
            s = raw.strip().upper()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _clean_tickers_from_request(req: BatchAnalysisRequest, *, body: Optional[Dict[str, Any]] = None) -> List[str]:
    raw: List[str] = []
    raw.extend(_extract_symbols_from_any(req.tickers))
    raw.extend(_extract_symbols_from_any(req.symbols))
    if body:
        raw.extend(_extract_symbols_from_any(body))
    return _normalize_symbols(raw)


# =============================================================================
# Quote shaping (placeholders + canonicalization)
# =============================================================================
def _make_placeholder(symbol: str, dq: str = "MISSING", err: str = "No Data") -> Dict[str, Any]:
    utc = _now_utc_iso()
    sym = str(symbol or "").strip().upper() or "UNKNOWN"
    return {
        "symbol": sym,
        "symbol_normalized": sym,
        "data_quality": dq,
        "error": err,
        "recommendation": "HOLD",
        "last_updated_utc": utc,
        "last_updated_riyadh": _to_riyadh_iso(utc),
        "forecast_updated_utc": None,
        "forecast_updated_riyadh": None,
    }


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


def _canonicalize_quote(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Bring various provider keys into the canonical keys used by the sheet.
    """
    if not isinstance(d, dict):
        return {}

    out = dict(d)

    # Price
    if out.get("price") is None:
        out["price"] = out.get("current_price")

    # Change percent
    if out.get("change_pct") is None:
        out["change_pct"] = out.get("percent_change")

    # ROI aliases (if backend provides sheet-style names)
    roi_aliases = {
        "expected_roi_1m": ["expected_roi_1m", "expected_roi_pct_1m", "expected_roi_percent_1m", "expected_roi1m"],
        "expected_roi_3m": ["expected_roi_3m", "expected_roi_pct_3m", "expected_roi_percent_3m", "expected_roi3m"],
        "expected_roi_12m": ["expected_roi_12m", "expected_roi_pct_12m", "expected_roi_percent_12m", "expected_roi12m"],
    }
    for canon, keys in roi_aliases.items():
        if out.get(canon) is None:
            for k in keys:
                if k in out and out.get(k) is not None:
                    out[canon] = out.get(k)
                    break

    # Forecast price aliases
    px_aliases = {
        "forecast_price_1m": ["forecast_price_1m", "forecast_1m", "forecast_price1m"],
        "forecast_price_3m": ["forecast_price_3m", "forecast_3m", "forecast_price3m"],
        "forecast_price_12m": ["forecast_price_12m", "forecast_12m", "forecast_price12m"],
    }
    for canon, keys in px_aliases.items():
        if out.get(canon) is None:
            for k in keys:
                if k in out and out.get(k) is not None:
                    out[canon] = out.get(k)
                    break

    # Forecast updated timestamps
    if out.get("forecast_updated_utc") is None:
        out["forecast_updated_utc"] = out.get("forecast_updated") or out.get("forecast_ts_utc")

    if out.get("forecast_updated_riyadh") is None:
        out["forecast_updated_riyadh"] = _to_riyadh_iso(out.get("forecast_updated_utc"))

    # last_updated Riyadh
    if out.get("last_updated_riyadh") is None:
        out["last_updated_riyadh"] = _to_riyadh_iso(out.get("last_updated_utc"))

    return out


def _quote_to_response(q: Any) -> SingleAnalysisResponse:
    d = q if isinstance(q, dict) else (q.__dict__ if hasattr(q, "__dict__") else {})
    d = _enrich_scores_best_effort(d)
    d = _canonicalize_quote(d)

    sym = str(d.get("symbol") or d.get("symbol_normalized") or "UNKNOWN").strip().upper()

    # ensure timestamps exist
    last_utc = d.get("last_updated_utc") or _now_utc_iso()
    last_riy = d.get("last_updated_riyadh") or _to_riyadh_iso(last_utc)

    f_utc = d.get("forecast_updated_utc")
    f_riy = d.get("forecast_updated_riyadh") or _to_riyadh_iso(f_utc) if f_utc else None

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
# Batch execution (chunked, concurrency-limited, timeout-protected)
# =============================================================================
async def _get_quotes_chunked(engine: Any, symbols: List[str], cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns a dict mapping symbol->quote(dict or object). Ensures every requested symbol has an entry.
    """
    if not symbols:
        return {}

    if not engine:
        return {s: _make_placeholder(s, err="No Engine") for s in symbols}

    batch_size = int(cfg["batch_size"])
    concurrency = int(cfg["concurrency"])
    timeout_sec = float(cfg["timeout_sec"])

    chunks = [symbols[i : i + batch_size] for i in range(0, len(symbols), batch_size)]
    sem = asyncio.Semaphore(concurrency)

    results: Dict[str, Any] = {}

    async def _call_engine_batch(chunk_syms: List[str]) -> None:
        async with sem:
            try:
                # Prefer batch API
                if hasattr(engine, "get_enriched_quotes") and callable(getattr(engine, "get_enriched_quotes")):
                    coro = engine.get_enriched_quotes(chunk_syms)
                    res = await asyncio.wait_for(_maybe_await(coro), timeout=timeout_sec)

                    if isinstance(res, dict):
                        # try to map keys to uppercase symbols
                        for k, v in res.items():
                            kk = str(k or "").strip().upper()
                            if kk:
                                results[kk] = v
                    elif isinstance(res, list):
                        for item in res:
                            if isinstance(item, dict):
                                sym = str(item.get("symbol") or item.get("symbol_normalized") or "").strip().upper()
                                if sym:
                                    results[sym] = item
                    else:
                        # unknown type: fill placeholders
                        raise RuntimeError("engine.get_enriched_quotes returned unsupported type")
                else:
                    # Serial fallback
                    for s in chunk_syms:
                        try:
                            coro = engine.get_enriched_quote(s)
                            q = await asyncio.wait_for(_maybe_await(coro), timeout=timeout_sec)
                            results[str(s).upper()] = q
                        except Exception as e:
                            results[str(s).upper()] = _make_placeholder(s, err=str(e))

            except asyncio.TimeoutError:
                for s in chunk_syms:
                    results[str(s).upper()] = _make_placeholder(s, err=f"Timeout>{timeout_sec}s")
            except Exception as e:
                logger.warning("Chunk failed (%s syms): %s", len(chunk_syms), e)
                for s in chunk_syms:
                    results[str(s).upper()] = _make_placeholder(s, err=str(e))

    await asyncio.gather(*[_call_engine_batch(c) for c in chunks])

    # Ensure all requested symbols exist in map
    for s in symbols:
        key = str(s).strip().upper()
        if key not in results:
            results[key] = _make_placeholder(key, err="Missing from engine response")

    return results


# =============================================================================
# Sheet helpers (headers + row mapping)
# =============================================================================
def _fallback_sheet_headers(sheet_name: str) -> List[str]:
    # Basic, stable default. Your canonical schemas should override this.
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


def _map_rows_for_sheet(headers: List[str], quotes_in_order: List[Dict[str, Any]]) -> List[List[Any]]:
    EnrichedQuote = _try_import_enriched_quote()
    rows: List[List[Any]] = []

    if EnrichedQuote:
        for q in quotes_in_order:
            try:
                if isinstance(q, dict):
                    eq = EnrichedQuote.from_unified(q)  # type: ignore
                    rows.append(eq.to_row(headers))  # type: ignore
                else:
                    rows.append([str(q)] + [None] * (len(headers) - 1))
            except Exception as e:
                sym = str(q.get("symbol") or "ERROR").upper() if isinstance(q, dict) else "ERROR"
                # put error best-effort in last column
                r = [None] * len(headers)
                if headers:
                    r[0] = sym
                    r[-1] = f"Mapping error: {e}"
                rows.append(r)
        return rows

    # Simple fallback mapping if EnrichedQuote missing:
    # We fill a few common columns by searching header names.
    def _hkey(s: str) -> str:
        return "".join(ch.lower() for ch in (s or "") if ch.isalnum())

    hmap = {_hkey(h): i for i, h in enumerate(headers or [])}

    def _set(row: List[Any], key_variants: Sequence[str], val: Any):
        for k in key_variants:
            idx = hmap.get(_hkey(k))
            if idx is not None and 0 <= idx < len(row):
                row[idx] = val
                return

    for q in quotes_in_order:
        d = q if isinstance(q, dict) else {}
        d = _canonicalize_quote(_enrich_scores_best_effort(d) if isinstance(d, dict) else {})
        row = [None] * len(headers)

        sym = str(d.get("symbol") or "UNKNOWN").upper()
        _set(row, ["Symbol", "Ticker"], sym)
        _set(row, ["Name"], d.get("name"))
        _set(row, ["Price"], d.get("price"))
        _set(row, ["Change %", "ChangePct"], d.get("change_pct"))
        _set(row, ["Overall Score"], d.get("overall_score"))
        _set(row, ["Recommendation"], d.get("recommendation"))
        _set(row, ["Rec Badge"], d.get("rec_badge"))
        _set(row, ["Expected ROI % (1M)"], d.get("expected_roi_1m"))
        _set(row, ["Forecast Price (1M)"], d.get("forecast_price_1m"))
        _set(row, ["Expected ROI % (3M)"], d.get("expected_roi_3m"))
        _set(row, ["Forecast Price (3M)"], d.get("forecast_price_3m"))
        _set(row, ["Expected ROI % (12M)"], d.get("expected_roi_12m"))
        _set(row, ["Forecast Price (12M)"], d.get("forecast_price_12m"))
        _set(row, ["Forecast Confidence"], d.get("forecast_confidence"))
        _set(row, ["Data Quality"], d.get("data_quality"))
        _set(row, ["Error"], d.get("error"))
        _set(row, ["Last Updated (UTC)"], d.get("last_updated_utc"))
        _set(row, ["Last Updated (Riyadh)"], d.get("last_updated_riyadh"))
        _set(row, ["Forecast Updated (UTC)"], d.get("forecast_updated_utc"))
        _set(row, ["Forecast Updated (Riyadh)"], d.get("forecast_updated_riyadh"))

        rows.append(row)

    return rows


# =============================================================================
# Routes
# =============================================================================
@router.get("/health")
async def health(request: Request):
    eng = await _resolve_engine(request)
    return {
        "status": "ok",
        "version": AI_ANALYSIS_VERSION,
        "engine": type(eng).__name__ if eng else "none",
        "time_utc": _now_utc_iso(),
        "time_riyadh": _riyadh_iso(),
        "auth_enabled": True if _allowed_tokens() else False,
    }


@router.post("/quote", response_model=SingleAnalysisResponse)
async def single_quote(
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
        coro = engine.get_enriched_quote(s)
        q = await _maybe_await(coro)
        if not q:
            q = _make_placeholder(s, err="Empty quote")
        return _quote_to_response(q)
    except Exception as e:
        return _quote_to_response(_make_placeholder(s, err=str(e)))


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
    Batch analysis endpoint.
    Returns structured JSON list of analyzed stocks.
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
        cap = min(cap, max(1, int(req.top_n)))

    if len(tickers) > cap:
        tickers = tickers[:cap]

    engine = await _resolve_engine(request)
    raw_map = await _get_quotes_chunked(engine, tickers, cfg)

    results: List[SingleAnalysisResponse] = []
    for t in tickers:
        q = raw_map.get(t) or _make_placeholder(t)
        results.append(_quote_to_response(q))

    return BatchAnalysisResponse(
        status="success",
        results=results,
        version=AI_ANALYSIS_VERSION,
        meta={
            "ok": True,
            "tickers_requested": len(tickers),
            "batch_size": cfg["batch_size"],
            "concurrency": cfg["concurrency"],
            "timeout_sec": cfg["timeout_sec"],
            "engine": type(engine).__name__ if engine else "none",
            "time_utc": _now_utc_iso(),
            "time_riyadh": _riyadh_iso(),
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
    1) PUSH MODE: receives {"items":[{sheet,headers,rows},...]} from Sheets to cache snapshots.
    2) COMPUTE MODE: receives {"tickers":[...]} to return analyzed grid rows.
    """
    if not _auth_ok(x_app_token, authorization, token):
        return {"status": "error", "error": "Unauthorized", "version": AI_ANALYSIS_VERSION}

    # -----------------------
    # 1) PUSH MODE
    # -----------------------
    if "items" in body and isinstance(body.get("items"), list):
        try:
            req = PushSheetRowsRequest.model_validate(body) if _PYDANTIC_V2 else PushSheetRowsRequest.parse_obj(body)  # type: ignore
            engine = await _resolve_engine(request)

            fn = getattr(engine, "set_cached_sheet_snapshot", None) if engine else None
            if not callable(fn):
                return {"status": "error", "error": "Engine does not support snapshots", "version": AI_ANALYSIS_VERSION}

            written: List[Dict[str, Any]] = []
            for item in req.items:
                try:
                    meta = {"source": "push_api", "ts_utc": _now_utc_iso(), "ts_riyadh": _riyadh_iso(), "route": "ai_analysis"}
                    # allow sync or async engine
                    await _maybe_await(fn(item.sheet, item.headers, item.rows, meta=meta))  # type: ignore
                    written.append({"sheet": item.sheet, "rows": len(item.rows), "status": "cached"})
                except Exception as e:
                    written.append({"sheet": item.sheet, "rows": len(item.rows), "status": "error", "error": str(e)})

            return {"status": "success", "mode": "push", "written": written, "version": AI_ANALYSIS_VERSION}

        except Exception as e:
            return {"status": "error", "error": str(e), "version": AI_ANALYSIS_VERSION}

    # -----------------------
    # 2) COMPUTE MODE
    # -----------------------
    sheet_name = str(body.get("sheet_name") or body.get("sheetName") or body.get("sheet") or "Analysis").strip() or "Analysis"

    # Normalize tickers from many shapes
    req_like = BatchAnalysisRequest(tickers=body.get("tickers", []), symbols=body.get("symbols", []), sheet_name=sheet_name)  # type: ignore
    tickers = _clean_tickers_from_request(req_like, body=body)

    if not tickers:
        return SheetAnalysisResponse(status="skipped", error="No tickers", headers=_get_headers_for_sheet(sheet_name), rows=[], meta={"ok": True})

    cfg = _cfg()
    if len(tickers) > cfg["max_tickers"]:
        tickers = tickers[: cfg["max_tickers"]]

    engine = await _resolve_engine(request)
    quotes_map = await _get_quotes_chunked(engine, tickers, cfg)

    # Ensure scoring + timestamps + canonical keys BEFORE mapping
    ordered_quotes: List[Dict[str, Any]] = []
    for t in tickers:
        q = quotes_map.get(t) or _make_placeholder(t)
        d = q if isinstance(q, dict) else (q.__dict__ if hasattr(q, "__dict__") else {})
        d = _enrich_scores_best_effort(d) if isinstance(d, dict) else _make_placeholder(t, err="Invalid quote type")
        d = _canonicalize_quote(d)
        # Always inject Riyadh forecast timestamp "now" if missing (route-level standard)
        if not d.get("forecast_updated_riyadh"):
            d["forecast_updated_riyadh"] = _riyadh_iso()
        if not d.get("forecast_updated_utc") and d.get("forecast_updated_riyadh"):
            # keep utc optional; do not fabricate unless desired
            pass
        ordered_quotes.append(d)

    headers = _get_headers_for_sheet(sheet_name)
    if not headers:
        headers = _fallback_sheet_headers(sheet_name)

    rows = _map_rows_for_sheet(headers, ordered_quotes)

    return SheetAnalysisResponse(
        status="success",
        headers=headers,
        rows=rows,
        version=AI_ANALYSIS_VERSION,
        meta={
            "ok": True,
            "sheet_name": sheet_name,
            "tickers_count": len(tickers),
            "batch_size": cfg["batch_size"],
            "concurrency": cfg["concurrency"],
            "timeout_sec": cfg["timeout_sec"],
            "engine": type(engine).__name__ if engine else "none",
            "time_utc": _now_utc_iso(),
            "time_riyadh": _riyadh_iso(),
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
    """
    Lightweight scoreboard for quick dashboards.
    Returns: {status, headers, rows, meta}
    """
    if not _auth_ok(x_app_token, authorization, token):
        return {"status": "error", "error": "Unauthorized", "version": AI_ANALYSIS_VERSION}

    syms = _normalize_symbols(_split_string_symbols(tickers))
    if not syms:
        return {"status": "skipped", "headers": ["Symbol", "Overall Score", "Recommendation"], "rows": [], "meta": {"ok": True}}

    cfg = _cfg()
    syms = syms[: min(len(syms), cfg["max_tickers"], top_n)]

    engine = await _resolve_engine(request)
    m = await _get_quotes_chunked(engine, syms, cfg)

    items: List[Dict[str, Any]] = []
    for s in syms:
        q = m.get(s) or _make_placeholder(s)
        d = q if isinstance(q, dict) else (q.__dict__ if hasattr(q, "__dict__") else {})
        d = _canonicalize_quote(_enrich_scores_best_effort(d) if isinstance(d, dict) else _make_placeholder(s, err="Invalid quote type"))
        items.append(d)

    # Sort by overall_score desc (None last)
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
        },
    }


__all__ = ["router"]
