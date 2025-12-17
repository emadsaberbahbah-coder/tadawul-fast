# routes/enriched_quote.py
"""
routes/enriched_quote.py
===========================================================
Enriched Quote Routes (Google Sheets + API) - v2.5.0 (PROD SAFE)

Key upgrades (2.5.0)
- Adds engine_version to /health so you can confirm Render deploy immediately.
- Applies a final UTF-8 mojibake fixer to text fields at API output level
  (defense-in-depth; even if upstream provider returns mojibake).
- Keeps endpoints stable:
    GET  /v1/enriched/quote?symbol=1120.SR
    POST /v1/enriched/quotes   {symbols:[...], sheet_name?: "..."}
    GET  /v1/enriched/headers?sheet_name=...
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Literal

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field

from core.config import get_settings
from core.data_engine_v2 import DataEngine, UnifiedQuote, normalize_symbol, ENGINE_VERSION as ENGINE_V2_VERSION
from core.enriched_quote import EnrichedQuote

# Optional core.schemas (preferred)
try:
    from core.schemas import BatchProcessRequest, get_headers_for_sheet  # type: ignore
except Exception:  # pragma: no cover
    BatchProcessRequest = None  # type: ignore
    get_headers_for_sheet = None  # type: ignore

logger = logging.getLogger("routes.enriched_quote")

ROUTE_VERSION = "2.5.0"
router = APIRouter(prefix="/v1/enriched", tags=["enriched"])


# =============================================================================
# UTF-8 mojibake fixer (defense in depth at API boundary)
# =============================================================================
def _fix_mojibake(s: Optional[str]) -> Optional[str]:
    if not s or not isinstance(s, str):
        return s
    if any(ch in s for ch in ("Ø", "Ù", "Ã", "Â", "�")):
        for enc in ("latin1", "cp1252"):
            try:
                return s.encode(enc, errors="strict").decode("utf-8", errors="strict")
            except Exception:
                continue
    return s


def _fix_quote_text_fields(d: Dict[str, Any]) -> Dict[str, Any]:
    # Fix known string fields if they exist
    for k in (
        "symbol",
        "name",
        "sector",
        "industry",
        "sub_sector",
        "market",
        "currency",
        "listing_date",
        "valuation_label",
        "analyst_rating",
        "recommendation",
        "data_source",
        "data_quality",
        "error",
    ):
        if k in d and isinstance(d.get(k), str):
            d[k] = _fix_mojibake(d.get(k))
    return d


# =============================================================================
# Fallback request model + headers helper
# =============================================================================
class _FallbackBatchProcessRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    operation: str = "refresh"
    sheet_name: Optional[str] = None
    symbols: List[str] = Field(default_factory=list)
    tickers: List[str] = Field(default_factory=list)  # alias support


def _fallback_headers() -> List[str]:
    return ["Symbol", "Error"]


def _resolve_headers(sheet_name: Optional[str]) -> List[str]:
    if get_headers_for_sheet:
        try:
            h = get_headers_for_sheet(sheet_name)
            if isinstance(h, list) and h and any(str(x).strip().lower() == "symbol" for x in h):
                return [str(x) for x in h]
        except Exception:
            pass
    return _fallback_headers()


# =============================================================================
# Engine resolution (prefer app.state.engine; else singleton)
# =============================================================================
_ENGINE: Optional[DataEngine] = None
_ENGINE_LOCK = asyncio.Lock()


def _get_app_engine(request: Request) -> Optional[DataEngine]:
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
                logger.info("[enriched] DataEngine initialized (routes singleton).")
            except Exception as exc:
                logger.exception("[enriched] Failed to init DataEngine: %s", exc)
                _ENGINE = None
    return _ENGINE


async def _resolve_engine(request: Request) -> Optional[DataEngine]:
    eng = _get_app_engine(request)
    if eng is not None:
        return eng
    return await _get_singleton_engine()


async def _engine_get_quotes(engine: DataEngine, syms: List[str]) -> List[UnifiedQuote]:
    if hasattr(engine, "get_enriched_quotes"):
        return await engine.get_enriched_quotes(syms)  # type: ignore
    if hasattr(engine, "get_quotes"):
        return await engine.get_quotes(syms)  # type: ignore
    out: List[UnifiedQuote] = []
    for s in syms:
        out.append(await engine.get_quote(s))  # type: ignore
    return out


async def _engine_get_quote(engine: DataEngine, sym: str) -> UnifiedQuote:
    if hasattr(engine, "get_quote"):
        return await engine.get_quote(sym)  # type: ignore
    res = await _engine_get_quotes(engine, [sym])
    return (res or [UnifiedQuote(symbol=sym, data_quality="MISSING", error="No data returned").finalize()])[0]


# =============================================================================
# Auth (X-APP-TOKEN)
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

    try:
        import env as env_mod  # type: ignore
        for attr in ("APP_TOKEN", "BACKUP_APP_TOKEN"):
            v = getattr(env_mod, attr, None)
            if isinstance(v, str) and v.strip():
                tokens.append(v.strip())
    except Exception:
        pass

    for k in ("APP_TOKEN", "BACKUP_APP_TOKEN"):
        v = os.getenv(k)
        if v and v.strip():
            tokens.append(v.strip())

    out: List[str] = []
    seen = set()
    for t in tokens:
        if t not in seen:
            seen.add(t)
            out.append(t)

    if not out:
        logger.warning("[enriched] No APP_TOKEN configured -> endpoints are OPEN (no auth).")
    return out


def _require_token(x_app_token: Optional[str]) -> None:
    allowed = _allowed_tokens()
    if not allowed:
        return
    if not x_app_token or x_app_token.strip() not in allowed:
        raise HTTPException(status_code=401, detail="Unauthorized (invalid or missing X-APP-TOKEN).")


# =============================================================================
# Limits + settings helpers
# =============================================================================
def _get_int_setting(name: str, default: int) -> int:
    try:
        s = get_settings()
        v = getattr(s, name, None)
        if isinstance(v, int) and v > 0:
            return v
    except Exception:
        pass

    try:
        import env as env_mod  # type: ignore
        v = getattr(env_mod, name, None)
        if isinstance(v, int) and v > 0:
            return v
    except Exception:
        pass

    try:
        ev = os.getenv(name)
        if ev:
            n = int(ev)
            if n > 0:
                return n
    except Exception:
        pass

    return default


def _get_float_setting(name: str, default: float) -> float:
    try:
        s = get_settings()
        v = getattr(s, name, None)
        if isinstance(v, (int, float)) and float(v) > 0:
            return float(v)
    except Exception:
        pass
    try:
        ev = os.getenv(name)
        if ev:
            f = float(ev)
            if f > 0:
                return f
    except Exception:
        pass
    return default


def _get_bool_setting(name: str, default: bool) -> bool:
    try:
        s = get_settings()
        v = getattr(s, name, None)
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            return v.strip().lower() in ("1", "true", "yes", "on", "y")
    except Exception:
        pass
    try:
        ev = os.getenv(name)
        if ev is not None:
            return str(ev).strip().lower() in ("1", "true", "yes", "on", "y")
    except Exception:
        pass
    return default


def _clean_symbols(symbols: Sequence[Any]) -> List[str]:
    out: List[str] = []
    seen = set()
    for x in symbols or []:
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


def _model_to_dict(obj: Any) -> Dict[str, Any]:
    try:
        return obj.model_dump(exclude_none=False)  # type: ignore
    except Exception:
        pass
    try:
        return obj.dict()  # type: ignore
    except Exception:
        pass
    try:
        return dict(getattr(obj, "__dict__", {}) or {})
    except Exception:
        return {}


def _build_row_payload(q: UnifiedQuote, headers: List[str]) -> Dict[str, Any]:
    eq = EnrichedQuote.from_unified(q)
    quote_dict = _model_to_dict(eq)
    quote_dict = _fix_quote_text_fields(quote_dict)
    return {
        "headers": list(headers),
        "row": eq.to_row(headers),
        "quote": quote_dict,
    }


def _chunk(items: List[str], size: int) -> List[List[str]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]


# =============================================================================
# Endpoints
# =============================================================================
@router.get("/health", tags=["system"])
async def enriched_health(request: Request) -> Dict[str, Any]:
    max_t = _get_int_setting("ENRICHED_MAX_TICKERS", 250)
    batch_sz = _get_int_setting("ENRICHED_BATCH_SIZE", 40)

    eng = await _resolve_engine(request)

    # These are informational (for your logs/ops); safe defaults
    timeouts = {
        "single_timeout_sec": _get_float_setting("ENRICHED_SINGLE_TIMEOUT_SEC", 12.0),
        "batch_timeout_sec": _get_float_setting("ENRICHED_BATCH_TIMEOUT_SEC", 22.0),
        "ksa_single_timeout_sec": _get_float_setting("KSA_SINGLE_TIMEOUT_SEC", 10.0),
        "ksa_batch_timeout_sec": _get_float_setting("KSA_BATCH_TIMEOUT_SEC", 20.0),
        "ksa_fallback_enabled": _get_bool_setting("KSA_FALLBACK_ENABLED", True),
        "ksa_fallback_route": os.getenv("KSA_FALLBACK_ROUTE", "/v1/argaam/quote"),
        "ksa_fallback_timeout_sec": _get_float_setting("KSA_FALLBACK_TIMEOUT_SEC", 7.0),
    }

    return {
        "status": "ok",
        "module": "routes.enriched_quote",
        "version": ROUTE_VERSION,
        "engine": "DataEngineV2",
        "engine_version": ENGINE_V2_VERSION,
        "providers": list(getattr(eng, "enabled_providers", []) or []) if eng else [],
        "limits": {"enriched_max_tickers": max_t, "enriched_batch_size": batch_sz},
        "timeouts": timeouts,
        "auth": "open" if not _allowed_tokens() else "token",
    }


@router.get("/headers")
async def enriched_headers(
    request: Request,
    sheet_name: Optional[str] = Query(default=None, description="Optional sheet name to resolve headers."),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> Dict[str, Any]:
    _require_token(x_app_token)
    headers = _resolve_headers(sheet_name)
    return {"sheet_name": sheet_name, "headers": headers, "count": len(headers)}


@router.get("/quote")
async def enriched_quote(
    request: Request,
    symbol: str = Query(..., description="Ticker symbol (e.g., 1120.SR, AAPL, ^GSPC)."),
    sheet_name: Optional[str] = Query(default=None, description="Optional sheet name for header/row alignment."),
    format: Literal["quote", "row", "both"] = Query(default="quote", description="Return quote, row, or both."),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> Dict[str, Any]:
    _require_token(x_app_token)

    sym = (symbol or "").strip()
    if not sym:
        raise HTTPException(status_code=400, detail="symbol is required")

    t0 = time.perf_counter()

    eng = await _resolve_engine(request)
    if eng is None:
        eq = EnrichedQuote.from_unified(
            UnifiedQuote(symbol=normalize_symbol(sym) or sym, data_quality="MISSING", error="Engine unavailable").finalize()
        )
        out_quote = _fix_quote_text_fields(_model_to_dict(eq))
        if format == "quote":
            return out_quote
        headers = _resolve_headers(sheet_name)
        row = eq.to_row(headers)
        if format == "row":
            return {"symbol": out_quote.get("symbol", normalize_symbol(sym) or sym), "headers": headers, "row": row}
        return {"sheet_name": sheet_name, "headers": headers, "row": row, "quote": out_quote, "_meta": {"elapsed_ms": int((time.perf_counter() - t0) * 1000)}}

    try:
        q = await _engine_get_quote(eng, sym)
    except Exception as exc:
        q = UnifiedQuote(symbol=normalize_symbol(sym) or sym, data_quality="MISSING", error=str(exc)).finalize()

    eq = EnrichedQuote.from_unified(q)
    quote_dict = _fix_quote_text_fields(_model_to_dict(eq))

    if format == "quote":
        quote_dict["_meta"] = {"elapsed_ms": int((time.perf_counter() - t0) * 1000)}
        return quote_dict

    headers = _resolve_headers(sheet_name)
    payload = _build_row_payload(q, headers)

    if format == "row":
        return {
            "symbol": payload["quote"].get("symbol", normalize_symbol(sym) or sym),
            "headers": payload["headers"],
            "row": payload["row"],
            "_meta": {"elapsed_ms": int((time.perf_counter() - t0) * 1000)},
        }

    payload["sheet_name"] = sheet_name
    payload["_meta"] = {"elapsed_ms": int((time.perf_counter() - t0) * 1000)}
    return payload


@router.post("/quotes")
async def enriched_quotes(
    request: Request,
    req: Any = Body(...),
    format: Literal["rows", "quotes", "both"] = Query(default="rows", description="Return rows, quotes, or both."),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> Dict[str, Any]:
    _require_token(x_app_token)

    t0 = time.perf_counter()

    Model = BatchProcessRequest or _FallbackBatchProcessRequest  # type: ignore
    try:
        parsed = req if isinstance(req, Model) else Model.model_validate(req)  # type: ignore
    except Exception:
        try:
            parsed = Model.parse_obj(req)  # type: ignore
        except Exception:
            parsed = _FallbackBatchProcessRequest()

    symbols_in = list(getattr(parsed, "symbols", []) or []) + list(getattr(parsed, "tickers", []) or [])
    sheet_name = getattr(parsed, "sheet_name", None)
    operation = getattr(parsed, "operation", "refresh")

    symbols = _clean_symbols(symbols_in)
    headers = _resolve_headers(sheet_name)

    if not symbols:
        return {
            "status": "skipped",
            "error": "No symbols provided",
            "operation": operation,
            "sheet_name": sheet_name,
            "count": 0,
            "symbols": [],
            "headers": headers,
            "rows": [] if format in ("rows", "both") else None,
            "quotes": [] if format in ("quotes", "both") else None,
            "_meta": {"elapsed_ms": int((time.perf_counter() - t0) * 1000)},
        }

    max_t = _get_int_setting("ENRICHED_MAX_TICKERS", 250)
    batch_sz = _get_int_setting("ENRICHED_BATCH_SIZE", 40)

    status = "success"
    error_msg: Optional[str] = None

    if len(symbols) > max_t:
        status = "partial"
        error_msg = f"Too many symbols ({len(symbols)}). Truncated to max {max_t}."
        symbols = symbols[:max_t]

    eng = await _resolve_engine(request)
    if eng is None:
        status = "error"
        error_msg = (error_msg + " " if error_msg else "") + "Engine unavailable"
        rows_out: List[List[Any]] = []
        quotes_out: List[Dict[str, Any]] = []
        for s in symbols:
            eq = EnrichedQuote.from_unified(UnifiedQuote(symbol=s, data_quality="MISSING", error="Engine unavailable").finalize())
            if format in ("rows", "both"):
                rows_out.append(eq.to_row(headers))
            if format in ("quotes", "both"):
                quotes_out.append(_fix_quote_text_fields(_model_to_dict(eq)))

        resp: Dict[str, Any] = {
            "status": status,
            "error": error_msg,
            "operation": operation,
            "sheet_name": sheet_name,
            "count": len(symbols),
            "symbols": symbols,
            "headers": headers,
            "_meta": {"elapsed_ms": int((time.perf_counter() - t0) * 1000)},
        }
        if format in ("rows", "both"):
            resp["rows"] = rows_out
        if format in ("quotes", "both"):
            resp["quotes"] = quotes_out
        return resp

    rows_out: List[List[Any]] = []
    quotes_out: List[Dict[str, Any]] = []

    for chunk in _chunk(symbols, batch_sz):
        try:
            quotes = await _engine_get_quotes(eng, chunk)
        except Exception as exc:
            status = "partial"
            error_msg = (error_msg + " " if error_msg else "") + f"Batch error: {exc}"
            quotes = [UnifiedQuote(symbol=s, data_quality="MISSING", error=str(exc)).finalize() for s in chunk]

        m = {getattr(q, "symbol", "").upper(): q for q in (quotes or []) if getattr(q, "symbol", None)}
        for s in chunk:
            uq = m.get(s.upper()) or UnifiedQuote(symbol=s.upper(), data_quality="MISSING", error="No data returned").finalize()
            eq = EnrichedQuote.from_unified(uq)

            if format in ("rows", "both"):
                rows_out.append(eq.to_row(headers))
            if format in ("quotes", "both"):
                quotes_out.append(_fix_quote_text_fields(_model_to_dict(eq)))

    resp: Dict[str, Any] = {
        "status": status,
        "error": error_msg,
        "operation": operation,
        "sheet_name": sheet_name,
        "count": len(symbols),
        "symbols": symbols,
        "headers": headers,
        "_meta": {"elapsed_ms": int((time.perf_counter() - t0) * 1000)},
    }
    if format in ("rows", "both"):
        resp["rows"] = rows_out
    if format in ("quotes", "both"):
        resp["quotes"] = quotes_out

    return resp


__all__ = ["router"]
