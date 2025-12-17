# routes/enriched_quote.py
"""
routes/enriched_quote.py
===========================================================
Enriched Quote Routes (Google Sheets + API) - v2.4.1 (PROD SAFE)

Goals
- Stable endpoints for Google Sheets / Apps Script:
    GET  /v1/enriched/quote?symbol=1120.SR
    POST /v1/enriched/quotes   {symbols:[...], sheet_name?: "..."}

- Return:
    - quote JSON (EnrichedQuote fields)
    - optional Google Sheets row aligned to headers

- Token guard via X-APP-TOKEN (APP_TOKEN / BACKUP_APP_TOKEN).
  If no token is set => open mode.

- Defensive limits:
    ENRICHED_MAX_TICKERS (default 250)
    ENRICHED_BATCH_SIZE  (default 40)

- Engine resolution:
    • prefer request.app.state.engine (created by main.py lifespan), else singleton here.

- PROD reliability improvements:
    • UTF-8 JSON responses (fixes Arabic output in clients)
    • Mojibake auto-fix (Ø…Ù… -> Arabic) for name/sector/industry/sub_sector
    • Route-level timeouts for single/batch
    • Optional KSA fallback to /v1/argaam/quote on timeout or missing price
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Literal

import httpx
from fastapi import APIRouter, Body, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from core.config import get_settings
from core.data_engine_v2 import DataEngine, UnifiedQuote, normalize_symbol, is_ksa_symbol
from core.enriched_quote import EnrichedQuote

# Optional core.schemas (preferred)
try:
    from core.schemas import BatchProcessRequest, get_headers_for_sheet  # type: ignore
except Exception:  # pragma: no cover
    BatchProcessRequest = None  # type: ignore
    get_headers_for_sheet = None  # type: ignore

logger = logging.getLogger("routes.enriched_quote")
ROUTE_VERSION = "2.4.1"


# =============================================================================
# UTF-8 JSON response (critical for Arabic text in clients like PowerShell)
# =============================================================================
class UTF8JSONResponse(JSONResponse):
    media_type = "application/json; charset=utf-8"


router = APIRouter(prefix="/v1/enriched", tags=["enriched"], default_response_class=UTF8JSONResponse)


# =============================================================================
# Fallback request model + headers helper (keeps service alive even if schemas missing)
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
        return  # open mode
    if not x_app_token or x_app_token.strip() not in allowed:
        raise HTTPException(status_code=401, detail="Unauthorized (invalid or missing X-APP-TOKEN).")


# =============================================================================
# Settings helpers
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
        import env as env_mod  # type: ignore
        v = getattr(env_mod, name, None)
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


def _timeouts_meta() -> Dict[str, Any]:
    return {
        "single_timeout_sec": _get_float_setting("ENRICHED_SINGLE_TIMEOUT_SEC", 12.0),
        "batch_timeout_sec": _get_float_setting("ENRICHED_BATCH_TIMEOUT_SEC", 22.0),
        "ksa_single_timeout_sec": _get_float_setting("ENRICHED_KSA_SINGLE_TIMEOUT_SEC", 10.0),
        "ksa_batch_timeout_sec": _get_float_setting("ENRICHED_KSA_BATCH_TIMEOUT_SEC", 20.0),
        "ksa_fallback_enabled": _get_bool_setting("ENRICHED_KSA_FALLBACK_ENABLED", True),
        "ksa_fallback_route": os.getenv("ENRICHED_KSA_FALLBACK_ROUTE", "/v1/argaam/quote").strip() or "/v1/argaam/quote",
        "ksa_fallback_timeout_sec": _get_float_setting("ENRICHED_KSA_FALLBACK_TIMEOUT_SEC", 7.0),
    }


# =============================================================================
# Data helpers
# =============================================================================
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


def _chunk(items: List[str], size: int) -> List[List[str]]:
    if size <= 0:
        return [items]
    return [items[i: i + size] for i in range(0, len(items), size)]


def _fix_mojibake_text(s: Optional[str]) -> Optional[str]:
    """
    Fix classic UTF-8 decoded as Latin-1 (e.g. Ø§Ù… -> Arabic).
    Safe: only tries when mojibake markers exist.
    """
    if not s:
        return s
    if "Ø" in s or "Ù" in s:
        try:
            fixed = s.encode("latin1", errors="ignore").decode("utf-8", errors="ignore").strip()
            return fixed or s
        except Exception:
            return s
    return s


def _postprocess_unified(q: UnifiedQuote) -> UnifiedQuote:
    # Fix Arabic fields regardless of provider
    try:
        q.name = _fix_mojibake_text(q.name)
        q.sector = _fix_mojibake_text(q.sector)
        q.industry = _fix_mojibake_text(q.industry)
        q.sub_sector = _fix_mojibake_text(q.sub_sector)
    except Exception:
        pass
    return q


def _build_row_payload(q: UnifiedQuote, headers: List[str]) -> Dict[str, Any]:
    q = _postprocess_unified(q)
    eq = EnrichedQuote.from_unified(q)
    return {
        "headers": list(headers),
        "row": eq.to_row(headers),
        "quote": _model_to_dict(eq),
    }


# =============================================================================
# KSA fallback (route-level) to /v1/argaam/quote
# =============================================================================
async def _ksa_fallback_quote(request: Request, symbol: str, timeout_sec: float) -> Optional[UnifiedQuote]:
    """
    Calls our own service endpoint /v1/argaam/quote as a fallback.
    Expects it to return json with at least last_price/price/current_price or close.
    """
    try:
        meta = _timeouts_meta()
        route = meta["ksa_fallback_route"] or "/v1/argaam/quote"
        base_url = str(getattr(request, "base_url", "") or "").rstrip("/")
        if not base_url:
            return None

        # /v1/argaam/quote typically expects numeric code; accept 1120.SR too
        code = (symbol or "").strip().upper().split(".", 1)[0]

        url = f"{base_url}{route}"
        params = {"symbol": code}

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_sec, connect=min(5.0, timeout_sec)),
            follow_redirects=True,
            headers={"User-Agent": "TadawulFastBridge/ksa-fallback"},
        ) as client:
            r = await client.get(url, params=params)
            if r.status_code >= 400:
                return None
            data = r.json() if r.headers.get("content-type", "").lower().startswith("application/json") else None
            if not isinstance(data, dict):
                return None

        # Coerce into UnifiedQuote
        cp = data.get("current_price") or data.get("last_price") or data.get("price") or data.get("close")
        pc = data.get("previous_close") or data.get("previousClose")

        uq = UnifiedQuote(
            symbol=normalize_symbol(symbol) or symbol,
            market="KSA",
            currency="SAR",
            name=_fix_mojibake_text(data.get("name")),
            current_price=float(cp) if cp is not None else None,
            previous_close=float(pc) if pc is not None else None,
            day_high=data.get("day_high") or data.get("high"),
            day_low=data.get("day_low") or data.get("low"),
            high_52w=data.get("high_52w") or data.get("high52w"),
            low_52w=data.get("low_52w") or data.get("low52w"),
            volume=data.get("volume"),
            value_traded=data.get("value_traded") or data.get("valueTraded"),
            price_change=data.get("price_change") or data.get("change"),
            percent_change=data.get("percent_change") or data.get("change_percent"),
            data_source="argaam_fallback",
            data_quality="PARTIAL",
        ).finalize()
        return _postprocess_unified(uq)
    except Exception:
        return None


# =============================================================================
# Endpoints
# =============================================================================
@router.get("/health", tags=["system"])
async def enriched_health(request: Request) -> Dict[str, Any]:
    max_t = _get_int_setting("ENRICHED_MAX_TICKERS", 250)
    batch_sz = _get_int_setting("ENRICHED_BATCH_SIZE", 40)
    eng = await _resolve_engine(request)

    return {
        "status": "ok",
        "module": "routes.enriched_quote",
        "version": ROUTE_VERSION,
        "engine": "DataEngineV2",
        "providers": list(getattr(eng, "enabled_providers", []) or []) if eng else [],
        "limits": {"enriched_max_tickers": max_t, "enriched_batch_size": batch_sz},
        "timeouts": _timeouts_meta(),
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
    tm = _timeouts_meta()
    is_ksa = is_ksa_symbol(sym) or is_ksa_symbol(normalize_symbol(sym))

    eng = await _resolve_engine(request)
    headers = _resolve_headers(sheet_name)

    # Engine unavailable
    if eng is None:
        uq = UnifiedQuote(symbol=normalize_symbol(sym) or sym, data_quality="MISSING", error="Engine unavailable").finalize()
        uq = _postprocess_unified(uq)
        eq = EnrichedQuote.from_unified(uq)
        resp = _model_to_dict(eq) if format == "quote" else None

        elapsed = int((time.perf_counter() - t0) * 1000)
        if format == "quote":
            resp["_meta"] = {"elapsed_ms": elapsed, "fallback_used": False}
            return resp

        row = eq.to_row(headers)
        if format == "row":
            return {"symbol": eq.symbol, "headers": headers, "row": row, "_meta": {"elapsed_ms": elapsed, "fallback_used": False}}

        return {
            "sheet_name": sheet_name,
            "headers": headers,
            "row": row,
            "quote": _model_to_dict(eq),
            "_meta": {"elapsed_ms": elapsed, "fallback_used": False},
        }

    # Normal engine fetch with timeout
    timeout_sec = tm["ksa_single_timeout_sec"] if is_ksa else tm["single_timeout_sec"]
    fallback_used = False

    try:
        uq = await asyncio.wait_for(_engine_get_quote(eng, sym), timeout=timeout_sec)
        uq = uq.finalize()
    except asyncio.TimeoutError:
        uq = UnifiedQuote(symbol=normalize_symbol(sym) or sym, data_quality="MISSING", error=f"Timeout after {timeout_sec:.1f}s").finalize()
    except Exception as exc:
        uq = UnifiedQuote(symbol=normalize_symbol(sym) or sym, data_quality="MISSING", error=str(exc)).finalize()

    uq = _postprocess_unified(uq)

    # Optional KSA fallback if missing price (or timeout)
    if is_ksa and tm["ksa_fallback_enabled"] and uq.current_price is None:
        fb = await _ksa_fallback_quote(request, sym, timeout_sec=tm["ksa_fallback_timeout_sec"])
        if fb is not None and fb.current_price is not None:
            uq = fb
            fallback_used = True

    if format == "quote":
        eq = EnrichedQuote.from_unified(uq)
        out = _model_to_dict(eq)
        out["_meta"] = {"elapsed_ms": int((time.perf_counter() - t0) * 1000), "fallback_used": fallback_used}
        return out

    payload = _build_row_payload(uq, headers)
    payload["_meta"] = {"elapsed_ms": int((time.perf_counter() - t0) * 1000), "fallback_used": fallback_used}

    if format == "row":
        return {"symbol": payload["quote"].get("symbol", normalize_symbol(sym) or sym), "headers": payload["headers"], "row": payload["row"], "_meta": payload["_meta"]}

    payload["sheet_name"] = sheet_name
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
            "_meta": {"elapsed_ms": int((time.perf_counter() - t0) * 1000), "fallback_used": False},
        }

    max_t = _get_int_setting("ENRICHED_MAX_TICKERS", 250)
    batch_sz = _get_int_setting("ENRICHED_BATCH_SIZE", 40)
    tm = _timeouts_meta()

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
            uq = UnifiedQuote(symbol=s, data_quality="MISSING", error="Engine unavailable").finalize()
            uq = _postprocess_unified(uq)
            eq = EnrichedQuote.from_unified(uq)
            if format in ("rows", "both"):
                rows_out.append(eq.to_row(headers))
            if format in ("quotes", "both"):
                quotes_out.append(_model_to_dict(eq))
        resp: Dict[str, Any] = {
            "status": status,
            "error": error_msg,
            "operation": operation,
            "sheet_name": sheet_name,
            "count": len(symbols),
            "symbols": symbols,
            "headers": headers,
            "_meta": {"elapsed_ms": int((time.perf_counter() - t0) * 1000), "fallback_used": False},
        }
        if format in ("rows", "both"):
            resp["rows"] = rows_out
        if format in ("quotes", "both"):
            resp["quotes"] = quotes_out
        return resp

    rows_out: List[List[Any]] = []
    quotes_out: List[Dict[str, Any]] = []
    fallback_used_any = False

    # Soft batching (keeps memory stable)
    for chunk in _chunk(symbols, batch_sz):
        # Choose timeout: if chunk contains ANY KSA => use ksa batch timeout (safer)
        chunk_has_ksa = any(is_ksa_symbol(s) for s in chunk)
        timeout_sec = tm["ksa_batch_timeout_sec"] if chunk_has_ksa else tm["batch_timeout_sec"]

        try:
            quotes = await asyncio.wait_for(_engine_get_quotes(eng, chunk), timeout=timeout_sec)
        except asyncio.TimeoutError:
            status = "partial"
            error_msg = (error_msg + " " if error_msg else "") + f"Batch timeout after {timeout_sec:.1f}s"
            quotes = [UnifiedQuote(symbol=s, data_quality="MISSING", error=f"Timeout after {timeout_sec:.1f}s").finalize() for s in chunk]
        except Exception as exc:
            status = "partial"
            error_msg = (error_msg + " " if error_msg else "") + f"Batch error: {exc}"
            quotes = [UnifiedQuote(symbol=s, data_quality="MISSING", error=str(exc)).finalize() for s in chunk]

        # Preserve order by symbol
        m = {getattr(q, "symbol", "").upper(): _postprocess_unified(q.finalize()) for q in (quotes or []) if getattr(q, "symbol", None)}

        for s in chunk:
            uq = m.get(s.upper()) or UnifiedQuote(symbol=s.upper(), data_quality="MISSING", error="No data returned").finalize()
            uq = _postprocess_unified(uq)

            # Optional per-symbol KSA fallback
            if tm["ksa_fallback_enabled"] and is_ksa_symbol(s) and uq.current_price is None:
                fb = await _ksa_fallback_quote(request, s, timeout_sec=tm["ksa_fallback_timeout_sec"])
                if fb is not None and fb.current_price is not None:
                    uq = fb
                    fallback_used_any = True

            eq = EnrichedQuote.from_unified(uq)
            if format in ("rows", "both"):
                rows_out.append(eq.to_row(headers))
            if format in ("quotes", "both"):
                quotes_out.append(_model_to_dict(eq))

    resp: Dict[str, Any] = {
        "status": status,
        "error": error_msg,
        "operation": operation,
        "sheet_name": sheet_name,
        "count": len(symbols),
        "symbols": symbols,
        "headers": headers,
        "_meta": {"elapsed_ms": int((time.perf_counter() - t0) * 1000), "fallback_used": fallback_used_any},
    }
    if format in ("rows", "both"):
        resp["rows"] = rows_out
    if format in ("quotes", "both"):
        resp["quotes"] = quotes_out

    return resp


__all__ = ["router"]
