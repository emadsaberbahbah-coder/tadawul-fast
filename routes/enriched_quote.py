# routes/enriched_quote.py
"""
routes/enriched_quote.py
===========================================================
Enriched Quote Routes (Google Sheets + API) - v2.4.0 (PROD SAFE)

Goals
- Stable endpoints for Google Sheets / Apps Script:
    GET  /v1/enriched/quote?symbol=1120.SR
    POST /v1/enriched/quotes   {symbols:[...], sheet_name?: "..."}
    GET  /v1/enriched/headers?sheet_name=...
- Return:
    - quote JSON (EnrichedQuote fields)
    - optional Google Sheets row aligned to headers
- Token guard via X-APP-TOKEN (APP_TOKEN / BACKUP_APP_TOKEN). If no token is set => open mode.
- Defensive limits (ENRICHED_MAX_TICKERS, ENRICHED_BATCH_SIZE).
- Engine resolution:
    • prefer request.app.state.engine (created by main.py lifespan), else a safe singleton here.
- TIMEOUT PROTECTION:
    • Prevents request hang by applying asyncio.wait_for around engine calls
    • Optional KSA fallback via internal gateway (/v1/argaam/quote) if configured/available
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

ROUTE_VERSION = "2.4.0"
router = APIRouter(prefix="/v1/enriched", tags=["enriched"])


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
    # Minimal survivable header set if schema is missing
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
# Timeouts / limits
# =============================================================================
def _env_int(name: str, default: int) -> int:
    try:
        v = os.getenv(name)
        if v is None:
            return default
        n = int(str(v).strip())
        return n if n > 0 else default
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        v = os.getenv(name)
        if v is None:
            return default
        n = float(str(v).strip())
        return n if n > 0 else default
    except Exception:
        return default


# Reasonable defaults for Render + Google Sheets
SINGLE_TIMEOUT_SEC = _env_float("ENRICHED_SINGLE_TIMEOUT_SEC", 12.0)
BATCH_TIMEOUT_SEC = _env_float("ENRICHED_BATCH_TIMEOUT_SEC", 22.0)
KSA_SINGLE_TIMEOUT_SEC = _env_float("ENRICHED_KSA_SINGLE_TIMEOUT_SEC", 10.0)
KSA_BATCH_TIMEOUT_SEC = _env_float("ENRICHED_KSA_BATCH_TIMEOUT_SEC", 20.0)

# Optional KSA fallback (internal self-call)
ENABLE_KSA_FALLBACK = (os.getenv("ENRICHED_ENABLE_KSA_FALLBACK", "1").strip().lower() in {"1", "true", "yes", "on"})
KSA_FALLBACK_ROUTE = os.getenv("ENRICHED_KSA_FALLBACK_ROUTE", "/v1/argaam/quote").strip() or "/v1/argaam/quote"
KSA_FALLBACK_TIMEOUT_SEC = _env_float("ENRICHED_KSA_FALLBACK_TIMEOUT_SEC", 7.0)


async def _with_timeout(coro: Any, timeout: float, label: str) -> Any:
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail=f"Timeout while {label} (>{timeout:.1f}s)")


# =============================================================================
# Engine resolution (prefer app.state.engine; else singleton)
# =============================================================================
_ENGINE: Optional[DataEngine] = None
_ENGINE_LOCK = asyncio.Lock()


def _get_app_engine(request: Request) -> Optional[Any]:
    """
    Prefer engine created in main.py lifespan: request.app.state.engine
    Accept duck-typed engines to avoid isinstance issues on reloads.
    """
    try:
        st = getattr(getattr(request, "app", None), "state", None)
        if not st:
            return None
        for attr in ("engine", "data_engine", "data_engine_v2"):
            eng = getattr(st, attr, None)
            if eng and (hasattr(eng, "get_quote") or hasattr(eng, "get_enriched_quote")):
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


async def _resolve_engine(request: Request) -> Optional[Any]:
    eng = _get_app_engine(request)
    if eng is not None:
        return eng
    return await _get_singleton_engine()


async def _engine_get_quotes(engine: Any, syms: List[str]) -> List[UnifiedQuote]:
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


async def _engine_get_quote(engine: Any, sym: str) -> UnifiedQuote:
    if hasattr(engine, "get_quote"):
        return await engine.get_quote(sym)  # type: ignore
    res = await _engine_get_quotes(engine, [sym])
    return (res or [UnifiedQuote(symbol=normalize_symbol(sym) or sym, data_quality="MISSING", error="No data returned").finalize()])[0]


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

    # env.py exports if present
    try:
        import env as env_mod  # type: ignore
        for attr in ("APP_TOKEN", "BACKUP_APP_TOKEN"):
            v = getattr(env_mod, attr, None)
            if isinstance(v, str) and v.strip():
                tokens.append(v.strip())
    except Exception:
        pass

    # env vars last resort
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
        logger.warning("[enriched] No APP_TOKEN configured -> endpoints are OPEN (no auth).")
    return out


def _require_token(x_app_token: Optional[str]) -> str:
    """
    Returns the cleaned token (strip) so we can safely reuse it for internal fallback calls.
    """
    allowed = _allowed_tokens()
    tok = (x_app_token or "").strip()
    if not allowed:
        return tok  # open mode
    if not tok or tok not in allowed:
        raise HTTPException(status_code=401, detail="Unauthorized (invalid or missing X-APP-TOKEN).")
    return tok


# =============================================================================
# Limits helpers
# =============================================================================
def _get_int_setting(name: str, default: int) -> int:
    # settings first
    try:
        s = get_settings()
        v = getattr(s, name, None)
        if isinstance(v, int) and v > 0:
            return v
    except Exception:
        pass

    # env.py next
    try:
        import env as env_mod  # type: ignore
        v = getattr(env_mod, name, None)
        if isinstance(v, int) and v > 0:
            return v
    except Exception:
        pass

    # env vars last
    try:
        ev = os.getenv(name)
        if ev:
            n = int(ev)
            if n > 0:
                return n
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
    # pydantic v2
    try:
        return obj.model_dump(exclude_none=False)  # type: ignore
    except Exception:
        pass
    # pydantic v1
    try:
        return obj.dict()  # type: ignore
    except Exception:
        pass
    # raw
    try:
        return dict(getattr(obj, "__dict__", {}) or {})
    except Exception:
        return {}


def _build_row_payload(q: UnifiedQuote, headers: List[str]) -> Dict[str, Any]:
    eq = EnrichedQuote.from_unified(q)
    return {"headers": list(headers), "row": eq.to_row(headers), "quote": _model_to_dict(eq)}


def _chunk(items: List[str], size: int) -> List[List[str]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]


# =============================================================================
# Optional KSA fallback via internal self-call to /v1/argaam/quote (or configured route)
# =============================================================================
def _request_base_url(request: Request) -> str:
    # Prefer explicit public base if set (Render/Proxy-safe)
    base = (os.getenv("PUBLIC_BASE_URL") or os.getenv("BASE_URL") or "").strip()
    if base:
        return base.rstrip("/")
    # Fallback to request base_url
    try:
        return str(request.base_url).rstrip("/")
    except Exception:
        return ""


def _ksa_code(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if s.endswith(".SR"):
        return s.split(".", 1)[0]
    if s.isdigit():
        return s
    # last resort: normalize then strip
    n = normalize_symbol(s)
    if n.endswith(".SR"):
        return n.split(".", 1)[0]
    return s


async def _fetch_ksa_fallback(request: Request, token: str, symbol: str) -> Optional[UnifiedQuote]:
    """
    Best-effort: call internal gateway endpoint and map result to UnifiedQuote.
    Works only if the fallback route exists in your app.
    """
    if not ENABLE_KSA_FALLBACK:
        return None

    base = _request_base_url(request)
    if not base:
        return None

    code = _ksa_code(symbol)
    url = f"{base}{KSA_FALLBACK_ROUTE}"
    params = {"symbol": code}

    headers: Dict[str, str] = {}
    if token:
        headers["X-APP-TOKEN"] = token

    timeout = httpx.Timeout(KSA_FALLBACK_TIMEOUT_SEC, connect=min(5.0, KSA_FALLBACK_TIMEOUT_SEC))
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            r = await client.get(url, params=params, headers=headers)
            if r.status_code >= 400:
                return None
            data = r.json()
    except Exception:
        return None

    if not isinstance(data, dict):
        return None

    # Map common field names defensively
    cp = data.get("current_price") or data.get("last_price") or data.get("price") or data.get("close")
    pc = data.get("previous_close") or data.get("previousClose")
    ch = data.get("price_change") or data.get("change")
    chp = data.get("percent_change") or data.get("change_percent") or data.get("changePercent")
    vol = data.get("volume")

    q = UnifiedQuote(
        symbol=normalize_symbol(symbol) or symbol,
        market="KSA",
        currency=data.get("currency") or "SAR",
        name=data.get("name") or data.get("company_name"),
        current_price=float(cp) if cp is not None else None,
        previous_close=float(pc) if pc is not None else None,
        price_change=float(ch) if ch is not None else None,
        percent_change=float(chp) if chp is not None else None,
        volume=float(vol) if vol is not None else None,
        data_source="ksa_fallback",
        data_quality="PARTIAL" if cp is not None else "MISSING",
        error=None if cp is not None else "KSA fallback returned no price",
    ).finalize()
    return q if q.current_price is not None else None


# =============================================================================
# Endpoints
# =============================================================================
@router.get("/health", tags=["system"])
async def enriched_health(request: Request) -> Dict[str, Any]:
    max_t = _get_int_setting("ENRICHED_MAX_TICKERS", 250)
    batch_sz = _get_int_setting("ENRICHED_BATCH_SIZE", 40)

    eng = await _resolve_engine(request)

    providers = []
    try:
        providers = list(getattr(eng, "enabled_providers", []) or []) if eng else []
    except Exception:
        providers = []

    return {
        "status": "ok",
        "module": "routes.enriched_quote",
        "version": ROUTE_VERSION,
        "engine": "DataEngineV2",
        "providers": providers,
        "limits": {"enriched_max_tickers": max_t, "enriched_batch_size": batch_sz},
        "timeouts": {
            "single_timeout_sec": SINGLE_TIMEOUT_SEC,
            "batch_timeout_sec": BATCH_TIMEOUT_SEC,
            "ksa_single_timeout_sec": KSA_SINGLE_TIMEOUT_SEC,
            "ksa_batch_timeout_sec": KSA_BATCH_TIMEOUT_SEC,
            "ksa_fallback_enabled": ENABLE_KSA_FALLBACK,
            "ksa_fallback_route": KSA_FALLBACK_ROUTE,
            "ksa_fallback_timeout_sec": KSA_FALLBACK_TIMEOUT_SEC,
        },
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
    token = _require_token(x_app_token)

    sym_raw = (symbol or "").strip()
    if not sym_raw:
        raise HTTPException(status_code=400, detail="symbol is required")

    sym_norm = normalize_symbol(sym_raw) or sym_raw
    headers = _resolve_headers(sheet_name)

    eng = await _resolve_engine(request)
    if eng is None:
        uq = UnifiedQuote(symbol=sym_norm, data_quality="MISSING", error="Engine unavailable").finalize()
        eq = EnrichedQuote.from_unified(uq)
        if format == "quote":
            return _model_to_dict(eq)
        row = eq.to_row(headers)
        if format == "row":
            return {"symbol": eq.symbol, "headers": headers, "row": row}
        return {"sheet_name": sheet_name, "headers": headers, "row": row, "quote": _model_to_dict(eq)}

    t0 = time.perf_counter()

    # Apply KSA vs Global timeout
    timeout = KSA_SINGLE_TIMEOUT_SEC if is_ksa_symbol(sym_norm) else SINGLE_TIMEOUT_SEC

    try:
        uq: UnifiedQuote = await _with_timeout(_engine_get_quote(eng, sym_raw), timeout, f"fetching quote for {sym_norm}")
    except HTTPException as he:
        # If timed out and KSA, try fallback
        if he.status_code == 504 and is_ksa_symbol(sym_norm):
            fb = await _fetch_ksa_fallback(request, token, sym_norm)
            if fb is not None:
                uq = fb
            else:
                uq = UnifiedQuote(symbol=sym_norm, data_quality="MISSING", error=he.detail).finalize()
        else:
            uq = UnifiedQuote(symbol=sym_norm, data_quality="MISSING", error=str(he.detail)).finalize()
    except Exception as exc:
        uq = UnifiedQuote(symbol=sym_norm, data_quality="MISSING", error=str(exc)).finalize()

    dt_ms = int((time.perf_counter() - t0) * 1000)

    if format == "quote":
        out = _model_to_dict(EnrichedQuote.from_unified(uq))
        out["_meta"] = {"elapsed_ms": dt_ms}
        return out

    payload = _build_row_payload(uq, headers)
    payload["_meta"] = {"elapsed_ms": dt_ms}

    if format == "row":
        return {
            "symbol": payload["quote"].get("symbol", sym_norm),
            "headers": payload["headers"],
            "row": payload["row"],
            "_meta": payload["_meta"],
        }

    payload["sheet_name"] = sheet_name
    return payload


@router.post("/quotes")
async def enriched_quotes(
    request: Request,
    req: Any = Body(...),
    format: Literal["rows", "quotes", "both"] = Query(default="rows", description="Return rows, quotes, or both."),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> Dict[str, Any]:
    token = _require_token(x_app_token)

    # Accept core.schemas.BatchProcessRequest if available, else fallback model
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
                quotes_out.append(_model_to_dict(eq))
        resp: Dict[str, Any] = {
            "status": status,
            "error": error_msg,
            "operation": operation,
            "sheet_name": sheet_name,
            "count": len(symbols),
            "symbols": symbols,
            "headers": headers,
        }
        if format in ("rows", "both"):
            resp["rows"] = rows_out
        if format in ("quotes", "both"):
            resp["quotes"] = quotes_out
        return resp

    rows_out: List[List[Any]] = []
    quotes_out: List[Dict[str, Any]] = []

    t0 = time.perf_counter()

    # Decide timeout: if chunk has any KSA symbols, use KSA batch timeout
    def _chunk_timeout(chunk_syms: List[str]) -> float:
        return KSA_BATCH_TIMEOUT_SEC if any(is_ksa_symbol(s) for s in chunk_syms) else BATCH_TIMEOUT_SEC

    # Soft batching
    for chunk in _chunk(symbols, batch_sz):
        try:
            quotes: List[UnifiedQuote] = await _with_timeout(
                _engine_get_quotes(eng, chunk),
                _chunk_timeout(chunk),
                f"fetching batch of {len(chunk)} quotes",
            )
        except HTTPException as he:
            status = "partial"
            error_msg = (error_msg + " " if error_msg else "") + str(he.detail)

            # Salvage: per-symbol with single timeouts + optional KSA fallback
            quotes = []
            for s in chunk:
                try:
                    tout = KSA_SINGLE_TIMEOUT_SEC if is_ksa_symbol(s) else SINGLE_TIMEOUT_SEC
                    uq = await _with_timeout(_engine_get_quote(eng, s), tout, f"fetching quote for {s}")
                    quotes.append(uq)
                except HTTPException as he2:
                    if he2.status_code == 504 and is_ksa_symbol(s):
                        fb = await _fetch_ksa_fallback(request, token, s)
                        if fb is not None:
                            quotes.append(fb)
                            continue
                    quotes.append(UnifiedQuote(symbol=s, data_quality="MISSING", error=str(he2.detail)).finalize())
                except Exception as exc2:
                    quotes.append(UnifiedQuote(symbol=s, data_quality="MISSING", error=str(exc2)).finalize())

        except Exception as exc:
            status = "partial"
            error_msg = (error_msg + " " if error_msg else "") + f"Batch error: {exc}"
            quotes = [UnifiedQuote(symbol=s, data_quality="MISSING", error=str(exc)).finalize() for s in chunk]

        # Preserve order by symbol
        m = {getattr(q, "symbol", "").upper(): q for q in (quotes or []) if getattr(q, "symbol", None)}
        for s in chunk:
            uq = m.get(s.upper()) or UnifiedQuote(symbol=s.upper(), data_quality="MISSING", error="No data returned").finalize()
            eq = EnrichedQuote.from_unified(uq)
            if format in ("rows", "both"):
                rows_out.append(eq.to_row(headers))
            if format in ("quotes", "both"):
                quotes_out.append(_model_to_dict(eq))

    dt_ms = int((time.perf_counter() - t0) * 1000)

    resp: Dict[str, Any] = {
        "status": status,
        "error": error_msg,
        "operation": operation,
        "sheet_name": sheet_name,
        "count": len(symbols),
        "symbols": symbols,
        "headers": headers,
        "_meta": {"elapsed_ms": dt_ms},
    }
    if format in ("rows", "both"):
        resp["rows"] = rows_out
    if format in ("quotes", "both"):
        resp["quotes"] = quotes_out

    return resp


__all__ = ["router"]
