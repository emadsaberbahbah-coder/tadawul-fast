# routes/enriched_quote.py
"""
routes/enriched_quote.py
===========================================================
Enriched Quote Routes (Google Sheets + API) - v2.4.0 (PROD SAFE)

Stable endpoints for Google Sheets / Apps Script:
    GET  /v1/enriched/quote?symbol=1120.SR
    POST /v1/enriched/quotes   {symbols:[...], sheet_name?: "..."}
    GET  /v1/enriched/headers?sheet_name=...
    GET  /v1/enriched/health

Design goals:
- Router must MOUNT even if optional modules are missing (Render-safe).
- Token guard via X-APP-TOKEN (APP_TOKEN / BACKUP_APP_TOKEN). If no token set => open mode.
- Defensive limits (ENRICHED_MAX_TICKERS, ENRICHED_BATCH_SIZE, ENRICHED_CONCURRENCY).
- Engine resolution:
    • prefer request.app.state.engine (or aliases), else safe singleton here.
- /quotes truncates to max and returns status="partial" (Sheets-friendly).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from functools import lru_cache
from importlib import import_module
from typing import Any, Dict, List, Optional, Sequence, Literal

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field
from starlette.responses import JSONResponse

logger = logging.getLogger("routes.enriched_quote")

ROUTE_VERSION = "2.4.0"
router = APIRouter(prefix="/v1/enriched", tags=["enriched"])


# =============================================================================
# Safe imports (do NOT break router mounting)
# =============================================================================
def _safe_import(path: str) -> Optional[Any]:
    try:
        return import_module(path)
    except Exception:
        return None


# Settings: prefer core.config, fallback to config, then env vars
_get_settings = None
for _p in ("core.config", "config"):
    _m = _safe_import(_p)
    if _m and hasattr(_m, "get_settings"):
        _get_settings = getattr(_m, "get_settings")
        break


def _settings_obj() -> Any:
    if _get_settings:
        try:
            return _get_settings()
        except Exception:
            return None
    return None


# Data engine & symbol normalizer (try v2 then legacy)
DataEngine = None
UnifiedQuote = None
normalize_symbol = None

for _p in ("core.data_engine_v2", "core.data_engine"):
    _m = _safe_import(_p)
    if not _m:
        continue
    if DataEngine is None and hasattr(_m, "DataEngine"):
        DataEngine = getattr(_m, "DataEngine")
    if UnifiedQuote is None and hasattr(_m, "UnifiedQuote"):
        UnifiedQuote = getattr(_m, "UnifiedQuote")
    if normalize_symbol is None and hasattr(_m, "normalize_symbol"):
        normalize_symbol = getattr(_m, "normalize_symbol")
    if DataEngine and UnifiedQuote and normalize_symbol:
        break


def _norm(sym: str) -> str:
    s = (sym or "").strip()
    if not s:
        return ""
    if normalize_symbol:
        try:
            return str(normalize_symbol(s)).strip()
        except Exception:
            return s
    return s


# EnrichedQuote: try core.enriched_quote then root enriched_quote, else fallback
EnrichedQuote = None
for _p in ("core.enriched_quote", "enriched_quote"):
    _m = _safe_import(_p)
    if _m and hasattr(_m, "EnrichedQuote"):
        EnrichedQuote = getattr(_m, "EnrichedQuote")
        break


class _FallbackEnrichedQuote(BaseModel):
    model_config = ConfigDict(extra="ignore")
    symbol: str = ""
    data_quality: str = "MISSING"
    error: Optional[str] = None

    @classmethod
    def from_unified(cls, uq: Any) -> "_FallbackEnrichedQuote":
        d = _model_to_dict(uq)
        return cls(
            symbol=str(d.get("symbol") or d.get("ticker") or ""),
            data_quality=str(d.get("data_quality") or "MISSING"),
            error=d.get("error"),
        )

    def to_row(self, headers: List[str]) -> List[Any]:
        d = self.model_dump()
        row: List[Any] = []
        for h in headers:
            key = str(h).strip()
            if not key:
                row.append("")
                continue
            k = key.lower()
            if k in ("symbol", "ticker"):
                row.append(self.symbol)
            elif k == "error":
                row.append(self.error or "")
            elif k == "data_quality":
                row.append(self.data_quality)
            else:
                row.append(d.get(key) or d.get(k) or "")
        return row


# Optional core.schemas helpers
BatchProcessRequest = None
get_headers_for_sheet = None
_m = _safe_import("core.schemas")
if _m:
    BatchProcessRequest = getattr(_m, "BatchProcessRequest", None)
    get_headers_for_sheet = getattr(_m, "get_headers_for_sheet", None)


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
    # Minimal but safe. If you want more default columns, expand here.
    return ["Symbol", "Data_Quality", "Error"]


def _resolve_headers(sheet_name: Optional[str]) -> List[str]:
    if get_headers_for_sheet:
        try:
            h = get_headers_for_sheet(sheet_name)
            if isinstance(h, list) and h:
                # Must include Symbol
                if any(str(x).strip().lower() == "symbol" for x in h):
                    return [str(x) for x in h]
        except Exception:
            pass
    return _fallback_headers()


# =============================================================================
# Engine resolution (prefer app.state.engine; else singleton)
# =============================================================================
_ENGINE: Optional[Any] = None
_ENGINE_LOCK = asyncio.Lock()


def _looks_like_engine(obj: Any) -> bool:
    if obj is None:
        return False
    return any(hasattr(obj, m) for m in ("get_enriched_quotes", "get_quotes", "get_quote", "get_enriched_quote"))


def _get_app_engine(request: Request) -> Optional[Any]:
    try:
        st = getattr(getattr(request, "app", None), "state", None)
        if not st:
            return None
        for attr in ("engine", "data_engine", "data_engine_v2"):
            eng = getattr(st, attr, None)
            if _looks_like_engine(eng):
                return eng
        return None
    except Exception:
        return None


async def _get_singleton_engine() -> Optional[Any]:
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE

    async with _ENGINE_LOCK:
        if _ENGINE is not None:
            return _ENGINE

        if DataEngine is None:
            logger.error("[enriched] DataEngine import missing -> engine unavailable.")
            _ENGINE = None
            return None

        try:
            _ENGINE = DataEngine()  # type: ignore
            logger.info("[enriched] Engine initialized (routes singleton).")
        except Exception as exc:
            logger.exception("[enriched] Failed to init engine: %s", exc)
            _ENGINE = None

    return _ENGINE


async def _resolve_engine(request: Request) -> Optional[Any]:
    eng = _get_app_engine(request)
    if eng is not None:
        return eng
    return await _get_singleton_engine()


# =============================================================================
# Helpers
# =============================================================================
def _model_to_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
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
    # dataclass-like / plain object
    try:
        return dict(getattr(obj, "__dict__", {}) or {})
    except Exception:
        return {}


def _get_int_setting(name: str, default: int) -> int:
    s = _settings_obj()
    if s is not None:
        try:
            v = getattr(s, name, None)
            if isinstance(v, int) and v > 0:
                return v
        except Exception:
            pass

    # env.py legacy exports if present
    _envm = _safe_import("env")
    if _envm is not None:
        try:
            v = getattr(_envm, name, None)
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


def _clean_symbols(symbols: Sequence[Any]) -> List[str]:
    out: List[str] = []
    seen = set()
    for x in symbols or []:
        if x is None:
            continue
        s = _norm(str(x))
        if not s:
            continue
        su = s.upper()
        if su in seen:
            continue
        seen.add(su)
        out.append(su)
    return out


def _chunk(items: List[str], size: int) -> List[List[str]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]


def _make_eq(uq: Any) -> Any:
    if EnrichedQuote:
        try:
            return EnrichedQuote.from_unified(uq)  # type: ignore
        except Exception:
            pass
    # fallback
    return _FallbackEnrichedQuote.from_unified(uq)


def _eq_to_row(eq: Any, headers: List[str]) -> List[Any]:
    try:
        return eq.to_row(headers)  # type: ignore
    except Exception:
        return _FallbackEnrichedQuote(**_model_to_dict(eq)).to_row(headers)


def _eq_to_dict(eq: Any) -> Dict[str, Any]:
    return _model_to_dict(eq)


# =============================================================================
# Auth (X-APP-TOKEN) => open mode if no token configured
# =============================================================================
@lru_cache(maxsize=1)
def _allowed_tokens() -> List[str]:
    tokens: List[str] = []

    s = _settings_obj()
    if s is not None:
        for attr in ("app_token", "backup_app_token", "APP_TOKEN", "BACKUP_APP_TOKEN"):
            try:
                v = getattr(s, attr, None)
                if isinstance(v, str) and v.strip():
                    tokens.append(v.strip())
            except Exception:
                pass

    _envm = _safe_import("env")
    if _envm is not None:
        for attr in ("APP_TOKEN", "BACKUP_APP_TOKEN"):
            try:
                v = getattr(_envm, attr, None)
                if isinstance(v, str) and v.strip():
                    tokens.append(v.strip())
            except Exception:
                pass

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


def _require_token(x_app_token: Optional[str]) -> None:
    allowed = _allowed_tokens()
    if not allowed:
        return  # open mode
    if not x_app_token or x_app_token.strip() not in allowed:
        raise HTTPException(status_code=401, detail="Unauthorized (invalid or missing X-APP-TOKEN).")


# =============================================================================
# Engine call shims
# =============================================================================
async def _engine_get_quotes(engine: Any, syms: List[str]) -> List[Any]:
    # prefer batch methods
    if hasattr(engine, "get_enriched_quotes"):
        return await engine.get_enriched_quotes(syms)  # type: ignore
    if hasattr(engine, "get_quotes"):
        return await engine.get_quotes(syms)  # type: ignore

    # fallback: gather per-symbol
    conc = _get_int_setting("ENRICHED_CONCURRENCY", 8)
    sem = asyncio.Semaphore(max(1, conc))

    async def _one(s: str) -> Any:
        async with sem:
            return await _engine_get_quote(engine, s)

    return list(await asyncio.gather(*[_one(s) for s in syms], return_exceptions=False))


async def _engine_get_quote(engine: Any, sym: str) -> Any:
    if hasattr(engine, "get_quote"):
        return await engine.get_quote(sym)  # type: ignore
    if hasattr(engine, "get_enriched_quote"):
        return await engine.get_enriched_quote(sym)  # type: ignore
    # last resort: batch one
    res = await _engine_get_quotes(engine, [sym])
    if res:
        return res[0]
    # construct minimal fallback UnifiedQuote-like dict
    return {"symbol": _norm(sym) or sym, "data_quality": "MISSING", "error": "No data returned"}


# =============================================================================
# Endpoints
# =============================================================================
@router.get("/health", tags=["system"])
async def enriched_health(request: Request) -> Dict[str, Any]:
    max_t = _get_int_setting("ENRICHED_MAX_TICKERS", 250)
    batch_sz = _get_int_setting("ENRICHED_BATCH_SIZE", 40)
    conc = _get_int_setting("ENRICHED_CONCURRENCY", 8)

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
        "engine_available": bool(eng),
        "providers": providers,
        "limits": {"enriched_max_tickers": max_t, "enriched_batch_size": batch_sz, "enriched_concurrency": conc},
        "auth": "open" if not _allowed_tokens() else "token",
    }


@router.get("/headers")
async def enriched_headers(
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

    sym = _norm(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="symbol is required")

    eng = await _resolve_engine(request)
    headers = _resolve_headers(sheet_name)

    if eng is None:
        eq = _make_eq({"symbol": sym, "data_quality": "MISSING", "error": "Engine unavailable"})
        if format == "quote":
            return _eq_to_dict(eq)
        row = _eq_to_row(eq, headers)
        if format == "row":
            return {"symbol": sym, "headers": headers, "row": row}
        return {"sheet_name": sheet_name, "headers": headers, "row": row, "quote": _eq_to_dict(eq)}

    try:
        uq = await _engine_get_quote(eng, sym)
    except Exception as exc:
        uq = {"symbol": sym, "data_quality": "MISSING", "error": str(exc)}

    eq = _make_eq(uq)

    if format == "quote":
        return _eq_to_dict(eq)

    row = _eq_to_row(eq, headers)

    if format == "row":
        return {"symbol": sym, "headers": headers, "row": row}

    return {"sheet_name": sheet_name, "headers": headers, "row": row, "quote": _eq_to_dict(eq)}


@router.post("/quotes")
async def enriched_quotes(
    request: Request,
    req: Any = Body(...),
    format: Literal["rows", "quotes", "both"] = Query(default="rows", description="Return rows, quotes, or both."),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> Dict[str, Any]:
    _require_token(x_app_token)

    Model = BatchProcessRequest or _FallbackBatchProcessRequest  # type: ignore

    # parse request safely
    parsed = None
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
    rows_out: List[List[Any]] = []
    quotes_out: List[Dict[str, Any]] = []

    if eng is None:
        status = "error" if status == "success" else status
        error_msg = (error_msg + " " if error_msg else "") + "Engine unavailable"
        for s in symbols:
            eq = _make_eq({"symbol": s, "data_quality": "MISSING", "error": "Engine unavailable"})
            if format in ("rows", "both"):
                rows_out.append(_eq_to_row(eq, headers))
            if format in ("quotes", "both"):
                quotes_out.append(_eq_to_dict(eq))

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

    # batched retrieval
    for chunk in _chunk(symbols, batch_sz):
        try:
            got = await _engine_get_quotes(eng, chunk)
        except Exception as exc:
            status = "partial"
            error_msg = (error_msg + " " if error_msg else "") + f"Batch error: {exc}"
            got = [{"symbol": s, "data_quality": "MISSING", "error": str(exc)} for s in chunk]

        # map by symbol upper
        m: Dict[str, Any] = {}
        for q in got or []:
            d = _model_to_dict(q)
            k = str(d.get("symbol") or d.get("ticker") or "").upper()
            if k:
                m[k] = q

        for s in chunk:
            uq = m.get(s.upper()) or {"symbol": s.upper(), "data_quality": "MISSING", "error": "No data returned"}
            eq = _make_eq(uq)

            if format in ("rows", "both"):
                rows_out.append(_eq_to_row(eq, headers))
            if format in ("quotes", "both"):
                quotes_out.append(_eq_to_dict(eq))

    resp2: Dict[str, Any] = {
        "status": status,
        "error": error_msg,
        "operation": operation,
        "sheet_name": sheet_name,
        "count": len(symbols),
        "symbols": symbols,
        "headers": headers,
    }
    if format in ("rows", "both"):
        resp2["rows"] = rows_out
    if format in ("quotes", "both"):
        resp2["quotes"] = quotes_out

    return resp2


__all__ = ["router"]
