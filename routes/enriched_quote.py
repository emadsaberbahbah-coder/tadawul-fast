# routes/enriched_quote.py
"""
routes/enriched_quote.py
===========================================================
Enriched Quote Routes (Google Sheets + API) - v2.2.0 (PROD SAFE)

Goals
- Stable endpoints for Google Sheets / Apps Script:
    GET  /v1/enriched/quote?symbol=1120.SR
    POST /v1/enriched/quotes   {symbols:[...], sheet_name?: "..."}
- Return:
    - quote JSON (EnrichedQuote fields)
    - optional Google Sheets row aligned to headers (typically 59 columns)
    - headers helper endpoint
- Token guard via X-APP-TOKEN (APP_TOKEN / BACKUP_APP_TOKEN). If no token is set => open mode.
- Defensive limits (ENRICHED_MAX_TICKERS, ENRICHED_BATCH_SIZE).
- Engine resolution:
    • prefer request.app.state.engine (created by main.py lifespan), else a safe singleton here.
"""

from __future__ import annotations

import asyncio
import logging
import os
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Literal

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request

from core.config import get_settings
from core.data_engine_v2 import DataEngine, UnifiedQuote, normalize_symbol
from core.enriched_quote import EnrichedQuote
from core.schemas import BatchProcessRequest, get_headers_for_sheet

logger = logging.getLogger("routes.enriched_quote")

ROUTE_VERSION = "2.2.0"
router = APIRouter(prefix="/v1/enriched", tags=["enriched"])


# =============================================================================
# Engine resolution (prefer app.state.engine; else singleton)
# =============================================================================

_ENGINE: Optional[DataEngine] = None
_ENGINE_LOCK = asyncio.Lock()


def _get_app_engine(request: Request) -> Optional[DataEngine]:
    """
    Prefer engine created in main.py lifespan:
      request.app.state.engine
    Also accept a few aliases if you used different names in main.py.
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


async def _get_singleton_engine() -> DataEngine:
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE
    async with _ENGINE_LOCK:
        if _ENGINE is None:
            _ENGINE = DataEngine()
            logger.info("[enriched] DataEngine initialized (routes singleton).")
    return _ENGINE


async def _resolve_engine(request: Request) -> DataEngine:
    eng = _get_app_engine(request)
    if eng is not None:
        return eng
    return await _get_singleton_engine()


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

    # Also support env.py exports if present
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


def _require_token(x_app_token: Optional[str]) -> None:
    allowed = _allowed_tokens()
    if not allowed:
        return  # open mode
    if not x_app_token or x_app_token.strip() not in allowed:
        raise HTTPException(status_code=401, detail="Unauthorized (invalid or missing X-APP-TOKEN).")


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
    return {
        "headers": list(headers),
        "row": eq.to_row(headers),
        "quote": _model_to_dict(eq),
    }


# =============================================================================
# Endpoints
# =============================================================================

@router.get("/health", tags=["system"])
async def enriched_health(request: Request) -> Dict[str, Any]:
    """
    Lightweight health endpoint for Apps Script & Render checks.
    """
    max_t = _get_int_setting("ENRICHED_MAX_TICKERS", 250)
    batch_sz = _get_int_setting("ENRICHED_BATCH_SIZE", 40)

    eng = await _resolve_engine(request)

    return {
        "status": "ok",
        "module": "routes.enriched_quote",
        "version": ROUTE_VERSION,
        "engine": "DataEngineV2",
        "providers": list(getattr(eng, "enabled_providers", []) or []),
        "limits": {"enriched_max_tickers": max_t, "enriched_batch_size": batch_sz},
        "auth": "open" if not _allowed_tokens() else "token",
    }


@router.get("/headers")
async def get_headers(
    request: Request,
    sheet_name: Optional[str] = Query(default=None, description="Optional sheet name to resolve headers."),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> Dict[str, Any]:
    _require_token(x_app_token)
    headers = get_headers_for_sheet(sheet_name)
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

    eng = await _resolve_engine(request)
    q = await eng.get_quote(sym)

    if format == "quote":
        return _model_to_dict(EnrichedQuote.from_unified(q))

    headers = get_headers_for_sheet(sheet_name)
    payload = _build_row_payload(q, headers)

    if format == "row":
        return {
            "symbol": payload["quote"].get("symbol", normalize_symbol(sym) or sym),
            "headers": payload["headers"],
            "row": payload["row"],
        }

    payload["sheet_name"] = sheet_name
    return payload


@router.post("/quotes")
async def enriched_quotes(
    request: Request,
    req: BatchProcessRequest = Body(...),
    format: Literal["rows", "quotes", "both"] = Query(default="rows", description="Return rows, quotes, or both."),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> Dict[str, Any]:
    _require_token(x_app_token)

    symbols = _clean_symbols(req.symbols)
    headers = get_headers_for_sheet(req.sheet_name)

    if not symbols:
        return {"operation": req.operation, "sheet_name": req.sheet_name, "count": 0, "symbols": [], "headers": headers, "rows": []}

    max_t = _get_int_setting("ENRICHED_MAX_TICKERS", 250)
    batch_sz = _get_int_setting("ENRICHED_BATCH_SIZE", 40)

    if len(symbols) > max_t:
        raise HTTPException(status_code=400, detail=f"Too many symbols ({len(symbols)}). Max allowed is {max_t}.")

    eng = await _resolve_engine(request)

    rows_out: List[List[Any]] = []
    quotes_out: List[Dict[str, Any]] = []

    # Soft batching (keeps memory stable)
    for i in range(0, len(symbols), batch_sz):
        chunk = symbols[i : i + batch_sz]
        quotes = await eng.get_quotes(chunk)

        if format in ("rows", "both"):
            for q in quotes:
                eq = EnrichedQuote.from_unified(q)
                rows_out.append(eq.to_row(headers))

        if format in ("quotes", "both"):
            for q in quotes:
                quotes_out.append(_model_to_dict(EnrichedQuote.from_unified(q)))

    resp: Dict[str, Any] = {
        "operation": req.operation,
        "sheet_name": req.sheet_name,
        "count": len(symbols),
        "symbols": symbols,
        "headers": headers,
    }

    if format in ("rows", "both"):
        resp["rows"] = rows_out
    if format in ("quotes", "both"):
        resp["quotes"] = quotes_out

    return resp


__all__ = ["router"]
