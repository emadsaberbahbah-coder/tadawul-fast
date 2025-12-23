# routes/enriched_quote.py
"""
routes/enriched_quote.py
===========================================================
Enriched Quote Routes (Google Sheets + API) - v2.5.0 (PROD SAFE)

Stable endpoints for Google Sheets / Apps Script:
    GET  /v1/enriched/quote?symbol=1120.SR
    POST /v1/enriched/quotes   {symbols:[...], tickers:[...], sheet_name?: "..."}
    GET  /v1/enriched/headers?sheet_name=...
    GET  /v1/enriched/health

Design goals:
- Router MUST MOUNT even if optional modules are missing (Render-safe).
- Token guard via X-APP-TOKEN (APP_TOKEN / BACKUP_APP_TOKEN). If no token set => open mode.
- Defensive limits (ENRICHED_MAX_TICKERS, ENRICHED_BATCH_SIZE, ENRICHED_CONCURRENCY, ENRICHED_TIMEOUT_SEC).
- Engine resolution:
    • prefer request.app.state.engine (or aliases), else safe singleton here.
- Sheets-friendly:
    • /quotes NEVER raises for normal usage; returns status + headers + rows (+ optional quotes)
    • partial failures return placeholder rows with Error filled.
"""

from __future__ import annotations

import asyncio
import logging
import os
from functools import lru_cache
from importlib import import_module
from typing import Any, Dict, List, Optional, Sequence, Tuple, Literal

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger("routes.enriched_quote")

ROUTE_VERSION = "2.5.0"
router = APIRouter(prefix="/v1/enriched", tags=["enriched"])


# =============================================================================
# Safe imports (do NOT break router mounting)
# =============================================================================
def _safe_import(path: str) -> Optional[Any]:
    try:
        return import_module(path)
    except Exception:
        return None


# settings
_get_settings = None
for _p in ("core.config", "routes.config", "config"):
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


# engine + normalizer (prefer v2, fallback legacy)
DataEngine = None
UnifiedQuote = None
normalize_symbol = None
for _p in ("core.data_engine_v2", "core.data_engine"):
    _m = _safe_import(_p)
    if not _m:
        continue
    DataEngine = DataEngine or getattr(_m, "DataEngine", None)
    UnifiedQuote = UnifiedQuote or getattr(_m, "UnifiedQuote", None)
    normalize_symbol = normalize_symbol or getattr(_m, "normalize_symbol", None)
    if DataEngine and UnifiedQuote and normalize_symbol:
        break


def _norm_symbol(sym: Any) -> str:
    s = str(sym or "").strip()
    if not s:
        return ""
    if normalize_symbol:
        try:
            return str(normalize_symbol(s)).strip()
        except Exception:
            return s
    return s


# EnrichedQuote model (preferred) — but never crash if missing
EnrichedQuote = None
for _p in ("core.enriched_quote", "enriched_quote"):
    _m = _safe_import(_p)
    if _m and hasattr(_m, "EnrichedQuote"):
        EnrichedQuote = getattr(_m, "EnrichedQuote")
        break


# schemas helpers (preferred)
BatchProcessRequest = None
get_headers_for_sheet = None
_schemas = _safe_import("core.schemas")
if _schemas:
    BatchProcessRequest = getattr(_schemas, "BatchProcessRequest", None)
    get_headers_for_sheet = getattr(_schemas, "get_headers_for_sheet", None)


# =============================================================================
# Fallbacks (models + headers)
# =============================================================================
_DEFAULT_HEADERS_59_FALLBACK: List[str] = [
    "Symbol", "Company Name", "Sector", "Sub-Sector", "Market", "Currency", "Listing Date",
    "Last Price", "Previous Close", "Price Change", "Percent Change", "Day High", "Day Low",
    "52W High", "52W Low", "52W Position %", "Volume", "Avg Volume (30D)", "Value Traded",
    "Turnover %", "Shares Outstanding", "Free Float %", "Market Cap", "Free Float Market Cap",
    "Liquidity Score", "EPS (TTM)", "Forward EPS", "P/E (TTM)", "Forward P/E", "P/B", "P/S",
    "EV/EBITDA", "Dividend Yield %", "Dividend Rate", "Payout Ratio %", "ROE %", "ROA %",
    "Net Margin %", "EBITDA Margin %", "Revenue Growth %", "Net Income Growth %", "Beta",
    "Volatility (30D)", "RSI (14)", "Fair Value", "Upside %", "Valuation Label",
    "Value Score", "Quality Score", "Momentum Score", "Opportunity Score", "Risk Score",
    "Overall Score", "Error", "Recommendation", "Data Source", "Data Quality",
    "Last Updated (UTC)", "Last Updated (Riyadh)",
]


class _FallbackBatchProcessRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    operation: str = "refresh"
    sheet_name: Optional[str] = None
    symbols: List[str] = Field(default_factory=list)
    tickers: List[str] = Field(default_factory=list)  # alias support


class _FallbackEnrichedQuote(BaseModel):
    model_config = ConfigDict(extra="ignore")
    symbol: str = ""
    data_quality: str = "MISSING"
    data_source: str = "none"
    error: Optional[str] = None

    @classmethod
    def from_unified(cls, uq: Any) -> "_FallbackEnrichedQuote":
        d = _model_to_dict(uq)
        return cls(
            symbol=str(d.get("symbol") or d.get("ticker") or ""),
            data_quality=str(d.get("data_quality") or "MISSING"),
            data_source=str(d.get("data_source") or d.get("provider") or "none"),
            error=d.get("error"),
        )

    def to_row(self, headers: List[str]) -> List[Any]:
        # Minimal mapping, but safe for all headers
        d = self.model_dump(exclude_none=False)
        out: List[Any] = []
        for h in headers:
            k = str(h or "").strip()
            lk = k.lower()
            if lk in ("symbol", "ticker"):
                out.append(self.symbol)
            elif lk in ("data quality", "data_quality"):
                out.append(self.data_quality)
            elif lk in ("data source", "data_source"):
                out.append(self.data_source)
            elif lk == "error":
                out.append(self.error or "")
            else:
                out.append(d.get(k) or d.get(lk) or "")
        return out


def _resolve_headers(sheet_name: Optional[str]) -> List[str]:
    if get_headers_for_sheet:
        try:
            h = get_headers_for_sheet(sheet_name)
            if isinstance(h, list) and h and any(str(x).strip().lower() == "symbol" for x in h):
                return [str(x) for x in h]
        except Exception:
            pass
    return list(_DEFAULT_HEADERS_59_FALLBACK)


# =============================================================================
# General helpers
# =============================================================================
def _model_to_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
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


def _get_int(name: str, default: int) -> int:
    # settings attr
    s = _settings_obj()
    if s is not None:
        try:
            v = getattr(s, name, None)
            if isinstance(v, int) and v > 0:
                return v
        except Exception:
            pass

    # env.py exports (legacy)
    env_mod = _safe_import("env")
    if env_mod is not None:
        try:
            v = getattr(env_mod, name, None)
            if isinstance(v, int) and v > 0:
                return v
        except Exception:
            pass

    # env vars
    try:
        ev = os.getenv(name)
        if ev:
            iv = int(str(ev).strip())
            if iv > 0:
                return iv
    except Exception:
        pass

    return default


def _get_float(name: str, default: float) -> float:
    s = _settings_obj()
    if s is not None:
        try:
            v = getattr(s, name, None)
            if isinstance(v, (int, float)) and float(v) > 0:
                return float(v)
        except Exception:
            pass
    try:
        ev = os.getenv(name)
        if ev:
            fv = float(str(ev).strip())
            if fv > 0:
                return fv
    except Exception:
        pass
    return default


def _clean_symbols(symbols: Sequence[Any]) -> List[str]:
    out: List[str] = []
    seen = set()
    for x in symbols or []:
        if x is None:
            continue
        s = _norm_symbol(x)
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
    return _FallbackEnrichedQuote.from_unified(uq)


def _eq_to_row(eq: Any, headers: List[str]) -> List[Any]:
    try:
        return eq.to_row(headers)  # type: ignore
    except Exception:
        fb = _FallbackEnrichedQuote(**_model_to_dict(eq))
        return fb.to_row(headers)


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

    env_mod = _safe_import("env")
    if env_mod is not None:
        for attr in ("APP_TOKEN", "BACKUP_APP_TOKEN"):
            try:
                v = getattr(env_mod, attr, None)
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
# Engine call shims (with optional timeout)
# =============================================================================
async def _engine_get_quote(engine: Any, sym: str, timeout_sec: float) -> Any:
    async def _call() -> Any:
        if hasattr(engine, "get_quote"):
            return await engine.get_quote(sym)  # type: ignore
        if hasattr(engine, "get_enriched_quote"):
            return await engine.get_enriched_quote(sym)  # type: ignore
        # last resort: batch 1
        res = await _engine_get_quotes(engine, [sym], timeout_sec=timeout_sec)
        return res[0] if res else {"symbol": sym, "data_quality": "MISSING", "error": "No data returned"}

    try:
        return await asyncio.wait_for(_call(), timeout=timeout_sec)
    except Exception as exc:
        return {"symbol": sym, "data_quality": "MISSING", "error": f"Engine quote error: {exc}"}


async def _engine_get_quotes(engine: Any, syms: List[str], timeout_sec: float) -> List[Any]:
    async def _call() -> List[Any]:
        if hasattr(engine, "get_enriched_quotes"):
            return await engine.get_enriched_quotes(syms)  # type: ignore
        if hasattr(engine, "get_quotes"):
            return await engine.get_quotes(syms)  # type: ignore

        conc = _get_int("ENRICHED_CONCURRENCY", 8)
        sem = asyncio.Semaphore(max(1, conc))

        async def _one(s: str) -> Any:
            async with sem:
                return await _engine_get_quote(engine, s, timeout_sec=timeout_sec)

        return list(await asyncio.gather(*[_one(s) for s in syms], return_exceptions=False))

    try:
        return await asyncio.wait_for(_call(), timeout=timeout_sec)
    except Exception as exc:
        return [{"symbol": s, "data_quality": "MISSING", "error": f"Engine batch error: {exc}"} for s in syms]


# =============================================================================
# Endpoints
# =============================================================================
@router.get("/health", tags=["system"])
async def enriched_health(request: Request) -> Dict[str, Any]:
    max_t = _get_int("ENRICHED_MAX_TICKERS", 250)
    batch_sz = _get_int("ENRICHED_BATCH_SIZE", 40)
    conc = _get_int("ENRICHED_CONCURRENCY", 8)
    timeout_sec = _get_float("ENRICHED_TIMEOUT_SEC", 45.0)

    eng = await _resolve_engine(request)
    providers: List[str] = []
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
        "limits": {
            "ENRICHED_MAX_TICKERS": max_t,
            "ENRICHED_BATCH_SIZE": batch_sz,
            "ENRICHED_CONCURRENCY": conc,
            "ENRICHED_TIMEOUT_SEC": timeout_sec,
        },
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

    sym = _norm_symbol(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="symbol is required")

    headers = _resolve_headers(sheet_name)
    timeout_sec = _get_float("ENRICHED_TIMEOUT_SEC", 45.0)

    eng = await _resolve_engine(request)
    if eng is None:
        eq = _make_eq({"symbol": sym, "data_quality": "MISSING", "error": "Engine unavailable"})
        if format == "quote":
            return _eq_to_dict(eq)
        row = _eq_to_row(eq, headers)
        if format == "row":
            return {"symbol": sym, "headers": headers, "row": row}
        return {"sheet_name": sheet_name, "headers": headers, "row": row, "quote": _eq_to_dict(eq)}

    uq = await _engine_get_quote(eng, sym, timeout_sec=timeout_sec)
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
    payload: Any = Body(...),
    format: Literal["rows", "quotes", "both"] = Query(default="rows", description="Return rows, quotes, or both."),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> Dict[str, Any]:
    """
    Sheets-safe: returns HTTP 200 always for normal usage.
    status: success | partial | error | skipped
    """
    _require_token(x_app_token)

    Model = BatchProcessRequest or _FallbackBatchProcessRequest  # type: ignore

    # parse safely (pydantic v2 then v1)
    parsed: Any
    try:
        parsed = payload if isinstance(payload, Model) else Model.model_validate(payload)  # type: ignore
    except Exception:
        try:
            parsed = Model.parse_obj(payload)  # type: ignore
        except Exception:
            parsed = _FallbackBatchProcessRequest()

    operation = str(getattr(parsed, "operation", "refresh") or "refresh")
    sheet_name = getattr(parsed, "sheet_name", None)

    raw_syms = list(getattr(parsed, "symbols", []) or []) + list(getattr(parsed, "tickers", []) or [])
    symbols = _clean_symbols(raw_syms)
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

    max_t = _get_int("ENRICHED_MAX_TICKERS", 250)
    batch_sz = _get_int("ENRICHED_BATCH_SIZE", 40)
    timeout_sec = _get_float("ENRICHED_TIMEOUT_SEC", 45.0)

    status = "success"
    err_parts: List[str] = []

    if len(symbols) > max_t:
        status = "partial"
        err_parts.append(f"Too many symbols ({len(symbols)}). Truncated to max {max_t}.")
        symbols = symbols[:max_t]

    eng = await _resolve_engine(request)
    if eng is None:
        status = "error" if status == "success" else status
        err_parts.append("Engine unavailable")

        rows_out: List[List[Any]] = []
        quotes_out: List[Dict[str, Any]] = []

        for s in symbols:
            eq = _make_eq({"symbol": s, "data_quality": "MISSING", "error": "Engine unavailable"})
            if format in ("rows", "both"):
                rows_out.append(_eq_to_row(eq, headers))
            if format in ("quotes", "both"):
                quotes_out.append(_eq_to_dict(eq))

        resp: Dict[str, Any] = {
            "status": status,
            "error": " ".join(err_parts).strip() or None,
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

    for part in _chunk(symbols, batch_sz):
        got = await _engine_get_quotes(eng, part, timeout_sec=timeout_sec)

        # map by symbol upper for stable ordering
        m: Dict[str, Any] = {}
        for q in got or []:
            d = _model_to_dict(q)
            k = str(d.get("symbol") or d.get("ticker") or "").upper()
            if k:
                m[k] = q

        for s in part:
            uq = m.get(s.upper()) or {"symbol": s.upper(), "data_quality": "MISSING", "error": "No data returned"}
            eq = _make_eq(uq)

            if format in ("rows", "both"):
                rows_out.append(_eq_to_row(eq, headers))
            if format in ("quotes", "both"):
                quotes_out.append(_eq_to_dict(eq))

    # mark partial if Error column has anything
    try:
        err_idx = next(i for i, h in enumerate(headers) if str(h).strip().lower() == "error")
        if any((r[err_idx] not in (None, "", "null")) for r in rows_out if len(r) > err_idx):
            if status == "success":
                status = "partial"
    except Exception:
        pass

    resp2: Dict[str, Any] = {
        "status": status,
        "error": " ".join(err_parts).strip() or None,
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
