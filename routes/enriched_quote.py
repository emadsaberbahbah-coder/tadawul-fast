# routes/enriched_quote.py
"""
routes/enriched_quote.py
===========================================================
Enriched Quote Routes (Google Sheets + API) – PROD SAFE (v2.6.0)

Stable endpoints for Google Sheets / Apps Script:
    GET  /v1/enriched/quote?symbol=1120.SR
    POST /v1/enriched/quotes       {symbols:[...], tickers:[...], sheet_name?: "..."}
    POST /v1/enriched/sheet-rows   {symbols/tickers:[...], sheet_name?: "..."}   (alias)
    GET  /v1/enriched/headers?sheet_name=...
    GET  /v1/enriched/health

Design goals
- Router MUST mount even if optional modules are missing (Render-safe).
- Token guard via X-APP-TOKEN (APP_TOKEN / BACKUP_APP_TOKEN). If no token set => open mode.
- Defensive limits + batching + bounded concurrency + timeout.
- Engine resolution:
    • prefer request.app.state.engine (or aliases), else safe singleton here.
- Sheets-friendly:
    • /quotes and /sheet-rows NEVER raise for normal usage; always return status + headers + rows (+ optional quotes)
    • partial failures return placeholder rows with Error filled.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from functools import lru_cache
from importlib import import_module
from typing import Any, Dict, List, Optional, Sequence, Tuple, Literal

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request

# Pydantic v2/v1 compatibility
try:
    from pydantic import BaseModel, Field, ConfigDict  # type: ignore

    _PYDANTIC_V2 = True
except Exception:  # pragma: no cover
    from pydantic import BaseModel, Field  # type: ignore

    ConfigDict = None  # type: ignore
    _PYDANTIC_V2 = False


logger = logging.getLogger("routes.enriched_quote")

ENRICHED_VERSION = "2.6.0"
router = APIRouter(prefix="/v1/enriched", tags=["enriched"])


# =============================================================================
# Safe imports (do NOT break router mounting)
# =============================================================================
def _safe_import(path: str) -> Optional[Any]:
    try:
        return import_module(path)
    except Exception:
        return None


# settings getter (try canonical -> shim -> legacy)
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


# engine + normalizer (prefer v2, fallback legacy wrapper)
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
    if DataEngine and normalize_symbol:
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
# Defaults (headers)
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
# Fallback request + fallback EnrichedQuote
# =============================================================================
class _ExtraIgnore(BaseModel):
    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")  # type: ignore
    else:  # pragma: no cover
        class Config:
            extra = "ignore"


class _FallbackBatchRequest(_ExtraIgnore):
    operation: str = "refresh"
    sheet_name: Optional[str] = None
    symbols: List[str] = Field(default_factory=list)
    tickers: List[str] = Field(default_factory=list)  # alias support


class _FallbackEnrichedQuote(_ExtraIgnore):
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
            data_source=str(d.get("data_source") or d.get("provider") or d.get("source") or "none"),
            error=d.get("error"),
        )

    def to_row(self, headers: List[str]) -> List[Any]:
        d = self.model_dump(exclude_none=False) if hasattr(self, "model_dump") else dict(self.__dict__)
        out: List[Any] = []
        for h in headers:
            key = str(h or "").strip()
            lk = key.lower()

            if lk in ("symbol", "ticker"):
                out.append(self.symbol)
            elif lk in ("data quality", "data_quality"):
                out.append(self.data_quality)
            elif lk in ("data source", "data_source"):
                out.append(self.data_source)
            elif lk == "error":
                out.append(self.error or "")
            else:
                # best-effort: try exact header label, then normalized keys
                out.append(d.get(key) or d.get(lk) or "")
        return out


# =============================================================================
# General helpers
# =============================================================================
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _model_to_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    try:
        return obj.model_dump(exclude_none=False)  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        return obj.dict()  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        return dict(getattr(obj, "__dict__", {}) or {})
    except Exception:
        return {}


def _safe_model_validate(Model: Any, payload: Any) -> Any:
    """
    Pydantic v2: Model.model_validate
    Pydantic v1: Model.parse_obj
    If payload already instance of Model, return it.
    """
    try:
        if isinstance(payload, Model):
            return payload
    except Exception:
        pass

    # v2
    try:
        return Model.model_validate(payload)  # type: ignore[attr-defined]
    except Exception:
        pass

    # v1
    try:
        return Model.parse_obj(payload)  # type: ignore[attr-defined]
    except Exception:
        return Model()  # last-resort default


def _get_int(name: str, default: int) -> int:
    s = _settings_obj()
    if s is not None:
        try:
            v = getattr(s, name, None)
            if isinstance(v, int) and v > 0:
                return v
        except Exception:
            pass

    env_mod = _safe_import("env")
    if env_mod is not None:
        try:
            v = getattr(env_mod, name, None)
            if isinstance(v, int) and v > 0:
                return v
        except Exception:
            pass

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


def _cfg() -> Dict[str, Any]:
    # env + defaults (clamped)
    max_t = _get_int("ENRICHED_MAX_TICKERS", 250)
    batch_sz = _get_int("ENRICHED_BATCH_SIZE", 40)
    conc = _get_int("ENRICHED_CONCURRENCY", 8)
    timeout_sec = _get_float("ENRICHED_TIMEOUT_SEC", 45.0)

    # sanity clamps
    max_t = max(10, min(2000, max_t))
    batch_sz = max(5, min(250, batch_sz))
    conc = max(1, min(50, conc))
    timeout_sec = max(5.0, min(180.0, timeout_sec))

    return {
        "max_tickers": max_t,
        "batch_size": batch_sz,
        "concurrency": conc,
        "timeout_sec": timeout_sec,
    }


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
    if EnrichedQuote is not None:
        try:
            return EnrichedQuote.from_unified(uq)  # type: ignore[attr-defined]
        except Exception:
            pass
    return _FallbackEnrichedQuote.from_unified(uq)


def _eq_to_row(eq: Any, headers: List[str]) -> List[Any]:
    try:
        return eq.to_row(headers)  # type: ignore[attr-defined]
    except Exception:
        fb = _FallbackEnrichedQuote(**_model_to_dict(eq))
        return fb.to_row(headers)


def _eq_to_dict(eq: Any) -> Dict[str, Any]:
    return _model_to_dict(eq)


def _has_error_in_rows(headers: List[str], rows: List[List[Any]]) -> bool:
    try:
        err_idx = next(i for i, h in enumerate(headers) if str(h).strip().lower() == "error")
        for r in rows:
            if len(r) > err_idx and (r[err_idx] not in (None, "", "null", "None")):
                return True
    except Exception:
        pass
    return False


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
            _ENGINE = DataEngine()  # type: ignore[operator]
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
# Engine call shims (timeout + batching)
# =============================================================================
def _placeholder_uq(symbol: str, err: str) -> Any:
    # Prefer UnifiedQuote if available and constructible; else dict
    if UnifiedQuote is not None:
        try:
            return UnifiedQuote(symbol=symbol, data_quality="MISSING", error=err).finalize()  # type: ignore
        except Exception:
            pass
    return {"symbol": symbol, "data_quality": "MISSING", "error": err}


async def _engine_get_quotes(engine: Any, syms: List[str], timeout_sec: float) -> List[Any]:
    async def _call() -> List[Any]:
        if hasattr(engine, "get_enriched_quotes"):
            return await engine.get_enriched_quotes(syms)  # type: ignore[attr-defined]
        if hasattr(engine, "get_quotes"):
            return await engine.get_quotes(syms)  # type: ignore[attr-defined]

        # per-symbol fallback
        conc = _cfg()["concurrency"]
        sem = asyncio.Semaphore(max(1, conc))

        async def _one(s: str) -> Any:
            async with sem:
                if hasattr(engine, "get_quote"):
                    return await engine.get_quote(s)  # type: ignore[attr-defined]
                if hasattr(engine, "get_enriched_quote"):
                    return await engine.get_enriched_quote(s)  # type: ignore[attr-defined]
                return _placeholder_uq(s, "Engine missing quote methods")

        return list(await asyncio.gather(*[_one(s) for s in syms], return_exceptions=False))

    try:
        return await asyncio.wait_for(_call(), timeout=timeout_sec)
    except Exception as exc:
        return [_placeholder_uq(s, f"Engine batch error: {exc}") for s in syms]


async def _get_quotes_chunked(
    engine: Optional[Any],
    symbols: List[str],
    *,
    batch_size: int,
    timeout_sec: float,
    max_concurrency: int,
) -> Dict[str, Any]:
    """
    Returns dict: SYMBOL_UPPER -> UnifiedQuote-ish (or dict placeholder).
    Never raises; failures become placeholders.
    """
    clean = _clean_symbols(symbols)
    if not clean:
        return {}

    if engine is None:
        return {s: _placeholder_uq(s, "Engine unavailable") for s in clean}

    chunks = _chunk(clean, batch_size)
    sem = asyncio.Semaphore(max(1, max_concurrency))

    async def _run_chunk(part: List[str]) -> Tuple[List[str], List[Any]]:
        async with sem:
            res = await _engine_get_quotes(engine, part, timeout_sec=timeout_sec)
            return part, list(res or [])

    # Run chunks concurrently (bounded)
    results = await asyncio.gather(*[_run_chunk(c) for c in chunks], return_exceptions=False)

    out: Dict[str, Any] = {}
    for requested_chunk, returned_list in results:
        # map returned by symbol upper
        m: Dict[str, Any] = {}
        for q in returned_list or []:
            d = _model_to_dict(q)
            k = str(d.get("symbol") or d.get("ticker") or "").strip().upper()
            if k:
                m[k] = q

        for s in requested_chunk:
            su = s.upper()
            out[su] = m.get(su) or _placeholder_uq(su, "No data returned")

    return out


# =============================================================================
# Endpoints
# =============================================================================
@router.get("/health", tags=["system"])
@router.get("/ping", tags=["system"])
async def enriched_health(request: Request) -> Dict[str, Any]:
    cfg = _cfg()
    eng = await _resolve_engine(request)

    providers: List[str] = []
    try:
        providers = list(getattr(eng, "enabled_providers", []) or []) if eng else []
    except Exception:
        providers = []

    return {
        "status": "ok",
        "module": "routes.enriched_quote",
        "version": ENRICHED_VERSION,
        "engine_available": bool(eng),
        "providers": providers,
        "limits": {
            "ENRICHED_MAX_TICKERS": cfg["max_tickers"],
            "ENRICHED_BATCH_SIZE": cfg["batch_size"],
            "ENRICHED_CONCURRENCY": cfg["concurrency"],
            "ENRICHED_TIMEOUT_SEC": cfg["timeout_sec"],
        },
        "auth": "open" if not _allowed_tokens() else "token",
        "timestamp_utc": _now_utc_iso(),
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
    cfg = _cfg()

    eng = await _resolve_engine(request)
    if eng is None:
        eq = _make_eq(_placeholder_uq(sym.upper(), "Engine unavailable"))
        if format == "quote":
            return _eq_to_dict(eq)
        row = _eq_to_row(eq, headers)
        if format == "row":
            return {"symbol": sym, "headers": headers, "row": row}
        return {"sheet_name": sheet_name, "headers": headers, "row": row, "quote": _eq_to_dict(eq)}

    # Use chunked path for consistent placeholder behavior
    m = await _get_quotes_chunked(
        eng,
        [sym],
        batch_size=1,
        timeout_sec=cfg["timeout_sec"],
        max_concurrency=1,
    )
    uq = m.get(sym.upper()) or _placeholder_uq(sym.upper(), "No data returned")
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

    Model = BatchProcessRequest or _FallbackBatchRequest  # type: ignore
    parsed = _safe_model_validate(Model, payload)

    operation = str(getattr(parsed, "operation", "refresh") or "refresh").strip() or "refresh"
    sheet_name = getattr(parsed, "sheet_name", None)

    raw_syms = list(getattr(parsed, "symbols", []) or []) + list(getattr(parsed, "tickers", []) or [])
    symbols = _clean_symbols(raw_syms)
    headers = _resolve_headers(sheet_name)
    cfg = _cfg()

    if not symbols:
        resp0: Dict[str, Any] = {
            "status": "skipped",
            "error": "No symbols provided",
            "operation": operation,
            "sheet_name": sheet_name,
            "count": 0,
            "symbols": [],
            "headers": headers,
            "version": ENRICHED_VERSION,
            "timestamp_utc": _now_utc_iso(),
        }
        if format in ("rows", "both"):
            resp0["rows"] = []
        if format in ("quotes", "both"):
            resp0["quotes"] = []
        return resp0

    status = "success"
    err_parts: List[str] = []

    if len(symbols) > cfg["max_tickers"]:
        status = "partial"
        err_parts.append(f"Too many symbols ({len(symbols)}). Truncated to max {cfg['max_tickers']}.")
        symbols = symbols[: cfg["max_tickers"]]

    eng = await _resolve_engine(request)
    if eng is None:
        status = "error" if status == "success" else status
        err_parts.append("Engine unavailable")

        rows_out: List[List[Any]] = []
        quotes_out: List[Dict[str, Any]] = []

        for s in symbols:
            eq = _make_eq(_placeholder_uq(s.upper(), "Engine unavailable"))
            if format in ("rows", "both"):
                rows_out.append(_eq_to_row(eq, headers))
            if format in ("quotes", "both"):
                quotes_out.append(_eq_to_dict(eq))

        resp1: Dict[str, Any] = {
            "status": status,
            "error": " ".join(err_parts).strip() or None,
            "operation": operation,
            "sheet_name": sheet_name,
            "count": len(symbols),
            "symbols": symbols,
            "headers": headers,
            "version": ENRICHED_VERSION,
            "timestamp_utc": _now_utc_iso(),
        }
        if format in ("rows", "both"):
            resp1["rows"] = rows_out
        if format in ("quotes", "both"):
            resp1["quotes"] = quotes_out
        return resp1

    # Pull all quotes (chunked + bounded concurrency)
    unified_map = await _get_quotes_chunked(
        eng,
        symbols,
        batch_size=cfg["batch_size"],
        timeout_sec=cfg["timeout_sec"],
        max_concurrency=cfg["concurrency"],
    )

    rows_out: List[List[Any]] = []
    quotes_out: List[Dict[str, Any]] = []

    for s in symbols:
        uq = unified_map.get(s.upper()) or _placeholder_uq(s.upper(), "No data returned")
        eq = _make_eq(uq)

        if format in ("rows", "both"):
            rows_out.append(_eq_to_row(eq, headers))
        if format in ("quotes", "both"):
            quotes_out.append(_eq_to_dict(eq))

    if format in ("rows", "both") and _has_error_in_rows(headers, rows_out):
        if status == "success":
            status = "partial"

    resp2: Dict[str, Any] = {
        "status": status,
        "error": " ".join(err_parts).strip() or None,
        "operation": operation,
        "sheet_name": sheet_name,
        "count": len(symbols),
        "symbols": symbols,
        "headers": headers,
        "version": ENRICHED_VERSION,
        "timestamp_utc": _now_utc_iso(),
    }
    if format in ("rows", "both"):
        resp2["rows"] = rows_out
    if format in ("quotes", "both"):
        resp2["quotes"] = quotes_out
    return resp2


@router.post("/sheet-rows")
async def enriched_sheet_rows(
    request: Request,
    payload: Any = Body(...),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> Dict[str, Any]:
    """
    Backward-compatible alias used by older clients/tests:
      POST /v1/enriched/sheet-rows
    Always returns: {status, headers, rows, ...}
    """
    # Force rows-only for Sheets usage
    return await enriched_quotes(
        request=request,
        payload=payload,
        format="rows",
        x_app_token=x_app_token,
    )


__all__ = ["router"]
