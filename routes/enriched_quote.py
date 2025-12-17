# routes/enriched_quote.py
"""
routes/enriched_quote.py
===========================================================
Enriched Quote Routes (Google Sheets + API) — v2.6.0 (PROD SAFE)

Endpoints
- GET  /v1/enriched/health
- GET  /v1/enriched/headers?sheet_name=...
- GET  /v1/enriched/quote?symbol=1120.SR&format=quote|row|both&sheet_name=...
- POST /v1/enriched/quotes?format=rows|quotes|both
    body: {symbols:[...], tickers:[...], sheet_name?: "...", operation?: "refresh"}
- POST /v1/enriched/sheet-rows
    body: {tickers:[...], sheet_name?: "..."}   (Sheets client expects {headers, rows})

Key Improvements (v2.6.0)
- Adds /sheet-rows endpoint (needed by google_sheets_service.py).
- Integrates price history storage (core/price_history_store.py) best-effort:
    • records each successful quote to history store (no schema changes).
- Hardens KSA fallback URL handling (absolute or relative route).
- Ensures Sheets-safe responses: headers+rows always exist, even on errors.
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
from core.data_engine_v2 import DataEngine, UnifiedQuote, normalize_symbol, is_ksa_symbol, _fix_mojibake  # type: ignore
from core.enriched_quote import EnrichedQuote

# Optional core.schemas (preferred)
try:
    from core.schemas import BatchProcessRequest, get_headers_for_sheet  # type: ignore
except Exception:  # pragma: no cover
    BatchProcessRequest = None  # type: ignore
    get_headers_for_sheet = None  # type: ignore

# Optional history store (NEW)
try:
    from core.price_history_store import get_price_history_store  # type: ignore
except Exception:  # pragma: no cover
    get_price_history_store = None  # type: ignore

logger = logging.getLogger("routes.enriched_quote")

ROUTE_VERSION = "2.6.0"
router = APIRouter(prefix="/v1/enriched", tags=["enriched"])


# =============================================================================
# Fallback request models + headers helper
# =============================================================================
class _FallbackBatchProcessRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    operation: str = "refresh"
    sheet_name: Optional[str] = None
    symbols: List[str] = Field(default_factory=list)
    tickers: List[str] = Field(default_factory=list)  # alias support


class _SheetRowsRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    sheet_name: Optional[str] = None
    tickers: List[str] = Field(default_factory=list)
    symbols: List[str] = Field(default_factory=list)  # alias support


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
    except Exception:
        pass
    try:
        import env as env_mod  # type: ignore
        v = getattr(env_mod, name, None)
        if isinstance(v, bool):
            return v
    except Exception:
        pass

    ev = os.getenv(name)
    if not ev:
        return default
    ev = ev.strip().lower()
    if ev in ("1", "true", "yes", "y", "on"):
        return True
    if ev in ("0", "false", "no", "n", "off"):
        return False
    return default


# =============================================================================
# Symbol + response helpers
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


def _timeout_for_single(sym: str) -> float:
    single = _get_float_setting("ENRICHED_SINGLE_TIMEOUT_SEC", 12.0)
    ksa_single = _get_float_setting("KSA_SINGLE_TIMEOUT_SEC", 10.0)
    return ksa_single if is_ksa_symbol(sym) else single


def _timeout_for_batch(symbols: List[str]) -> float:
    batch = _get_float_setting("ENRICHED_BATCH_TIMEOUT_SEC", 22.0)
    ksa_batch = _get_float_setting("KSA_BATCH_TIMEOUT_SEC", 20.0)
    return ksa_batch if any(is_ksa_symbol(s) for s in (symbols or [])) else batch


def _get_fallback_cfg() -> Dict[str, Any]:
    return {
        "enabled": _get_bool_setting("KSA_FALLBACK_ENABLED", True),
        "route": os.getenv("KSA_FALLBACK_ROUTE") or "/v1/argaam/quote",
        "timeout_sec": _get_float_setting("KSA_FALLBACK_TIMEOUT_SEC", 7.0),
    }


async def _engine_get_quote(engine: DataEngine, sym: str, timeout_sec: float) -> UnifiedQuote:
    async def _call() -> UnifiedQuote:
        if hasattr(engine, "get_quote"):
            return await engine.get_quote(sym)  # type: ignore
        if hasattr(engine, "get_enriched_quote"):
            return await engine.get_enriched_quote(sym)  # type: ignore
        raise RuntimeError("Engine has no get_quote/get_enriched_quote")
    return await asyncio.wait_for(_call(), timeout=timeout_sec)


async def _engine_get_quotes(engine: DataEngine, syms: List[str], timeout_sec: float) -> List[UnifiedQuote]:
    async def _call() -> List[UnifiedQuote]:
        if hasattr(engine, "get_enriched_quotes"):
            return await engine.get_enriched_quotes(syms)  # type: ignore
        if hasattr(engine, "get_quotes"):
            return await engine.get_quotes(syms)  # type: ignore
        out: List[UnifiedQuote] = []
        for s in syms:
            out.append(await engine.get_quote(s))  # type: ignore
        return out
    return await asyncio.wait_for(_call(), timeout=timeout_sec)


def _abs_or_join(base: str, route_or_url: str) -> str:
    u = (route_or_url or "").strip()
    if not u:
        return base
    if u.lower().startswith("http://") or u.lower().startswith("https://"):
        return u
    b = (base or "").rstrip("/")
    r = "/" + u.lstrip("/")
    return b + r


async def _try_ksa_fallback(request: Request, sym: str, x_app_token: Optional[str]) -> Optional[Dict[str, Any]]:
    cfg = _get_fallback_cfg()
    if not cfg.get("enabled"):
        return None
    if not is_ksa_symbol(sym):
        return None

    base_sym = sym.split(".", 1)[0]  # 1120.SR -> 1120
    base_url = str(request.base_url).rstrip("/")
    url = _abs_or_join(base_url, str(cfg.get("route") or "/v1/argaam/quote"))
    params = {"symbol": base_sym}

    headers: Dict[str, str] = {}
    if x_app_token:
        headers["X-APP-TOKEN"] = x_app_token

    try:
        async with httpx.AsyncClient(timeout=float(cfg.get("timeout_sec") or 7.0)) as c:
            r = await c.get(url, params=params, headers=headers)
            if r.status_code >= 400:
                return None
            data = r.json()
            return data if isinstance(data, dict) else None
    except Exception:
        return None


def _sf(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if not s or s in {"-", "—", "N/A", "NA", "null", "None"}:
            return None
        s = s.replace("%", "").replace(",", "").strip()
        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()
        return float(s)
    except Exception:
        return None


def _merge_fallback_into_quote(q: UnifiedQuote, fb: Dict[str, Any]) -> UnifiedQuote:
    """
    Merge fallback JSON into UnifiedQuote WITHOUT overwriting good data.
    Accepts either EnrichedQuote-style keys or UnifiedQuote-style keys.
    """
    if not fb:
        return q

    upd: Dict[str, Any] = {}

    def pick(dst: str, *cands: str, numeric: bool = False) -> None:
        cur = getattr(q, dst, None)
        if cur is not None:
            return
        for k in cands:
            v = fb.get(k)
            if v is None:
                continue
            if numeric:
                vv = _sf(v)
                if vv is None:
                    continue
                upd[dst] = vv
            else:
                upd[dst] = v
            return

    pick("name", "name")
    pick("current_price", "current_price", "last_price", "price", numeric=True)
    pick("previous_close", "previous_close", "previousClose", numeric=True)
    pick("open", "open", numeric=True)
    pick("day_high", "day_high", "high", numeric=True)
    pick("day_low", "day_low", "low", numeric=True)
    pick("volume", "volume", numeric=True)
    pick("value_traded", "value_traded", "valueTraded", numeric=True)

    pick("shares_outstanding", "shares_outstanding", "sharesOutstanding", numeric=True)
    pick("market_cap", "market_cap", "marketCap", numeric=True)

    pick("high_52w", "high_52w", "high52w", "yearHigh", numeric=True)
    pick("low_52w", "low_52w", "low52w", "yearLow", numeric=True)

    pick("eps_ttm", "eps_ttm", "eps", numeric=True)
    pick("pe_ttm", "pe_ttm", "pe", numeric=True)
    pick("dividend_yield", "dividend_yield", "divYield", numeric=True)

    q2 = q.model_copy(update=upd) if upd else q

    # Source labeling
    ds = (getattr(q2, "data_source", None) or "").strip()
    fb_ds = str(fb.get("data_source") or fb.get("source") or "ksa_fallback").strip()
    q2.data_source = ds if ds else fb_ds

    return q2.finalize()


def _build_row_payload(q: UnifiedQuote, headers: List[str]) -> Dict[str, Any]:
    eq = EnrichedQuote.from_unified(q)
    return {
        "headers": list(headers),
        "row": eq.to_row(headers),
        "quote": _model_to_dict(eq),
    }


def _safe_row_from_quote(uq: UnifiedQuote, headers: List[str]) -> List[Any]:
    try:
        eq = EnrichedQuote.from_unified(uq)
        return eq.to_row(headers)
    except Exception as exc:
        # Sheets-safe fallback row
        sym = getattr(uq, "symbol", None) or ""
        msg = getattr(uq, "error", None) or str(exc) or "Error"
        if "Symbol" in headers and "Error" in headers:
            row = [None] * len(headers)
            row[headers.index("Symbol")] = sym
            row[headers.index("Error")] = msg
            return row
        return [sym, msg]


def _record_history_best_effort(uq: UnifiedQuote) -> bool:
    """
    Store successful quotes into history store (if available).
    Never raises.
    """
    try:
        if not get_price_history_store:
            return False
        if uq is None:
            return False
        if getattr(uq, "current_price", None) is None:
            return False
        store = get_price_history_store()
        return bool(store.record_quote(uq))
    except Exception:
        return False


# =============================================================================
# Endpoints
# =============================================================================
@router.get("/health", tags=["system"])
async def enriched_health(request: Request) -> Dict[str, Any]:
    max_t = _get_int_setting("ENRICHED_MAX_TICKERS", 250)
    batch_sz = _get_int_setting("ENRICHED_BATCH_SIZE", 40)

    eng = await _resolve_engine(request)
    cfg = _get_fallback_cfg()

    hist_enabled = bool(get_price_history_store)
    hist_path = os.getenv("PRICE_HISTORY_PATH", "") if hist_enabled else ""

    return {
        "status": "ok",
        "module": "routes.enriched_quote",
        "version": ROUTE_VERSION,
        "engine": "DataEngineV2",
        "providers": list(getattr(eng, "enabled_providers", []) or []) if eng else [],
        "limits": {"enriched_max_tickers": max_t, "enriched_batch_size": batch_sz},
        "timeouts": {
            "single_timeout_sec": _get_float_setting("ENRICHED_SINGLE_TIMEOUT_SEC", 12.0),
            "batch_timeout_sec": _get_float_setting("ENRICHED_BATCH_TIMEOUT_SEC", 22.0),
            "ksa_single_timeout_sec": _get_float_setting("KSA_SINGLE_TIMEOUT_SEC", 10.0),
            "ksa_batch_timeout_sec": _get_float_setting("KSA_BATCH_TIMEOUT_SEC", 20.0),
            "ksa_fallback_enabled": bool(cfg.get("enabled")),
            "ksa_fallback_route": str(cfg.get("route") or "/v1/argaam/quote"),
            "ksa_fallback_timeout_sec": float(cfg.get("timeout_sec") or 7.0),
        },
        "history_store": {"enabled": hist_enabled, "persist_path": hist_path or None},
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

    t0 = time.perf_counter()

    sym = normalize_symbol((symbol or "").strip())
    if not sym:
        raise HTTPException(status_code=400, detail="symbol is required")

    eng = await _resolve_engine(request)
    fallback_used = False
    history_recorded = False

    if eng is None:
        uq = UnifiedQuote(symbol=sym, data_quality="MISSING", error="Engine unavailable").finalize()
    else:
        try:
            uq = await _engine_get_quote(eng, sym, _timeout_for_single(sym))
        except Exception as exc:
            uq = UnifiedQuote(symbol=sym, data_quality="MISSING", error=str(exc)).finalize()

    # KSA fallback if still missing core
    if is_ksa_symbol(sym) and (uq.current_price is None or uq.data_quality == "MISSING"):
        fb = await _try_ksa_fallback(request, sym, x_app_token)
        if fb:
            uq = _merge_fallback_into_quote(uq, fb)
            fallback_used = True

    uq = uq.finalize()
    uq.name = _fix_mojibake(uq.name)

    history_recorded = _record_history_best_effort(uq)

    elapsed_ms = int((time.perf_counter() - t0) * 1000)

    if format == "quote":
        out = _model_to_dict(EnrichedQuote.from_unified(uq))
        out["_meta"] = {"elapsed_ms": elapsed_ms, "fallback_used": fallback_used, "history_recorded": history_recorded}
        return out

    headers = _resolve_headers(sheet_name)

    if format == "row":
        row = _safe_row_from_quote(uq, headers)
        return {
            "symbol": getattr(uq, "symbol", sym),
            "headers": headers,
            "row": row,
            "_meta": {"elapsed_ms": elapsed_ms, "fallback_used": fallback_used, "history_recorded": history_recorded},
        }

    payload = _build_row_payload(uq, headers)
    payload["sheet_name"] = sheet_name
    payload["_meta"] = {"elapsed_ms": elapsed_ms, "fallback_used": fallback_used, "history_recorded": history_recorded}
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
            "_meta": {"elapsed_ms": int((time.perf_counter() - t0) * 1000), "fallback_used": False, "history_recorded": False},
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
            uq = UnifiedQuote(symbol=s, data_quality="MISSING", error="Engine unavailable").finalize()
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
        }
        if format in ("rows", "both"):
            resp["rows"] = rows_out
        if format in ("quotes", "both"):
            resp["quotes"] = quotes_out
        resp["_meta"] = {"elapsed_ms": int((time.perf_counter() - t0) * 1000), "fallback_used": False, "history_recorded": False}
        return resp

    rows_out: List[List[Any]] = []
    quotes_out: List[Dict[str, Any]] = []
    fallback_used_any = False
    history_recorded_any = False

    batch_timeout = _timeout_for_batch(symbols)

    for chunk in _chunk(symbols, batch_sz):
        try:
            quotes = await _engine_get_quotes(eng, chunk, batch_timeout)
        except Exception as exc:
            status = "partial"
            error_msg = (error_msg + " " if error_msg else "") + f"Batch error: {exc}"
            quotes = [UnifiedQuote(symbol=s, data_quality="MISSING", error=str(exc)).finalize() for s in chunk]

        m = {getattr(q, "symbol", "").upper(): q for q in (quotes or []) if getattr(q, "symbol", None)}
        for s in chunk:
            uq = m.get(s.upper()) or UnifiedQuote(symbol=s.upper(), data_quality="MISSING", error="No data returned").finalize()

            # KSA fallback if missing
            if is_ksa_symbol(s) and (uq.current_price is None or uq.data_quality == "MISSING"):
                fb = await _try_ksa_fallback(request, s, x_app_token)
                if fb:
                    uq = _merge_fallback_into_quote(uq, fb)
                    fallback_used_any = True

            uq = uq.finalize()
            uq.name = _fix_mojibake(uq.name)

            if _record_history_best_effort(uq):
                history_recorded_any = True

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
    }
    if format in ("rows", "both"):
        resp["rows"] = rows_out
    if format in ("quotes", "both"):
        resp["quotes"] = quotes_out

    resp["_meta"] = {
        "elapsed_ms": int((time.perf_counter() - t0) * 1000),
        "fallback_used": fallback_used_any,
        "history_recorded": history_recorded_any,
    }
    return resp


@router.post("/sheet-rows")
async def enriched_sheet_rows(
    request: Request,
    req: Any = Body(...),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> Dict[str, Any]:
    """
    Sheets client endpoint. Always returns a Sheets-safe payload:
      { headers: [...], rows: [[...], ...], status: success|partial|error|skipped, error?: str }
    """
    _require_token(x_app_token)
    t0 = time.perf_counter()

    # Parse request
    try:
        parsed = _SheetRowsRequest.model_validate(req)  # type: ignore
    except Exception:
        try:
            parsed = _SheetRowsRequest.parse_obj(req)  # type: ignore
        except Exception:
            parsed = _SheetRowsRequest()

    sheet_name = getattr(parsed, "sheet_name", None)
    symbols_in = list(getattr(parsed, "tickers", []) or []) + list(getattr(parsed, "symbols", []) or [])
    symbols = _clean_symbols(symbols_in)
    headers = _resolve_headers(sheet_name)

    if not symbols:
        return {
            "status": "skipped",
            "error": "No tickers provided",
            "sheet_name": sheet_name,
            "headers": headers,
            "rows": [],
            "_meta": {"elapsed_ms": int((time.perf_counter() - t0) * 1000), "fallback_used": False, "history_recorded": False},
        }

    max_t = _get_int_setting("ENRICHED_MAX_TICKERS", 250)
    batch_sz = _get_int_setting("ENRICHED_BATCH_SIZE", 40)

    status = "success"
    error_msg: Optional[str] = None

    if len(symbols) > max_t:
        status = "partial"
        error_msg = f"Too many tickers ({len(symbols)}). Truncated to max {max_t}."
        symbols = symbols[:max_t]

    eng = await _resolve_engine(request)
    if eng is None:
        rows = []
        for s in symbols:
            uq = UnifiedQuote(symbol=s, data_quality="MISSING", error="Engine unavailable").finalize()
            rows.append(_safe_row_from_quote(uq, headers))
        return {
            "status": "error",
            "error": (error_msg + " " if error_msg else "") + "Engine unavailable",
            "sheet_name": sheet_name,
            "headers": headers,
            "rows": rows,
            "_meta": {"elapsed_ms": int((time.perf_counter() - t0) * 1000), "fallback_used": False, "history_recorded": False},
        }

    rows_out: List[List[Any]] = []
    fallback_used_any = False
    history_recorded_any = False

    batch_timeout = _timeout_for_batch(symbols)

    for chunk in _chunk(symbols, batch_sz):
        try:
            quotes = await _engine_get_quotes(eng, chunk, batch_timeout)
        except Exception as exc:
            status = "partial"
            error_msg = (error_msg + " " if error_msg else "") + f"Batch error: {exc}"
            quotes = [UnifiedQuote(symbol=s, data_quality="MISSING", error=str(exc)).finalize() for s in chunk]

        m = {getattr(q, "symbol", "").upper(): q for q in (quotes or []) if getattr(q, "symbol", None)}
        for s in chunk:
            uq = m.get(s.upper()) or UnifiedQuote(symbol=s.upper(), data_quality="MISSING", error="No data returned").finalize()

            # KSA fallback if missing
            if is_ksa_symbol(s) and (uq.current_price is None or uq.data_quality == "MISSING"):
                fb = await _try_ksa_fallback(request, s, x_app_token)
                if fb:
                    uq = _merge_fallback_into_quote(uq, fb)
                    fallback_used_any = True

            uq = uq.finalize()
            uq.name = _fix_mojibake(uq.name)

            if _record_history_best_effort(uq):
                history_recorded_any = True

            rows_out.append(_safe_row_from_quote(uq, headers))

    elapsed_ms = int((time.perf_counter() - t0) * 1000)
    return {
        "status": status,
        "error": error_msg,
        "sheet_name": sheet_name,
        "headers": headers,
        "rows": rows_out,
        "_meta": {"elapsed_ms": elapsed_ms, "fallback_used": fallback_used_any, "history_recorded": history_recorded_any},
    }


__all__ = ["router"]
