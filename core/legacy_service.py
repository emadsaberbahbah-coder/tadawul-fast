# core/legacy_service.py  (FULL REPLACEMENT)
from __future__ import annotations

"""
core/legacy_service.py
------------------------------------------------------------
Compatibility shim (quiet + useful) — v1.6.0

Goals
- Provide a stable legacy router that NEVER breaks app startup.
- Never raise outward from endpoints (always HTTP 200 with a JSON body).
- Best-effort engine discovery:
    1) request.app.state.engine
    2) core.data_engine_v2.get_engine() (singleton) if available
    3) core.data_engine_v2.DataEngine() temp
    4) core.data_engine.DataEngine() temp
- Support BOTH async and sync engine method implementations.
- Accept both {"symbols":[...]} and {"tickers":[...]} payload shapes.
- Optional external override router (if you have one) via env flag.

Env
- ENABLE_EXTERNAL_LEGACY_ROUTER=false (default)
- LOG_EXTERNAL_LEGACY_IMPORT_FAILURE=false (default)
- DEBUG_ERRORS=1 (adds traceback if ?debug=1 too)
"""

import asyncio
import importlib
import os
import traceback
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Query, Request
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from starlette.responses import JSONResponse

VERSION = "1.6.0"

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in _TRUTHY


ENABLE_EXTERNAL_LEGACY_ROUTER = _env_bool("ENABLE_EXTERNAL_LEGACY_ROUTER", False)
LOG_EXTERNAL_IMPORT_FAILURE = _env_bool("LOG_EXTERNAL_LEGACY_IMPORT_FAILURE", False)

_external_loaded_from: Optional[str] = None


def _safe_mod_file(mod: Any) -> str:
    try:
        return str(getattr(mod, "__file__", "") or "")
    except Exception:
        return ""


def _try_import_external_router() -> Optional[APIRouter]:
    """
    Optional override:
      - legacy_service.router
      - routes.legacy_service.router

    Guard against circular import.
    """
    global _external_loaded_from
    try:
        mod = importlib.import_module("legacy_service")
        if _safe_mod_file(mod).replace("\\", "/").endswith("/core/legacy_service.py"):
            raise RuntimeError("circular import: legacy_service points to core.legacy_service")
        r = getattr(mod, "router", None)
        if isinstance(r, APIRouter):
            _external_loaded_from = "legacy_service"
            return r
        raise RuntimeError("legacy_service.router missing/not APIRouter")
    except Exception as exc1:
        try:
            mod2 = importlib.import_module("routes.legacy_service")
            r2 = getattr(mod2, "router", None)
            if isinstance(r2, APIRouter):
                _external_loaded_from = "routes.legacy_service"
                return r2
            raise RuntimeError("routes.legacy_service.router missing/not APIRouter")
        except Exception as exc2:
            if LOG_EXTERNAL_IMPORT_FAILURE:
                try:
                    print(
                        "External legacy router not importable. Using internal router. "
                        f"errors=[{exc1.__class__.__name__}] / [{exc2.__class__.__name__}]"
                    )
                except Exception:
                    pass
            return None


router: APIRouter = APIRouter(prefix="/v1/legacy", tags=["legacy_compat"])


# -----------------------------------------------------------------------------
# Models
# -----------------------------------------------------------------------------
class SymbolsIn(BaseModel):
    """
    Accepts BOTH shapes:
      {"symbols":[...]}
      {"tickers":[...]}
    """
    symbols: List[str] = []
    tickers: List[str] = []

    def normalized(self) -> List[str]:
        items = self.symbols or self.tickers or []
        out: List[str] = []
        seen = set()
        for x in items:
            s = str(x or "").strip()
            if not s:
                continue
            su = s.upper()
            if su in seen:
                continue
            seen.add(su)
            out.append(s)
        return out


class SheetRowsIn(BaseModel):
    """
    Lightweight compatibility payload for sheet-rows-like output.
    """
    symbols: List[str] = []
    tickers: List[str] = []
    sheet_name: str = ""
    sheetName: str = ""


# -----------------------------------------------------------------------------
# Engine discovery / calling helpers
# -----------------------------------------------------------------------------
async def _get_engine_best_effort(request: Request) -> Tuple[Optional[Any], str]:
    """
    Returns (engine, source_label).
    """
    # 1) already attached
    eng = getattr(request.app.state, "engine", None)
    if eng is not None:
        return eng, "app.state.engine"

    # 2) v2 singleton getter
    try:
        from core.data_engine_v2 import get_engine as v2_get_engine  # type: ignore

        eng2 = await v2_get_engine()
        request.app.state.engine = eng2
        return eng2, "core.data_engine_v2.get_engine(singleton)"
    except Exception:
        pass

    # 3) v2 temp class
    try:
        from core.data_engine_v2 import DataEngine as V2Engine  # type: ignore

        eng3 = V2Engine()
        request.app.state.engine = eng3
        return eng3, "core.data_engine_v2.DataEngine(temp)"
    except Exception:
        pass

    # 4) legacy v1 temp class
    try:
        from core.data_engine import DataEngine as V1Engine  # type: ignore

        eng4 = V1Engine()
        request.app.state.engine = eng4
        return eng4, "core.data_engine.DataEngine(temp)"
    except Exception:
        return None, "none"


def _safe_err(e: BaseException) -> str:
    msg = str(e).strip()
    return msg or e.__class__.__name__


async def _maybe_await(x: Any) -> Any:
    """
    Supports:
    - async functions returning coroutine
    - sync functions returning value
    """
    try:
        if asyncio.iscoroutine(x):
            return await x
    except Exception:
        pass
    return x


async def _call_engine_method(engine: Any, method_names: Sequence[str], *args, **kwargs) -> Tuple[Optional[Any], str]:
    """
    Try first callable method from method_names.
    Returns (result, method_name_used_or_error).
    """
    for name in method_names:
        fn = getattr(engine, name, None)
        if callable(fn):
            try:
                res = fn(*args, **kwargs)
                res2 = await _maybe_await(res)
                return res2, name
            except Exception as e:
                return None, f"{name} failed: {_safe_err(e)}"
    return None, "missing"


def _sheet_name_from(payload: SheetRowsIn) -> str:
    nm = (payload.sheet_name or payload.sheetName or "").strip()
    return nm


def _headers_fallback(sheet_name: str) -> List[str]:
    """
    Best-effort: use core.schemas.get_headers_for_sheet if available.
    Otherwise a small stable legacy set.
    """
    try:
        from core.schemas import get_headers_for_sheet  # type: ignore

        if callable(get_headers_for_sheet) and sheet_name:
            h = get_headers_for_sheet(sheet_name)  # type: ignore
            if isinstance(h, list) and h:
                return [str(x) for x in h]
    except Exception:
        pass

    return [
        "Symbol",
        "Name",
        "Market",
        "Currency",
        "Price",
        "Change",
        "Change %",
        "Volume",
        "Market Cap",
        "P/E (TTM)",
        "Data Quality",
        "Data Source",
        "Error",
    ]


def _quote_to_row(q: Any, headers: List[str]) -> List[Any]:
    """
    Convert a quote dict/model into a row aligned with headers.
    We only fill what we can.
    """
    d: Dict[str, Any] = {}
    if isinstance(q, dict):
        d = q
    else:
        try:
            # pydantic model or object
            if hasattr(q, "model_dump"):
                d = q.model_dump()  # type: ignore
            elif hasattr(q, "dict"):
                d = q.dict()  # type: ignore
            else:
                d = dict(getattr(q, "__dict__", {}) or {})
        except Exception:
            d = {}

    def g(*keys: str) -> Any:
        for k in keys:
            if k in d:
                return d.get(k)
        return None

    # Map common legacy fields
    mapped: Dict[str, Any] = {
        "Symbol": g("symbol", "ticker"),
        "Name": g("name", "company_name"),
        "Market": g("market", "market_region"),
        "Currency": g("currency"),
        "Price": g("current_price", "last_price", "price"),
        "Change": g("price_change", "change"),
        "Change %": g("percent_change", "change_pct"),
        "Volume": g("volume"),
        "Market Cap": g("market_cap"),
        "P/E (TTM)": g("pe_ttm", "pe"),
        "Data Quality": g("data_quality"),
        "Data Source": g("data_source", "source"),
        "Error": g("error"),
    }

    row: List[Any] = []
    for h in headers:
        row.append(mapped.get(h, d.get(h)))
    return row


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@router.get("/health", summary="Legacy compatibility health")
async def legacy_health(request: Request):
    eng = getattr(request.app.state, "engine", None)
    info: Dict[str, Any] = {
        "ok": True,
        "status": "ok",
        "router": "core.legacy_service",
        "version": VERSION,
        "mode": "internal",
        "engine_present": eng is not None,
        "external_router_enabled": ENABLE_EXTERNAL_LEGACY_ROUTER,
    }

    try:
        from core.data_engine_v2 import ENGINE_VERSION  # type: ignore

        info["engine_version"] = ENGINE_VERSION
    except Exception:
        info["engine_version"] = "unknown"

    if _external_loaded_from:
        info["external_loaded_from"] = _external_loaded_from

    if eng is not None:
        try:
            info["engine_class"] = eng.__class__.__name__
            info["engine_module"] = eng.__class__.__module__
        except Exception:
            pass

    # Try to resolve a working engine (soft)
    try:
        e2, src = await _get_engine_best_effort(request)
        info["engine_resolve_source"] = src
        info["engine_resolved"] = bool(e2 is not None)
    except Exception:
        info["engine_resolve_source"] = "error"
        info["engine_resolved"] = False

    return info


@router.get("/quote", summary="Legacy quote endpoint (UnifiedQuote)")
async def legacy_quote(
    request: Request,
    symbol: str = Query(..., min_length=1),
    debug: int = Query(0, description="Set 1 to include traceback (or enable DEBUG_ERRORS=1)"),
):
    dbg = _env_bool("DEBUG_ERRORS", False) or bool(debug)

    try:
        eng, src = await _get_engine_best_effort(request)
        if eng is None:
            out = {
                "status": "error",
                "symbol": (symbol or "").strip(),
                "data_quality": "MISSING",
                "data_source": "none",
                "error": "Legacy engine not available (no working provider).",
            }
            return JSONResponse(status_code=200, content=jsonable_encoder(out))

        q, used = await _call_engine_method(eng, ("get_quote", "get_enriched_quote"), symbol)
        if q is None:
            out = {
                "status": "error",
                "symbol": (symbol or "").strip(),
                "data_quality": "MISSING",
                "data_source": "none",
                "error": f"Engine call failed (source={src}, method={used}).",
            }
            return JSONResponse(status_code=200, content=jsonable_encoder(out))

        # Return quote as-is (legacy behavior)
        return JSONResponse(status_code=200, content=jsonable_encoder(q))

    except Exception as e:
        out: Dict[str, Any] = {
            "status": "error",
            "symbol": (symbol or "").strip(),
            "data_quality": "MISSING",
            "data_source": "none",
            "error": _safe_err(e),
        }
        if dbg:
            out["traceback"] = traceback.format_exc()[:8000]
        return JSONResponse(status_code=200, content=jsonable_encoder(out))


@router.post("/quotes", summary="Legacy batch quotes endpoint (list[UnifiedQuote])")
async def legacy_quotes(
    request: Request,
    payload: SymbolsIn,
    debug: int = Query(0, description="Set 1 to include traceback (or enable DEBUG_ERRORS=1)"),
):
    dbg = _env_bool("DEBUG_ERRORS", False) or bool(debug)

    try:
        symbols = payload.normalized()
        if not symbols:
            out = []
            return JSONResponse(status_code=200, content=jsonable_encoder(out))

        eng, src = await _get_engine_best_effort(request)
        if eng is None:
            # Legacy endpoint: historically returns a list; keep that shape.
            out = [
                {
                    "status": "error",
                    "symbol": s,
                    "data_quality": "MISSING",
                    "data_source": "none",
                    "error": "Legacy engine not available (no working provider).",
                }
                for s in symbols
            ]
            return JSONResponse(status_code=200, content=jsonable_encoder(out))

        items, used = await _call_engine_method(eng, ("get_quotes", "get_enriched_quotes"), symbols)
        if items is None:
            out = [
                {
                    "status": "error",
                    "symbol": s,
                    "data_quality": "MISSING",
                    "data_source": "none",
                    "error": f"Engine call failed (source={src}, method={used}).",
                }
                for s in symbols
            ]
            if dbg:
                out.append({"debug": True, "engine_source": src, "method": used})
            return JSONResponse(status_code=200, content=jsonable_encoder(out))

        return JSONResponse(status_code=200, content=jsonable_encoder(items))

    except Exception as e:
        out: Dict[str, Any] = {"status": "error", "count": 0, "items": [], "error": _safe_err(e)}
        if dbg:
            out["traceback"] = traceback.format_exc()[:8000]
        return JSONResponse(status_code=200, content=jsonable_encoder(out))


@router.post("/sheet-rows", summary="Legacy sheet-rows helper (headers + rows)")
async def legacy_sheet_rows(
    request: Request,
    payload: SheetRowsIn,
    debug: int = Query(0, description="Set 1 to include traceback (or enable DEBUG_ERRORS=1)"),
):
    """
    Returns a Sheets-friendly payload:
      { status, headers, rows, error? }

    This is optional convenience and does not replace /v1/enriched/sheet-rows.
    """
    dbg = _env_bool("DEBUG_ERRORS", False) or bool(debug)

    try:
        # Normalize symbols
        symbols_in = SymbolsIn(symbols=payload.symbols or [], tickers=payload.tickers or [])
        symbols = symbols_in.normalized()
        if not symbols:
            return JSONResponse(
                status_code=200,
                content=jsonable_encoder({"status": "skipped", "headers": [], "rows": [], "error": "No symbols provided"}),
            )

        sheet_name = _sheet_name_from(payload)
        headers = _headers_fallback(sheet_name)

        eng, src = await _get_engine_best_effort(request)
        if eng is None:
            rows = [[s, None, None, None, None, None, None, None, None, None, "MISSING", "none", "Legacy engine not available"] for s in symbols]
            return JSONResponse(
                status_code=200,
                content=jsonable_encoder({"status": "error", "headers": headers, "rows": rows, "error": "Legacy engine not available", "engine_source": src}),
            )

        items, used = await _call_engine_method(eng, ("get_quotes", "get_enriched_quotes"), symbols)
        if not isinstance(items, list):
            # If engine returns a dict or something else, wrap into error rows
            rows = [[s, None, None, None, None, None, None, None, None, None, "MISSING", "none", f"Engine returned non-list (method={used})"] for s in symbols]
            return JSONResponse(
                status_code=200,
                content=jsonable_encoder({"status": "error", "headers": headers, "rows": rows, "error": "Engine returned non-list", "engine_source": src, "method": used}),
            )

        rows = [_quote_to_row(q, headers) for q in items]
        return JSONResponse(
            status_code=200,
            content=jsonable_encoder(
                {
                    "status": "success",
                    "headers": headers,
                    "rows": rows,
                    "count": len(rows),
                    "engine_source": src,
                    "method": used,
                }
            ),
        )

    except Exception as e:
        out: Dict[str, Any] = {"status": "error", "headers": [], "rows": [], "error": _safe_err(e)}
        if dbg:
            out["traceback"] = traceback.format_exc()[:8000]
        return JSONResponse(status_code=200, content=jsonable_encoder(out))


# -----------------------------------------------------------------------------
# Optional external override router
# -----------------------------------------------------------------------------
if ENABLE_EXTERNAL_LEGACY_ROUTER:
    ext = _try_import_external_router()
    if ext is not None:
        router = ext  # type: ignore


def get_router() -> APIRouter:
    return router


__all__ = ["router", "get_router"]
