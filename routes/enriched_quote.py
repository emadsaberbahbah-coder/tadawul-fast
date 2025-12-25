# routes/enriched_quote.py
"""
routes/enriched_quote.py
------------------------------------------------------------
Enriched Quote Router – PROD SAFE + DEBUGGABLE (v2.3.0)

Key guarantees
- Never crashes app startup (no core imports at module import-time)
- Always returns HTTP 200 with a status field (client simplicity)
- Always returns a non-empty `error` on failure
- Always returns `symbol` (normalized or input)
- Uses app.state.engine when available (preferred)
- Falls back safely to temporary v2 engine, then legacy module-level
- Optional debug trace via env DEBUG_ERRORS=1 or query ?debug=1
- Adds batch endpoint /v1/enriched/quotes for convenience

Endpoints
- GET /v1/enriched/quote?symbol=AAPL
- GET /v1/enriched/quote?symbol=MSFT.US
- GET /v1/enriched/quote?symbol=1120.SR
- GET /v1/enriched/quote?symbol=1120
- GET /v1/enriched/quotes?symbols=AAPL,MSFT,1120.SR
"""

from __future__ import annotations

import inspect
import os
import traceback
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Query, Request
from fastapi.encoders import jsonable_encoder
from starlette.responses import JSONResponse

router = APIRouter(prefix="/v1/enriched", tags=["enriched_quote"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY


def _split_symbols(raw: str) -> List[str]:
    s = (raw or "").replace("\n", " ").replace("\t", " ").strip()
    if not s:
        return []
    if "," in s:
        parts = [p.strip() for p in s.split(",")]
    else:
        parts = [p.strip() for p in s.split(" ")]
    return [p for p in parts if p]


def _normalize_symbol_safe(raw: str) -> str:
    """
    Preferred normalization: core.data_engine_v2.normalize_symbol (if available).
    Fallback normalization (safe + consistent):
      - trims + uppercases
      - keeps Yahoo special symbols (^GSPC, GC=F, EURUSD=X)
      - numeric => 1120.SR
      - alpha => AAPL.US
      - has '.' suffix => keep as is
    """
    s = (raw or "").strip()
    if not s:
        return ""

    # Prefer canonical normalizer if present (lazy import; never at module load)
    try:
        from core.data_engine_v2 import normalize_symbol as _norm  # type: ignore

        ns = _norm(s)
        return (ns or "").strip().upper()
    except Exception:
        pass

    s = s.strip().upper()

    # Tadawul-style prefixes (defensive)
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", "")

    # Special Yahoo symbols
    if any(ch in s for ch in ("=", "^")):
        return s

    # Already has suffix
    if "." in s:
        return s

    # Numeric-only -> KSA
    if s.isdigit():
        return f"{s}.SR"

    # Alpha -> assume US
    if s.isalpha():
        return f"{s}.US"

    return s


def _safe_error_message(e: BaseException) -> str:
    msg = str(e).strip()
    return msg or e.__class__.__name__


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _as_payload(obj: Any) -> Dict[str, Any]:
    """
    Convert any return type into JSON-safe dict without throwing.
    """
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return jsonable_encoder(obj)

    # Pydantic v2
    md = getattr(obj, "model_dump", None)
    if callable(md):
        try:
            return jsonable_encoder(md())
        except Exception:
            pass

    # Pydantic v1
    d = getattr(obj, "dict", None)
    if callable(d):
        try:
            return jsonable_encoder(d())
        except Exception:
            pass

    # dataclass-ish
    od = getattr(obj, "__dict__", None)
    if isinstance(od, dict) and od:
        try:
            return jsonable_encoder(dict(od))
        except Exception:
            pass

    # fallback
    return {"value": str(obj)}


async def _call_engine_best_effort(request: Request, symbol: str) -> Tuple[Optional[Any], Optional[str]]:
    """
    Try in this order:
      1) app.state.engine (preferred)
      2) core.data_engine_v2.DataEngine() temporary
      3) core.data_engine_v2.get_enriched_quote (module-level)
      4) core.data_engine.get_enriched_quote (legacy module-level)
      5) core.data_engine.DataEngine() temporary

    Returns (result, source_label) or (None, None) if all attempts fail.
    """
    # 1) Preferred: shared engine in app.state
    eng = getattr(request.app.state, "engine", None)
    if eng is not None:
        for fn_name in ("get_enriched_quote", "get_quote"):
            fn = getattr(eng, fn_name, None)
            if callable(fn):
                try:
                    return await _maybe_await(fn(symbol)), f"app.state.engine.{fn_name}"
                except Exception:
                    # try next method / fallback paths
                    pass

    # 2) v2 temporary engine
    try:
        from core.data_engine_v2 import DataEngine as V2Engine  # type: ignore

        tmp = V2Engine()
        try:
            fn = getattr(tmp, "get_enriched_quote", None) or getattr(tmp, "get_quote", None)
            if callable(fn):
                return await _maybe_await(fn(symbol)), "core.data_engine_v2.DataEngine(temp)"
        finally:
            aclose = getattr(tmp, "aclose", None)
            if callable(aclose):
                try:
                    await _maybe_await(aclose())
                except Exception:
                    pass
    except Exception:
        pass

    # 3) v2 module-level function
    try:
        from core.data_engine_v2 import get_enriched_quote as v2_get  # type: ignore

        return await _maybe_await(v2_get(symbol)), "core.data_engine_v2.get_enriched_quote"
    except Exception:
        pass

    # 4) legacy module-level
    try:
        from core.data_engine import get_enriched_quote as v1_get  # type: ignore

        return await _maybe_await(v1_get(symbol)), "core.data_engine.get_enriched_quote"
    except Exception:
        pass

    # 5) legacy temporary engine
    try:
        from core.data_engine import DataEngine as V1Engine  # type: ignore

        tmp2 = V1Engine()
        try:
            fn2 = getattr(tmp2, "get_enriched_quote", None) or getattr(tmp2, "get_quote", None)
            if callable(fn2):
                return await _maybe_await(fn2(symbol)), "core.data_engine.DataEngine(temp)"
        finally:
            aclose2 = getattr(tmp2, "aclose", None)
            if callable(aclose2):
                try:
                    await _maybe_await(aclose2())
                except Exception:
                    pass
    except Exception:
        pass

    return None, None


def _finalize_payload(payload: Dict[str, Any], *, raw: str, norm: str, source: str) -> Dict[str, Any]:
    """
    Ensure required fields and consistent defaults.
    """
    if not payload.get("symbol"):
        payload["symbol"] = norm or raw

    # Keep these helpful for debugging client-side (non-breaking)
    payload.setdefault("symbol_input", raw)
    payload.setdefault("symbol_normalized", norm or raw)

    # Status normalization
    if "status" not in payload:
        payload["status"] = "success"

    # Ensure error is never None (but allow empty string on success)
    if payload.get("error") is None:
        payload["error"] = ""

    # Best-effort source stamp if missing
    if not payload.get("data_source"):
        payload["data_source"] = source or "unknown"

    return payload


@router.get("/quote")
async def enriched_quote(
    request: Request,
    symbol: str = Query(..., description="Ticker symbol (AAPL, MSFT.US, 1120.SR, ^GSPC, GC=F)"),
    debug: int = Query(0, description="Set 1 to include traceback (or enable DEBUG_ERRORS=1)"),
):
    dbg = _truthy(os.getenv("DEBUG_ERRORS", "0")) or bool(debug)

    raw = (symbol or "").strip()
    norm = _normalize_symbol_safe(raw)

    if not raw:
        return JSONResponse(
            status_code=200,
            content={
                "status": "error",
                "symbol": "",
                "symbol_input": "",
                "symbol_normalized": "",
                "data_quality": "MISSING",
                "data_source": "none",
                "error": "Empty symbol",
            },
        )

    try:
        result, source = await _call_engine_best_effort(request, norm or raw)
        if result is None:
            out = {
                "status": "error",
                "symbol": norm or raw,
                "symbol_input": raw,
                "symbol_normalized": norm or raw,
                "data_quality": "MISSING",
                "data_source": "none",
                "error": "Enriched quote engine not available (no working provider).",
            }
            return JSONResponse(status_code=200, content=out)

        payload = _as_payload(result)
        payload = _finalize_payload(payload, raw=raw, norm=norm, source=source or "unknown")
        return JSONResponse(status_code=200, content=payload)

    except Exception as e:
        tb = traceback.format_exc()
        msg = _safe_error_message(e)

        out: Dict[str, Any] = {
            "status": "error",
            "symbol": norm or raw,
            "symbol_input": raw,
            "symbol_normalized": norm or raw,
            "data_quality": "MISSING",
            "data_source": "none",
            "error": msg,
        }
        if dbg:
            out["traceback"] = tb[:8000]

        return JSONResponse(status_code=200, content=out)


@router.get("/quotes")
async def enriched_quotes(
    request: Request,
    symbols: str = Query(..., description="Comma/space-separated symbols, e.g. AAPL,MSFT,1120.SR"),
    debug: int = Query(0, description="Set 1 to include traceback (or enable DEBUG_ERRORS=1)"),
):
    dbg = _truthy(os.getenv("DEBUG_ERRORS", "0")) or bool(debug)

    raw_list = _split_symbols(symbols)
    if not raw_list:
        return JSONResponse(
            status_code=200,
            content={
                "status": "error",
                "error": "Empty symbols list",
                "items": [],
            },
        )

    items: List[Dict[str, Any]] = []
    for raw in raw_list:
        norm = _normalize_symbol_safe(raw)

        try:
            result, source = await _call_engine_best_effort(request, norm or raw)
            if result is None:
                items.append(
                    {
                        "status": "error",
                        "symbol": norm or raw,
                        "symbol_input": raw,
                        "symbol_normalized": norm or raw,
                        "data_quality": "MISSING",
                        "data_source": "none",
                        "error": "Engine not available for this symbol.",
                    }
                )
                continue

            payload = _as_payload(result)
            payload = _finalize_payload(payload, raw=raw, norm=norm, source=source or "unknown")
            items.append(payload)

        except Exception as e:
            tb = traceback.format_exc()
            msg = _safe_error_message(e)

            out: Dict[str, Any] = {
                "status": "error",
                "symbol": norm or raw,
                "symbol_input": raw,
                "symbol_normalized": norm or raw,
                "data_quality": "MISSING",
                "data_source": "none",
                "error": msg,
            }
            if dbg:
                out["traceback"] = tb[:8000]
            items.append(out)

    return JSONResponse(status_code=200, content={"status": "success", "count": len(items), "items": items})


@router.get("/health", include_in_schema=False)
async def enriched_health():
    return {"status": "ok", "module": "routes.enriched_quote", "version": "2.3.0"}


__all__ = ["router"]
