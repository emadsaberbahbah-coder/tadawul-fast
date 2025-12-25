# core/enriched_quote.py  (FULL REPLACEMENT)
"""
core/enriched_quote.py
------------------------------------------------------------
Compatibility Router: Enriched Quote (PROD SAFE) – v2.3.3

What’s improved vs v2.3.2:
- ✅ Batch acceleration for /v1/enriched/quotes:
    tries engine.get_enriched_quotes/get_quotes (single call) before falling back to per-symbol calls.
- ✅ Still ALWAYS returns HTTP 200 with {"status": "..."} for client simplicity.
- ✅ Optional debug trace via env DEBUG_ERRORS=1 or query ?debug=1
- ✅ Best-effort schema-fill (UnifiedQuote.model_fields) to prevent missing keys
"""

from __future__ import annotations

import inspect
import os
import traceback
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Query, Request
from fastapi.encoders import jsonable_encoder
from starlette.responses import JSONResponse

router = APIRouter(prefix="/v1/enriched", tags=["enriched"])

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
    Fallback:
      - trims + uppercases
      - keeps special Yahoo symbols (^GSPC, GC=F, EURUSD=X)
      - numeric => 1120.SR
      - alpha => AAPL.US
      - has '.' suffix => keep as is
    """
    s = (raw or "").strip()
    if not s:
        return ""

    # Prefer canonical normalizer if present (lazy import)
    try:
        from core.data_engine_v2 import normalize_symbol as _norm  # type: ignore

        ns = _norm(s)
        return (ns or "").strip().upper()
    except Exception:
        pass

    s = s.strip().upper()

    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", "")

    if any(ch in s for ch in ("=", "^")):
        return s

    if "." in s:
        return s

    if s.isdigit():
        return f"{s}.SR"

    if s.isalpha():
        return f"{s}.US"

    return s


def _safe_error_message(e: BaseException) -> str:
    msg = str(e).strip()
    return msg or e.__class__.__name__


async def _maybe_await(v: Any) -> Any:
    if inspect.isawaitable(v):
        return await v
    return v


def _as_payload(obj: Any) -> Dict[str, Any]:
    """
    Convert any return type into JSON-safe dict without throwing.
    """
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return jsonable_encoder(obj)

    md = getattr(obj, "model_dump", None)  # Pydantic v2
    if callable(md):
        try:
            return jsonable_encoder(md())
        except Exception:
            pass

    d = getattr(obj, "dict", None)  # Pydantic v1
    if callable(d):
        try:
            return jsonable_encoder(d())
        except Exception:
            pass

    od = getattr(obj, "__dict__", None)
    if isinstance(od, dict) and od:
        try:
            return jsonable_encoder(dict(od))
        except Exception:
            pass

    return {"value": str(obj)}


def _schema_fill_best_effort(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensure payload contains every UnifiedQuote field (best-effort),
    so Sheets/clients never miss expected keys even if legacy engines return partial dicts.
    """
    try:
        from core.data_engine_v2 import UnifiedQuote as UQ  # type: ignore

        mf = getattr(UQ, "model_fields", None)
        if isinstance(mf, dict) and mf:
            for k in mf.keys():
                payload.setdefault(k, None)
    except Exception:
        pass
    return payload


async def _call_engine_best_effort(request: Request, symbol: str) -> Tuple[Optional[Any], Optional[str]]:
    """
    Try in this order:
      1) app.state.engine (preferred)
      2) core.data_engine_v2.get_enriched_quote (module-level singleton)
      3) core.data_engine_v2.DataEngine() temporary instance
      4) legacy module-level core.data_engine.get_enriched_quote
      5) legacy engine class core.data_engine.DataEngine() temporary instance

    Returns (result, source_label) or (None, None).
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
                    pass

    # 2) v2 module-level singleton
    try:
        from core.data_engine_v2 import get_enriched_quote as v2_get  # type: ignore

        return await _maybe_await(v2_get(symbol)), "core.data_engine_v2.get_enriched_quote(singleton)"
    except Exception:
        pass

    # 3) v2 temporary engine
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


async def _call_engine_batch_best_effort(
    request: Request, symbols_norm: List[str]
) -> Tuple[Optional[List[Any]], Optional[str], Optional[str]]:
    """
    Batch attempt (single call) for /quotes.

    Returns (results_list, source_label, error_message)
    - results_list: list of objects (UnifiedQuote/dict/etc) aligned with the request order when possible.
    """
    if not symbols_norm:
        return None, None, "empty"

    # 1) app.state.engine batch
    eng = getattr(request.app.state, "engine", None)
    if eng is not None:
        for fn_name in ("get_enriched_quotes", "get_quotes"):
            fn = getattr(eng, fn_name, None)
            if callable(fn):
                try:
                    res = await _maybe_await(fn(symbols_norm))
                    if isinstance(res, list):
                        return res, f"app.state.engine.{fn_name}", None
                except Exception as e:
                    return None, f"app.state.engine.{fn_name}", _safe_error_message(e)

    # 2) core.data_engine_v2 module-level batch
    try:
        from core.data_engine_v2 import get_enriched_quotes as v2_batch  # type: ignore

        res2 = await _maybe_await(v2_batch(symbols_norm))
        if isinstance(res2, list):
            return res2, "core.data_engine_v2.get_enriched_quotes(singleton)", None
    except Exception:
        pass

    return None, None, None


def _finalize_payload(payload: Dict[str, Any], *, raw: str, norm: str, source: str) -> Dict[str, Any]:
    """
    Ensure required fields and consistent defaults + schema fill.
    """
    if not payload.get("symbol"):
        payload["symbol"] = norm or raw

    payload.setdefault("symbol_input", raw)
    payload.setdefault("symbol_normalized", norm or raw)

    if "status" not in payload:
        payload["status"] = "success"

    if payload.get("error") is None:
        payload["error"] = ""

    # Preserve an existing provider label if set, otherwise use engine source label
    if not payload.get("data_source"):
        payload["data_source"] = source or "unknown"

    return _schema_fill_best_effort(payload)


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
        out = {
            "status": "error",
            "symbol": "",
            "symbol_input": "",
            "symbol_normalized": "",
            "data_quality": "MISSING",
            "data_source": "none",
            "error": "Empty symbol",
        }
        return JSONResponse(status_code=200, content=_schema_fill_best_effort(out))

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
            return JSONResponse(status_code=200, content=_schema_fill_best_effort(out))

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
        return JSONResponse(status_code=200, content=_schema_fill_best_effort(out))


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

    # Preserve order; normalize each
    norms = [_normalize_symbol_safe(r) or (r or "").strip() for r in raw_list]
    norms = [n for n in norms if n]

    # 1) Try batch call first (FAST PATH)
    batch_res, batch_source, batch_err = await _call_engine_batch_best_effort(request, norms)

    items: List[Dict[str, Any]] = []

    if isinstance(batch_res, list) and batch_res:
        # If engine returned fewer/more than requested, we still try to align best-effort by index.
        for i, raw in enumerate(raw_list):
            norm = _normalize_symbol_safe(raw) or raw
            try:
                obj = batch_res[i] if i < len(batch_res) else None
                if obj is None:
                    out = {
                        "status": "error",
                        "symbol": norm,
                        "symbol_input": raw,
                        "symbol_normalized": norm,
                        "data_quality": "MISSING",
                        "data_source": "none",
                        "error": "Engine returned no item for this symbol.",
                    }
                    items.append(_schema_fill_best_effort(out))
                    continue

                payload = _as_payload(obj)
                payload = _finalize_payload(payload, raw=raw, norm=norm, source=batch_source or "unknown")
                items.append(payload)
            except Exception as e:
                tb = traceback.format_exc()
                msg = _safe_error_message(e)
                out = {
                    "status": "error",
                    "symbol": norm,
                    "symbol_input": raw,
                    "symbol_normalized": norm,
                    "data_quality": "MISSING",
                    "data_source": "none",
                    "error": msg,
                }
                if dbg:
                    out["traceback"] = tb[:8000]
                items.append(_schema_fill_best_effort(out))

        return JSONResponse(status_code=200, content={"status": "success", "count": len(items), "items": items})

    # 2) If batch failed, fall back to per-symbol calls (SLOW PATH)
    #    (We keep batch_err only for debug visibility)
    for raw in raw_list:
        norm = _normalize_symbol_safe(raw)
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
                    "error": "Engine not available for this symbol.",
                }
                if dbg and batch_err:
                    out["batch_error_hint"] = str(batch_err)[:1200]
                items.append(_schema_fill_best_effort(out))
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
                if batch_err:
                    out["batch_error_hint"] = str(batch_err)[:1200]
            items.append(_schema_fill_best_effort(out))

    return JSONResponse(status_code=200, content={"status": "success", "count": len(items), "items": items})


@router.get("/health", include_in_schema=False)
async def enriched_health():
    return {"status": "ok", "module": "core.enriched_quote", "version": "2.3.3"}


def get_router() -> APIRouter:
    return router


__all__ = ["router", "get_router"]
