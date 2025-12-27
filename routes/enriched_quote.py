# routes/enriched_quote.py  (FULL REPLACEMENT)
"""
routes/enriched_quote.py
------------------------------------------------------------
Enriched Quote Router – PROD SAFE + DEBUGGABLE (v2.4.0) — ALIGNED

Key guarantees
- Never crashes app startup (no network at import time; core imports are lazy inside functions)
- Always returns HTTP 200 with a status field
- Always returns non-empty `error` on failure
- Always returns `symbol` (normalized or input)
- Best-effort schema-fill: returns all UnifiedQuote keys (None if unavailable)
- Batch endpoint:
    1) FAST PATH: try engine batch call (get_enriched_quotes/get_quotes) once
    2) FALLBACK: safe concurrency per-symbol (ENRICHED_BATCH_CONCURRENCY) preserving order

Normalization preference (safe)
1) core.data_engine.normalize_symbol (legacy adapter, uses v2 if available)
2) core.data_engine_v2.normalize_symbol (if exists)
3) fallback rules
"""

from __future__ import annotations

import asyncio
import inspect
import os
import traceback
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Query, Request
from fastapi.encoders import jsonable_encoder
from starlette.responses import JSONResponse

router = APIRouter(prefix="/v1/enriched", tags=["enriched_quote"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}

# Cached UnifiedQuote schema keys (best-effort)
_UQ_KEYS: Optional[List[str]] = None


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
    Preferred normalization:
      1) core.data_engine.normalize_symbol (legacy adapter; v2-aware)
      2) core.data_engine_v2.normalize_symbol (if exported)
    Fallback normalization:
      - trims + uppercases
      - keeps Yahoo special symbols (^GSPC, GC=F, EURUSD=X)
      - numeric => 1120.SR
      - alpha => AAPL.US
      - has '.' suffix => keep as is
    """
    s = (raw or "").strip()
    if not s:
        return ""

    # 1) legacy adapter normalizer (often the best option)
    try:
        from core.data_engine import normalize_symbol as _norm1  # type: ignore

        ns = _norm1(s)
        return (ns or "").strip().upper()
    except Exception:
        pass

    # 2) v2 normalizer (only if explicitly exported)
    try:
        from core.data_engine_v2 import normalize_symbol as _norm2  # type: ignore

        ns = _norm2(s)
        return (ns or "").strip().upper()
    except Exception:
        pass

    su = s.strip().upper()

    if su.startswith("TADAWUL:"):
        su = su.split(":", 1)[1].strip()
    if su.endswith(".TADAWUL"):
        su = su.replace(".TADAWUL", "")

    # Yahoo special tickers
    if any(ch in su for ch in ("=", "^")):
        return su

    if "." in su:
        return su

    if su.isdigit():
        return f"{su}.SR"

    if su.isalpha():
        return f"{su}.US"

    return su


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

    md = getattr(obj, "model_dump", None)
    if callable(md):
        try:
            return jsonable_encoder(md())
        except Exception:
            pass

    d = getattr(obj, "dict", None)
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


def _get_uq_keys() -> List[str]:
    """
    Best-effort get UnifiedQuote field names (cached).
    If import fails, returns [] (no schema-fill).
    """
    global _UQ_KEYS
    if isinstance(_UQ_KEYS, list):
        return _UQ_KEYS

    # Prefer legacy adapter export (works even if v2 missing => stub)
    try:
        from core.data_engine import UnifiedQuote as UQ  # type: ignore

        mf = getattr(UQ, "model_fields", None)
        if isinstance(mf, dict) and mf:
            _UQ_KEYS = list(mf.keys())
            return _UQ_KEYS
    except Exception:
        pass

    # Fallback: try v2 directly if it exports UnifiedQuote
    try:
        from core.data_engine_v2 import UnifiedQuote as UQ2  # type: ignore

        mf2 = getattr(UQ2, "model_fields", None)
        if isinstance(mf2, dict) and mf2:
            _UQ_KEYS = list(mf2.keys())
            return _UQ_KEYS
    except Exception:
        pass

    _UQ_KEYS = []
    return _UQ_KEYS


def _schema_fill_best_effort(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensure payload contains every UnifiedQuote field (best-effort),
    so Sheets/clients never miss expected keys even if engines return partial dicts.
    """
    keys = _get_uq_keys()
    if not keys:
        return payload
    for k in keys:
        payload.setdefault(k, None)
    return payload


async def _call_engine_best_effort(request: Request, symbol: str) -> Tuple[Optional[Any], Optional[str]]:
    """
    Try in this order:
      1) app.state.engine (preferred)
      2) core.data_engine_v2.get_enriched_quote (module-level singleton)
      3) core.data_engine_v2.DataEngine() temporary
      4) legacy module-level core.data_engine.get_enriched_quote
      5) legacy engine class core.data_engine.DataEngine() temporary
    """
    # 1) app.state.engine
    try:
        eng = getattr(request.app.state, "engine", None)
    except Exception:
        eng = None

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
    - results_list: list aligned with request order when possible.
    """
    if not symbols_norm:
        return None, None, "empty"

    # 1) app.state.engine batch
    eng = None
    try:
        eng = getattr(request.app.state, "engine", None)
    except Exception:
        eng = None

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
    Ensure required fields and consistent defaults + schema-fill best-effort.
    """
    sym = (norm or raw or "").strip()

    if not payload.get("symbol"):
        payload["symbol"] = sym

    payload["symbol_input"] = payload.get("symbol_input") or raw
    payload["symbol_normalized"] = payload.get("symbol_normalized") or sym

    if not payload.get("data_source"):
        payload["data_source"] = source or "unknown"

    # error always a string
    if payload.get("error") is None:
        payload["error"] = ""
    if not isinstance(payload.get("error"), str):
        payload["error"] = str(payload.get("error") or "")

    # status always present; if error non-empty => error
    st = payload.get("status")
    if not isinstance(st, str) or not st.strip():
        payload["status"] = "success"
    if str(payload.get("error") or "").strip():
        payload["status"] = "error"

    # data_quality best-effort
    if not payload.get("data_quality"):
        payload["data_quality"] = "MISSING" if payload.get("current_price") is None else "PARTIAL"

    return _schema_fill_best_effort(payload)


def _align_batch_results_by_index_or_symbol(
    raw_list: List[str],
    norms: List[str],
    batch_res: List[Any],
    batch_source: str,
) -> List[Dict[str, Any]]:
    """
    Aligns batch results to requested order.
    Strategy:
      - If lengths match => index alignment
      - Else => attempt symbol mapping (stable + supports providers that reorder)
    """
    items: List[Dict[str, Any]] = []

    # 1) index alignment if same length
    if len(batch_res) == len(norms):
        for i, raw in enumerate(raw_list):
            norm = norms[i] if i < len(norms) else (_normalize_symbol_safe(raw) or raw)
            obj = batch_res[i]
            payload = _as_payload(obj)
            payload = _finalize_payload(payload, raw=raw, norm=norm, source=batch_source or "unknown")
            items.append(payload)
        return items

    # 2) symbol mapping (supports reorder + duplicates best-effort)
    buckets: Dict[str, List[Any]] = {}
    for obj in batch_res:
        d = _as_payload(obj)
        key = (d.get("symbol_normalized") or d.get("symbol") or "").strip().upper()
        if not key:
            # keep under empty bucket
            buckets.setdefault("", []).append(obj)
        else:
            buckets.setdefault(key, []).append(obj)

    for raw, norm in zip(raw_list, norms):
        k = (norm or raw or "").strip().upper()
        candidate = None

        if k in buckets and buckets[k]:
            candidate = buckets[k].pop(0)
        elif "" in buckets and buckets[""]:
            candidate = buckets[""].pop(0)

        if candidate is None:
            out = {
                "status": "error",
                "symbol": norm or raw,
                "symbol_input": raw,
                "symbol_normalized": norm or raw,
                "data_quality": "MISSING",
                "data_source": "none",
                "error": "Engine returned no item for this symbol.",
            }
            items.append(_schema_fill_best_effort(out))
        else:
            payload = _as_payload(candidate)
            payload = _finalize_payload(payload, raw=raw, norm=norm, source=batch_source or "unknown")
            items.append(payload)

    return items


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
        out: Dict[str, Any] = {
            "status": "error",
            "symbol": norm or raw,
            "symbol_input": raw,
            "symbol_normalized": norm or raw,
            "data_quality": "MISSING",
            "data_source": "none",
            "error": _safe_error_message(e),
        }
        if dbg:
            out["traceback"] = traceback.format_exc()[:8000]
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
            content={"status": "error", "error": "Empty symbols list", "items": []},
        )

    # Preserve order; normalize each (keep duplicates)
    norms = [(_normalize_symbol_safe(r) or (r or "").strip()) for r in raw_list]
    norms = [n for n in norms if n]

    if not norms:
        return JSONResponse(
            status_code=200,
            content={"status": "error", "error": "No valid symbols after normalization", "items": []},
        )

    # 1) FAST PATH: try batch call first
    batch_res, batch_source, batch_err = await _call_engine_batch_best_effort(request, norms)

    if isinstance(batch_res, list) and batch_res:
        try:
            items = _align_batch_results_by_index_or_symbol(raw_list, norms, batch_res, batch_source or "unknown")
            return JSONResponse(status_code=200, content={"status": "success", "count": len(items), "items": items})
        except Exception as e:
            # fall through to slow path (keep hint for debug)
            batch_err = batch_err or _safe_error_message(e)

    # 2) SLOW PATH: per-symbol with safe concurrency while preserving order
    try:
        conc = int(os.getenv("ENRICHED_BATCH_CONCURRENCY", "8"))
        if conc <= 0:
            conc = 8
        if conc > 25:
            conc = 25
    except Exception:
        conc = 8

    sem = asyncio.Semaphore(conc)

    async def _one(raw: str) -> Dict[str, Any]:
        raw2 = (raw or "").strip()
        norm2 = _normalize_symbol_safe(raw2)

        try:
            async with sem:
                result, source = await _call_engine_best_effort(request, norm2 or raw2)

            if result is None:
                out = {
                    "status": "error",
                    "symbol": norm2 or raw2,
                    "symbol_input": raw2,
                    "symbol_normalized": norm2 or raw2,
                    "data_quality": "MISSING",
                    "data_source": "none",
                    "error": "Engine not available for this symbol.",
                }
                if dbg and batch_err:
                    out["batch_error_hint"] = str(batch_err)[:1200]
                return _schema_fill_best_effort(out)

            payload = _as_payload(result)
            payload = _finalize_payload(payload, raw=raw2, norm=norm2, source=source or "unknown")
            return payload

        except Exception as e:
            out: Dict[str, Any] = {
                "status": "error",
                "symbol": norm2 or raw2,
                "symbol_input": raw2,
                "symbol_normalized": norm2 or raw2,
                "data_quality": "MISSING",
                "data_source": "none",
                "error": _safe_error_message(e),
            }
            if dbg:
                out["traceback"] = traceback.format_exc()[:8000]
                if batch_err:
                    out["batch_error_hint"] = str(batch_err)[:1200]
            return _schema_fill_best_effort(out)

    tasks = [_one(r) for r in raw_list]
    items: List[Dict[str, Any]] = await asyncio.gather(*tasks)
    return JSONResponse(status_code=200, content={"status": "success", "count": len(items), "items": items})


@router.get("/health", include_in_schema=False)
async def enriched_health():
    return {"status": "ok", "module": "routes.enriched_quote", "version": "2.4.0"}


def get_router() -> APIRouter:
    return router


__all__ = ["router", "get_router"]
