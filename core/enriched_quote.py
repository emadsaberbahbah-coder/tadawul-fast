# core/enriched_quote.py  (FULL REPLACEMENT)
"""
core/enriched_quote.py
------------------------------------------------------------
Compatibility Router: Enriched Quote (PROD SAFE) — v2.3.8

What this router is for
- Legacy/compat endpoint under /v1/enriched/* (kept because some clients still call it)
- Always returns HTTP 200 with a `status` field (client simplicity)
- Works with BOTH async and sync engines
- Attempts batch fast-path first; falls back per-symbol safely

v2.3.8 improvements vs v2.3.7
- ✅ Fixes potential "last_err referenced before assignment" edge case
- ✅ More defensive: never raises in finalize/schema fill
- ✅ Keeps behavior identical for clients (HTTP 200, status field, schema fill)
"""

from __future__ import annotations

import inspect
import os
import traceback
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Query, Request
from fastapi.encoders import jsonable_encoder
from starlette.responses import JSONResponse

ROUTER_VERSION = "2.3.8"

router = APIRouter(prefix="/v1/enriched", tags=["enriched"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_UQ_KEYS: Optional[List[str]] = None


def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY


def _clamp(s: Any, n: int = 2000) -> str:
    t = (str(s) if s is not None else "").strip()
    if not t:
        return ""
    return t if len(t) <= n else (t[: n - 12] + " ...TRUNC...")


def _split_symbols(raw: str) -> List[str]:
    s = (raw or "").replace("\n", " ").replace("\t", " ").strip()
    if not s:
        return []
    s = s.replace(",", " ")
    return [p.strip() for p in s.split(" ") if p.strip()]


def _normalize_symbol_safe(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    # Prefer engine's normalize if available
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
    # indices/commodities etc
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
    Convert engine return types into a JSON-safe dict.
    - dict: passthrough
    - pydantic: model_dump()/dict()
    - dataclass-ish: __dict__
    - fallback: {"value": "..."}
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
    Cached list of UnifiedQuote fields, if available.
    Never raises.
    """
    global _UQ_KEYS
    if isinstance(_UQ_KEYS, list):
        return _UQ_KEYS

    try:
        from core.data_engine_v2 import UnifiedQuote as UQ  # type: ignore

        mf = getattr(UQ, "model_fields", None)
        if isinstance(mf, dict) and mf:
            _UQ_KEYS = list(mf.keys())
            return _UQ_KEYS
    except Exception:
        pass

    _UQ_KEYS = []
    return _UQ_KEYS


def _schema_fill_best_effort(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensure keys exist for downstream sheet writers.
    Does not overwrite existing values.
    """
    try:
        keys = _get_uq_keys()
        for k in keys:
            payload.setdefault(k, None)
    except Exception:
        pass
    return payload


async def _call_engine_best_effort(request: Request, symbol: str) -> Tuple[Optional[Any], Optional[str], Optional[str]]:
    """
    Returns: (result, source, error)
    """
    # 1) app.state.engine
    try:
        eng = getattr(request.app.state, "engine", None)
    except Exception:
        eng = None

    if eng is not None:
        last_err: Optional[str] = None
        for fn_name in ("get_enriched_quote", "get_quote"):
            fn = getattr(eng, fn_name, None)
            if callable(fn):
                try:
                    return await _maybe_await(fn(symbol)), f"app.state.engine.{fn_name}", None
                except Exception as e:
                    last_err = _safe_error_message(e)
                    continue
        return None, "app.state.engine", last_err or "engine call failed"

    # 2) v2 module singleton function (if present)
    try:
        from core.data_engine_v2 import get_enriched_quote as v2_get  # type: ignore

        return await _maybe_await(v2_get(symbol)), "core.data_engine_v2.get_enriched_quote(singleton)", None
    except Exception:
        pass

    # 3) v2 temp engine
    try:
        from core.data_engine_v2 import DataEngine as V2Engine  # type: ignore

        tmp = V2Engine()
        try:
            fn = getattr(tmp, "get_enriched_quote", None) or getattr(tmp, "get_quote", None)
            if callable(fn):
                return await _maybe_await(fn(symbol)), "core.data_engine_v2.DataEngine(temp)", None
        finally:
            aclose = getattr(tmp, "aclose", None)
            if callable(aclose):
                try:
                    await _maybe_await(aclose())
                except Exception:
                    pass
            close = getattr(tmp, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass
    except Exception:
        pass

    # 4) legacy module-level singleton
    try:
        from core.data_engine import get_enriched_quote as v1_get  # type: ignore

        return await _maybe_await(v1_get(symbol)), "core.data_engine.get_enriched_quote", None
    except Exception:
        pass

    return None, None, "no provider/engine available"


async def _call_engine_batch_best_effort(
    request: Request, symbols_norm: List[str]
) -> Tuple[Optional[List[Any]], Optional[str], Optional[str]]:
    """
    Returns: (list_result, source, error)
    """
    if not symbols_norm:
        return None, None, "empty"

    # 1) app.state.engine batch
    try:
        eng = getattr(request.app.state, "engine", None)
    except Exception:
        eng = None

    if eng is not None:
        last_err: Optional[str] = None
        for fn_name in ("get_enriched_quotes", "get_quotes"):
            fn = getattr(eng, fn_name, None)
            if callable(fn):
                try:
                    res = await _maybe_await(fn(symbols_norm))
                    if isinstance(res, list):
                        return res, f"app.state.engine.{fn_name}", None
                    last_err = "batch returned non-list"
                except Exception as e:
                    last_err = _safe_error_message(e)
        return None, "app.state.engine(batch)", last_err or "batch call failed"

    # 2) v2 singleton batch
    try:
        from core.data_engine_v2 import get_enriched_quotes as v2_batch  # type: ignore

        res2 = await _maybe_await(v2_batch(symbols_norm))
        if isinstance(res2, list):
            return res2, "core.data_engine_v2.get_enriched_quotes(singleton)", None
        return None, "core.data_engine_v2.get_enriched_quotes(singleton)", "batch returned non-list"
    except Exception:
        pass

    return None, None, None


def _finalize_payload(payload: Dict[str, Any], *, raw: str, norm: str, source: str) -> Dict[str, Any]:
    """
    Ensure standard metadata + status/error behavior is consistent.
    """
    sym = (norm or raw or "").strip()
    if not sym:
        sym = ""

    try:
        payload.setdefault("symbol", sym)
        payload["symbol_input"] = payload.get("symbol_input") or raw
        payload["symbol_normalized"] = payload.get("symbol_normalized") or sym

        # normalize error field to string
        if payload.get("error") is None:
            payload["error"] = ""
        if not isinstance(payload.get("error"), str):
            payload["error"] = str(payload.get("error") or "")

        # set status consistently
        if str(payload.get("error") or "").strip():
            payload["status"] = "error"
        else:
            if not str(payload.get("status") or "").strip():
                payload["status"] = "success"

        # data_source/data_quality defaults
        if not payload.get("data_source"):
            payload["data_source"] = source or "unknown"

        if not payload.get("data_quality"):
            payload["data_quality"] = "MISSING" if payload.get("current_price") is None else "PARTIAL"

        return _schema_fill_best_effort(payload)
    except Exception:
        # absolute last-resort safety
        return _schema_fill_best_effort(
            {
                "status": "error",
                "symbol": sym,
                "symbol_input": raw,
                "symbol_normalized": sym,
                "data_quality": "MISSING",
                "data_source": source or "unknown",
                "error": "payload_finalize_failed",
            }
        )


def _payload_symbol_key(p: Dict[str, Any]) -> str:
    """
    Extract a robust symbol key for alignment.
    """
    try:
        for k in ("symbol_normalized", "symbol", "Symbol", "ticker", "code"):
            v = p.get(k)
            if v is not None:
                s = str(v).strip().upper()
                if s:
                    return s
    except Exception:
        pass
    return ""


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
        out = _schema_fill_best_effort(
            {
                "status": "error",
                "symbol": "",
                "symbol_input": "",
                "symbol_normalized": "",
                "data_quality": "MISSING",
                "data_source": "none",
                "error": "Empty symbol",
            }
        )
        return JSONResponse(status_code=200, content=out)

    try:
        result, source, err = await _call_engine_best_effort(request, norm or raw)
        if result is None:
            out = _schema_fill_best_effort(
                {
                    "status": "error",
                    "symbol": norm or raw,
                    "symbol_input": raw,
                    "symbol_normalized": norm or raw,
                    "data_quality": "MISSING",
                    "data_source": "none",
                    "error": err or "Enriched quote engine not available (no working provider).",
                }
            )
            return JSONResponse(status_code=200, content=out)

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
            out["traceback"] = _clamp(traceback.format_exc(), 8000)
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
        return JSONResponse(status_code=200, content={"status": "error", "error": "Empty symbols list", "items": []})

    norms = [_normalize_symbol_safe(r) or (r or "").strip() for r in raw_list]
    norms = [n for n in norms if n]

    batch_res, batch_source, batch_err = await _call_engine_batch_best_effort(request, norms)

    items: List[Dict[str, Any]] = []

    # ------------------------------------------------------------
    # FAST PATH: batch list returned
    # ------------------------------------------------------------
    if isinstance(batch_res, list) and batch_res:
        payloads: List[Dict[str, Any]] = [_as_payload(x) for x in batch_res]

        # Case A: length matches input -> index align
        if len(payloads) == len(raw_list):
            for i, raw in enumerate(raw_list):
                norm = _normalize_symbol_safe(raw) or raw
                p = payloads[i] if i < len(payloads) else {}
                items.append(_finalize_payload(p, raw=raw, norm=norm, source=batch_source or "unknown"))

        # Case B: mismatch -> map by symbol key
        else:
            mp: Dict[str, Dict[str, Any]] = {}
            for p in payloads:
                k = _payload_symbol_key(p)
                if k and k not in mp:
                    mp[k] = p

            for raw in raw_list:
                norm = (_normalize_symbol_safe(raw) or raw).strip().upper()
                p = mp.get(norm)
                if p is None:
                    out = {
                        "status": "error",
                        "symbol": norm,
                        "symbol_input": raw,
                        "symbol_normalized": norm,
                        "data_quality": "MISSING",
                        "data_source": "none",
                        "error": "Engine returned no item for this symbol.",
                    }
                    if dbg and batch_err:
                        out["batch_error_hint"] = _clamp(batch_err, 1200)
                    items.append(_schema_fill_best_effort(out))
                else:
                    items.append(_finalize_payload(p, raw=raw, norm=norm, source=batch_source or "unknown"))

        return JSONResponse(status_code=200, content={"status": "success", "count": len(items), "items": items})

    # ------------------------------------------------------------
    # SLOW PATH: per-symbol
    # ------------------------------------------------------------
    for raw in raw_list:
        norm = _normalize_symbol_safe(raw)
        try:
            result, source, err = await _call_engine_best_effort(request, norm or raw)
            if result is None:
                out = {
                    "status": "error",
                    "symbol": norm or raw,
                    "symbol_input": raw,
                    "symbol_normalized": norm or raw,
                    "data_quality": "MISSING",
                    "data_source": "none",
                    "error": err or "Engine not available for this symbol.",
                }
                if dbg and batch_err:
                    out["batch_error_hint"] = _clamp(batch_err, 1200)
                items.append(_schema_fill_best_effort(out))
                continue

            payload = _as_payload(result)
            items.append(_finalize_payload(payload, raw=raw, norm=norm, source=source or "unknown"))

        except Exception as e:
            out2: Dict[str, Any] = {
                "status": "error",
                "symbol": norm or raw,
                "symbol_input": raw,
                "symbol_normalized": norm or raw,
                "data_quality": "MISSING",
                "data_source": "none",
                "error": _safe_error_message(e),
            }
            if dbg:
                out2["traceback"] = _clamp(traceback.format_exc(), 8000)
                if batch_err:
                    out2["batch_error_hint"] = _clamp(batch_err, 1200)
            items.append(_schema_fill_best_effort(out2))

    return JSONResponse(status_code=200, content={"status": "success", "count": len(items), "items": items})


@router.get("/health", include_in_schema=False)
async def enriched_health():
    return {"status": "ok", "module": "core.enriched_quote", "version": ROUTER_VERSION}


def get_router() -> APIRouter:
    return router


__all__ = ["router", "get_router"]
