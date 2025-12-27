# core/enriched_quote.py  (FULL REPLACEMENT)
"""
core/enriched_quote.py
------------------------------------------------------------
Compatibility Router: Enriched Quote (PROD SAFE) – v2.4.0

Guarantees
- ✅ ALWAYS returns HTTP 200 with JSON body containing "status"
- ✅ Single:  GET /v1/enriched/quote?symbol=...
- ✅ Batch:   GET /v1/enriched/quotes?symbols=AAPL,MSFT,1120.SR
- ✅ Uses app.state.engine when available (preferred)
- ✅ Batch acceleration: uses engine.get_enriched_quotes/get_quotes when available
- ✅ Safe symbol normalization (prefers core.data_engine_v2.normalize_symbol)
- ✅ Best-effort schema fill (stable keys) to avoid missing columns in Sheets
- ✅ Optional debug traceback via env DEBUG_ERRORS=1 or query ?debug=1
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


# A stable keyset that matches your DataEngineV2 output contract.
# (We still try UnifiedQuote.model_fields if it exists, but we don't depend on it.)
DEFAULT_FIELDS: List[str] = [
    # core
    "status",
    "symbol",
    "symbol_input",
    "symbol_normalized",
    "market",
    "currency",
    "name",
    "data_source",
    "data_quality",
    "last_updated_utc",
    "error",
    # price / trading
    "current_price",
    "previous_close",
    "open",
    "day_high",
    "day_low",
    "volume",
    "avg_volume_30d",
    "value_traded",
    "price_change",
    "percent_change",
    # 52w / derived
    "high_52w",
    "low_52w",
    "position_52w_percent",
    # fundamentals / ratios
    "market_cap",
    "shares_outstanding",
    "free_float",
    "free_float_market_cap",
    "eps_ttm",
    "pe_ttm",
    "pb",
    "ps",
    "dividend_yield",
    "dividend_rate",
    "payout_ratio",
    "roe",
    "roa",
    "beta",
    "net_margin",
    "debt_to_equity",
    "current_ratio",
    "quick_ratio",
    "ev_ebitda",
    # scoring
    "quality_score",
    "value_score",
    "momentum_score",
    "risk_score",
    "opportunity_score",
]


def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY


def _split_symbols(raw: str) -> List[str]:
    s = (raw or "").replace("\n", " ").replace("\t", " ").strip()
    if not s:
        return []
    # allow comma-separated or space-separated
    parts = [p.strip() for p in (s.split(",") if "," in s else s.split(" "))]
    return [p for p in parts if p]


def _normalize_symbol_safe(raw: str) -> str:
    """
    Preferred: core.data_engine_v2.normalize_symbol
    Fallback:
      - trims + uppercases
      - keeps Yahoo special symbols (^GSPC, GC=F, EURUSD=X)
      - numeric => 1120.SR
      - alpha => AAPL.US
      - has '.' => keep
    """
    s = (raw or "").strip()
    if not s:
        return ""

    try:
        from core.data_engine_v2 import normalize_symbol as _norm  # type: ignore

        ns = _norm(s)
        return (ns or "").strip().upper()
    except Exception:
        pass

    su = s.upper()

    if su.startswith("TADAWUL:"):
        su = su.split(":", 1)[1].strip()
    if su.endswith(".TADAWUL"):
        su = su.replace(".TADAWUL", "")

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


async def _maybe_await(v: Any) -> Any:
    if inspect.isawaitable(v):
        return await v
    return v


def _as_payload(obj: Any) -> Dict[str, Any]:
    """
    Convert any return type into a JSON-safe dict without throwing.
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
    Ensure payload contains every expected key so clients (Sheets) don't break.
    """
    # 1) If a UnifiedQuote model exists somewhere, use its field list (best-effort).
    try:
        UQ = None
        try:
            from core.data_engine_v2 import UnifiedQuote as UQ  # type: ignore
        except Exception:
            try:
                from core.schemas import UnifiedQuote as UQ  # type: ignore
            except Exception:
                UQ = None

        mf = getattr(UQ, "model_fields", None)
        if isinstance(mf, dict) and mf:
            for k in mf.keys():
                payload.setdefault(k, None)
            return payload
    except Exception:
        pass

    # 2) Fallback to our stable contract keys.
    for k in DEFAULT_FIELDS:
        payload.setdefault(k, None)
    return payload


def _finalize_payload(payload: Dict[str, Any], *, raw: str, norm: str, source: str) -> Dict[str, Any]:
    """
    Ensure consistent required fields + schema fill.
    """
    payload.setdefault("symbol_input", raw)
    payload.setdefault("symbol_normalized", norm or raw)

    if not payload.get("symbol"):
        payload["symbol"] = norm or raw

    if "status" not in payload or not payload.get("status"):
        payload["status"] = "success"

    if payload.get("error") is None:
        payload["error"] = ""

    if not payload.get("data_source"):
        payload["data_source"] = source or "unknown"

    if not payload.get("data_quality"):
        payload["data_quality"] = "MISSING" if payload.get("status") == "error" else "OK"

    return _schema_fill_best_effort(payload)


async def _call_engine_best_effort(request: Request, symbol_norm: str) -> Tuple[Optional[Any], Optional[str]]:
    """
    Try in this order (never throws):
      1) app.state.engine.get_enriched_quote / get_quote
      2) core.data_engine_v2.get_enriched_quote (module singleton)
      3) core.data_engine_v2.DataEngine(temp).get_enriched_quote / get_quote
      4) core.data_engine.get_enriched_quote (legacy adapter)
      5) core.data_engine.DataEngine(temp).get_enriched_quote / get_quote
    """
    # 1) Preferred: shared engine in app.state
    eng = getattr(request.app.state, "engine", None)
    if eng is not None:
        for fn_name in ("get_enriched_quote", "get_quote"):
            fn = getattr(eng, fn_name, None)
            if callable(fn):
                try:
                    return await _maybe_await(fn(symbol_norm)), f"app.state.engine.{fn_name}"
                except Exception:
                    pass

    # 2) v2 module-level singleton
    try:
        from core.data_engine_v2 import get_enriched_quote as v2_get  # type: ignore

        return await _maybe_await(v2_get(symbol_norm)), "core.data_engine_v2.get_enriched_quote(singleton)"
    except Exception:
        pass

    # 3) v2 temporary engine
    try:
        from core.data_engine_v2 import DataEngine as V2Engine  # type: ignore

        tmp = V2Engine()
        try:
            fn = getattr(tmp, "get_enriched_quote", None) or getattr(tmp, "get_quote", None)
            if callable(fn):
                return await _maybe_await(fn(symbol_norm)), "core.data_engine_v2.DataEngine(temp)"
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

        return await _maybe_await(v1_get(symbol_norm)), "core.data_engine.get_enriched_quote(legacy)"
    except Exception:
        pass

    # 5) legacy temporary engine
    try:
        from core.data_engine import DataEngine as V1Engine  # type: ignore

        tmp2 = V1Engine()
        try:
            fn2 = getattr(tmp2, "get_enriched_quote", None) or getattr(tmp2, "get_quote", None)
            if callable(fn2):
                return await _maybe_await(fn2(symbol_norm)), "core.data_engine.DataEngine(temp-legacy)"
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
    request: Request, symbols_norm_unique: List[str], *, enrich: bool = True
) -> Tuple[Optional[List[Any]], Optional[str], Optional[str]]:
    """
    Batch attempt (single call).
    Returns (results_list, source_label, error_message)
    """
    if not symbols_norm_unique:
        return None, None, "empty"

    # 1) app.state.engine batch
    eng = getattr(request.app.state, "engine", None)
    if eng is not None:
        for fn_name in ("get_enriched_quotes", "get_quotes"):
            fn = getattr(eng, fn_name, None)
            if callable(fn):
                try:
                    res = await _maybe_await(fn(symbols_norm_unique))
                    if isinstance(res, list):
                        return res, f"app.state.engine.{fn_name}", None
                except Exception as e:
                    return None, f"app.state.engine.{fn_name}", _safe_error_message(e)

    # 2) v2 module-level batch
    try:
        if enrich:
            from core.data_engine_v2 import get_enriched_quotes as v2_batch  # type: ignore

            res2 = await _maybe_await(v2_batch(symbols_norm_unique))
        else:
            from core.data_engine_v2 import get_engine as _get_engine  # type: ignore

            e = await _maybe_await(_get_engine())
            fn = getattr(e, "get_quotes", None)
            if callable(fn):
                res2 = await _maybe_await(fn(symbols_norm_unique))
            else:
                res2 = None

        if isinstance(res2, list):
            return res2, "core.data_engine_v2.batch(singleton)", None
    except Exception as e:
        return None, "core.data_engine_v2.batch(singleton)", _safe_error_message(e)

    return None, None, None


@router.get("/quote")
async def enriched_quote(
    request: Request,
    symbol: str = Query(..., description="Ticker (AAPL, MSFT.US, 1120.SR, ^GSPC, GC=F)"),
    debug: int = Query(0, description="Set 1 to include traceback (or env DEBUG_ERRORS=1)"),
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
    debug: int = Query(0, description="Set 1 to include traceback (or env DEBUG_ERRORS=1)"),
):
    dbg = _truthy(os.getenv("DEBUG_ERRORS", "0")) or bool(debug)

    raw_list = _split_symbols(symbols)
    if not raw_list:
        return JSONResponse(status_code=200, content={"status": "error", "error": "Empty symbols list", "items": []})

    norms_all = [(_normalize_symbol_safe(r) or (r or "").strip()) for r in raw_list]

    # Build unique list for batch speed (preserve first occurrence order)
    seen = set()
    norms_unique: List[str] = []
    for n in norms_all:
        nu = (n or "").strip().upper()
        if not nu:
            continue
        if nu in seen:
            continue
        seen.add(nu)
        norms_unique.append(n)

    # 1) FAST PATH: batch call
    batch_res, batch_source, batch_err = await _call_engine_batch_best_effort(request, norms_unique, enrich=True)

    # Map batch results back to symbols (best-effort)
    by_symbol: Dict[str, Any] = {}
    if isinstance(batch_res, list) and batch_res:
        for i, sym in enumerate(norms_unique):
            obj = batch_res[i] if i < len(batch_res) else None
            if obj is not None:
                by_symbol[(sym or "").strip().upper()] = obj

    items: List[Dict[str, Any]] = []

    # If batch worked for at least one item, use it and only fallback for misses
    batch_worked = bool(by_symbol)

    for raw, norm in zip(raw_list, norms_all):
        rn = (norm or "").strip()
        key = rn.upper()

        try:
            if batch_worked and key in by_symbol:
                payload = _as_payload(by_symbol[key])
                payload = _finalize_payload(payload, raw=raw, norm=rn, source=batch_source or "unknown")
                items.append(payload)
                continue

            # 2) SLOW PATH: per-symbol call
            result, source = await _call_engine_best_effort(request, rn or raw)
            if result is None:
                out = {
                    "status": "error",
                    "symbol": rn or raw,
                    "symbol_input": raw,
                    "symbol_normalized": rn or raw,
                    "data_quality": "MISSING",
                    "data_source": "none",
                    "error": "Engine not available for this symbol.",
                }
                if dbg and batch_err:
                    out["batch_error_hint"] = str(batch_err)[:1200]
                items.append(_schema_fill_best_effort(out))
                continue

            payload = _as_payload(result)
            payload = _finalize_payload(payload, raw=raw, norm=rn, source=source or "unknown")
            items.append(payload)

        except Exception as e:
            out: Dict[str, Any] = {
                "status": "error",
                "symbol": rn or raw,
                "symbol_input": raw,
                "symbol_normalized": rn or raw,
                "data_quality": "MISSING",
                "data_source": "none",
                "error": _safe_error_message(e),
            }
            if dbg:
                out["traceback"] = traceback.format_exc()[:8000]
                if batch_err:
                    out["batch_error_hint"] = str(batch_err)[:1200]
            items.append(_schema_fill_best_effort(out))

    return JSONResponse(status_code=200, content={"status": "success", "count": len(items), "items": items})


@router.get("/health", include_in_schema=False)
async def enriched_health():
    return {"status": "ok", "module": "core.enriched_quote", "version": "2.4.0"}


def get_router() -> APIRouter:
    return router


__all__ = ["router", "get_router"]
