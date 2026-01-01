# routes/enriched_quote.py
"""
routes/enriched_quote.py
------------------------------------------------------------
Enriched Quote Router — PROD SAFE (await-safe + singleton-engine friendly) — v5.3.0

Aligned with your plan:
- ✅ Await-safe everywhere (engine + get_engine may be sync OR async)
- ✅ Never crashes: always returns HTTP 200 with a status field
- ✅ Prefers app.state.engine, else uses core.data_engine_v2.get_engine() (singleton)
- ✅ Supports refresh=1 and fields=... (best-effort pass-through)
- ✅ Batch endpoint:
    - uses engine.get_enriched_quotes if available
    - handles list OR dict OR {"items":[...]} shapes
- ✅ If engine returns dict without status, we inject status="ok" (Sheets/client safety)

Endpoints
- GET /v1/enriched/quote?symbol=1120.SR
- GET /v1/enriched/quotes?symbols=AAPL,MSFT,1120.SR
- GET /v1/enriched/health
"""

from __future__ import annotations

import inspect
import os
import re
import traceback
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Query, Request

router = APIRouter(tags=["enriched"])

ENRICHED_ROUTE_VERSION = "5.3.0"


# -----------------------------
# Helpers
# -----------------------------
def _clamp(s: Any, n: int = 2000) -> str:
    t = (str(s) if s is not None else "").strip()
    if not t:
        return ""
    return t if len(t) <= n else (t[: n - 12] + " ...TRUNC...")


async def _maybe_await(x: Any) -> Any:
    """Await x if it's awaitable; otherwise return as-is."""
    if inspect.isawaitable(x):
        return await x
    return x


def _normalize_symbol(sym: str) -> str:
    return (sym or "").strip()


def _parse_symbols_list(raw: str) -> List[str]:
    """
    Accepts:
      "AAPL,MSFT,1120.SR"
      "AAPL MSFT 1120.SR"
      "AAPL, MSFT  ,1120.SR"
    Returns a clean list (keeps order).
    """
    s = (raw or "").strip()
    if not s:
        return []
    parts = re.split(r"[\s,]+", s)
    return [p.strip() for p in parts if p and p.strip()]


def _to_jsonable(payload: Any) -> Any:
    """Convert pydantic/dataclass-ish objects to dict when possible."""
    if payload is None:
        return None
    if isinstance(payload, (str, int, float, bool, list, dict)):
        return payload

    # pydantic v2
    md = getattr(payload, "model_dump", None)
    if callable(md):
        try:
            return md()
        except Exception:
            pass

    # pydantic v1
    dct = getattr(payload, "dict", None)
    if callable(dct):
        try:
            return dct()
        except Exception:
            pass

    # dataclasses
    try:
        import dataclasses

        if dataclasses.is_dataclass(payload):
            return dataclasses.asdict(payload)
    except Exception:
        pass

    # best-effort
    try:
        return dict(payload)  # type: ignore[arg-type]
    except Exception:
        return str(payload)


def _debug_enabled(debug_q: int) -> bool:
    if debug_q:
        return True
    v = (os.getenv("DEBUG_ERRORS") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _ensure_status_dict(d: Dict[str, Any], *, symbol: str = "", engine_source: str = "") -> Dict[str, Any]:
    """
    Ensure returned dict always includes a `status`.
    Does not overwrite existing keys (except filling missing).
    """
    out = dict(d)
    if "status" not in out:
        out["status"] = "ok"
    if symbol and "symbol" not in out:
        out["symbol"] = symbol
    if engine_source and "engine_source" not in out:
        out["engine_source"] = engine_source
    return out


async def _get_engine_from_request(request: Request) -> Tuple[Optional[Any], str]:
    """
    Prefer app.state.engine.
    Fallback: core.data_engine_v2.get_engine() singleton (preferred).
    Last fallback: instantiate DataEngineV2/DataEngine (rare).
    """
    # 1) app.state.engine
    try:
        eng = getattr(request.app.state, "engine", None)
        if eng is not None:
            return eng, "app.state.engine"
    except Exception:
        pass

    # 2) singleton engine (preferred)
    try:
        from core.data_engine_v2 import get_engine  # type: ignore

        maybe = get_engine()
        eng = await _maybe_await(maybe)
        return eng, "core.data_engine_v2.get_engine()"
    except Exception:
        pass

    # 3) last resort instantiation
    try:
        from core.data_engine_v2 import DataEngineV2 as _V2  # type: ignore

        return _V2(), "core.data_engine_v2.DataEngineV2()"
    except Exception:
        pass

    try:
        from core.data_engine_v2 import DataEngine as _V2Compat  # type: ignore

        return _V2Compat(), "core.data_engine_v2.DataEngine()"
    except Exception:
        return None, "none"


def _call_engine_method_best_effort(
    eng: Any,
    method_name: str,
    symbol: str,
    refresh: bool,
    fields: Optional[str],
) -> Any:
    """
    Tries common method signatures in a safe order, WITHOUT assuming the engine API.
    Returns raw result (may be awaitable).
    """
    fn = getattr(eng, method_name, None)
    if not callable(fn):
        raise AttributeError(f"Engine missing method {method_name}")

    # kwargs-first
    try:
        return fn(symbol, refresh=refresh, fields=fields)
    except TypeError:
        pass

    try:
        return fn(symbol, refresh=refresh)
    except TypeError:
        pass

    try:
        return fn(symbol, fields=fields)
    except TypeError:
        pass

    # positional variants
    try:
        return fn(symbol, refresh)
    except TypeError:
        pass

    # minimal
    return fn(symbol)


def _call_engine_batch_best_effort(
    eng: Any,
    symbols: List[str],
    refresh: bool,
    fields: Optional[str],
) -> Any:
    """
    Prefer engine.get_enriched_quotes(symbols, ...) if available, else None.
    Returns raw result (may be awaitable) or None if unsupported.
    """
    fn = getattr(eng, "get_enriched_quotes", None)
    if not callable(fn):
        return None

    try:
        return fn(symbols, refresh=refresh, fields=fields)
    except TypeError:
        pass

    try:
        return fn(symbols, refresh=refresh)
    except TypeError:
        pass

    try:
        return fn(symbols, fields=fields)
    except TypeError:
        pass

    try:
        return fn(symbols, refresh)
    except TypeError:
        pass

    return fn(symbols)


# -----------------------------
# Routes
# -----------------------------
@router.get("/v1/enriched/health")
async def enriched_health(request: Request) -> Dict[str, Any]:
    eng, eng_src = await _get_engine_from_request(request)
    engine_name = type(eng).__name__ if eng is not None else "none"
    return {"status": "ok", "module": "routes.enriched_quote", "version": ENRICHED_ROUTE_VERSION, "engine": engine_name, "engine_source": eng_src}


@router.get("/v1/enriched/quote")
async def enriched_quote(
    request: Request,
    symbol: str = Query(..., description="Ticker symbol, e.g. 1120.SR or AAPL"),
    refresh: int = Query(0, description="refresh=1 asks engine to bypass cache (if supported)"),
    fields: Optional[str] = Query(None, description="Optional hint to engine (comma/space-separated fields)"),
    debug: int = Query(0, description="debug=1 includes a traceback on failure (or set DEBUG_ERRORS=1)"),
) -> Dict[str, Any]:
    sym = _normalize_symbol(symbol)
    eng, eng_src = await _get_engine_from_request(request)
    dbg = _debug_enabled(debug)

    if not sym:
        return {"status": "error", "error": "Missing symbol", "symbol": symbol, "engine_source": eng_src}

    if eng is None:
        return {"status": "error", "error": "Engine not available", "symbol": sym, "engine_source": eng_src}

    try:
        raw = _call_engine_method_best_effort(
            eng=eng,
            method_name="get_enriched_quote",
            symbol=sym,
            refresh=bool(refresh),
            fields=fields,
        )
        res = await _maybe_await(raw)

        if isinstance(res, dict):
            return _ensure_status_dict(res, symbol=sym, engine_source=eng_src)

        # wrap for backward compatibility
        return {"status": "ok", "symbol": sym, "engine_source": eng_src, "value": _to_jsonable(res)}

    except Exception as e:
        out: Dict[str, Any] = {
            "status": "error",
            "symbol": sym,
            "engine_source": eng_src,
            "error": _clamp(e),
        }
        if dbg:
            out["trace"] = _clamp(traceback.format_exc(), 8000)
        return out


@router.get("/v1/enriched/quotes")
async def enriched_quotes(
    request: Request,
    symbols: str = Query(..., description="Comma/space-separated list, e.g. AAPL,MSFT,1120.SR"),
    refresh: int = Query(0, description="refresh=1 asks engine to bypass cache (if supported)"),
    fields: Optional[str] = Query(None, description="Optional hint to engine (comma/space-separated fields)"),
    debug: int = Query(0, description="debug=1 includes a traceback on failure (or set DEBUG_ERRORS=1)"),
    max_symbols: int = Query(800, description="Safety limit for very large requests"),
) -> Dict[str, Any]:
    eng, eng_src = await _get_engine_from_request(request)
    dbg = _debug_enabled(debug)

    syms = _parse_symbols_list(symbols)
    if max_symbols and max_symbols > 0 and len(syms) > max_symbols:
        syms = syms[:max_symbols]

    if not syms:
        return {"status": "error", "error": "No valid symbols", "engine_source": eng_src, "items": []}

    if eng is None:
        return {"status": "error", "error": "Engine not available", "engine_source": eng_src, "items": []}

    items: List[Any] = []
    try:
        # Prefer batch call if engine supports it (faster + consistent)
        raw_batch = _call_engine_batch_best_effort(eng, syms, refresh=bool(refresh), fields=fields)
        if raw_batch is not None:
            res = await _maybe_await(raw_batch)

            # common: {"items":[...]} already shaped
            if isinstance(res, dict) and isinstance(res.get("items"), list):
                out = dict(res)
                out.setdefault("status", "ok")
                out.setdefault("engine_source", eng_src)
                out.setdefault("count", len(out.get("items") or []))
                return out

            # list: align by index when possible
            if isinstance(res, list):
                for i, it in enumerate(res):
                    if isinstance(it, dict):
                        items.append(_ensure_status_dict(it, symbol=syms[i] if i < len(syms) else "", engine_source=eng_src))
                    else:
                        items.append({"status": "ok", "symbol": syms[i] if i < len(syms) else "", "engine_source": eng_src, "value": _to_jsonable(it)})
                return {"status": "ok", "engine_source": eng_src, "count": len(items), "items": items}

            # dict mapping {symbol: quote}
            if isinstance(res, dict):
                for s in syms:
                    v = res.get(s) or res.get(s.upper()) or res.get(s.lower())
                    if isinstance(v, dict):
                        items.append(_ensure_status_dict(v, symbol=s, engine_source=eng_src))
                    elif v is None:
                        items.append({"status": "error", "symbol": s, "engine_source": eng_src, "error": "Missing in batch response"})
                    else:
                        items.append({"status": "ok", "symbol": s, "engine_source": eng_src, "value": _to_jsonable(v)})
                return {"status": "ok", "engine_source": eng_src, "count": len(items), "items": items}

        # Fallback: per-symbol loop (still PROD SAFE)
        fn = getattr(eng, "get_enriched_quote", None)
        if not callable(fn):
            return {"status": "error", "error": "Engine missing method get_enriched_quote", "engine_source": eng_src, "items": []}

        for s in syms:
            try:
                raw = _call_engine_method_best_effort(
                    eng=eng,
                    method_name="get_enriched_quote",
                    symbol=s,
                    refresh=bool(refresh),
                    fields=fields,
                )
                res = await _maybe_await(raw)
                if isinstance(res, dict):
                    items.append(_ensure_status_dict(res, symbol=s, engine_source=eng_src))
                else:
                    items.append({"status": "ok", "symbol": s, "engine_source": eng_src, "value": _to_jsonable(res)})
            except Exception as ex:
                err_item: Dict[str, Any] = {"status": "error", "symbol": s, "engine_source": eng_src, "error": _clamp(ex)}
                if dbg:
                    err_item["trace"] = _clamp(traceback.format_exc(), 4000)
                items.append(err_item)

        return {"status": "ok", "engine_source": eng_src, "count": len(items), "items": items}

    except Exception as e:
        out: Dict[str, Any] = {"status": "error", "engine_source": eng_src, "error": _clamp(e), "items": items}
        if dbg:
            out["trace"] = _clamp(traceback.format_exc(), 8000)
        return out


__all__ = ["router"]
