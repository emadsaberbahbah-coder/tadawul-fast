# routes/enriched_quote.py — FULL REPLACEMENT — v5.2.0
"""
routes/enriched_quote.py
------------------------------------------------------------
Enriched Quote Router — PROD SAFE (await-safe + singleton-engine friendly) — v5.2.0

Aligned with your plan:
- ✅ Always awaits async engine methods (prevents "<coroutine object ...>")
- ✅ Works with BOTH async and sync engines
- ✅ Never crashes: always returns HTTP 200 with status
- ✅ Prefers app.state.engine, else uses core.data_engine_v2.get_engine() (singleton)
    -> avoids creating a NEW engine per request (keeps cache warm + consistent)
- ✅ Supports refresh=1 and fields=... (best-effort pass-through)
- ✅ Batch endpoint uses engine.get_enriched_quotes if available (fast), else loops safely
- ✅ Backward-compat: if engine returns dict -> return as-is; else wrap {"value": ...}

Endpoints
- GET /v1/enriched/quote?symbol=1120.SR
- GET /v1/enriched/quotes?symbols=AAPL,MSFT,1120.SR
"""

from __future__ import annotations

import inspect
import traceback
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Query, Request

router = APIRouter(tags=["enriched"])


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


def _normalize_symbols_csv(csv: str) -> List[str]:
    raw = (csv or "").strip()
    if not raw:
        return []
    return [s.strip() for s in raw.split(",") if s.strip()]


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

        eng = await get_engine()
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

    # Most explicit first (kwargs)
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

    # Positional variants
    try:
        return fn(symbol, refresh)
    except TypeError:
        pass

    # Minimal
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

    # kwargs-first
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
@router.get("/v1/enriched/quote")
async def enriched_quote(
    request: Request,
    symbol: str = Query(..., description="Ticker symbol, e.g. 1120.SR or AAPL"),
    refresh: int = Query(0, description="refresh=1 asks engine to bypass cache (if supported)"),
    fields: Optional[str] = Query(None, description="Optional hint to engine (comma-separated fields)"),
    debug: int = Query(0, description="debug=1 includes a traceback on failure"),
) -> Dict[str, Any]:
    sym = _normalize_symbol(symbol)
    eng, eng_src = await _get_engine_from_request(request)

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

        # If engine already returns a dict (recommended), return it as-is
        if isinstance(res, dict):
            return res

        # Otherwise wrap for backward compatibility
        return {"status": "ok", "symbol": sym, "engine_source": eng_src, "value": _to_jsonable(res)}

    except Exception as e:
        out: Dict[str, Any] = {
            "status": "error",
            "symbol": sym,
            "engine_source": eng_src,
            "error": _clamp(e),
        }
        if debug:
            out["trace"] = _clamp(traceback.format_exc(), 8000)
        return out


@router.get("/v1/enriched/quotes")
async def enriched_quotes(
    request: Request,
    symbols: str = Query(..., description="Comma-separated list, e.g. AAPL,MSFT,1120.SR"),
    refresh: int = Query(0, description="refresh=1 asks engine to bypass cache (if supported)"),
    fields: Optional[str] = Query(None, description="Optional hint to engine (comma-separated fields)"),
    debug: int = Query(0, description="debug=1 includes a traceback on failure"),
) -> Dict[str, Any]:
    eng, eng_src = await _get_engine_from_request(request)
    syms = _normalize_symbols_csv(symbols)

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
            if isinstance(res, list):
                # Ensure each item is dict-ish
                for i, it in enumerate(res):
                    if isinstance(it, dict):
                        items.append(it)
                    else:
                        items.append({"status": "ok", "symbol": syms[i] if i < len(syms) else "", "value": _to_jsonable(it)})
                return {"status": "ok", "engine_source": eng_src, "count": len(items), "items": items}

        # Fallback: per-symbol loop (still PROD SAFE)
        fn = getattr(eng, "get_enriched_quote", None)
        if not callable(fn):
            return {
                "status": "error",
                "error": "Engine missing method get_enriched_quote",
                "engine_source": eng_src,
                "items": [],
            }

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
                items.append(res if isinstance(res, dict) else {"status": "ok", "symbol": s, "value": _to_jsonable(res)})
            except Exception as ex:
                err_item: Dict[str, Any] = {"status": "error", "symbol": s, "error": _clamp(ex)}
                if debug:
                    err_item["trace"] = _clamp(traceback.format_exc(), 4000)
                items.append(err_item)

        return {"status": "ok", "engine_source": eng_src, "count": len(items), "items": items}

    except Exception as e:
        out: Dict[str, Any] = {"status": "error", "engine_source": eng_src, "error": _clamp(e), "items": items}
        if debug:
            out["trace"] = _clamp(traceback.format_exc(), 8000)
        return out
