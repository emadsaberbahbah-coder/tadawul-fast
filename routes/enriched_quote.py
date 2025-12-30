# routes/enriched_quote.py
"""
routes/enriched_quote.py
------------------------------------------------------------
Enriched Quote Router — PROD SAFE (await-safe) — v5.1.3

Fix:
- ✅ Always awaits async engine methods (prevents returning "<coroutine object ...>").
- ✅ Works with BOTH async and sync engines.
- ✅ Never crashes: always returns HTTP 200 with status.
- ✅ Keeps backward-compat: returns dict payloads as-is; otherwise wraps into {"value": ...}.
- ✅ Batch endpoint: /v1/enriched/quotes?symbols=AAPL,1120.SR

Endpoints:
- GET /v1/enriched/quote?symbol=1120.SR
- GET /v1/enriched/quote?symbol=AAPL
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


def _get_engine_from_request(request: Request) -> Tuple[Optional[Any], str]:
    """
    Prefer app.state.engine.
    Fallback: best-effort instantiate engine (only if missing).
    """
    try:
        eng = getattr(request.app.state, "engine", None)
        if eng is not None:
            return eng, "app.state.engine"
    except Exception:
        pass

    # fallback (should be rare)
    try:
        from core.data_engine_v2 import DataEngineV2 as _V2  # type: ignore

        return _V2(), "core.data_engine_v2.DataEngineV2()"
    except Exception:
        pass

    try:
        from core.data_engine_v2 import DataEngine as _V2Compat  # type: ignore

        return _V2Compat(), "core.data_engine_v2.DataEngine()"
    except Exception:
        pass

    try:
        from core.data_engine import DataEngine as _Legacy  # type: ignore

        return _Legacy(), "core.data_engine.DataEngine()"
    except Exception:
        return None, "none"


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

    # best-effort
    try:
        return dict(payload)  # type: ignore[arg-type]
    except Exception:
        return str(payload)


def _normalize_symbol(sym: str) -> str:
    s = (sym or "").strip()
    return s


# -----------------------------
# Routes
# -----------------------------
@router.get("/v1/enriched/quote")
async def enriched_quote(
    request: Request,
    symbol: str = Query(..., description="Ticker symbol, e.g. 1120.SR or AAPL"),
    debug: int = Query(0, description="debug=1 includes a traceback on failure"),
) -> Dict[str, Any]:
    sym = _normalize_symbol(symbol)
    eng, eng_src = _get_engine_from_request(request)

    if not sym:
        return {"status": "error", "error": "Missing symbol", "symbol": symbol, "engine_source": eng_src}

    if eng is None:
        return {"status": "error", "error": "Engine not available", "symbol": sym, "engine_source": eng_src}

    try:
        fn = getattr(eng, "get_enriched_quote", None)
        if not callable(fn):
            return {
                "status": "error",
                "error": "Engine missing method get_enriched_quote",
                "symbol": sym,
                "engine_source": eng_src,
            }

        # ✅ FIX: await if coroutine
        res = await _maybe_await(fn(sym))

        # If engine already returns a dict (recommended), return it as-is
        if isinstance(res, dict):
            return res

        # Otherwise wrap for backward compatibility
        return {"status": "ok", "symbol": sym, "engine_source": eng_src, "value": _to_jsonable(res)}

    except Exception as e:
        out = {
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
    debug: int = Query(0, description="debug=1 includes a traceback on failure"),
) -> Dict[str, Any]:
    eng, eng_src = _get_engine_from_request(request)
    raw = (symbols or "").strip()
    if not raw:
        return {"status": "error", "error": "Missing symbols", "engine_source": eng_src, "items": []}

    syms = [s.strip() for s in raw.split(",") if s.strip()]
    if not syms:
        return {"status": "error", "error": "No valid symbols", "engine_source": eng_src, "items": []}

    if eng is None:
        return {"status": "error", "error": "Engine not available", "engine_source": eng_src, "items": []}

    items: List[Any] = []
    try:
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
                res = await _maybe_await(fn(s))
                items.append(res if isinstance(res, dict) else {"status": "ok", "symbol": s, "value": _to_jsonable(res)})
            except Exception as ex:
                err_item = {"status": "error", "symbol": s, "error": _clamp(ex)}
                if debug:
                    err_item["trace"] = _clamp(traceback.format_exc(), 4000)
                items.append(err_item)

        return {"status": "ok", "engine_source": eng_src, "count": len(items), "items": items}

    except Exception as e:
        out = {"status": "error", "engine_source": eng_src, "error": _clamp(e), "items": items}
        if debug:
            out["trace"] = _clamp(traceback.format_exc(), 8000)
        return out
