```python
# core/enriched_quote.py  (FULL REPLACEMENT)
"""
core/enriched_quote.py
------------------------------------------------------------
Compatibility Router: Enriched Quote (PROD SAFE) – v2.2.0

Why this file exists:
- main.py mounts routers from multiple candidate import paths:
    ("enriched_quote", ["routes.enriched_quote", "enriched_quote", "core.enriched_quote"])
- If routes/enriched_quote.py moves or fails import, this module keeps the API alive.

Behavior:
- Provides: router (APIRouter) + get_router()
- Endpoint: GET /v1/enriched/quote?symbol=AAPL
- Uses app.state.engine when available, otherwise best-effort fallbacks
- Always returns HTTP 200 with {"status": "..."} for client simplicity
- Optional debug trace via env DEBUG_ERRORS=1 or query ?debug=1
"""

from __future__ import annotations

import os
import traceback
from typing import Any, Dict

from fastapi import APIRouter, Query, Request
from starlette.responses import JSONResponse

router = APIRouter(prefix="/v1/enriched", tags=["enriched"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY


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

    # Prefer canonical normalizer if present
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


def _as_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj

    # pydantic v2
    md = getattr(obj, "model_dump", None)
    if callable(md):
        try:
            return md()
        except Exception:
            pass

    # pydantic v1
    d = getattr(obj, "dict", None)
    if callable(d):
        try:
            return d()
        except Exception:
            pass

    # dataclass-ish / object with __dict__
    try:
        od = getattr(obj, "__dict__", None)
        if isinstance(od, dict) and od:
            return dict(od)
    except Exception:
        pass

    return {"value": str(obj)}


async def _call_engine(request: Request, symbol: str) -> Any:
    """
    Try in this order:
      1) app.state.engine (preferred)
      2) core.data_engine_v2.DataEngine (temporary instance)
      3) legacy module-level core.data_engine.get_enriched_quote
      4) legacy engine class core.data_engine.DataEngine (temporary instance)
    """
    # 1) Preferred: shared engine in app.state
    eng = getattr(request.app.state, "engine", None)
    if eng is not None:
        for fn_name in ("get_enriched_quote", "get_quote"):
            fn = getattr(eng, fn_name, None)
            if callable(fn):
                return await fn(symbol)

    # 2) v2 temporary engine
    try:
        from core.data_engine_v2 import DataEngine as V2Engine  # type: ignore

        tmp = V2Engine()
        try:
            return await tmp.get_enriched_quote(symbol)
        finally:
            aclose = getattr(tmp, "aclose", None)
            if callable(aclose):
                try:
                    await aclose()
                except Exception:
                    pass
    except Exception:
        pass

    # 3) legacy module-level
    try:
        from core.data_engine import get_enriched_quote as v1_get  # type: ignore

        return await v1_get(symbol)
    except Exception:
        pass

    # 4) legacy engine temporary instance
    from core.data_engine import DataEngine as V1Engine  # type: ignore

    tmp2 = V1Engine()
    try:
        return await tmp2.get_enriched_quote(symbol)
    finally:
        aclose2 = getattr(tmp2, "aclose", None)
        if callable(aclose2):
            try:
                await aclose2()
            except Exception:
                pass


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
            "data_quality": "MISSING",
            "data_source": "none",
            "error": "Empty symbol",
        }
        return JSONResponse(status_code=200, content=out)

    try:
        result = await _call_engine(request, norm or raw)
        payload = _as_dict(result)

        if not payload.get("symbol"):
            payload["symbol"] = norm or raw

        if payload.get("error") is None:
            payload["error"] = ""

        if "status" not in payload:
            payload["status"] = "success"

        return JSONResponse(status_code=200, content=payload)

    except Exception as e:
        tb = traceback.format_exc()
        msg = str(e).strip() or e.__class__.__name__

        out: Dict[str, Any] = {
            "status": "error",
            "symbol": norm or raw,
            "data_quality": "MISSING",
            "data_source": "none",
            "error": msg,
        }
        if dbg:
            out["traceback"] = tb[:8000]

        return JSONResponse(status_code=200, content=out)


def get_router() -> APIRouter:
    return router


__all__ = ["router", "get_router"]
```
