# routes/enriched_quote.py  (FULL REPLACEMENT)
"""
routes/enriched_quote.py
------------------------------------------------------------
Enriched Quote Router – PROD SAFE + DEBUGGABLE

Fixes:
- Always returns `symbol` (normalized or input) even on errors
- Never returns blank `error` (captures exception message)
- Supports best-effort engine usage via app.state.engine
- Falls back to module-level functions if engine isn't available
- Optional debug trace via env DEBUG_ERRORS=1 or query ?debug=1

Endpoint:
- GET /v1/enriched/quote?symbol=AAPL
- GET /v1/enriched/quote?symbol=MSFT.US
"""

from __future__ import annotations

import os
import traceback
from typing import Any, Dict, Optional

from fastapi import APIRouter, Query, Request
from starlette.responses import JSONResponse

router = APIRouter(prefix="/v1/enriched", tags=["enriched"])


_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY


def _norm_symbol(s: str) -> str:
    """
    Safe normalization: if user passes "MSFT" assume US and convert to "MSFT.US".
    Do NOT touch KSA (.SR) or numeric tickers.
    """
    s = (s or "").strip().upper()
    if not s:
        return s

    # Already has suffix like .US / .SR etc.
    if "." in s:
        return s

    # Numeric-only could be KSA raw (e.g., 1120) -> leave as is
    if s.isdigit():
        return s

    # Alphabetic ticker -> assume US
    # (Matches your AAPL behavior where AAPL became AAPL.US)
    if s.isalpha():
        return f"{s}.US"

    return s


async def _call_engine(request: Request, symbol: str) -> Any:
    """
    Try app.state.engine first, then core.data_engine_v2, then legacy.
    """
    eng = getattr(request.app.state, "engine", None)
    if eng is not None:
        fn = getattr(eng, "get_enriched_quote", None)
        if callable(fn):
            return await fn(symbol)

    # v2 module-level
    try:
        from core.data_engine_v2 import get_enriched_quote as v2_get  # type: ignore

        return await v2_get(symbol)
    except Exception:
        pass

    # legacy module-level
    from core.data_engine import get_enriched_quote as v1_get  # type: ignore

    return await v1_get(symbol)


def _as_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    # pydantic v2
    md = getattr(obj, "model_dump", None)
    if callable(md):
        return md()
    # pydantic v1
    d = getattr(obj, "dict", None)
    if callable(d):
        return d()
    # fallback
    try:
        return dict(obj)  # type: ignore
    except Exception:
        return {"value": str(obj)}


@router.get("/quote")
async def enriched_quote(
    request: Request,
    symbol: str = Query(..., description="Ticker symbol (AAPL, MSFT, GOOGL.US, 1120.SR)"),
    debug: int = Query(0, description="Set 1 to include traceback (if DEBUG_ERRORS enabled)"),
):
    dbg = _truthy(os.getenv("DEBUG_ERRORS", "0")) or bool(debug)

    raw = (symbol or "").strip()
    norm = _norm_symbol(raw)

    try:
        result = await _call_engine(request, norm)
        payload = _as_dict(result)

        # Ensure symbol always present
        if not payload.get("symbol"):
            payload["symbol"] = norm or raw

        # Ensure consistent success/error layout
        # If engine returns its own `status`, keep it; else infer.
        if "status" not in payload:
            payload["status"] = "success" if payload.get("data_quality") not in {"MISSING", "NONE"} else "success"

        # Avoid blank error fields
        if "error" in payload and payload["error"] is None:
            payload["error"] = ""

        return JSONResponse(status_code=200, content=payload)

    except Exception as e:
        tb = traceback.format_exc()
        msg = str(e) or e.__class__.__name__

        out = {
            "status": "error",
            "symbol": norm or raw,
            "data_quality": "MISSING",
            "data_source": "none",
            "error": msg,
        }
        if dbg:
            out["traceback"] = tb[:8000]

        return JSONResponse(status_code=200, content=out)
