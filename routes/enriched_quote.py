# routes/enriched_quote.py  (FULL REPLACEMENT)
"""
routes/enriched_quote.py
------------------------------------------------------------
Enriched Quote Router – PROD SAFE + DEBUGGABLE (v2.4.0)

Fix in v2.4.0 (CRITICAL)
- ✅ Do NOT force status=error just because `error` is non-empty
  (engine may return warnings in `error` while still success)
- ✅ status logic:
    - if current_price exists => success (even with warnings)
    - if no price            => error
- ✅ Keeps schema-fill guarantees for Sheets (no missing columns)
- ✅ Keeps PROD-safe import behavior (no core imports at module import-time)
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
    Preferred normalization: core.data_engine_v2.normalize_symbol (if available).
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
    Ensure payload contains every UnifiedQuote field (best-effort),
    so Sheets/clients never miss expected keys even if engines return partial dicts.
    """
    keys = _get_uq_keys()
    if not keys:
        return payload
    for k in keys:
        payload.setdefault(k, None)
    return payload


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        if isinstance(v, bool):
            return None
        f = float(v)
        if f != f:  # NaN
            return None
        return f
    except Exception:
        return None


def _has_price(payload: Dict[str, Any]) -> bool:
    f = _safe_float(payload.get("current_price"))
    return f is not None


async def _call_engine_best_effort(request: Request, symbol: str) -> Tuple[Optional[Any], Optional[str]]:
    """
    Try in this order:
      1) app.state.engine (preferred)
      2) core.data_engine_v2.get_enriched_quote (module-level singleton)
      3) core.data_engine_v2.DataEngine() temporary
      4) core.data_engine.get_enriched_quote (legacy module-level)
      5) core.data_engine.DataEngine() temporary
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


def _finalize_payload(payload: Dict[str, Any], *, raw: str, norm: str, source: str) -> Dict[str, Any]:
    """
    Ensure required fields and consistent defaults + schema-fill best-effort.

    CRITICAL:
    - Do NOT force status=error just because error text exists.
    - If current_price exists -> status=success (error treated as warning text).
    - If no current_price -> status=error.
    """
    sym = (norm or raw or "").strip()

    if not payload.get("symbol"):
        payload["symbol"] = sym

    payload["symbol_input"] = payload.get("symbol_input") or raw
    payload["symbol_normalized"] = payload.get("symbol_normalized") or sym

    if not payload.get("data_source"):
        payload["data_source"] = source or "unknown"

    # Normalize error to string (never None)
    err = payload.get("error")
    if err is None:
        err_s = ""
    elif isinstance(err, str):
        err_s = err.strip()
    else:
        err_s = str(err).strip()
    payload["error"] = err_s

    # Determine has_price
    has_price = _has_price(payload)

    # Respect engine status when possible
    st_raw = str(payload.get("status") or "").strip().lower()

    if has_price:
        # If we have a price, we succeed even if warnings exist.
        payload["status"] = "success"

        # If there is error text and it is not already prefixed, treat as warning text
        if err_s and not err_s.lower().startswith("warning:"):
            payload["error"] = f"warning: {err_s}"

        if not payload.get("data_quality"):
            payload["data_quality"] = "PARTIAL"

    else:
        # No price => error
        payload["status"] = "error"
        if not payload.get("data_quality"):
            payload["data_quality"] = "MISSING"
        if not payload.get("error"):
            payload["error"] = "No price returned"

    # Schema fill last (adds any missing keys as None)
    payload = _schema_fill_best_effort(payload)
    return payload


def _env_int(name: str, default: int) -> int:
    try:
        v = int(str(os.getenv(name, "") or "").strip() or default)
        return v if v > 0 else default
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        v = float(str(os.getenv(name, "") or "").strip() or default)
        return v if v > 0 else default
    except Exception:
        return default


def _enriched_concurrency() -> int:
    return _env_int("ENRICHED_CONCURRENCY", _env_int("ENRICHED_BATCH_CONCURRENCY", 8))


def _enriched_max_tickers() -> int:
    return _env_int("ENRICHED_MAX_TICKERS", 250)


def _enriched_timeout_sec() -> float:
    return _env_float("ENRICHED_TIMEOUT_SEC", 45.0)


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
        out = _schema_fill_best_effort(out)
        return JSONResponse(status_code=200, content=out)

    timeout_sec = _enriched_timeout_sec()

    try:
        result, source = await asyncio.wait_for(
            _call_engine_best_effort(request, norm or raw),
            timeout=timeout_sec,
        )

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
            out = _schema_fill_best_effort(out)
            return JSONResponse(status_code=200, content=out)

        payload = _as_payload(result)
        payload = _finalize_payload(payload, raw=raw, norm=norm, source=source or "unknown")
        return JSONResponse(status_code=200, content=payload)

    except asyncio.TimeoutError:
        out: Dict[str, Any] = {
            "status": "error",
            "symbol": norm or raw,
            "symbol_input": raw,
            "symbol_normalized": norm or raw,
            "data_quality": "MISSING",
            "data_source": "none",
            "error": f"Timeout after {timeout_sec:.0f}s",
        }
        out = _schema_fill_best_effort(out)
        return JSONResponse(status_code=200, content=out)

    except Exception as e:
        tb = traceback.format_exc()
        msg = _safe_error_message(e)

        out = {
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

        out = _schema_fill_best_effort(out)
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
            content={"status": "error", "error": "Empty symbols list", "items": []},
        )

    max_allowed = _enriched_max_tickers()
    timeout_sec = _enriched_timeout_sec()

    conc = _enriched_concurrency()
    sem = asyncio.Semaphore(conc)

    async def _one(raw: str) -> Dict[str, Any]:
        norm = _normalize_symbol_safe(raw)

        try:
            async with sem:
                result, source = await asyncio.wait_for(
                    _call_engine_best_effort(request, norm or raw),
                    timeout=timeout_sec,
                )

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
                return _schema_fill_best_effort(out)

            payload = _as_payload(result)
            payload = _finalize_payload(payload, raw=raw, norm=norm, source=source or "unknown")
            return payload

        except asyncio.TimeoutError:
            out = {
                "status": "error",
                "symbol": norm or raw,
                "symbol_input": raw,
                "symbol_normalized": norm or raw,
                "data_quality": "MISSING",
                "data_source": "none",
                "error": f"Timeout after {timeout_sec:.0f}s",
            }
            return _schema_fill_best_effort(out)

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
            return _schema_fill_best_effort(out)

    tasks: List[asyncio.Task] = []
    items: List[Dict[str, Any]] = []

    trimmed = [r.strip() for r in raw_list if r and r.strip()]
    for idx, raw in enumerate(trimmed):
        if idx < max_allowed:
            tasks.append(asyncio.create_task(_one(raw)))
        else:
            norm = _normalize_symbol_safe(raw)
            out = {
                "status": "error",
                "symbol": norm or raw,
                "symbol_input": raw,
                "symbol_normalized": norm or raw,
                "data_quality": "MISSING",
                "data_source": "none",
                "error": f"Too many symbols requested. Max allowed is {max_allowed}.",
            }
            items.append(_schema_fill_best_effort(out))

    if tasks:
        first_batch = await asyncio.gather(*tasks)
        items = first_batch + items

    return JSONResponse(
        status_code=200,
        content={
            "status": "success",
            "count": len(items),
            "max_allowed": max_allowed,
            "concurrency": conc,
            "timeout_sec": timeout_sec,
            "items": items,
        },
    )


@router.get("/health", include_in_schema=False)
async def enriched_health():
    return {"status": "ok", "module": "routes.enriched_quote", "version": "2.4.0"}


__all__ = ["router"]
