# routes/enriched_quote.py
"""
routes/enriched_quote.py
------------------------------------------------------------
Enriched Quote Router — PROD SAFE (await-safe + singleton-engine friendly) — v5.4.1

Key guarantees
- ✅ Await-safe everywhere (engine + get_engine may be sync OR async)
- ✅ Never crashes: always returns HTTP 200 with a status field
- ✅ Avoids FastAPI 422 for missing symbol/symbols by making them optional
- ✅ Prefers app.state.engine, else uses core.data_engine_v2.get_engine() (singleton)
- ✅ Supports refresh=1 and fields=... (best-effort pass-through)
- ✅ Batch endpoint supports:
    - engine.get_enriched_quotes (preferred) OR engine.get_quotes (fallback)
    - list OR dict OR {"items":[...]} shapes
- ✅ If engine returns dict without status, we inject status="ok" (Sheets/client safety)
- ✅ Alignment: Recommendation standardized to BUY/HOLD/REDUCE/SELL (always UPPERCASE, always non-empty)
- ✅ Token guard via:
    • X-APP-TOKEN
    • Authorization: Bearer <token>
    • (optional) ?token=... (disabled unless ALLOW_QUERY_TOKEN=1)
  If no token is set => open mode.

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
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Header, Query, Request

router = APIRouter(tags=["enriched"])

ENRICHED_ROUTE_VERSION = "5.4.1"

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# =============================================================================
# Settings shim (safe)
# =============================================================================
try:
    from core.config import get_settings  # type: ignore
except Exception:  # pragma: no cover

    def get_settings():  # type: ignore
        return None


# =============================================================================
# Helpers
# =============================================================================
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


def _unwrap_tuple_payload(x: Any) -> Any:
    """Some engines return (payload, meta). Keep payload."""
    if isinstance(x, tuple) and len(x) == 2:
        return x[0]
    return x


# =============================================================================
# Normalization (lazy prefer core.data_engine_v2.normalize_symbol; fallback always)
# =============================================================================
def _fallback_normalize(raw: str) -> str:
    s = (raw or "").strip().upper()
    if not s:
        return ""
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", "")
    if any(ch in s for ch in ("^", "=")):  # indices / formula-like
        return s
    if s.isdigit():
        return f"{s}.SR"
    if s.endswith(".SR") or s.endswith(".US"):
        return s
    if "." in s:
        return s
    return f"{s}.US"


@lru_cache(maxsize=1)
def _try_import_v2_normalizer() -> Any:
    """Return normalize_symbol callable if available, else fallback. Never raises."""
    try:
        from core.data_engine_v2 import normalize_symbol as _NS  # type: ignore

        return _NS
    except Exception:
        return _fallback_normalize


def _normalize_symbol(sym: str) -> str:
    raw = (sym or "").strip()
    if not raw:
        return ""
    try:
        ns = _try_import_v2_normalizer()
        out = (ns(raw) or "").strip()
        return out
    except Exception:
        return _fallback_normalize(raw)


def _parse_symbols_list(raw: str) -> List[str]:
    """
    Accepts:
      "AAPL,MSFT,1120.SR"
      "AAPL MSFT 1120.SR"
      "AAPL, MSFT  ,1120.SR"
    Returns a clean list (keeps order), normalized.
    """
    s = (raw or "").strip()
    if not s:
        return []
    parts = re.split(r"[\s,]+", s)
    out: List[str] = []
    for p in parts:
        if not p or not p.strip():
            continue
        out.append(_normalize_symbol(p))
    # keep order, remove empties/dupes
    seen = set()
    final: List[str] = []
    for x in out:
        if not x:
            continue
        k = x.upper()
        if k in seen:
            continue
        seen.add(k)
        final.append(x)
    return final


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
    if int(debug_q or 0):
        return True
    v = (os.getenv("DEBUG_ERRORS") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


# =============================================================================
# ✅ Recommendation normalization (ONE ENUM everywhere)
# =============================================================================
_RECO_ENUM = ("BUY", "HOLD", "REDUCE", "SELL")


def _normalize_recommendation(x: Any) -> Optional[str]:
    if x is None:
        return None
    try:
        s = str(x).strip().upper()
    except Exception:
        return None
    if not s:
        return None
    if s in _RECO_ENUM:
        return s

    s2 = re.sub(r"[\s\-_/]+", " ", s).strip()

    buy_like = {"STRONG BUY", "BUY", "ACCUMULATE", "ADD", "OUTPERFORM", "OVERWEIGHT", "LONG"}
    hold_like = {"HOLD", "NEUTRAL", "MAINTAIN", "MARKET PERFORM", "EQUAL WEIGHT", "WAIT"}
    reduce_like = {"REDUCE", "TRIM", "LIGHTEN", "UNDERWEIGHT", "PARTIAL SELL", "TAKE PROFIT", "TAKE PROFITS"}
    sell_like = {"SELL", "STRONG SELL", "EXIT", "AVOID", "UNDERPERFORM", "SHORT"}

    if s2 in buy_like:
        return "BUY"
    if s2 in hold_like:
        return "HOLD"
    if s2 in reduce_like:
        return "REDUCE"
    if s2 in sell_like:
        return "SELL"

    if "SELL" in s2:
        return "SELL"
    if "REDUCE" in s2 or "TRIM" in s2 or "UNDERWEIGHT" in s2:
        return "REDUCE"
    if "HOLD" in s2 or "NEUTRAL" in s2 or "MAINTAIN" in s2:
        return "HOLD"
    if "BUY" in s2 or "ACCUMULATE" in s2 or "OVERWEIGHT" in s2:
        return "BUY"
    return None


def _coerce_reco_enum(x: Any) -> str:
    return _normalize_recommendation(x) or "HOLD"


def _ensure_reco(payload: Any) -> Any:
    """
    Ensure recommendation exists and is standardized when payload is dict-like.
    Never raises. Returns payload (possibly copied dict).
    """
    try:
        if isinstance(payload, dict):
            out = dict(payload)
            out["recommendation"] = _coerce_reco_enum(out.get("recommendation"))
            return out
    except Exception:
        pass
    return payload


def _ensure_status_dict(d: Dict[str, Any], *, symbol: str = "", engine_source: str = "") -> Dict[str, Any]:
    """
    Ensure returned dict always includes a `status`.
    Also enforces recommendation enum always.
    """
    out = dict(d)
    out.setdefault("status", "ok")
    if symbol:
        out.setdefault("symbol", symbol)
    if engine_source:
        out.setdefault("engine_source", engine_source)
    out.setdefault("route_version", ENRICHED_ROUTE_VERSION)
    out.setdefault("time_utc", _utc_iso())
    out["recommendation"] = _coerce_reco_enum(out.get("recommendation"))
    return out


# =============================================================================
# Auth (X-APP-TOKEN / Authorization Bearer / optional ?token)
# =============================================================================
def _read_token_attr(obj: Any, attr: str) -> Optional[str]:
    try:
        v = getattr(obj, attr, None)
        if isinstance(v, str) and v.strip():
            return v.strip()
    except Exception:
        pass
    return None


@lru_cache(maxsize=1)
def _allowed_tokens() -> List[str]:
    tokens: List[str] = []

    # 1) settings
    try:
        s = get_settings()
        for attr in ("app_token", "backup_app_token"):
            v = _read_token_attr(s, attr)
            if v:
                tokens.append(v)
    except Exception:
        pass

    # 2) env.settings (optional)
    try:
        from env import settings as env_settings  # type: ignore

        for attr in ("app_token", "backup_app_token"):
            v = _read_token_attr(env_settings, attr)
            if v:
                tokens.append(v)
    except Exception:
        pass

    # 3) env vars
    for k in ("APP_TOKEN", "BACKUP_APP_TOKEN"):
        v = (os.getenv(k) or "").strip()
        if v:
            tokens.append(v)

    out: List[str] = []
    seen = set()
    for t in tokens:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _extract_token(
    x_app_token: Optional[str],
    authorization: Optional[str],
    token_qs: Optional[str],
) -> Optional[str]:
    t = (x_app_token or "").strip()
    if t:
        return t

    auth = (authorization or "").strip()
    if auth:
        low = auth.lower()
        if low.startswith("bearer "):
            return auth.split(" ", 1)[1].strip() or None
        return auth.strip() or None

    if _truthy(os.getenv("ALLOW_QUERY_TOKEN", "0")):
        tq = (token_qs or "").strip()
        if tq:
            return tq

    return None


def _auth_ok(provided: Optional[str]) -> bool:
    allowed = _allowed_tokens()
    if not allowed:
        return True  # open mode
    pt = (provided or "").strip()
    return bool(pt and pt in allowed)


# =============================================================================
# Engine resolution
# =============================================================================
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

        eng = await _maybe_await(get_engine())
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
) -> Tuple[Optional[str], Any]:
    """
    Prefer engine.get_enriched_quotes(symbols, ...) if available.
    Fallback: engine.get_quotes(symbols, ...) if available.
    Returns (method_name_used, raw_result) OR (None, None) if unsupported.
    """
    for name in ("get_enriched_quotes", "get_quotes"):
        fn = getattr(eng, name, None)
        if not callable(fn):
            continue

        try:
            return name, fn(symbols, refresh=refresh, fields=fields)
        except TypeError:
            pass
        try:
            return name, fn(symbols, refresh=refresh)
        except TypeError:
            pass
        try:
            return name, fn(symbols, fields=fields)
        except TypeError:
            pass
        try:
            return name, fn(symbols, refresh)
        except TypeError:
            pass
        return name, fn(symbols)

    return None, None


# =============================================================================
# Routes
# =============================================================================
@router.get("/v1/enriched/health")
async def enriched_health(request: Request) -> Dict[str, Any]:
    eng, eng_src = await _get_engine_from_request(request)
    engine_name = type(eng).__name__ if eng is not None else "none"
    return {
        "status": "ok",
        "module": "routes.enriched_quote",
        "route_version": ENRICHED_ROUTE_VERSION,
        "engine": engine_name,
        "engine_source": eng_src,
        "auth": "open" if not _allowed_tokens() else "token",
        "time_utc": _utc_iso(),
    }


@router.get("/v1/enriched/quote")
async def enriched_quote(
    request: Request,
    symbol: str = Query("", description="Ticker symbol, e.g. 1120.SR or AAPL or 1120"),
    refresh: int = Query(0, description="refresh=1 asks engine to bypass cache (if supported)"),
    fields: Optional[str] = Query(None, description="Optional hint to engine (comma/space-separated fields)"),
    debug: int = Query(0, description="debug=1 includes a traceback on failure (or set DEBUG_ERRORS=1)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    token: Optional[str] = Query(default=None, description="(optional) query token if ALLOW_QUERY_TOKEN=1"),
) -> Dict[str, Any]:
    eng, eng_src = await _get_engine_from_request(request)
    dbg = _debug_enabled(debug)

    sym = _normalize_symbol(symbol)

    provided = _extract_token(x_app_token, authorization, token)
    if not _auth_ok(provided):
        return _ensure_status_dict(
            {"status": "error", "error": "Unauthorized: invalid or missing token (X-APP-TOKEN or Authorization: Bearer)."},
            symbol=sym or (symbol or ""),
            engine_source=eng_src,
        )

    if not sym:
        return _ensure_status_dict({"status": "error", "error": "Missing symbol"}, symbol=(symbol or ""), engine_source=eng_src)

    if eng is None:
        return _ensure_status_dict({"status": "error", "error": "Engine not available"}, symbol=sym, engine_source=eng_src)

    try:
        # prefer enriched; fallback to get_quote (wrapped)
        try:
            raw = _call_engine_method_best_effort(
                eng=eng,
                method_name="get_enriched_quote",
                symbol=sym,
                refresh=bool(int(refresh or 0)),
                fields=fields,
            )
            res = _unwrap_tuple_payload(await _maybe_await(raw))
        except Exception:
            raw = _call_engine_method_best_effort(
                eng=eng,
                method_name="get_quote",
                symbol=sym,
                refresh=bool(int(refresh or 0)),
                fields=fields,
            )
            res = _unwrap_tuple_payload(await _maybe_await(raw))

        if isinstance(res, dict):
            return _ensure_status_dict(_ensure_reco(res), symbol=sym, engine_source=eng_src)

        wrapped = {"status": "ok", "symbol": sym, "engine_source": eng_src, "value": _to_jsonable(res)}
        return _ensure_status_dict(wrapped, symbol=sym, engine_source=eng_src)

    except Exception as e:
        out: Dict[str, Any] = {
            "status": "error",
            "symbol": sym,
            "engine_source": eng_src,
            "error": _clamp(e),
            "recommendation": "HOLD",
        }
        if dbg:
            out["trace"] = _clamp(traceback.format_exc(), 8000)
        return _ensure_status_dict(out, symbol=sym, engine_source=eng_src)


@router.get("/v1/enriched/quotes")
async def enriched_quotes(
    request: Request,
    symbols: str = Query("", description="Comma/space-separated list, e.g. AAPL,MSFT,1120.SR"),
    refresh: int = Query(0, description="refresh=1 asks engine to bypass cache (if supported)"),
    fields: Optional[str] = Query(None, description="Optional hint to engine (comma/space-separated fields)"),
    debug: int = Query(0, description="debug=1 includes a traceback on failure (or set DEBUG_ERRORS=1)"),
    max_symbols: int = Query(800, description="Safety limit for very large requests"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    token: Optional[str] = Query(default=None, description="(optional) query token if ALLOW_QUERY_TOKEN=1"),
) -> Dict[str, Any]:
    eng, eng_src = await _get_engine_from_request(request)
    dbg = _debug_enabled(debug)

    provided = _extract_token(x_app_token, authorization, token)
    if not _auth_ok(provided):
        return {
            "status": "error",
            "error": "Unauthorized: invalid or missing token (X-APP-TOKEN or Authorization: Bearer).",
            "engine_source": eng_src,
            "route_version": ENRICHED_ROUTE_VERSION,
            "time_utc": _utc_iso(),
            "count": 0,
            "items": [],
        }

    syms = _parse_symbols_list(symbols)
    if max_symbols and max_symbols > 0 and len(syms) > max_symbols:
        syms = syms[:max_symbols]

    if not syms:
        return {
            "status": "error",
            "error": "No valid symbols",
            "engine_source": eng_src,
            "route_version": ENRICHED_ROUTE_VERSION,
            "time_utc": _utc_iso(),
            "count": 0,
            "items": [],
        }

    if eng is None:
        return {
            "status": "error",
            "error": "Engine not available",
            "engine_source": eng_src,
            "route_version": ENRICHED_ROUTE_VERSION,
            "time_utc": _utc_iso(),
            "count": 0,
            "items": [],
        }

    items: List[Any] = []
    try:
        method_used, raw_batch = _call_engine_batch_best_effort(eng, syms, refresh=bool(int(refresh or 0)), fields=fields)
        if raw_batch is not None:
            res = _unwrap_tuple_payload(await _maybe_await(raw_batch))

            # common: {"items":[...]} already shaped
            if isinstance(res, dict) and isinstance(res.get("items"), list):
                out = dict(res)
                out.setdefault("status", "ok")
                out.setdefault("engine_source", eng_src)
                out.setdefault("route_version", ENRICHED_ROUTE_VERSION)
                out.setdefault("time_utc", _utc_iso())
                out["items"] = [
                    _ensure_status_dict(_ensure_reco(it) if isinstance(it, dict) else {"status": "ok", "value": _to_jsonable(it)}, symbol="", engine_source=eng_src)
                    for it in (out.get("items") or [])
                ]
                out["count"] = int(out.get("count") or len(out["items"]))
                out["method"] = method_used or "batch"
                return out

            # list: align by index when possible
            if isinstance(res, list):
                for i, it in enumerate(res):
                    sym = syms[i] if i < len(syms) else ""
                    if isinstance(it, dict):
                        items.append(_ensure_status_dict(_ensure_reco(it), symbol=sym, engine_source=eng_src))
                    else:
                        items.append(_ensure_status_dict({"status": "ok", "symbol": sym, "engine_source": eng_src, "value": _to_jsonable(it)}, symbol=sym, engine_source=eng_src))
                return {
                    "status": "ok",
                    "engine_source": eng_src,
                    "route_version": ENRICHED_ROUTE_VERSION,
                    "time_utc": _utc_iso(),
                    "method": method_used or "batch",
                    "count": len(items),
                    "items": items,
                }

            # dict mapping {symbol: quote}
            if isinstance(res, dict):
                for s in syms:
                    v = res.get(s) or res.get(s.upper()) or res.get(s.lower())
                    if isinstance(v, dict):
                        items.append(_ensure_status_dict(_ensure_reco(v), symbol=s, engine_source=eng_src))
                    elif v is None:
                        items.append(_ensure_status_dict({"status": "error", "symbol": s, "engine_source": eng_src, "error": "Missing in batch response"}, symbol=s, engine_source=eng_src))
                    else:
                        items.append(_ensure_status_dict({"status": "ok", "symbol": s, "engine_source": eng_src, "value": _to_jsonable(v)}, symbol=s, engine_source=eng_src))
                return {
                    "status": "ok",
                    "engine_source": eng_src,
                    "route_version": ENRICHED_ROUTE_VERSION,
                    "time_utc": _utc_iso(),
                    "method": method_used or "batch",
                    "count": len(items),
                    "items": items,
                }

        # Fallback: per-symbol loop
        for s in syms:
            try:
                try:
                    raw = _call_engine_method_best_effort(eng, "get_enriched_quote", s, refresh=bool(int(refresh or 0)), fields=fields)
                    res = _unwrap_tuple_payload(await _maybe_await(raw))
                except Exception:
                    raw = _call_engine_method_best_effort(eng, "get_quote", s, refresh=bool(int(refresh or 0)), fields=fields)
                    res = _unwrap_tuple_payload(await _maybe_await(raw))

                if isinstance(res, dict):
                    items.append(_ensure_status_dict(_ensure_reco(res), symbol=s, engine_source=eng_src))
                else:
                    items.append(_ensure_status_dict({"status": "ok", "symbol": s, "engine_source": eng_src, "value": _to_jsonable(res)}, symbol=s, engine_source=eng_src))
            except Exception as ex:
                err_item: Dict[str, Any] = {"status": "error", "symbol": s, "engine_source": eng_src, "error": _clamp(ex)}
                if dbg:
                    err_item["trace"] = _clamp(traceback.format_exc(), 4000)
                items.append(_ensure_status_dict(err_item, symbol=s, engine_source=eng_src))

        return {
            "status": "ok",
            "engine_source": eng_src,
            "route_version": ENRICHED_ROUTE_VERSION,
            "time_utc": _utc_iso(),
            "method": "per_symbol",
            "count": len(items),
            "items": items,
        }

    except Exception as e:
        out: Dict[str, Any] = {
            "status": "error",
            "engine_source": eng_src,
            "route_version": ENRICHED_ROUTE_VERSION,
            "time_utc": _utc_iso(),
            "error": _clamp(e),
            "items": items,
            "count": len(items),
        }
        if dbg:
            out["trace"] = _clamp(traceback.format_exc(), 8000)
        return out


__all__ = ["router"]
