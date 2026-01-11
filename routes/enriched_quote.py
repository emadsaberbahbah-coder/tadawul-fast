# routes/enriched_quote.py
"""
routes/enriched_quote.py
------------------------------------------------------------
Enriched Quote Router — PROD SAFE (await-safe + singleton-engine friendly) — v5.5.0

What’s fixed / improved in v5.5.0
- ✅ Global symbol routing fix: tries RAW symbol first (e.g., AAPL), then exchange-mapped variants (e.g., AAPL.US)
  so engines/providers that expect either format can succeed.
- ✅ KSA symbol routing fix: supports both "1120" and "1120.SR" by trying both variants (without breaking indices/FX).
- ✅ Batch endpoint “rescue”: for items that come back missing/empty provenance (data_source=none) or status=error,
  it retries per-symbol with variants (bounded, safe).
- ✅ Provenance normalization: if provider returns provider/source under different keys, we normalize to data_source.
- ✅ Still: always HTTP 200 with a status field; token guard unchanged; recommendation enum enforced.

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

ENRICHED_ROUTE_VERSION = "5.5.0"

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


def _debug_enabled(debug_q: int) -> bool:
    if int(debug_q or 0):
        return True
    v = (os.getenv("DEBUG_ERRORS") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _is_index_or_fx(sym: str) -> bool:
    s = (sym or "").strip().upper()
    if not s:
        return False
    # indices (^GSPC), Yahoo FX/commodities (EURUSD=X, GC=F), etc.
    return ("^" in s) or s.endswith("=X") or s.endswith("=F") or ("=" in s)


def _is_ksa(sym: str) -> bool:
    s = (sym or "").strip().upper()
    if not s:
        return False
    if s.endswith(".SR"):
        return True
    # allow naked digits as KSA
    return s.isdigit()


# =============================================================================
# Normalization (router-level; do NOT over-impose .US)
# =============================================================================
def _fallback_normalize(raw: str) -> str:
    """
    Router-side normalize:
    - Keep indices/FX/commodities as-is
    - Digits -> {digits}.SR
    - Keep any explicit suffix (.SR/.US/...) as-is (upper)
    - Plain tickers -> UPPER (do NOT force .US here; we try .US as a variant)
    """
    s = (raw or "").strip().upper()
    if not s:
        return ""
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip().upper()
    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", "")
    if _is_index_or_fx(s):
        return s
    if s.isdigit():
        return f"{s}.SR"
    if "." in s:
        return s
    return s


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
    # Prefer engine normalizer when present, BUT keep router behavior safe:
    # If engine normalizer returns empty or explodes, fallback.
    try:
        ns = _try_import_v2_normalizer()
        out = (ns(raw) or "").strip()
        return out if out else _fallback_normalize(raw)
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
    """Ensure recommendation exists and is standardized when payload is dict-like."""
    try:
        if isinstance(payload, dict):
            out = dict(payload)
            out["recommendation"] = _coerce_reco_enum(out.get("recommendation"))
            return out
    except Exception:
        pass
    return payload


# =============================================================================
# Provenance normalization (fix data_source: none when provider returns alt keys)
# =============================================================================
def _norm_str(x: Any) -> str:
    try:
        return (str(x) if x is not None else "").strip()
    except Exception:
        return ""


def _ensure_provenance(d: Dict[str, Any], *, engine_source: str) -> Dict[str, Any]:
    """
    Normalize common provider keys to:
      - data_source
      - data_quality
    Without lying: if missing, we keep empty; only replace explicit "none"/"null" with "".
    """
    out = dict(d)

    # normalize data_source
    ds = (
        out.get("data_source")
        or out.get("dataSource")
        or out.get("provider")
        or out.get("source")
        or out.get("primary_source")
        or out.get("quote_source")
    )
    ds_s = _norm_str(ds)
    if ds_s.lower() in ("none", "null", "na", "n/a"):
        ds_s = ""
    if ds_s:
        out["data_source"] = ds_s
    else:
        # keep blank; DO NOT force to EODHD/Yahoo here.
        # But never keep the literal string "none".
        if "data_source" in out and _norm_str(out.get("data_source")).lower() in ("none", "null"):
            out["data_source"] = ""

    # normalize data_quality if present under other keys
    dq = out.get("data_quality") or out.get("dataQuality") or out.get("quality")
    dq_s = _norm_str(dq)
    if dq_s:
        out["data_quality"] = dq_s

    # always keep engine_source for debugging
    out.setdefault("engine_source", engine_source)
    return out


def _ensure_status_dict(d: Dict[str, Any], *, symbol: str = "", engine_source: str = "") -> Dict[str, Any]:
    """Ensure returned dict always includes a `status` + metadata + reco enum."""
    out = dict(d)
    out.setdefault("status", "ok")
    if symbol:
        out.setdefault("symbol", symbol)
    if engine_source:
        out.setdefault("engine_source", engine_source)
    out.setdefault("route_version", ENRICHED_ROUTE_VERSION)
    out.setdefault("time_utc", _utc_iso())
    out["recommendation"] = _coerce_reco_enum(out.get("recommendation"))
    out = _ensure_provenance(out, engine_source=engine_source)
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
    hint: Optional[str],
) -> Any:
    """
    Tries common method signatures in a safe order, WITHOUT assuming the engine API.
    Adds an optional hint kw (e.g. "market_hint") best-effort.
    Returns raw result (may be awaitable).
    """
    fn = getattr(eng, method_name, None)
    if not callable(fn):
        raise AttributeError(f"Engine missing method {method_name}")

    hint_kwargs: List[Dict[str, Any]] = []
    if hint:
        # try a few common kw names; all are best-effort (TypeError-safe)
        hint_kwargs = [
            {"market": hint},
            {"market_hint": hint},
            {"region": hint},
            {"scope": hint},
        ]
    else:
        hint_kwargs = [{}]

    # try with hints, then without
    for hk in (hint_kwargs + [{}]):
        # kwargs-first
        try:
            return fn(symbol, refresh=refresh, fields=fields, **hk)
        except TypeError:
            pass
        try:
            return fn(symbol, refresh=refresh, **hk)
        except TypeError:
            pass
        try:
            return fn(symbol, fields=fields, **hk)
        except TypeError:
            pass

        # positional variants
        try:
            return fn(symbol, refresh, **hk)
        except TypeError:
            pass

        # minimal
        try:
            return fn(symbol, **hk)
        except TypeError:
            pass

    # if we got here, re-raise a consistent error
    return fn(symbol)


def _call_engine_batch_best_effort(
    eng: Any,
    symbols: List[str],
    refresh: bool,
    fields: Optional[str],
    hint: Optional[str],
) -> Tuple[Optional[str], Any]:
    """
    Prefer engine.get_enriched_quotes(symbols, ...) if available.
    Fallback: engine.get_quotes(symbols, ...) if available.
    Returns (method_name_used, raw_result) OR (None, None) if unsupported.
    """
    hint_kwargs: List[Dict[str, Any]] = []
    if hint:
        hint_kwargs = [
            {"market": hint},
            {"market_hint": hint},
            {"region": hint},
            {"scope": hint},
        ]
    else:
        hint_kwargs = [{}]

    for name in ("get_enriched_quotes", "get_quotes"):
        fn = getattr(eng, name, None)
        if not callable(fn):
            continue

        for hk in (hint_kwargs + [{}]):
            try:
                return name, fn(symbols, refresh=refresh, fields=fields, **hk)
            except TypeError:
                pass
            try:
                return name, fn(symbols, refresh=refresh, **hk)
            except TypeError:
                pass
            try:
                return name, fn(symbols, fields=fields, **hk)
            except TypeError:
                pass
            try:
                return name, fn(symbols, refresh, **hk)
            except TypeError:
                pass
            try:
                return name, fn(symbols, **hk)
            except TypeError:
                pass

        return name, fn(symbols)

    return None, None


# =============================================================================
# Symbol variants (fix Global vs KSA mapping issues)
# =============================================================================
def _symbol_variants(sym_norm: str) -> List[str]:
    """
    Generate safe variants so providers can match:
    - Global: AAPL -> [AAPL, AAPL.US]
    - Global: AAPL.US -> [AAPL.US, AAPL]
    - KSA: 1120.SR -> [1120.SR, 1120]
    - KSA: 1120 -> [1120.SR, 1120]
    Indices/FX/commodities: keep only itself.
    """
    s = (sym_norm or "").strip()
    if not s:
        return []
    u = s.upper()

    if _is_index_or_fx(u):
        return [u]

    out: List[str] = []

    # KSA
    if _is_ksa(u):
        if u.isdigit():
            out.append(f"{u}.SR")
            out.append(u)
        elif u.endswith(".SR"):
            out.append(u)
            out.append(u.replace(".SR", ""))
        else:
            out.append(u)
        # de-dupe preserve order
        seen = set()
        final: List[str] = []
        for x in out:
            if x and x not in seen:
                seen.add(x)
                final.append(x)
        return final

    # Global (default)
    # keep as-is, then add .US form (common for EODHD)
    out.append(u)
    if u.endswith(".US"):
        base = u[:-3]
        if base:
            out.append(base)
    else:
        # only add .US for plain tickers (no dot)
        if "." not in u:
            out.append(f"{u}.US")

    seen = set()
    final = []
    for x in out:
        if x and x not in seen:
            seen.add(x)
            final.append(x)
    return final


def _market_hint_for(sym_norm: str) -> str:
    u = (sym_norm or "").strip().upper()
    if _is_ksa(u):
        return "KSA"
    if _is_index_or_fx(u):
        return "GLOBAL"
    return "GLOBAL"


def _looks_ok(d: Dict[str, Any]) -> bool:
    """
    Best-effort 'did we get real data?'
    """
    st = _norm_str(d.get("status")).lower()
    if st and st != "ok":
        return False
    # common price keys
    for k in ("price", "last", "close", "current_price", "last_price"):
        v = d.get(k)
        try:
            if v is not None and float(v) == float(v):  # NaN-safe
                return True
        except Exception:
            continue
    # sometimes provider returns just name/market cap etc; treat as ok if any meaningful field exists
    for k in ("name", "market_cap", "currency", "exchange", "timestamp", "time_utc", "updated_at"):
        if _norm_str(d.get(k)):
            return True
    return False


def _needs_rescue(d: Dict[str, Any]) -> bool:
    """
    When batch returns data_source='none' (or missing) and/or status error, try variants per-symbol.
    """
    st = _norm_str(d.get("status")).lower()
    if st and st != "ok":
        return True
    ds = _norm_str(d.get("data_source")).lower()
    if ds in ("none", "null"):
        return True
    # if empty and looks not ok, rescue
    if (not ds) and (not _looks_ok(d)):
        return True
    return False


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


async def _get_one_quote_with_variants(
    *,
    eng: Any,
    eng_src: str,
    sym_norm: str,
    refresh: bool,
    fields: Optional[str],
    debug: bool,
) -> Dict[str, Any]:
    """
    Try symbol variants until we get a good result.
    """
    hint = _market_hint_for(sym_norm)
    attempts: List[Dict[str, Any]] = []

    for s_try in _symbol_variants(sym_norm):
        try:
            # prefer enriched; fallback to get_quote
            try:
                raw = _call_engine_method_best_effort(
                    eng=eng,
                    method_name="get_enriched_quote",
                    symbol=s_try,
                    refresh=refresh,
                    fields=fields,
                    hint=hint,
                )
                res = _unwrap_tuple_payload(await _maybe_await(raw))
            except Exception:
                raw = _call_engine_method_best_effort(
                    eng=eng,
                    method_name="get_quote",
                    symbol=s_try,
                    refresh=refresh,
                    fields=fields,
                    hint=hint,
                )
                res = _unwrap_tuple_payload(await _maybe_await(raw))

            if isinstance(res, dict):
                out = _ensure_status_dict(_ensure_reco(res), symbol=s_try, engine_source=eng_src)
                if debug:
                    attempts.append(
                        {
                            "symbol_try": s_try,
                            "status": out.get("status"),
                            "data_source": out.get("data_source") or "",
                            "data_quality": out.get("data_quality") or "",
                        }
                    )
                if _looks_ok(out):
                    # Keep original normalized as "requested_symbol"
                    out.setdefault("requested_symbol", sym_norm)
                    if debug:
                        out["attempts"] = attempts
                    return out

                # keep last non-ok-ish dict in case nothing better appears
                last = out
            else:
                wrapped = {"status": "ok", "symbol": s_try, "engine_source": eng_src, "value": _to_jsonable(res)}
                out = _ensure_status_dict(wrapped, symbol=s_try, engine_source=eng_src)
                out.setdefault("requested_symbol", sym_norm)
                if debug:
                    attempts.append(
                        {
                            "symbol_try": s_try,
                            "status": out.get("status"),
                            "data_source": out.get("data_source") or "",
                            "data_quality": out.get("data_quality") or "",
                        }
                    )
                    out["attempts"] = attempts
                return out

        except Exception as e:
            if debug:
                attempts.append({"symbol_try": s_try, "status": "error", "error": _clamp(e)})
            last = _ensure_status_dict(
                {"status": "error", "error": _clamp(e), "recommendation": "HOLD"},
                symbol=s_try,
                engine_source=eng_src,
            )

    # if all attempts failed, return the last captured
    if isinstance(last, dict):
        last.setdefault("requested_symbol", sym_norm)
        if debug:
            last["attempts"] = attempts
        return last

    return _ensure_status_dict(
        {"status": "error", "error": "Failed to resolve symbol via variants", "recommendation": "HOLD"},
        symbol=sym_norm,
        engine_source=eng_src,
    )


@router.get("/v1/enriched/quote")
async def enriched_quote(
    request: Request,
    symbol: str = Query("", description="Ticker symbol, e.g. 1120.SR or AAPL or 1120"),
    refresh: int = Query(0, description="refresh=1 asks engine to bypass cache (if supported)"),
    fields: Optional[str] = Query(None, description="Optional hint to engine (comma/space-separated fields)"),
    debug: int = Query(0, description="debug=1 includes attempts/trace on failure (or set DEBUG_ERRORS=1)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    token: Optional[str] = Query(default=None, description="(optional) query token if ALLOW_QUERY_TOKEN=1"),
) -> Dict[str, Any]:
    eng, eng_src = await _get_engine_from_request(request)
    dbg = _debug_enabled(debug)

    sym_norm = _normalize_symbol(symbol)

    provided = _extract_token(x_app_token, authorization, token)
    if not _auth_ok(provided):
        return _ensure_status_dict(
            {"status": "error", "error": "Unauthorized: invalid or missing token (X-APP-TOKEN or Authorization: Bearer)."},
            symbol=sym_norm or (symbol or ""),
            engine_source=eng_src,
        )

    if not sym_norm:
        return _ensure_status_dict({"status": "error", "error": "Missing symbol"}, symbol=(symbol or ""), engine_source=eng_src)

    if eng is None:
        return _ensure_status_dict({"status": "error", "error": "Engine not available"}, symbol=sym_norm, engine_source=eng_src)

    try:
        out = await _get_one_quote_with_variants(
            eng=eng,
            eng_src=eng_src,
            sym_norm=sym_norm,
            refresh=bool(int(refresh or 0)),
            fields=fields,
            debug=dbg,
        )
        return out
    except Exception as e:
        resp: Dict[str, Any] = {
            "status": "error",
            "symbol": sym_norm,
            "engine_source": eng_src,
            "error": _clamp(e),
            "recommendation": "HOLD",
        }
        if dbg:
            resp["trace"] = _clamp(traceback.format_exc(), 8000)
        return _ensure_status_dict(resp, symbol=sym_norm, engine_source=eng_src)


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

    items: List[Dict[str, Any]] = []
    try:
        # Batch attempt (best-effort). Use GLOBAL hint because list can mix; engine should ignore if not supported.
        method_used, raw_batch = _call_engine_batch_best_effort(
            eng,
            syms,
            refresh=bool(int(refresh or 0)),
            fields=fields,
            hint="GLOBAL",
        )

        if raw_batch is not None:
            res = _unwrap_tuple_payload(await _maybe_await(raw_batch))

            # common: {"items":[...]} already shaped
            if isinstance(res, dict) and isinstance(res.get("items"), list):
                out = dict(res)
                out.setdefault("status", "ok")
                out.setdefault("engine_source", eng_src)
                out.setdefault("route_version", ENRICHED_ROUTE_VERSION)
                out.setdefault("time_utc", _utc_iso())

                shaped: List[Dict[str, Any]] = []
                raw_items = out.get("items") or []
                for i, it in enumerate(raw_items):
                    sym_i = syms[i] if i < len(syms) else ""
                    if isinstance(it, dict):
                        shaped.append(_ensure_status_dict(_ensure_reco(it), symbol=sym_i or it.get("symbol", ""), engine_source=eng_src))
                    else:
                        shaped.append(
                            _ensure_status_dict(
                                {"status": "ok", "symbol": sym_i, "engine_source": eng_src, "value": _to_jsonable(it)},
                                symbol=sym_i,
                                engine_source=eng_src,
                            )
                        )

                # rescue weak items (status error or data_source none/empty and looks bad)
                rescued: List[Dict[str, Any]] = []
                for i, q in enumerate(shaped):
                    sym_i = syms[i] if i < len(syms) else q.get("symbol", "")
                    if sym_i and _needs_rescue(q):
                        fixed = await _get_one_quote_with_variants(
                            eng=eng,
                            eng_src=eng_src,
                            sym_norm=sym_i,
                            refresh=bool(int(refresh or 0)),
                            fields=fields,
                            debug=dbg,
                        )
                        rescued.append(fixed)
                    else:
                        rescued.append(q)

                out["items"] = rescued
                out["count"] = int(out.get("count") or len(rescued))
                out["method"] = method_used or "batch"
                return out

            # list: align by index when possible
            if isinstance(res, list):
                shaped2: List[Dict[str, Any]] = []
                for i, it in enumerate(res):
                    sym_i = syms[i] if i < len(syms) else ""
                    if isinstance(it, dict):
                        shaped2.append(_ensure_status_dict(_ensure_reco(it), symbol=sym_i or it.get("symbol", ""), engine_source=eng_src))
                    else:
                        shaped2.append(
                            _ensure_status_dict(
                                {"status": "ok", "symbol": sym_i, "engine_source": eng_src, "value": _to_jsonable(it)},
                                symbol=sym_i,
                                engine_source=eng_src,
                            )
                        )

                # rescue
                final_items: List[Dict[str, Any]] = []
                for i, q in enumerate(shaped2):
                    sym_i = syms[i] if i < len(syms) else q.get("symbol", "")
                    if sym_i and _needs_rescue(q):
                        final_items.append(
                            await _get_one_quote_with_variants(
                                eng=eng,
                                eng_src=eng_src,
                                sym_norm=sym_i,
                                refresh=bool(int(refresh or 0)),
                                fields=fields,
                                debug=dbg,
                            )
                        )
                    else:
                        final_items.append(q)

                return {
                    "status": "ok",
                    "engine_source": eng_src,
                    "route_version": ENRICHED_ROUTE_VERSION,
                    "time_utc": _utc_iso(),
                    "method": method_used or "batch",
                    "count": len(final_items),
                    "items": final_items,
                }

            # dict mapping {symbol: quote}
            if isinstance(res, dict):
                shaped3: List[Dict[str, Any]] = []
                for s in syms:
                    v = res.get(s) or res.get(s.upper()) or res.get(s.lower())
                    if isinstance(v, dict):
                        shaped3.append(_ensure_status_dict(_ensure_reco(v), symbol=s, engine_source=eng_src))
                    elif v is None:
                        shaped3.append(
                            _ensure_status_dict(
                                {"status": "error", "symbol": s, "engine_source": eng_src, "error": "Missing in batch response"},
                                symbol=s,
                                engine_source=eng_src,
                            )
                        )
                    else:
                        shaped3.append(
                            _ensure_status_dict(
                                {"status": "ok", "symbol": s, "engine_source": eng_src, "value": _to_jsonable(v)},
                                symbol=s,
                                engine_source=eng_src,
                            )
                        )

                # rescue
                final_items = []
                for q in shaped3:
                    s = _norm_str(q.get("symbol"))
                    if s and _needs_rescue(q):
                        final_items.append(
                            await _get_one_quote_with_variants(
                                eng=eng,
                                eng_src=eng_src,
                                sym_norm=s,
                                refresh=bool(int(refresh or 0)),
                                fields=fields,
                                debug=dbg,
                            )
                        )
                    else:
                        final_items.append(q)

                return {
                    "status": "ok",
                    "engine_source": eng_src,
                    "route_version": ENRICHED_ROUTE_VERSION,
                    "time_utc": _utc_iso(),
                    "method": method_used or "batch",
                    "count": len(final_items),
                    "items": final_items,
                }

        # Fallback: per-symbol loop with variants
        for s in syms:
            try:
                out = await _get_one_quote_with_variants(
                    eng=eng,
                    eng_src=eng_src,
                    sym_norm=s,
                    refresh=bool(int(refresh or 0)),
                    fields=fields,
                    debug=dbg,
                )
                items.append(out)
            except Exception as ex:
                err_item: Dict[str, Any] = {"status": "error", "symbol": s, "engine_source": eng_src, "error": _clamp(ex), "recommendation": "HOLD"}
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
