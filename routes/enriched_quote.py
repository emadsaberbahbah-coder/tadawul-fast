# routes/enriched_quote.py
"""
routes/enriched_quote.py
------------------------------------------------------------
Enriched Quote Router — PROD SAFE (await-safe + singleton-engine friendly) — v5.7.0

v5.7.0 changes (FULL UPDATE)
- ✅ Mixed-market batch support:
    - Groups symbols by market hint (KSA / GLOBAL / INDEXFX) and runs batch per group when possible
    - Fixes the old behavior that always forced hint="GLOBAL" for batch requests
- ✅ Stronger batch-to-request alignment:
    - Always returns items in the SAME order as requested
    - Always includes requested_symbol on every item
    - Adds normalized_symbol (the router normalized form) for debugging / Sheets mapping
- ✅ Better query parsing:
    - Accepts comma/space strings and repeated query params for BOTH symbols and tickers
    - Accepts single aliases: ticker/symbol
    - Accepts both lower/upper + trims + dedupes while preserving order
- ✅ Rescue logic improved:
    - If batch item is missing/weak/error → retry per-symbol with variants & proper market hint
- ✅ Provenance normalization hardened:
    - data_source + data_quality inferred from common provider keys (without fabricating)
- ✅ Keeps PROD SAFE contract:
    - HTTP 200 always with {"status": "...", ...}
    - Token guard (X-APP-TOKEN / Authorization: Bearer / optional ?token when ALLOW_QUERY_TOKEN=1)
    - Recommendation enum enforced (BUY/HOLD/REDUCE/SELL)
    - Variant rescue for Global/KSA symbols (AAPL <-> AAPL.US, 1120 <-> 1120.SR)
    - Handles list/dict/bizarre batch payloads safely

Endpoints
- GET /v1/enriched/quote?symbol=1120.SR
- GET /v1/enriched/quotes?symbols=AAPL,MSFT,1120.SR
- GET /v1/enriched/quotes?symbols=AAPL&symbols=MSFT
- GET /v1/enriched/quotes?tickers=1120.SR,2222.SR
- GET /v1/enriched/quotes?ticker=1120.SR
- GET /v1/enriched/health
"""

from __future__ import annotations

import inspect
import importlib
import os
import re
import traceback
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple, Iterable

from fastapi import APIRouter, Header, Query, Request

router = APIRouter(tags=["enriched"])

ENRICHED_ROUTE_VERSION = "5.7.0"

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


def _norm_str(x: Any) -> str:
    try:
        return (str(x) if x is not None else "").strip()
    except Exception:
        return ""


def _norm_key(x: Any) -> str:
    return _norm_str(x).strip().upper()


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
def _try_import_shared_normalizer() -> Any:
    """
    Return normalize_symbol callable if available, else fallback.
    Never raises.
    """
    try:
        from core.symbols.normalize import normalize_symbol as _NS  # type: ignore

        return _NS
    except Exception:
        return _fallback_normalize


def _normalize_symbol(sym: str) -> str:
    raw = (sym or "").strip()
    if not raw:
        return ""
    try:
        ns = _try_import_shared_normalizer()
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

    # keep order, remove empties/dupes (case-insensitive)
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


def _flatten_query_value(v: Any) -> str:
    """
    v can be:
    - None
    - "AAPL,MSFT"
    - ["AAPL", "MSFT"]
    - ["AAPL,MSFT"]  (when user passes comma string but param typed as list)
    Returns a single string that _parse_symbols_list can handle.
    """
    if v is None:
        return ""
    if isinstance(v, (list, tuple)):
        parts: List[str] = []
        for it in v:
            s = _norm_str(it)
            if s:
                parts.append(s)
        return ",".join(parts)
    return _norm_str(v)


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
    hold_like = {"HOLD", "NEUTRAL", "MAINTAIN", "MARKET PERFORM", "EQUAL WEIGHT", "WAIT", "KEEP"}
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
# Provenance normalization
# =============================================================================
def _ensure_provenance(d: Dict[str, Any], *, engine_source: str) -> Dict[str, Any]:
    """
    Normalize common provider keys to:
      - data_source
      - data_quality
    Without lying: only map if data exists under known keys.
    """
    out = dict(d)

    ds = (
        out.get("data_source")
        or out.get("dataSource")
        or out.get("provider")
        or out.get("source")
        or out.get("primary_source")
        or out.get("quote_source")
    )
    ds_s = _norm_str(ds)
    if ds_s.lower() in ("null", "na", "n/a"):
        ds_s = ""
    if ds_s.lower() == "none":
        ds_s = "none"
    if ds_s:
        out["data_source"] = ds_s

    dq = out.get("data_quality") or out.get("dataQuality") or out.get("quality")
    dq_s = _norm_str(dq)
    if dq_s:
        out["data_quality"] = dq_s

    out.setdefault("engine_source", engine_source)
    return out


def _infer_status(d: Dict[str, Any]) -> str:
    """
    If status isn't explicitly ok/error, infer:
    - error when error exists OR data_quality==BAD
    - else ok
    """
    st = _norm_str(d.get("status")).lower()
    if st in ("ok", "error"):
        return st
    if _norm_str(d.get("error")):
        return "error"
    if _norm_str(d.get("data_quality")).upper() == "BAD":
        return "error"
    return "ok"


def _ensure_status_dict(
    d: Dict[str, Any],
    *,
    symbol: str = "",
    engine_source: str = "",
    requested_symbol: str = "",
    normalized_symbol: str = "",
) -> Dict[str, Any]:
    """Ensure returned dict always includes a `status` + metadata + reco enum."""
    out = dict(d)

    out["status"] = _infer_status(out)

    if symbol:
        out.setdefault("symbol", symbol)
    if engine_source:
        out.setdefault("engine_source", engine_source)

    # ALWAYS include mapping helpers when provided
    if requested_symbol:
        out.setdefault("requested_symbol", requested_symbol)
    if normalized_symbol:
        out.setdefault("normalized_symbol", normalized_symbol)

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
    Last fallback: instantiate DataEngineV2/DataEngine.
    """
    # 1) app.state.engine
    try:
        st = getattr(request.app, "state", None)
        eng = getattr(st, "engine", None) if st else None
        if eng is not None:
            return eng, "app.state.engine"
    except Exception:
        pass

    # 2) singleton engine (preferred)
    try:
        m = importlib.import_module("core.data_engine_v2")
        get_engine = getattr(m, "get_engine", None)
        if callable(get_engine):
            eng = await _maybe_await(get_engine())
            return eng, "core.data_engine_v2.get_engine()"
    except Exception:
        pass

    # 3) last resort instantiation
    try:
        m = importlib.import_module("core.data_engine_v2")
        for cls_name in ("DataEngineV2", "DataEngine"):
            cls = getattr(m, cls_name, None)
            if cls is not None:
                return cls(), f"core.data_engine_v2.{cls_name}()"
    except Exception:
        pass

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
    Adds an optional hint kw (e.g. market_hint) best-effort.
    Returns raw result (may be awaitable).
    """
    fn = getattr(eng, method_name, None)
    if not callable(fn):
        raise AttributeError(f"Engine missing method {method_name}")

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

        try:
            return name, fn(symbols)
        except TypeError:
            return None, None

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

        seen = set()
        final: List[str] = []
        for x in out:
            if x and x not in seen:
                seen.add(x)
                final.append(x)
        return final

    # Global (default)
    out.append(u)
    if u.endswith(".US"):
        base = u[:-3]
        if base:
            out.append(base)
    else:
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
    if _is_index_or_fx(u):
        return "INDEXFX"
    if _is_ksa(u):
        return "KSA"
    return "GLOBAL"


def _looks_ok(d: Dict[str, Any]) -> bool:
    """Best-effort: did we get real data?"""
    st = _norm_str(d.get("status")).lower()
    if st and st != "ok":
        return False

    for k in ("current_price", "price", "last", "close", "last_price"):
        v = d.get(k)
        try:
            if v is not None and float(v) == float(v):  # NaN-safe
                return True
        except Exception:
            continue

    for k in ("name", "market_cap", "currency", "exchange", "time_utc", "updated_at", "last_updated_utc"):
        if _norm_str(d.get(k)):
            return True

    return False


def _needs_rescue(d: Dict[str, Any]) -> bool:
    """
    When response looks weak or indicates errors, try variants per-symbol.
    """
    st = _norm_str(d.get("status")).lower()
    if st and st != "ok":
        return True

    if _norm_str(d.get("error")):
        return True

    dq = _norm_str(d.get("data_quality")).upper()
    if dq == "BAD":
        return True

    ds = _norm_str(d.get("data_source")).lower()
    if ds in ("none", "null"):
        return not _looks_ok(d)

    if (not ds) and (not _looks_ok(d)):
        return True

    return False


# =============================================================================
# Query alias resolution (fix "No valid symbols")
# =============================================================================
def _first_nonempty(*vals: Any) -> str:
    for v in vals:
        s = _flatten_query_value(v)
        if s:
            return s
    return ""


def _resolve_symbols_from_query(
    *,
    symbols: Any,
    tickers: Any,
    ticker: Optional[str],
    symbol: Optional[str],
) -> List[str]:
    """
    Accepts multiple aliases and returns a normalized list.
    Priority:
      1) symbols / tickers (multi)
      2) ticker / symbol (single)
    Supports:
      - comma/space strings
      - repeated query params (list)
    """
    multi_raw = _first_nonempty(symbols, tickers)
    if multi_raw:
        return _parse_symbols_list(multi_raw)

    single_raw = _norm_str(ticker) or _norm_str(symbol)
    if single_raw:
        s = _normalize_symbol(single_raw)
        return [s] if s else []

    return []


def _case_insensitive_mapping_get(m: Dict[str, Any], key: str) -> Any:
    if not isinstance(m, dict):
        return None
    if key in m:
        return m.get(key)
    kU = key.upper()
    kL = key.lower()
    if kU in m:
        return m.get(kU)
    if kL in m:
        return m.get(kL)

    target = key.strip().upper()
    for kk, vv in m.items():
        if _norm_key(kk) == target:
            return vv
    return None


def _mapping_get_with_variants(m: Dict[str, Any], sym: str) -> Any:
    """
    Batch dict responses sometimes key by variants:
      requested: AAPL
      returned:  AAPL.US
    This tries exact + case-insensitive + variants.
    """
    if not isinstance(m, dict):
        return None

    v = _case_insensitive_mapping_get(m, sym)
    if v is not None:
        return v

    for s2 in _symbol_variants(sym):
        v2 = _case_insensitive_mapping_get(m, s2)
        if v2 is not None:
            return v2

    return None


# =============================================================================
# Batch grouping (v5.7.0)
# =============================================================================
def _group_symbols_by_hint(syms: List[str]) -> Dict[str, List[str]]:
    """
    Returns dict: hint -> list of symbols (preserving original order within each group)
    Hints: GLOBAL / KSA / INDEXFX
    """
    groups: Dict[str, List[str]] = {"GLOBAL": [], "KSA": [], "INDEXFX": []}
    for s in syms:
        h = _market_hint_for(s)
        if h not in groups:
            groups[h] = []
        groups[h].append(s)
    # remove empties
    return {k: v for k, v in groups.items() if v}


def _empty_item(
    *,
    sym: str,
    eng_src: str,
    msg: str,
    requested_symbol: str,
    normalized_symbol: str,
) -> Dict[str, Any]:
    return _ensure_status_dict(
        {
            "status": "error",
            "symbol": sym,
            "engine_source": eng_src,
            "error": msg,
            "recommendation": "HOLD",
        },
        symbol=sym,
        engine_source=eng_src,
        requested_symbol=requested_symbol,
        normalized_symbol=normalized_symbol,
    )


def _shape_batch_result(
    *,
    syms: List[str],
    res: Any,
    eng_src: str,
) -> Dict[str, Dict[str, Any]]:
    """
    Convert batch response into mapping: requested_symbol -> shaped_item
    Accepts list (aligned by index) or dict mapping.
    """
    out: Dict[str, Dict[str, Any]] = {}

    # list-aligned
    if isinstance(res, list):
        for i, s in enumerate(syms):
            it = res[i] if i < len(res) else None
            if isinstance(it, dict):
                shaped = _ensure_status_dict(
                    _ensure_reco(it),
                    symbol=_norm_str(it.get("symbol") or s) or s,
                    engine_source=eng_src,
                    requested_symbol=s,
                    normalized_symbol=s,
                )
                out[s] = shaped
            elif it is None:
                out[s] = _empty_item(
                    sym=s,
                    eng_src=eng_src,
                    msg="Missing item in batch list",
                    requested_symbol=s,
                    normalized_symbol=s,
                )
            else:
                out[s] = _ensure_status_dict(
                    {
                        "status": "ok",
                        "symbol": s,
                        "engine_source": eng_src,
                        "value": _to_jsonable(it),
                        "recommendation": "HOLD",
                    },
                    symbol=s,
                    engine_source=eng_src,
                    requested_symbol=s,
                    normalized_symbol=s,
                )
        return out

    # dict mapping
    if isinstance(res, dict):
        for s in syms:
            v = _mapping_get_with_variants(res, s)
            if isinstance(v, dict):
                out[s] = _ensure_status_dict(
                    _ensure_reco(v),
                    symbol=_norm_str(v.get("symbol") or s) or s,
                    engine_source=eng_src,
                    requested_symbol=s,
                    normalized_symbol=s,
                )
            elif v is None:
                out[s] = _empty_item(
                    sym=s,
                    eng_src=eng_src,
                    msg="Missing in batch response",
                    requested_symbol=s,
                    normalized_symbol=s,
                )
            else:
                out[s] = _ensure_status_dict(
                    {
                        "status": "ok",
                        "symbol": s,
                        "engine_source": eng_src,
                        "value": _to_jsonable(v),
                        "recommendation": "HOLD",
                    },
                    symbol=s,
                    engine_source=eng_src,
                    requested_symbol=s,
                    normalized_symbol=s,
                )
        return out

    # unsupported
    for s in syms:
        out[s] = _empty_item(
            sym=s,
            eng_src=eng_src,
            msg=f"Unsupported batch payload type: {type(res).__name__}",
            requested_symbol=s,
            normalized_symbol=s,
        )
    return out


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
    """Try symbol variants until we get a good result."""
    hint = _market_hint_for(sym_norm)
    attempts: List[Dict[str, Any]] = []

    last: Dict[str, Any] = _ensure_status_dict(
        {"status": "error", "error": "No attempts executed", "recommendation": "HOLD"},
        symbol=sym_norm,
        engine_source=eng_src,
        requested_symbol=sym_norm,
        normalized_symbol=sym_norm,
    )

    variants = _symbol_variants(sym_norm) or [sym_norm]
    for s_try in variants:
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
                out = _ensure_status_dict(
                    _ensure_reco(res),
                    symbol=_norm_str(res.get("symbol") or s_try) or s_try,
                    engine_source=eng_src,
                    requested_symbol=sym_norm,
                    normalized_symbol=sym_norm,
                )

                if debug:
                    attempts.append(
                        {
                            "symbol_try": s_try,
                            "status": out.get("status"),
                            "data_source": out.get("data_source") or "",
                            "data_quality": out.get("data_quality") or "",
                            "hint": hint,
                        }
                    )

                if _looks_ok(out):
                    if debug:
                        out["attempts"] = attempts
                    return out

                last = out
                continue

            wrapped = {
                "status": "ok",
                "symbol": s_try,
                "engine_source": eng_src,
                "value": _to_jsonable(res),
                "recommendation": "HOLD",
            }
            out2 = _ensure_status_dict(
                wrapped,
                symbol=s_try,
                engine_source=eng_src,
                requested_symbol=sym_norm,
                normalized_symbol=sym_norm,
            )
            if debug:
                attempts.append(
                    {
                        "symbol_try": s_try,
                        "status": out2.get("status"),
                        "data_source": out2.get("data_source") or "",
                        "data_quality": out2.get("data_quality") or "",
                        "hint": hint,
                    }
                )
                out2["attempts"] = attempts
            return out2

        except Exception as e:
            if debug:
                attempts.append({"symbol_try": s_try, "status": "error", "error": _clamp(e), "hint": hint})
            last = _ensure_status_dict(
                {"status": "error", "error": _clamp(e), "recommendation": "HOLD"},
                symbol=s_try,
                engine_source=eng_src,
                requested_symbol=sym_norm,
                normalized_symbol=sym_norm,
            )

    if debug:
        last["attempts"] = attempts
    return last


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
            requested_symbol=sym_norm or (symbol or ""),
            normalized_symbol=sym_norm or (symbol or ""),
        )

    if not sym_norm:
        return _ensure_status_dict(
            {"status": "error", "error": "Missing symbol"},
            symbol=(symbol or ""),
            engine_source=eng_src,
            requested_symbol=(symbol or ""),
            normalized_symbol=(symbol or ""),
        )

    if eng is None:
        return _ensure_status_dict(
            {"status": "error", "error": "Engine not available"},
            symbol=sym_norm,
            engine_source=eng_src,
            requested_symbol=sym_norm,
            normalized_symbol=sym_norm,
        )

    try:
        return await _get_one_quote_with_variants(
            eng=eng,
            eng_src=eng_src,
            sym_norm=sym_norm,
            refresh=bool(int(refresh or 0)),
            fields=fields,
            debug=dbg,
        )
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
        return _ensure_status_dict(
            resp,
            symbol=sym_norm,
            engine_source=eng_src,
            requested_symbol=sym_norm,
            normalized_symbol=sym_norm,
        )


@router.get("/v1/enriched/quotes")
async def enriched_quotes(
    request: Request,
    # accept repeated params too
    symbols: Optional[List[str]] = Query(
        default=None,
        description="Comma/space-separated OR repeated, e.g. symbols=AAPL,MSFT OR symbols=AAPL&symbols=MSFT",
    ),
    tickers: Optional[List[str]] = Query(default=None, description="Alias of symbols (comma/space-separated OR repeated)"),
    ticker: Optional[str] = Query(None, description="Single symbol alias (e.g. 1120.SR)"),
    symbol: Optional[str] = Query(None, description="Single symbol alias (e.g. 1120.SR)"),
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

    syms = _resolve_symbols_from_query(symbols=symbols, tickers=tickers, ticker=ticker, symbol=symbol)

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

    # Final output must preserve request order
    final_by_req: Dict[str, Dict[str, Any]] = {}

    try:
        # v5.7.0: group by market hint and attempt batch per group
        groups = _group_symbols_by_hint(syms)
        batch_method_any: Optional[str] = None
        batch_used = False

        for hint, gsyms in groups.items():
            method_used, raw_batch = _call_engine_batch_best_effort(
                eng,
                gsyms,
                refresh=bool(int(refresh or 0)),
                fields=fields,
                hint=hint if hint != "INDEXFX" else None,  # many engines ignore/skip hint for indices/FX
            )

            if raw_batch is None:
                continue

            batch_used = True
            batch_method_any = batch_method_any or method_used or "batch"

            res = _unwrap_tuple_payload(await _maybe_await(raw_batch))
            shaped_map = _shape_batch_result(syms=gsyms, res=res, eng_src=eng_src)

            # rescue weak items immediately
            for req_sym in gsyms:
                item = shaped_map.get(req_sym) or _empty_item(
                    sym=req_sym,
                    eng_src=eng_src,
                    msg="Missing shaped batch item",
                    requested_symbol=req_sym,
                    normalized_symbol=req_sym,
                )

                if _needs_rescue(item):
                    item = await _get_one_quote_with_variants(
                        eng=eng,
                        eng_src=eng_src,
                        sym_norm=req_sym,
                        refresh=bool(int(refresh or 0)),
                        fields=fields,
                        debug=dbg,
                    )

                # enforce requested_symbol/normalized_symbol always
                item = _ensure_status_dict(
                    item,
                    symbol=_norm_str(item.get("symbol") or req_sym) or req_sym,
                    engine_source=eng_src,
                    requested_symbol=req_sym,
                    normalized_symbol=req_sym,
                )
                final_by_req[req_sym] = item

        # If batch not supported at all, do per-symbol loop
        if not batch_used:
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
                    out = _ensure_status_dict(
                        out,
                        symbol=_norm_str(out.get("symbol") or s) or s,
                        engine_source=eng_src,
                        requested_symbol=s,
                        normalized_symbol=s,
                    )
                    final_by_req[s] = out
                except Exception as ex:
                    err_item: Dict[str, Any] = {
                        "status": "error",
                        "symbol": s,
                        "engine_source": eng_src,
                        "error": _clamp(ex),
                        "recommendation": "HOLD",
                    }
                    if dbg:
                        err_item["trace"] = _clamp(traceback.format_exc(), 4000)
                    final_by_req[s] = _ensure_status_dict(
                        err_item,
                        symbol=s,
                        engine_source=eng_src,
                        requested_symbol=s,
                        normalized_symbol=s,
                    )

        # Assemble in requested order
        items: List[Dict[str, Any]] = []
        for s in syms:
            items.append(
                final_by_req.get(s)
                or _empty_item(
                    sym=s,
                    eng_src=eng_src,
                    msg="Missing final item (internal alignment)",
                    requested_symbol=s,
                    normalized_symbol=s,
                )
            )

        return {
            "status": "ok",
            "engine_source": eng_src,
            "route_version": ENRICHED_ROUTE_VERSION,
            "time_utc": _utc_iso(),
            "method": (batch_method_any or ("per_symbol" if not batch_used else "batch")),
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
        }
        if dbg:
            out["trace"] = _clamp(traceback.format_exc(), 8000)

        # still return whatever we have in request order
        items: List[Dict[str, Any]] = []
        for s in syms:
            items.append(
                final_by_req.get(s)
                or _empty_item(
                    sym=s,
                    eng_src=eng_src,
                    msg="Failed during processing",
                    requested_symbol=s,
                    normalized_symbol=s,
                )
            )
        out["items"] = items
        out["count"] = len(items)
        return out


__all__ = ["router"]
