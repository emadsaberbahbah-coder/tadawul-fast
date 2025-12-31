# core/enriched_quote.py  (FULL REPLACEMENT)
"""
core/enriched_quote.py
------------------------------------------------------------
Compatibility Router + Row Mapper: Enriched Quote (PROD SAFE) — v2.4.0

What this module is for
- Legacy/compat endpoints under /v1/enriched/* (kept because some clients still call it)
- Always returns HTTP 200 with a `status` field (client simplicity)
- Works with BOTH async and sync engines
- Attempts batch fast-path first; falls back per-symbol safely

✅ v2.4.0 enhancements
- Adds EnrichedQuote helper class with:
    - from_unified(payload)  (dict or attribute object)
    - to_row(headers)        (robust header-driven mapping aligned with core.schemas v3.4.0)
- Standardizes recommendation everywhere to: BUY / HOLD / REDUCE / SELL
- Better batch compatibility:
    - engine may return list OR dict OR (payload, err)
- More defensive schema filling:
    - uses UnifiedQuote.model_fields if available, else fills using canonical header mappings
- Riyadh timestamp fill (Last Updated Riyadh) from UTC if missing
- 52W Position % computed if missing (from 52W high/low + current price)

NOTE
- This module is import-safe (no hard DataEngine dependency at import time).
"""

from __future__ import annotations

import inspect
import os
import re
import traceback
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from fastapi import APIRouter, Query, Request
from fastapi.encoders import jsonable_encoder
from starlette.responses import JSONResponse

ROUTER_VERSION = "2.4.0"
router = APIRouter(prefix="/v1/enriched", tags=["enriched"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_UQ_KEYS: Optional[List[str]] = None


# =============================================================================
# Small helpers
# =============================================================================
def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY


def _clamp(s: Any, n: int = 2000) -> str:
    t = (str(s) if s is not None else "").strip()
    if not t:
        return ""
    return t if len(t) <= n else (t[: n - 12] + " ...TRUNC...")


def _split_symbols(raw: str) -> List[str]:
    s = (raw or "").replace("\n", " ").replace("\t", " ").strip()
    if not s:
        return []
    s = s.replace(",", " ")
    return [p.strip() for p in s.split(" ") if p.strip()]


def _safe_error_message(e: BaseException) -> str:
    msg = str(e).strip()
    return msg or e.__class__.__name__


async def _maybe_await(v: Any) -> Any:
    if inspect.isawaitable(v):
        return await v
    return v


def _safe_get(obj: Any, *names: str) -> Any:
    if obj is None:
        return None
    if isinstance(obj, dict):
        for n in names:
            if n in obj and obj[n] is not None:
                return obj[n]
        return None
    for n in names:
        try:
            v = getattr(obj, n, None)
            if v is not None:
                return v
        except Exception:
            pass
    return None


def _as_payload(obj: Any) -> Dict[str, Any]:
    """
    Convert engine return types into a JSON-safe dict.
    - dict: passthrough
    - pydantic: model_dump()/dict()
    - dataclass-ish: __dict__
    - fallback: {"value": "..."}
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


def _unwrap_tuple_payload(x: Any) -> Any:
    # support (payload, err)
    if isinstance(x, tuple) and len(x) == 2:
        return x[0]
    return x


# =============================================================================
# Recommendation normalization (ONE ENUM everywhere)
# =============================================================================
_RECO_ENUM = ("BUY", "HOLD", "REDUCE", "SELL")


def _normalize_recommendation(x: Any) -> str:
    """
    Always returns one of: BUY/HOLD/REDUCE/SELL
    """
    if x is None:
        return "HOLD"
    try:
        s = str(x).strip().upper()
    except Exception:
        return "HOLD"
    if not s:
        return "HOLD"
    if s in _RECO_ENUM:
        return s

    s2 = re.sub(r"[\s\-_/]+", " ", s).strip()

    buy_like = {
        "STRONG BUY",
        "BUY",
        "ACCUMULATE",
        "ADD",
        "OUTPERFORM",
        "OVERWEIGHT",
        "LONG",
    }
    hold_like = {
        "HOLD",
        "NEUTRAL",
        "MAINTAIN",
        "MARKET PERFORM",
        "EQUAL WEIGHT",
        "WAIT",
    }
    reduce_like = {
        "REDUCE",
        "TRIM",
        "LIGHTEN",
        "UNDERWEIGHT",
        "PARTIAL SELL",
        "TAKE PROFIT",
        "TAKE PROFITS",
    }
    sell_like = {
        "SELL",
        "STRONG SELL",
        "EXIT",
        "AVOID",
        "UNDERPERFORM",
        "SHORT",
    }

    if s2 in buy_like:
        return "BUY"
    if s2 in hold_like:
        return "HOLD"
    if s2 in reduce_like:
        return "REDUCE"
    if s2 in sell_like:
        return "SELL"

    # heuristic contains
    if "SELL" in s2:
        return "SELL"
    if "REDUCE" in s2 or "TRIM" in s2 or "UNDERWEIGHT" in s2:
        return "REDUCE"
    if "HOLD" in s2 or "NEUTRAL" in s2 or "MAINTAIN" in s2:
        return "HOLD"
    if "BUY" in s2 or "ACCUMULATE" in s2 or "OVERWEIGHT" in s2:
        return "BUY"

    return "HOLD"


# =============================================================================
# Symbol normalization (PROD SAFE)
# =============================================================================
def _normalize_symbol_safe(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    # Prefer engine's normalize if available (guarded)
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
    # indices/commodities etc
    if any(ch in s for ch in ("=", "^")):
        return s
    if "." in s:
        return s
    if s.isdigit():
        return f"{s}.SR"
    if s.isalpha():
        return f"{s}.US"
    return s


# =============================================================================
# Canonical schema fill (best-effort)
# =============================================================================
@lru_cache(maxsize=1)
def _try_import_schemas():
    try:
        from core import schemas as _schemas  # type: ignore

        return _schemas
    except Exception:
        return None


def _get_uq_keys() -> List[str]:
    """
    Cached list of UnifiedQuote fields, if available.
    Never raises.
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
    Ensure keys exist for downstream sheet writers.
    Does not overwrite existing values.
    """
    try:
        # Prefer UnifiedQuote fields if available
        keys = _get_uq_keys()
        if keys:
            for k in keys:
                payload.setdefault(k, None)
            return payload

        # Fallback: fill minimal known canonical fields using schemas mapping
        sch = _try_import_schemas()
        if sch is not None:
            try:
                # HEADER_TO_FIELD contains canonical field names; fill them
                for f in set(str(v) for v in getattr(sch, "HEADER_TO_FIELD", {}).values()):
                    if f:
                        payload.setdefault(f, None)
            except Exception:
                pass

        return payload
    except Exception:
        return payload


# =============================================================================
# Time helpers
# =============================================================================
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso_or_none(x: Any) -> Optional[str]:
    if x is None or x == "":
        return None
    try:
        if isinstance(x, datetime):
            dt = x
        else:
            dt = datetime.fromisoformat(str(x))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        try:
            return str(x)
        except Exception:
            return None


def _to_riyadh_iso(utc_any: Any) -> Optional[str]:
    if not utc_any:
        return None
    try:
        from zoneinfo import ZoneInfo  # py3.9+

        tz = ZoneInfo("Asia/Riyadh")
        if isinstance(utc_any, datetime):
            dt = utc_any
        else:
            dt = datetime.fromisoformat(str(utc_any))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(tz).isoformat()
    except Exception:
        return None


def _safe_float_or_none(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None


def _compute_52w_position_pct(cp: Any, low_52w: Any, high_52w: Any) -> Optional[float]:
    try:
        cp_f = float(cp)
        lo = float(low_52w)
        hi = float(high_52w)
        if hi == lo:
            return None
        return round(((cp_f - lo) / (hi - lo)) * 100.0, 2)
    except Exception:
        return None


def _ratio_to_percent(v: Any) -> Any:
    if v is None:
        return None
    try:
        f = float(v)
        return (f * 100.0) if -1.0 <= f <= 1.0 else f
    except Exception:
        return v


# =============================================================================
# EnrichedQuote helper (used by routes/advanced_analysis.py)
# =============================================================================
class EnrichedQuote:
    """
    Lightweight mapper that can convert a UnifiedQuote-like payload into sheet rows.

    - from_unified(payload): accepts dict or attribute object
    - to_row(headers): returns values aligned to provided headers
    """

    def __init__(self, payload: Dict[str, Any]):
        self.payload = payload or {}

    @classmethod
    def from_unified(cls, uq: Any) -> "EnrichedQuote":
        p = _as_payload(_unwrap_tuple_payload(uq))
        # Ensure recommendation enum
        try:
            p["recommendation"] = _normalize_recommendation(p.get("recommendation"))
        except Exception:
            pass
        # Ensure timestamps
        try:
            if not p.get("last_updated_utc") and p.get("as_of_utc"):
                p["last_updated_utc"] = p.get("as_of_utc")
        except Exception:
            pass
        return cls(p)

    def _value_for_header(self, header: str) -> Any:
        sch = _try_import_schemas()

        # Canonical+aliases candidates per header (best-effort)
        candidates: Tuple[str, ...] = ()
        if sch is not None:
            try:
                candidates = tuple(getattr(sch, "header_field_candidates")(header))  # type: ignore
            except Exception:
                candidates = ()
        if not candidates:
            # fallback guess
            candidates = (str(header or "").strip(),)

        hk = str(header or "").strip().lower()

        # Computed 52W position %
        if hk in ("52w position %", "52w position"):
            v = self.payload.get("position_52w_percent")
            if v is not None:
                return v
            cp = _safe_get(self.payload, "current_price", "last_price", "price")
            lo = _safe_get(self.payload, "week_52_low", "low_52w", "52w_low")
            hi = _safe_get(self.payload, "week_52_high", "high_52w", "52w_high")
            return _compute_52w_position_pct(cp, lo, hi)

        # Last Updated (Riyadh) fill from UTC
        if hk == "last updated (riyadh)":
            v = self.payload.get("last_updated_riyadh")
            if not v:
                u = self.payload.get("last_updated_utc") or self.payload.get("as_of_utc")
                v = _to_riyadh_iso(u)
                try:
                    self.payload["last_updated_riyadh"] = v
                except Exception:
                    pass
            return _iso_or_none(v) or v

        # Last Updated (UTC) normalization
        if hk == "last updated (utc)":
            v = self.payload.get("last_updated_utc") or self.payload.get("as_of_utc")
            return _iso_or_none(v) or v

        # Recommendation forced enum
        if hk == "recommendation":
            return _normalize_recommendation(self.payload.get("recommendation"))

        # Percent-style headers (best-effort)
        pct_headers = {
            "turnover %",
            "free float %",
            "dividend yield %",
            "payout ratio %",
            "roe %",
            "roa %",
            "net margin %",
            "ebitda margin %",
            "revenue growth %",
            "net income growth %",
            "volatility (30d)",
            "upside %",
            "percent change",
        }

        # Look up first available candidate
        val = None
        for f in candidates:
            if not f:
                continue
            if f in self.payload and self.payload.get(f) is not None:
                val = self.payload.get(f)
                break

        # Fallback alias resolution (common engine variants)
        if val is None:
            # Handle 52w high/low naming
            if hk == "52w high":
                val = _safe_get(self.payload, "week_52_high", "high_52w", "52w_high")
            elif hk == "52w low":
                val = _safe_get(self.payload, "week_52_low", "low_52w", "52w_low")

        # Apply percent normalization for ratio fields
        if hk in pct_headers:
            return _ratio_to_percent(val)

        return val

    def to_row(self, headers: Sequence[str]) -> List[Any]:
        hs = [str(h) for h in (headers or [])]
        row = [self._value_for_header(h) for h in hs]
        if len(row) < len(hs):
            row += [None] * (len(hs) - len(row))
        return row[: len(hs)]

    def to_payload(self) -> Dict[str, Any]:
        return dict(self.payload)


# =============================================================================
# Engine calls (best-effort)
# =============================================================================
async def _call_engine_best_effort(request: Request, symbol: str) -> Tuple[Optional[Any], Optional[str], Optional[str]]:
    """
    Returns: (result, source, error)
    """
    # 1) app.state.engine
    try:
        eng = getattr(request.app.state, "engine", None)
    except Exception:
        eng = None

    if eng is not None:
        last_err: Optional[str] = None
        for fn_name in ("get_enriched_quote", "get_quote"):
            fn = getattr(eng, fn_name, None)
            if callable(fn):
                try:
                    res = await _maybe_await(fn(symbol))
                    return _unwrap_tuple_payload(res), f"app.state.engine.{fn_name}", None
                except Exception as e:
                    last_err = _safe_error_message(e)
                    continue
        return None, "app.state.engine", last_err or "engine call failed"

    # 2) v2 module singleton function (if present)
    try:
        from core.data_engine_v2 import get_enriched_quote as v2_get  # type: ignore

        res2 = await _maybe_await(v2_get(symbol))
        return _unwrap_tuple_payload(res2), "core.data_engine_v2.get_enriched_quote(singleton)", None
    except Exception:
        pass

    # 3) v2 temp engine
    try:
        from core.data_engine_v2 import DataEngine as V2Engine  # type: ignore

        tmp = V2Engine()
        try:
            fn = getattr(tmp, "get_enriched_quote", None) or getattr(tmp, "get_quote", None)
            if callable(fn):
                res3 = await _maybe_await(fn(symbol))
                return _unwrap_tuple_payload(res3), "core.data_engine_v2.DataEngine(temp)", None
        finally:
            aclose = getattr(tmp, "aclose", None)
            if callable(aclose):
                try:
                    await _maybe_await(aclose())
                except Exception:
                    pass
            close = getattr(tmp, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass
    except Exception:
        pass

    # 4) legacy module-level singleton
    try:
        from core.data_engine import get_enriched_quote as v1_get  # type: ignore

        res4 = await _maybe_await(v1_get(symbol))
        return _unwrap_tuple_payload(res4), "core.data_engine.get_enriched_quote", None
    except Exception:
        pass

    return None, None, "no provider/engine available"


async def _call_engine_batch_best_effort(
    request: Request, symbols_norm: List[str]
) -> Tuple[Optional[Union[List[Any], Dict[str, Any]]], Optional[str], Optional[str]]:
    """
    Returns: (batch_result, source, error)
    batch_result may be:
      - list[payload]
      - dict[symbol->payload]
      - (payload, err) already unwrapped by _unwrap_tuple_payload at higher level
    """
    if not symbols_norm:
        return None, None, "empty"

    # 1) app.state.engine batch
    try:
        eng = getattr(request.app.state, "engine", None)
    except Exception:
        eng = None

    if eng is not None:
        last_err: Optional[str] = None
        for fn_name in ("get_enriched_quotes", "get_quotes"):
            fn = getattr(eng, fn_name, None)
            if callable(fn):
                try:
                    res = await _maybe_await(fn(symbols_norm))
                    res = _unwrap_tuple_payload(res)
                    if isinstance(res, (list, dict)):
                        return res, f"app.state.engine.{fn_name}", None
                    last_err = "batch returned non-list/non-dict"
                except Exception as e:
                    last_err = _safe_error_message(e)
        return None, "app.state.engine(batch)", last_err or "batch call failed"

    # 2) v2 singleton batch
    try:
        from core.data_engine_v2 import get_enriched_quotes as v2_batch  # type: ignore

        res2 = await _maybe_await(v2_batch(symbols_norm))
        res2 = _unwrap_tuple_payload(res2)
        if isinstance(res2, (list, dict)):
            return res2, "core.data_engine_v2.get_enriched_quotes(singleton)", None
        return None, "core.data_engine_v2.get_enriched_quotes(singleton)", "batch returned non-list/non-dict"
    except Exception:
        pass

    return None, None, None


# =============================================================================
# Finalization rules for API payloads
# =============================================================================
def _finalize_payload(payload: Dict[str, Any], *, raw: str, norm: str, source: str) -> Dict[str, Any]:
    """
    Ensure standard metadata + status/error behavior is consistent.
    """
    sym = (norm or raw or "").strip().upper()
    if not sym:
        sym = ""

    try:
        payload.setdefault("symbol", sym)
        payload["symbol_input"] = payload.get("symbol_input") or raw
        payload["symbol_normalized"] = payload.get("symbol_normalized") or sym

        # recommendation enum
        payload["recommendation"] = _normalize_recommendation(payload.get("recommendation"))

        # normalize error field to string
        if payload.get("error") is None:
            payload["error"] = ""
        if not isinstance(payload.get("error"), str):
            payload["error"] = str(payload.get("error") or "")

        # set status consistently
        if str(payload.get("error") or "").strip():
            payload["status"] = "error"
        else:
            if not str(payload.get("status") or "").strip():
                payload["status"] = "success"

        # data_source/data_quality defaults
        if not payload.get("data_source"):
            payload["data_source"] = source or "unknown"

        if not payload.get("data_quality"):
            payload["data_quality"] = "MISSING" if payload.get("current_price") is None else "PARTIAL"

        # timestamps
        if not payload.get("last_updated_utc"):
            payload["last_updated_utc"] = payload.get("as_of_utc") or _now_utc().isoformat()
        if not payload.get("last_updated_riyadh"):
            payload["last_updated_riyadh"] = _to_riyadh_iso(payload.get("last_updated_utc")) or ""

        return _schema_fill_best_effort(payload)
    except Exception:
        # absolute last-resort safety
        return _schema_fill_best_effort(
            {
                "status": "error",
                "symbol": sym,
                "symbol_input": raw,
                "symbol_normalized": sym,
                "recommendation": "HOLD",
                "data_quality": "MISSING",
                "data_source": source or "unknown",
                "error": "payload_finalize_failed",
            }
        )


def _payload_symbol_key(p: Dict[str, Any]) -> str:
    """
    Extract a robust symbol key for alignment.
    """
    try:
        for k in ("symbol_normalized", "symbol", "Symbol", "ticker", "code"):
            v = p.get(k)
            if v is not None:
                s = str(v).strip().upper()
                if s:
                    return s
    except Exception:
        pass
    return ""


# =============================================================================
# Routes
# =============================================================================
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
        out = _schema_fill_best_effort(
            {
                "status": "error",
                "symbol": "",
                "symbol_input": "",
                "symbol_normalized": "",
                "recommendation": "HOLD",
                "data_quality": "MISSING",
                "data_source": "none",
                "error": "Empty symbol",
                "last_updated_utc": _now_utc().isoformat(),
                "last_updated_riyadh": "",
            }
        )
        return JSONResponse(status_code=200, content=out)

    try:
        result, source, err = await _call_engine_best_effort(request, norm or raw)
        if result is None:
            out = _schema_fill_best_effort(
                {
                    "status": "error",
                    "symbol": norm or raw,
                    "symbol_input": raw,
                    "symbol_normalized": norm or raw,
                    "recommendation": "HOLD",
                    "data_quality": "MISSING",
                    "data_source": "none",
                    "error": err or "Enriched quote engine not available (no working provider).",
                    "last_updated_utc": _now_utc().isoformat(),
                    "last_updated_riyadh": "",
                }
            )
            return JSONResponse(status_code=200, content=out)

        payload = _as_payload(result)
        payload = _finalize_payload(payload, raw=raw, norm=norm, source=source or "unknown")
        return JSONResponse(status_code=200, content=payload)

    except Exception as e:
        out: Dict[str, Any] = {
            "status": "error",
            "symbol": norm or raw,
            "symbol_input": raw,
            "symbol_normalized": norm or raw,
            "recommendation": "HOLD",
            "data_quality": "MISSING",
            "data_source": "none",
            "error": _safe_error_message(e),
            "last_updated_utc": _now_utc().isoformat(),
            "last_updated_riyadh": "",
        }
        if dbg:
            out["traceback"] = _clamp(traceback.format_exc(), 8000)
        return JSONResponse(status_code=200, content=_schema_fill_best_effort(out))


@router.get("/quotes")
async def enriched_quotes(
    request: Request,
    symbols: str = Query(..., description="Comma/space-separated symbols, e.g. AAPL,MSFT,1120.SR"),
    debug: int = Query(0, description="Set 1 to include traceback (or enable DEBUG_ERRORS=1)"),
):
    dbg = _truthy(os.getenv("DEBUG_ERRORS", "0")) or bool(debug)

    raw_list = _split_symbols(symbols)
    if not raw_list:
        return JSONResponse(status_code=200, content={"status": "error", "error": "Empty symbols list", "items": []})

    norms = [_normalize_symbol_safe(r) or (r or "").strip() for r in raw_list]
    norms = [n for n in norms if n]

    batch_res, batch_source, batch_err = await _call_engine_batch_best_effort(request, norms)

    items: List[Dict[str, Any]] = []

    # ------------------------------------------------------------
    # FAST PATH: batch returned list or dict
    # ------------------------------------------------------------
    if isinstance(batch_res, (list, dict)) and batch_res:
        # Case dict: map by key
        if isinstance(batch_res, dict):
            mp: Dict[str, Dict[str, Any]] = {}
            for k, v in batch_res.items():
                kk = str(k or "").strip().upper()
                pv = _as_payload(_unwrap_tuple_payload(v))
                if kk and kk not in mp:
                    mp[kk] = pv

            for raw in raw_list:
                norm = (_normalize_symbol_safe(raw) or raw).strip().upper()
                p = mp.get(norm)
                if p is None:
                    out = {
                        "status": "error",
                        "symbol": norm,
                        "symbol_input": raw,
                        "symbol_normalized": norm,
                        "recommendation": "HOLD",
                        "data_quality": "MISSING",
                        "data_source": "none",
                        "error": "Engine returned no item for this symbol.",
                        "last_updated_utc": _now_utc().isoformat(),
                        "last_updated_riyadh": "",
                    }
                    if dbg and batch_err:
                        out["batch_error_hint"] = _clamp(batch_err, 1200)
                    items.append(_schema_fill_best_effort(out))
                else:
                    items.append(_finalize_payload(p, raw=raw, norm=norm, source=batch_source or "unknown"))

            return JSONResponse(status_code=200, content={"status": "success", "count": len(items), "items": items})

        # Case list
        payloads: List[Dict[str, Any]] = [_as_payload(_unwrap_tuple_payload(x)) for x in list(batch_res)]

        # Case A: length matches input -> index align
        if len(payloads) == len(raw_list):
            for i, raw in enumerate(raw_list):
                norm = _normalize_symbol_safe(raw) or raw
                p = payloads[i] if i < len(payloads) else {}
                items.append(_finalize_payload(p, raw=raw, norm=norm, source=batch_source or "unknown"))
            return JSONResponse(status_code=200, content={"status": "success", "count": len(items), "items": items})

        # Case B: mismatch -> map by symbol key
        mp2: Dict[str, Dict[str, Any]] = {}
        for p in payloads:
            k = _payload_symbol_key(p)
            if k and k not in mp2:
                mp2[k] = p

        for raw in raw_list:
            norm = (_normalize_symbol_safe(raw) or raw).strip().upper()
            p = mp2.get(norm)
            if p is None:
                out = {
                    "status": "error",
                    "symbol": norm,
                    "symbol_input": raw,
                    "symbol_normalized": norm,
                    "recommendation": "HOLD",
                    "data_quality": "MISSING",
                    "data_source": "none",
                    "error": "Engine returned no item for this symbol.",
                    "last_updated_utc": _now_utc().isoformat(),
                    "last_updated_riyadh": "",
                }
                if dbg and batch_err:
                    out["batch_error_hint"] = _clamp(batch_err, 1200)
                items.append(_schema_fill_best_effort(out))
            else:
                items.append(_finalize_payload(p, raw=raw, norm=norm, source=batch_source or "unknown"))

        return JSONResponse(status_code=200, content={"status": "success", "count": len(items), "items": items})

    # ------------------------------------------------------------
    # SLOW PATH: per-symbol
    # ------------------------------------------------------------
    for raw in raw_list:
        norm = _normalize_symbol_safe(raw)
        try:
            result, source, err = await _call_engine_best_effort(request, norm or raw)
            if result is None:
                out = {
                    "status": "error",
                    "symbol": norm or raw,
                    "symbol_input": raw,
                    "symbol_normalized": norm or raw,
                    "recommendation": "HOLD",
                    "data_quality": "MISSING",
                    "data_source": "none",
                    "error": err or "Engine not available for this symbol.",
                    "last_updated_utc": _now_utc().isoformat(),
                    "last_updated_riyadh": "",
                }
                if dbg and batch_err:
                    out["batch_error_hint"] = _clamp(batch_err, 1200)
                items.append(_schema_fill_best_effort(out))
                continue

            payload = _as_payload(result)
            items.append(_finalize_payload(payload, raw=raw, norm=norm, source=source or "unknown"))

        except Exception as e:
            out2: Dict[str, Any] = {
                "status": "error",
                "symbol": norm or raw,
                "symbol_input": raw,
                "symbol_normalized": norm or raw,
                "recommendation": "HOLD",
                "data_quality": "MISSING",
                "data_source": "none",
                "error": _safe_error_message(e),
                "last_updated_utc": _now_utc().isoformat(),
                "last_updated_riyadh": "",
            }
            if dbg:
                out2["traceback"] = _clamp(traceback.format_exc(), 8000)
                if batch_err:
                    out2["batch_error_hint"] = _clamp(batch_err, 1200)
            items.append(_schema_fill_best_effort(out2))

    return JSONResponse(status_code=200, content={"status": "success", "count": len(items), "items": items})


@router.get("/health", include_in_schema=False)
async def enriched_health():
    return {"status": "ok", "module": "core.enriched_quote", "version": ROUTER_VERSION}


def get_router() -> APIRouter:
    return router


__all__ = ["router", "get_router", "EnrichedQuote"]
