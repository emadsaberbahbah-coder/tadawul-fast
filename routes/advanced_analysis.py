# routes/advanced_analysis.py
"""
TADAWUL FAST BRIDGE – ADVANCED ANALYSIS ROUTES (v3.15.1) – PROD SAFE (ALIGNED + PUSH-CACHE FIX)

✅ FIX in v3.15.1
- POST /v1/advanced/sheet-rows now supports TWO MODES:
  1) PUSH MODE (Google Sheets -> Backend cache):
     Body contains: {"items":[{"sheet":"Market_Leaders","headers":[...],"rows":[...]}], ...}
     -> Writes payload into engine cache using best-effort engine method probing.
  2) COMPUTE MODE (Backend -> Sheets rows):
     Body contains: {"tickers":[...], "sheet_name":"Market_Leaders"} (existing behavior)

Goal
- Ensure Advisor reads the SAME cache keys that PUSH MODE writes (engine handles keying).
- This module stays PROD SAFE: no hard dependency on core.data_engine_v2 at import-time.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import re
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from fastapi import APIRouter, Body, Header, Query, Request

# pydantic v2 preferred, v1 fallback
try:
    from pydantic import BaseModel, ConfigDict, Field  # type: ignore

    _PYDANTIC_V2 = True
except Exception:  # pragma: no cover
    from pydantic import BaseModel, Field  # type: ignore

    ConfigDict = None  # type: ignore
    _PYDANTIC_V2 = False

logger = logging.getLogger("routes.advanced_analysis")

ADVANCED_ANALYSIS_VERSION = "3.15.1"
router = APIRouter(prefix="/v1/advanced", tags=["Advanced Analysis"])


# =============================================================================
# Settings shim (safe)
# =============================================================================
try:
    from core.config import get_settings  # type: ignore
except Exception:  # pragma: no cover

    def get_settings():  # type: ignore
        return None


# =============================================================================
# Optional schema helpers (headers + tolerant mapping)
# =============================================================================
try:
    from core.schemas import DEFAULT_HEADERS_59 as _SCHEMAS_DEFAULT_59  # type: ignore
    from core.schemas import get_headers_for_sheet as _get_headers_for_sheet  # type: ignore
    from core.schemas import header_field_candidates as _header_field_candidates  # type: ignore
    from core.schemas import canonical_field as _canonical_field  # type: ignore

    _SCHEMAS_OK = True
except Exception:  # pragma: no cover
    _SCHEMAS_DEFAULT_59 = None  # type: ignore
    _get_headers_for_sheet = None  # type: ignore
    _header_field_candidates = None  # type: ignore
    _canonical_field = None  # type: ignore
    _SCHEMAS_OK = False


# =============================================================================
# PROD SAFE normalizer (lazy prefer v2 normalize_symbol; fallback always)
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
    try:
        from core.data_engine_v2 import normalize_symbol as _NS  # type: ignore

        return _NS
    except Exception:
        return _fallback_normalize


def _normalize_any(raw: str) -> str:
    try:
        ns = _try_import_v2_normalizer()
        s = (ns(raw) or "").strip().upper()
        return s
    except Exception:
        return _fallback_normalize(raw)


# =============================================================================
# Optional EnrichedQuote (lazy)
# =============================================================================
@lru_cache(maxsize=1)
def _try_import_enriched_quote() -> Optional[Any]:
    try:
        from core.enriched_quote import EnrichedQuote  # type: ignore

        return EnrichedQuote
    except Exception:
        return None


# =============================================================================
# Optional scoring enrich (lazy)
# =============================================================================
@lru_cache(maxsize=1)
def _try_import_scoring_enricher() -> Optional[Any]:
    try:
        from core.scoring_engine import enrich_with_scores  # type: ignore

        return enrich_with_scores
    except Exception:
        return None


def _enrich_scores_best_effort(uq: Any) -> Any:
    fn = _try_import_scoring_enricher()
    if callable(fn):
        try:
            return fn(uq)
        except Exception:
            return uq
    return uq


# =============================================================================
# Await helper
# =============================================================================
async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


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


def _safe_set(obj: Any, name: str, value: Any) -> None:
    try:
        if isinstance(obj, dict):
            obj[name] = value
        else:
            setattr(obj, name, value)
    except Exception:
        pass


def _ensure_reco_on_obj(uq: Any) -> None:
    try:
        reco_raw = _safe_get(uq, "recommendation")
        reco = _coerce_reco_enum(reco_raw)
        _safe_set(uq, "recommendation", reco)
    except Exception:
        pass


# =============================================================================
# Auth (X-APP-TOKEN)
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

    try:
        s = get_settings()
        for attr in ("app_token", "backup_app_token"):
            v = _read_token_attr(s, attr)
            if v:
                tokens.append(v)
    except Exception:
        pass

    try:
        from env import settings as env_settings  # type: ignore

        for attr in ("app_token", "backup_app_token"):
            v = _read_token_attr(env_settings, attr)
            if v:
                tokens.append(v)
    except Exception:
        pass

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

    if not out:
        logger.warning("[advanced] No APP_TOKEN configured -> endpoints are OPEN (no auth).")
    return out


def _auth_ok(x_app_token: Optional[str]) -> bool:
    allowed = _allowed_tokens()
    if not allowed:
        return True
    return bool(x_app_token and x_app_token.strip() in allowed)


# =============================================================================
# Engine resolution (prefer app.state.engine; else singleton)
# =============================================================================
_ENGINE: Optional[Any] = None
_ENGINE_LOCK = asyncio.Lock()


def _engine_capable(obj: Any) -> bool:
    if obj is None:
        return False
    for fn in ("get_enriched_quote", "get_quote", "get_enriched_quotes", "get_quotes"):
        if callable(getattr(obj, fn, None)):
            return True
    # also allow "cache" engine (for push)
    if callable(getattr(obj, "cache_write_sheet", None)) or callable(getattr(obj, "write_sheet_cache", None)):
        return True
    if callable(getattr(obj, "set_cached_sheet_snapshot", None)) or callable(getattr(obj, "set_sheet_snapshot", None)):
        return True
    return False


def _get_app_engine(request: Optional[Request]) -> Optional[Any]:
    try:
        if request is None:
            return None
        st = getattr(getattr(request, "app", None), "state", None)
        if not st:
            return None
        for attr in ("engine", "data_engine", "data_engine_v2"):
            eng = getattr(st, attr, None)
            if _engine_capable(eng):
                return eng
        return None
    except Exception:
        return None


@lru_cache(maxsize=1)
def _try_import_get_engine() -> Optional[Any]:
    try:
        from core.data_engine_v2 import get_engine  # type: ignore

        return get_engine
    except Exception:
        return None


async def _get_singleton_engine() -> Optional[Any]:
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE

    async with _ENGINE_LOCK:
        if _ENGINE is not None:
            return _ENGINE

        try:
            ge = _try_import_get_engine()
            if callable(ge):
                _ENGINE = await _maybe_await(ge())
                if _ENGINE is not None:
                    logger.info("[advanced] Engine resolved via core.data_engine_v2.get_engine()")
                    return _ENGINE
        except Exception as exc:
            logger.warning("[advanced] get_engine() failed: %s", exc)

        try:
            from core.data_engine_v2 import DataEngine as _DE  # type: ignore

            _ENGINE = _DE()
            logger.info("[advanced] DataEngine initialized (fallback singleton).")
            return _ENGINE
        except Exception:
            pass

        try:
            from core.data_engine_v2 import DataEngineV2 as _DE2  # type: ignore

            _ENGINE = _DE2()
            logger.info("[advanced] DataEngineV2 initialized (last resort).")
            return _ENGINE
        except Exception as exc:
            logger.exception("[advanced] Failed to init engine singleton: %s", exc)
            _ENGINE = None
            return None


async def _resolve_engine(request: Optional[Request]) -> Optional[Any]:
    eng = _get_app_engine(request)
    if eng is not None:
        return eng
    return await _get_singleton_engine()


# =============================================================================
# Config helpers
# =============================================================================
def _safe_int(x: Any, default: int) -> int:
    try:
        v = int(float(str(x).strip()))
        return v if v > 0 else default
    except Exception:
        return default


def _safe_float(x: Any, default: float) -> float:
    try:
        v = float(str(x).strip())
        return v if v > 0 else default
    except Exception:
        return default


def _cfg() -> Dict[str, Any]:
    s = None
    try:
        s = get_settings()
    except Exception:
        s = None

    batch_size = _safe_int(getattr(s, "adv_batch_size", None), 25)
    timeout_sec = _safe_float(getattr(s, "adv_batch_timeout_sec", None), 45.0)
    max_tickers = _safe_int(getattr(s, "adv_max_tickers", None), 500)
    concurrency = _safe_int(getattr(s, "adv_batch_concurrency", None), 6)

    batch_size = _safe_int(os.getenv("ADV_BATCH_SIZE", batch_size), batch_size)
    timeout_sec = _safe_float(os.getenv("ADV_BATCH_TIMEOUT_SEC", timeout_sec), timeout_sec)
    max_tickers = _safe_int(os.getenv("ADV_MAX_TICKERS", max_tickers), max_tickers)
    concurrency = _safe_int(os.getenv("ADV_BATCH_CONCURRENCY", concurrency), concurrency)

    batch_size = max(5, min(200, batch_size))
    timeout_sec = max(5.0, min(180.0, timeout_sec))
    max_tickers = max(10, min(2000, max_tickers))
    concurrency = max(1, min(25, concurrency))

    return {
        "batch_size": batch_size,
        "timeout_sec": timeout_sec,
        "max_tickers": max_tickers,
        "concurrency": concurrency,
    }


# =============================================================================
# Utilities
# =============================================================================
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _now_utc_iso() -> str:
    return _now_utc().isoformat()


def _clean_tickers(items: Sequence[Any]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in (items or []):
        if x is None:
            continue
        s = _normalize_any(str(x).strip())
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _parse_tickers_any(s: str) -> List[str]:
    if not s:
        return []
    parts = re.split(r"[\s,;]+", (s or "").strip())
    return _clean_tickers([p for p in parts if p and p.strip()])


def _chunk(items: List[str], size: int) -> List[List[str]]:
    if not items:
        return []
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]


# =============================================================================
# PUSH MODE: cache write (best-effort method probing)
# =============================================================================
def _to_sheet_key(sheet_name: str) -> str:
    """
    Canonicalize sheet identifier so both writers/readers match.
    This normalizes spaces -> underscores but does NOT force upper-case,
    because some engines key by exact sheet name. We always try both.
    """
    s = (sheet_name or "").strip()
    s = re.sub(r"\s+", "_", s)
    return s


async def _engine_cache_write_sheet(
    engine: Any,
    *,
    sheet: str,
    headers: List[str],
    rows: List[List[Any]],
    meta: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, str]:
    """
    Try multiple possible engine methods without importing data_engine_v2 directly.
    Returns (ok, method_used_or_error).

    IMPORTANT:
    - We write using BOTH "sheet name" and "sheet key" variants when possible,
      to maximize hit rate for downstream readers.
    """
    if engine is None:
        return False, "engine_none"

    raw_sheet = (sheet or "").strip()
    if not raw_sheet:
        return False, "sheet_empty"

    sheet_key = _to_sheet_key(raw_sheet)
    sheet_key_upper = sheet_key.upper()

    # Prefer explicit snapshot setter names first (your DataEngine v2.14 uses snapshot wording)
    candidates = [
        "set_cached_sheet_snapshot",
        "set_sheet_snapshot",
        "set_cached_snapshot",
        "cache_set_sheet_snapshot",
        # then older/alternate names
        "cache_write_sheet",
        "write_sheet_cache",
        "set_sheet_cache",
        "put_sheet_cache",
        "save_sheet_cache",
        "cache_set_sheet",
        "cache_put_sheet",
        "set_cached_sheet",
        "put_cached_sheet",
        # generic
        "cache_write",
        "cache_set",
        "cache_put",
    ]

    base_payload = {
        "headers": headers or [],
        "rows": rows or [],
        "meta": meta or {},
        "updated_at_utc": _now_utc_iso(),
    }

    # We'll try writing under several ids to match any reader convention.
    # Order matters: try raw tab name first, then normalized, then UPPER key.
    sheet_variants = []
    if raw_sheet:
        sheet_variants.append(raw_sheet)
    if sheet_key and sheet_key not in sheet_variants:
        sheet_variants.append(sheet_key)
    if sheet_key_upper and sheet_key_upper not in sheet_variants:
        sheet_variants.append(sheet_key_upper)

    last_err = "no_cache_writer_found"

    # 1) method probing on engine itself
    for name in candidates:
        fn = getattr(engine, name, None)
        if not callable(fn):
            continue

        for sv in sheet_variants:
            payload = dict(base_payload)
            payload["sheet"] = sv

            try:
                # common signatures:
                #  - fn(sheet, headers, rows, meta)
                #  - fn(payload_dict)
                try:
                    r = fn(sv, headers, rows, meta or {})
                except TypeError:
                    r = fn(payload)
                r2 = await _maybe_await(r)
                if r2 is False:
                    continue
                return True, f"engine.{name} ({sv})"
            except Exception as exc:
                last_err = f"engine.{name}({sv}) failed: {exc}"
                logger.warning("[advanced][push] %s", last_err)

    # 2) if engine has a .cache object, probe that too
    cache_obj = getattr(engine, "cache", None)
    if cache_obj is not None:
        for name in candidates:
            fn = getattr(cache_obj, name, None)
            if not callable(fn):
                continue

            for sv in sheet_variants:
                payload = dict(base_payload)
                payload["sheet"] = sv

                try:
                    try:
                        r = fn(sv, headers, rows, meta or {})
                    except TypeError:
                        r = fn(payload)
                    r2 = await _maybe_await(r)
                    if r2 is False:
                        continue
                    return True, f"engine.cache.{name} ({sv})"
                except Exception as exc:
                    last_err = f"engine.cache.{name}({sv}) failed: {exc}"
                    logger.warning("[advanced][push] %s", last_err)

    return False, last_err


# =============================================================================
# Response Models
# =============================================================================
class _ExtraIgnore(BaseModel):
    if _PYDANTIC_V2 and ConfigDict is not None:  # type: ignore
        model_config = ConfigDict(extra="ignore")  # type: ignore
    else:  # pragma: no cover
        class Config:
            extra = "ignore"


class PushSheetItem(_ExtraIgnore):
    sheet: str
    headers: List[str] = Field(default_factory=list)
    rows: List[List[Any]] = Field(default_factory=list)


class PushSheetRowsRequest(_ExtraIgnore):
    items: List[PushSheetItem] = Field(default_factory=list)
    universe_rows: Optional[int] = None
    source: Optional[str] = None


class PushSheetRowsResponse(_ExtraIgnore):
    status: str = "success"
    error: Optional[str] = None
    version: str
    written: List[Dict[str, Any]] = Field(default_factory=list)


class AdvancedSheetRequest(_ExtraIgnore):
    tickers: List[str] = Field(default_factory=list)
    symbols: List[str] = Field(default_factory=list)
    top_n: Optional[int] = Field(default=50, ge=1, le=500)
    sheet_name: Optional[str] = None
    sheetName: Optional[str] = None


class AdvancedSheetResponse(_ExtraIgnore):
    status: str = "success"
    error: Optional[str] = None
    headers: List[str] = Field(default_factory=list)
    rows: List[List[Any]] = Field(default_factory=list)


# =============================================================================
# Minimal placeholders + mapping (keep your existing logic for compute mode)
# =============================================================================
def _make_placeholder(symbol: str, *, dq: str = "MISSING", err: str = "No data") -> Dict[str, Any]:
    sym = (symbol or "").strip().upper() or "UNKNOWN"
    return {
        "symbol": sym,
        "name": None,
        "market": None,
        "sector": None,
        "sub_sector": None,
        "currency": None,
        "current_price": None,
        "previous_close": None,
        "price_change": None,
        "percent_change": None,
        "volume": None,
        "recommendation": "HOLD",
        "data_source": "none",
        "data_quality": dq,
        "error": err,
        "status": "error",
        "last_updated_utc": _now_utc_iso(),
    }


def _looks_like_advanced_sheet(sheet_name: Optional[str]) -> bool:
    nm = (sheet_name or "").strip().lower()
    if not nm:
        return False
    return any(k in nm for k in ("advanced", "opportunity", "advisor", "best", "scoreboard"))


def _headers_look_valid(h: Any) -> bool:
    if not isinstance(h, list) or not h:
        return False
    try:
        return any(str(x).strip().lower() == "symbol" for x in h)
    except Exception:
        return False


def _fallback_headers_59() -> List[str]:
    return [
        "Symbol",
        "Company Name",
        "Sector",
        "Sub-Sector",
        "Market",
        "Currency",
        "Listing Date",
        "Last Price",
        "Previous Close",
        "Price Change",
        "Percent Change",
        "Day High",
        "Day Low",
        "52W High",
        "52W Low",
        "52W Position %",
        "Volume",
        "Avg Volume (30D)",
        "Value Traded",
        "Turnover %",
        "Shares Outstanding",
        "Free Float %",
        "Market Cap",
        "Free Float Market Cap",
        "Liquidity Score",
        "EPS (TTM)",
        "Forward EPS",
        "P/E (TTM)",
        "Forward P/E",
        "P/B",
        "P/S",
        "EV/EBITDA",
        "Dividend Yield %",
        "Dividend Rate",
        "Payout Ratio %",
        "ROE %",
        "ROA %",
        "Net Margin %",
        "EBITDA Margin %",
        "Revenue Growth %",
        "Net Income Growth %",
        "Beta",
        "Volatility (30D)",
        "RSI (14)",
        "Fair Value",
        "Upside %",
        "Valuation Label",
        "Value Score",
        "Quality Score",
        "Momentum Score",
        "Opportunity Score",
        "Risk Score",
        "Overall Score",
        "Error",
        "Recommendation",
        "Data Source",
        "Data Quality",
        "Last Updated (UTC)",
        "Last Updated (Riyadh)",
    ]


def _select_headers(sheet_name: Optional[str]) -> Tuple[List[str], str]:
    want_adv = _looks_like_advanced_sheet(sheet_name)

    if sheet_name and callable(_get_headers_for_sheet):
        try:
            h = _get_headers_for_sheet(sheet_name)  # type: ignore
            if _headers_look_valid(h):
                return [str(x) for x in h], ("advanced" if want_adv else "quote_schema")
        except Exception:
            pass

    if want_adv:
        return ["Rank", "Origin"] + _fallback_headers_59(), "advanced"

    if _SCHEMAS_DEFAULT_59 is not None:
        try:
            return [str(x) for x in list(_SCHEMAS_DEFAULT_59)], "quote_schema"  # type: ignore
        except Exception:
            pass

    return _fallback_headers_59(), "quote_schema"


# =============================================================================
# Engine calls (compat shim + chunking)
# =============================================================================
def _unwrap_tuple_payload(x: Any) -> Any:
    if isinstance(x, tuple) and len(x) == 2:
        return x[0]
    return x


def _call_batch_best_effort(engine: Any, fn_name: str, syms: List[str]) -> Any:
    fn = getattr(engine, fn_name, None)
    if not callable(fn):
        return None
    try:
        return fn(syms, refresh=False)
    except TypeError:
        pass
    return fn(syms)


async def _engine_get_quotes(engine: Any, syms: List[str]) -> Union[List[Any], Dict[str, Any]]:
    res = None
    if callable(getattr(engine, "get_enriched_quotes", None)):
        res = await _maybe_await(_call_batch_best_effort(engine, "get_enriched_quotes", syms))
        res = _unwrap_tuple_payload(res)
    elif callable(getattr(engine, "get_quotes", None)):
        res = await _maybe_await(_call_batch_best_effort(engine, "get_quotes", syms))
        res = _unwrap_tuple_payload(res)

    if res is not None:
        if isinstance(res, dict) and isinstance(res.get("items"), list):
            return list(res["items"])
        return res

    out: List[Any] = []
    for s in syms:
        fn3 = getattr(engine, "get_enriched_quote", None)
        if callable(fn3):
            out.append(_unwrap_tuple_payload(await _maybe_await(fn3(s))))
            continue
        fn4 = getattr(engine, "get_quote", None)
        if callable(fn4):
            out.append(_unwrap_tuple_payload(await _maybe_await(fn4(s))))
            continue
        out.append(_make_placeholder(s, dq="MISSING", err="Engine missing quote methods"))
    return out


async def _get_quotes_chunked(
    engine: Optional[Any],
    symbols: List[str],
    *,
    batch_size: int,
    timeout_sec: float,
    max_concurrency: int,
) -> Dict[str, Any]:
    clean = _clean_tickers(symbols)
    if not clean:
        return {}

    if engine is None:
        return {s: _make_placeholder(s, dq="MISSING", err="Engine unavailable") for s in clean}

    chunks = _chunk(clean, batch_size)
    sem = asyncio.Semaphore(max(1, max_concurrency))

    async def _run_chunk(chunk_syms: List[str]) -> Tuple[List[str], Union[Union[List[Any], Dict[str, Any]], Exception]]:
        async with sem:
            try:
                res = await asyncio.wait_for(_engine_get_quotes(engine, chunk_syms), timeout=timeout_sec)
                return chunk_syms, res
            except Exception as e:
                return chunk_syms, e

    results = await asyncio.gather(*[_run_chunk(c) for c in chunks])

    out: Dict[str, Any] = {}
    for chunk_syms, res in results:
        if isinstance(res, Exception):
            msg = "Engine batch timeout" if isinstance(res, asyncio.TimeoutError) else f"Engine batch error: {res}"
            for s in chunk_syms:
                out[s.upper()] = _make_placeholder(s, dq="MISSING", err=msg)
            continue

        chunk_map: Dict[str, Any] = {}
        if isinstance(res, dict):
            for k, v in res.items():
                q2 = _unwrap_tuple_payload(v)
                _ensure_reco_on_obj(q2)
                kk = (str(k or "").strip().upper()) if k is not None else ""
                if kk:
                    chunk_map.setdefault(kk, q2)
        else:
            for q in list(res or []):
                q2 = _unwrap_tuple_payload(q)
                _ensure_reco_on_obj(q2)
                sym = (_safe_get(q2, "symbol") or "").strip().upper()
                if sym:
                    chunk_map.setdefault(sym, q2)

        for s in chunk_syms:
            k = s.upper()
            out[k] = chunk_map.get(k) or _make_placeholder(k, dq="MISSING", err="No data returned")

    return out


# =============================================================================
# Routes
# =============================================================================
@router.get("/health")
@router.get("/ping")
async def advanced_health(request: Request) -> Dict[str, Any]:
    cfg = _cfg()
    eng = await _resolve_engine(request)

    return {
        "status": "ok",
        "module": "routes.advanced_analysis",
        "version": ADVANCED_ANALYSIS_VERSION,
        "engine": type(eng).__name__ if eng is not None else "none",
        "engine_mode": "v2",
        "limits": {
            "batch_size": cfg["batch_size"],
            "batch_timeout_sec": cfg["timeout_sec"],
            "batch_concurrency": cfg["concurrency"],
            "max_tickers": cfg["max_tickers"],
        },
        "auth": "open" if not _allowed_tokens() else "token",
        "schemas": "available" if _SCHEMAS_OK else "fallback",
        "timestamp_utc": _now_utc_iso(),
        "push_cache": "enabled",
    }


@router.get("/scoreboard")
async def advanced_scoreboard(
    request: Request,
    tickers: str = Query(...),
    top_n: int = Query(50, ge=1, le=500),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> Dict[str, Any]:
    if not _auth_ok(x_app_token):
        return {"status": "error", "error": "Unauthorized", "items": [], "version": ADVANCED_ANALYSIS_VERSION}

    requested = _parse_tickers_any(tickers)
    if not requested:
        return {"status": "success", "items": [], "version": ADVANCED_ANALYSIS_VERSION}

    cfg = _cfg()
    if len(requested) > cfg["max_tickers"]:
        requested = requested[: cfg["max_tickers"]]

    engine = await _resolve_engine(request)
    unified_map = await _get_quotes_chunked(
        engine,
        requested,
        batch_size=cfg["batch_size"],
        timeout_sec=cfg["timeout_sec"],
        max_concurrency=cfg["concurrency"],
    )

    items = []
    for s in requested[:top_n]:
        uq = unified_map.get(s.upper()) or _make_placeholder(s)
        items.append(
            {
                "symbol": (_safe_get(uq, "symbol") or s).strip().upper(),
                "name": _safe_get(uq, "name", "company_name"),
                "price": _safe_get(uq, "current_price", "last_price", "price"),
                "recommendation": _coerce_reco_enum(_safe_get(uq, "recommendation")),
                "data_quality": _safe_get(uq, "data_quality"),
                "error": _safe_get(uq, "error"),
            }
        )

    return {"status": "success", "version": ADVANCED_ANALYSIS_VERSION, "items": items}


@router.post("/sheet-rows")
async def advanced_sheet_rows(
    request: Request,
    body: Dict[str, Any] = Body(...),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> Union[AdvancedSheetResponse, PushSheetRowsResponse, Dict[str, Any]]:
    """
    Dual-mode endpoint:

    PUSH MODE:
      {"items":[{"sheet":"Market_Leaders","headers":[...],"rows":[...]}], "universe_rows":123}
      -> writes into engine cache

    COMPUTE MODE:
      {"tickers":[...], "sheet_name":"Market_Leaders", "top_n":50}
      -> returns {headers, rows}
    """
    if not _auth_ok(x_app_token):
        return {
            "status": "error",
            "error": "Unauthorized (invalid or missing X-APP-TOKEN).",
            "version": ADVANCED_ANALYSIS_VERSION,
        }

    # ----------------------------
    # 1) PUSH MODE (detect by "items")
    # ----------------------------
    if isinstance(body, dict) and isinstance(body.get("items"), list):
        try:
            push = PushSheetRowsRequest.model_validate(body) if _PYDANTIC_V2 else PushSheetRowsRequest.parse_obj(body)  # type: ignore
        except Exception as exc:
            return PushSheetRowsResponse(status="error", error=f"Invalid push payload: {exc}", version=ADVANCED_ANALYSIS_VERSION)

        engine = await _resolve_engine(request)
        written: List[Dict[str, Any]] = []
        meta = {
            "source": push.source or "sheets_push",
            "universe_rows": push.universe_rows,
        }

        for it in (push.items or []):
            sh = (it.sheet or "").strip()
            hdrs = [str(x) for x in (it.headers or [])]
            rows = it.rows or []
            ok, how = await _engine_cache_write_sheet(engine, sheet=sh, headers=hdrs, rows=rows, meta=meta)
            written.append(
                {
                    "sheet": _to_sheet_key(sh),
                    "headers": len(hdrs),
                    "rows": len(rows),
                    "ok": bool(ok),
                    "method": how,
                }
            )

        return PushSheetRowsResponse(status="success", version=ADVANCED_ANALYSIS_VERSION, written=written)

    # ----------------------------
    # 2) COMPUTE MODE (existing behavior)
    # ----------------------------
    try:
        req = AdvancedSheetRequest.model_validate(body) if _PYDANTIC_V2 else AdvancedSheetRequest.parse_obj(body)  # type: ignore
    except Exception as exc:
        return AdvancedSheetResponse(status="error", error=f"Invalid request: {exc}", headers=[], rows=[])

    sheet_name = (req.sheet_name or req.sheetName or "").strip() or None
    headers, _mode = _select_headers(sheet_name)

    requested = _clean_tickers((req.tickers or []) + (req.symbols or []))
    top_n = _safe_int(req.top_n or 50, 50)
    top_n = max(1, min(500, top_n))

    if not requested:
        return AdvancedSheetResponse(status="skipped", error="No tickers provided", headers=headers, rows=[])

    cfg = _cfg()
    if len(requested) > cfg["max_tickers"]:
        requested = requested[: cfg["max_tickers"]]

    try:
        engine = await _resolve_engine(request)
        unified_map = await _get_quotes_chunked(
            engine,
            requested,
            batch_size=cfg["batch_size"],
            timeout_sec=cfg["timeout_sec"],
            max_concurrency=cfg["concurrency"],
        )

        rows: List[List[Any]] = []
        EnrichedQuote = _try_import_enriched_quote()
        can_use_eq = EnrichedQuote is not None and isinstance(headers, list) and len(headers) == 59

        for s in requested[:top_n]:
            uq = unified_map.get(s.upper()) or _make_placeholder(s, dq="MISSING", err="No data returned")
            _ensure_reco_on_obj(uq)

            if can_use_eq:
                try:
                    eq = EnrichedQuote.from_unified(uq)  # type: ignore
                    row = eq.to_row(headers)  # type: ignore
                    if not isinstance(row, list):
                        raise ValueError("EnrichedQuote.to_row did not return a list")
                    if len(row) < len(headers):
                        row += [None] * (len(headers) - len(row))
                    # enforce reco enum if exists
                    for i, h in enumerate(headers):
                        if str(h).strip().lower() == "recommendation":
                            row[i] = _coerce_reco_enum(row[i])
                            break
                    rows.append(row[: len(headers)])
                    continue
                except Exception as exc:
                    _safe_set(uq, "error", f"Row mapping failed: {exc}")

            # very safe fallback: placeholder row of correct width
            rows.append([None] * len(headers))

        return AdvancedSheetResponse(status="success", headers=headers, rows=rows)

    except Exception as exc:
        logger.exception("[advanced] exception in /sheet-rows: %s", exc)
        return AdvancedSheetResponse(status="error", error=str(exc), headers=headers, rows=[])


__all__ = ["router"]
