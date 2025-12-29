# routes/ai_analysis.py  (FULL REPLACEMENT)
"""
AI & QUANT ANALYSIS ROUTES – GOOGLE SHEETS FRIENDLY (v3.7.3) – PROD SAFE / LOW-MISSING

Key goals
- Engine-driven only: prefer request.app.state.engine; fallback singleton (lazy).
- PROD SAFE: DO NOT import core.data_engine_v2 at module import-time.
- Defensive batching: chunking + timeout + bounded concurrency.
- Sheets-safe: /sheet-rows ALWAYS returns HTTP 200 with {headers, rows, status}.
- Canonical headers: core.schemas.get_headers_for_sheet (59 columns) when available.
- Token guard via X-APP-TOKEN (APP_TOKEN / BACKUP_APP_TOKEN). If no token is set => open.
- Header-driven row mapping + computed 52W position + Riyadh timestamp fill.

Compatible with engines returning:
- list[dict|model] OR dict[symbol->dict|model] OR (payload, err)

v3.7.3 notes
- ✅ No module-import crash if core.data_engine_v2 has syntax/import errors
- ✅ 52W mapping aligned to DataEngineV2 keys: week_52_high/week_52_low
- ✅ Placeholder rows do not depend on UnifiedQuote existing
- ✅ /health reports engine_version/providers when available
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger("routes.ai_analysis")

AI_ANALYSIS_VERSION = "3.7.3"
router = APIRouter(prefix="/v1/analysis", tags=["AI & Analysis"])


# =============================================================================
# Settings shim (safe)
# =============================================================================
try:
    from core.config import get_settings  # type: ignore
except Exception:  # pragma: no cover
    def get_settings():  # type: ignore
        return None


# =============================================================================
# Canonical headers (59 cols) when available
# =============================================================================
try:
    from core.schemas import get_headers_for_sheet  # type: ignore
except Exception:  # pragma: no cover
    get_headers_for_sheet = None  # type: ignore


# =============================================================================
# Lazy imports: V2 helpers (PROD SAFE)
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


def _try_import_v2_symbols() -> Tuple[Optional[Any], Optional[Any], Any]:
    """
    Returns (DataEngine, UnifiedQuote, normalize_symbol_callable).
    Never raises.
    """
    try:
        from core.data_engine_v2 import DataEngine as _DE  # type: ignore
        from core.data_engine_v2 import UnifiedQuote as _UQ  # type: ignore
        from core.data_engine_v2 import normalize_symbol as _NS  # type: ignore
        return _DE, _UQ, _NS
    except Exception:
        return None, None, _fallback_normalize


def _normalize_any(raw: str) -> str:
    # Try v2 normalize_symbol at runtime; fallback if anything fails.
    try:
        _, _, ns = _try_import_v2_symbols()
        s = (ns(raw) or "").strip().upper()
        return s
    except Exception:
        return (raw or "").strip().upper()


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
    """
    Token source priority:
    1) core.config.get_settings(): app_token / backup_app_token
    2) env.settings (if present): app_token / backup_app_token
    3) OS env vars: APP_TOKEN / BACKUP_APP_TOKEN
    If none => open mode.
    """
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

    # 2) env.settings
    try:
        from env import settings as env_settings  # type: ignore
        for attr in ("app_token", "backup_app_token"):
            v = _read_token_attr(env_settings, attr)
            if v:
                tokens.append(v)
    except Exception:
        pass

    # 3) environment
    for k in ("APP_TOKEN", "BACKUP_APP_TOKEN"):
        v = (os.getenv(k) or "").strip()
        if v:
            tokens.append(v)

    # de-dup preserve order
    out: List[str] = []
    seen = set()
    for t in tokens:
        if t not in seen:
            seen.add(t)
            out.append(t)

    if not out:
        logger.warning("[analysis] No APP_TOKEN configured -> endpoints are OPEN (no auth).")
    return out


def _auth_ok(x_app_token: Optional[str]) -> bool:
    allowed = _allowed_tokens()
    if not allowed:
        return True  # open mode
    return bool(x_app_token and x_app_token.strip() in allowed)


def _require_token_or_401(x_app_token: Optional[str]) -> None:
    if not _auth_ok(x_app_token):
        raise HTTPException(status_code=401, detail="Unauthorized (invalid or missing X-APP-TOKEN).")


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


async def _get_singleton_engine() -> Optional[Any]:
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE

    async with _ENGINE_LOCK:
        if _ENGINE is None:
            try:
                DE, _, _ = _try_import_v2_symbols()
                if DE is None:
                    _ENGINE = None
                else:
                    _ENGINE = DE()
                    logger.info("[analysis] DataEngine initialized (fallback singleton).")
            except Exception as exc:
                logger.exception("[analysis] Failed to init DataEngine: %s", exc)
                _ENGINE = None
    return _ENGINE


async def _resolve_engine(request: Optional[Request]) -> Optional[Any]:
    eng = _get_app_engine(request)
    if eng is not None:
        return eng
    return await _get_singleton_engine()


# =============================================================================
# Settings / defaults
# =============================================================================
def _safe_int(x: Any, default: int) -> int:
    try:
        v = int(str(x).strip())
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

    batch_size = _safe_int(getattr(s, "ai_batch_size", None), 20)
    timeout_sec = _safe_float(getattr(s, "ai_batch_timeout_sec", None), 45.0)
    concurrency = _safe_int(getattr(s, "ai_batch_concurrency", None), 5)
    max_tickers = _safe_int(getattr(s, "ai_max_tickers", None), 500)

    # env overrides
    batch_size = _safe_int(os.getenv("AI_BATCH_SIZE", batch_size), batch_size)
    timeout_sec = _safe_float(os.getenv("AI_BATCH_TIMEOUT_SEC", timeout_sec), timeout_sec)
    concurrency = _safe_int(os.getenv("AI_BATCH_CONCURRENCY", concurrency), concurrency)
    max_tickers = _safe_int(os.getenv("AI_MAX_TICKERS", max_tickers), max_tickers)

    # clamps
    batch_size = max(5, min(200, batch_size))
    timeout_sec = max(5.0, min(180.0, timeout_sec))
    concurrency = max(1, min(25, concurrency))
    max_tickers = max(10, min(2000, max_tickers))

    return {
        "batch_size": batch_size,
        "timeout_sec": timeout_sec,
        "concurrency": concurrency,
        "max_tickers": max_tickers,
    }


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _now_utc_iso() -> str:
    return _now_utc().isoformat()


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


# =============================================================================
# Models
# =============================================================================
class _ExtraIgnoreBase(BaseModel):
    model_config = ConfigDict(extra="ignore")


class SingleAnalysisResponse(_ExtraIgnoreBase):
    symbol: str
    name: Optional[str] = None
    market_region: Optional[str] = None

    price: Optional[float] = None
    change_pct: Optional[float] = None
    market_cap: Optional[float] = None
    pe_ttm: Optional[float] = None
    pb: Optional[float] = None
    dividend_yield: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None

    data_quality: str = "MISSING"

    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    risk_score: Optional[float] = None
    overall_score: Optional[float] = None
    recommendation: Optional[str] = None
    confidence: Optional[float] = None

    fair_value: Optional[float] = None
    upside_percent: Optional[float] = None
    valuation_label: Optional[str] = None

    last_updated_utc: Optional[str] = None
    sources: List[str] = Field(default_factory=list)
    error: Optional[str] = None


class BatchAnalysisRequest(_ExtraIgnoreBase):
    tickers: List[str] = Field(default_factory=list)
    symbols: List[str] = Field(default_factory=list)
    sheet_name: Optional[str] = None


class BatchAnalysisResponse(_ExtraIgnoreBase):
    results: List[SingleAnalysisResponse] = Field(default_factory=list)


class SheetAnalysisResponse(_ExtraIgnoreBase):
    headers: List[str] = Field(default_factory=list)
    rows: List[List[Any]] = Field(default_factory=list)
    status: str = "success"  # success | partial | error | skipped
    error: Optional[str] = None


# =============================================================================
# Helpers
# =============================================================================
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


def _finalize_quote(uq: Any) -> Any:
    try:
        fn = getattr(uq, "finalize", None)
        if callable(fn):
            return fn()
    except Exception:
        pass
    return uq


def _make_placeholder(symbol: str, *, dq: str = "MISSING", err: str = "No data") -> Dict[str, Any]:
    sym = (symbol or "").strip().upper() or "UNKNOWN"
    return {
        "symbol": sym,
        "name": None,
        "market": None,
        "currency": None,
        "current_price": None,
        "previous_close": None,
        "day_high": None,
        "day_low": None,
        "week_52_high": None,
        "week_52_low": None,
        "position_52w_percent": None,
        "volume": None,
        "avg_volume_30d": None,
        "value_traded": None,
        "market_cap": None,
        "pe_ttm": None,
        "pb": None,
        "dividend_yield": None,
        "roe": None,
        "roa": None,
        "data_source": "none",
        "data_quality": dq,
        "error": err,
        "status": "error",
        "last_updated_utc": _now_utc_iso(),
    }


def _clean_symbols(items: Sequence[Any]) -> List[str]:
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


def _chunk(items: List[str], size: int) -> List[List[str]]:
    if not items:
        return []
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]


def _extract_sources_from_any(x: Any) -> List[str]:
    if not x:
        return []
    if isinstance(x, list):
        return [str(a).strip() for a in x if str(a).strip()]
    s = str(x).strip()
    if not s:
        return []
    if "," in s:
        parts = [p.strip() for p in s.split(",") if p.strip()]
        return parts or [s]
    return [s]


def _ratio_to_percent(v: Any) -> Any:
    if v is None:
        return None
    try:
        f = float(v)
        return (f * 100.0) if -1.0 <= f <= 1.0 else f
    except Exception:
        return v


def _hkey(h: str) -> str:
    s = str(h or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


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


# ---- Canonical 59 headers fallback (matches core.schemas.DEFAULT_HEADERS_59)
_DEFAULT_HEADERS_59: List[str] = [
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


def _select_headers(sheet_name: Optional[str]) -> List[str]:
    if get_headers_for_sheet:
        try:
            h = get_headers_for_sheet(sheet_name)
            if isinstance(h, list) and len(h) == 59 and any(str(x).strip().lower() == "symbol" for x in h):
                return [str(x) for x in h]
        except Exception:
            pass
    return list(_DEFAULT_HEADERS_59)


def _idx(headers: List[str], name: str) -> Optional[int]:
    k = name.strip().lower()
    for i, h in enumerate(headers):
        if str(h).strip().lower() == k:
            return i
    return None


def _error_row(symbol: str, headers: List[str], err: str) -> List[Any]:
    row = [None] * len(headers)
    i_sym = _idx(headers, "Symbol")
    if i_sym is not None:
        row[i_sym] = symbol
    i_err = _idx(headers, "Error")
    if i_err is not None:
        row[i_err] = err
    return row


def _status_from_rows(headers: List[str], rows: List[List[Any]]) -> str:
    i_err = _idx(headers, "Error")
    if i_err is None:
        return "success"
    for r in rows:
        if len(r) > i_err and (r[i_err] not in (None, "", "null", "None")):
            return "partial"
    return "success"


def _snake_guess(header: str) -> str:
    s = str(header or "").strip().lower()
    s = s.replace("%", " percent ")
    s = re.sub(r"[^\w]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


# Header -> (candidate fields..., transform?)
_HEADER_MAP: Dict[str, Tuple[Tuple[str, ...], Optional[Any]]] = {
    "symbol": (("symbol",), None),
    "company name": (("name", "company_name"), None),
    "sector": (("sector",), None),
    "sub-sector": (("sub_sector", "subsector"), None),
    "sub sector": (("sub_sector", "subsector"), None),
    "market": (("market", "market_region"), None),
    "currency": (("currency",), None),
    "listing date": (("listing_date", "ipo"), None),
    "last price": (("current_price", "last_price", "price"), None),
    "previous close": (("previous_close",), None),
    "price change": (("price_change", "change"), None),
    "percent change": (("percent_change", "change_percent", "change_pct"), None),
    "day high": (("day_high",), None),
    "day low": (("day_low",), None),
    # ✅ align to V2 keys
    "52w high": (("week_52_high", "high_52w", "52w_high"), None),
    "52w low": (("week_52_low", "low_52w", "52w_low"), None),
    "52w position %": (("position_52w_percent", "position_52w"), None),
    "volume": (("volume",), None),
    "avg volume (30d)": (("avg_volume_30d", "avg_volume"), None),
    "value traded": (("value_traded",), None),
    "turnover %": (("turnover_percent", "turnover"), _ratio_to_percent),
    "shares outstanding": (("shares_outstanding",), None),
    "free float %": (("free_float", "free_float_percent"), _ratio_to_percent),
    "market cap": (("market_cap",), None),
    "free float market cap": (("free_float_market_cap",), None),
    "liquidity score": (("liquidity_score",), None),
    "eps (ttm)": (("eps_ttm", "eps"), None),
    "forward eps": (("forward_eps",), None),
    "p/e (ttm)": (("pe_ttm",), None),
    "forward p/e": (("forward_pe",), None),
    "p/b": (("pb",), None),
    "p/s": (("ps",), None),
    "ev/ebitda": (("ev_ebitda",), None),
    "dividend yield %": (("dividend_yield",), _ratio_to_percent),
    "dividend rate": (("dividend_rate",), None),
    "payout ratio %": (("payout_ratio",), _ratio_to_percent),
    "roe %": (("roe",), _ratio_to_percent),
    "roa %": (("roa",), _ratio_to_percent),
    "net margin %": (("net_margin",), _ratio_to_percent),
    "ebitda margin %": (("ebitda_margin",), _ratio_to_percent),
    "revenue growth %": (("revenue_growth",), _ratio_to_percent),
    "net income growth %": (("net_income_growth",), _ratio_to_percent),
    "beta": (("beta",), None),
    "volatility (30d)": (("volatility_30d",), _ratio_to_percent),
    "rsi (14)": (("rsi_14",), None),
    "fair value": (("fair_value",), None),
    "upside %": (("upside_percent",), _ratio_to_percent),
    "valuation label": (("valuation_label",), None),
    "value score": (("value_score",), None),
    "quality score": (("quality_score",), None),
    "momentum score": (("momentum_score",), None),
    "opportunity score": (("opportunity_score",), None),
    "risk score": (("risk_score",), None),
    "overall score": (("overall_score",), None),
    "recommendation": (("recommendation",), None),
    "data source": (("data_source", "source"), None),
    "data quality": (("data_quality",), None),
    "last updated (utc)": (("last_updated_utc", "as_of_utc"), _iso_or_none),
    "last updated (riyadh)": (("last_updated_riyadh",), _iso_or_none),
    "error": (("error",), None),
}


def _value_for_header(header: str, uq: Any) -> Any:
    hk = _hkey(header)

    if hk in ("52w position %", "52w position"):
        v = _safe_get(uq, "position_52w_percent", "position_52w")
        if v is not None:
            return v
        cp = _safe_get(uq, "current_price", "last_price", "price")
        lo = _safe_get(uq, "week_52_low", "low_52w")
        hi = _safe_get(uq, "week_52_high", "high_52w")
        return _compute_52w_position_pct(cp, lo, hi)

    spec = _HEADER_MAP.get(hk)
    if spec:
        fields, transform = spec
        val = _safe_get(uq, *fields)
        if transform and val is not None:
            try:
                return transform(val)
            except Exception:
                return val
        return val

    guess = _snake_guess(header)
    val = _safe_get(uq, guess)
    if val is not None:
        return val

    return None


def _row_from_headers(headers: List[str], uq: Any) -> List[Any]:
    # fill Riyadh if requested but missing
    if _idx(headers, "Last Updated (Riyadh)") is not None:
        last_utc = _safe_get(uq, "last_updated_utc", "as_of_utc")
        last_riy = _safe_get(uq, "last_updated_riyadh")
        if not last_riy and last_utc:
            try:
                riy = _to_riyadh_iso(last_utc)
                if isinstance(uq, dict):
                    uq["last_updated_riyadh"] = riy
                else:
                    setattr(uq, "last_updated_riyadh", riy)
            except Exception:
                pass

    row = [_value_for_header(h, uq) for h in headers]
    if len(row) < len(headers):
        row += [None] * (len(headers) - len(row))
    return row[: len(headers)]


# =============================================================================
# Engine calls (defensive batching)
# =============================================================================
async def _maybe_await(x: Any) -> Any:
    if asyncio.iscoroutine(x):
        return await x
    return x


def _unwrap_tuple_payload(x: Any) -> Any:
    if isinstance(x, tuple) and len(x) == 2:
        return x[0]
    return x


async def _engine_get_quotes(engine: Any, syms: List[str]) -> List[Any]:
    """
    Compatibility shim:
    - Prefer engine.get_enriched_quotes if present
    - Else engine.get_quotes
    - Else per-symbol engine.get_enriched_quote/get_quote
    Accepts engines returning list OR dict OR (payload, err).
    """
    fn = getattr(engine, "get_enriched_quotes", None)
    if callable(fn):
        res = _unwrap_tuple_payload(await _maybe_await(fn(syms)))
        if isinstance(res, dict):
            return [_unwrap_tuple_payload(v) for v in list(res.values())]
        return [_unwrap_tuple_payload(v) for v in list(res or [])]

    fn2 = getattr(engine, "get_quotes", None)
    if callable(fn2):
        res = _unwrap_tuple_payload(await _maybe_await(fn2(syms)))
        if isinstance(res, dict):
            return [_unwrap_tuple_payload(v) for v in list(res.values())]
        return [_unwrap_tuple_payload(v) for v in list(res or [])]

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


def _index_keys_for_quote(q: Any) -> List[str]:
    keys: List[str] = []
    sym = (_safe_get(q, "symbol") or "").strip().upper()
    if sym:
        keys.append(sym)

    sn = (_safe_get(q, "symbol_normalized") or "").strip().upper()
    if sn:
        keys.append(sn)

    si = (_safe_get(q, "symbol_input") or "").strip()
    if si:
        try:
            keys.append(_normalize_any(si))
        except Exception:
            keys.append(si.strip().upper())

    out: List[str] = []
    seen = set()
    for k in keys:
        if k and k not in seen:
            seen.add(k)
            out.append(k)
    return out


async def _get_quotes_chunked(
    request: Optional[Request],
    symbols: List[str],
    *,
    batch_size: int,
    timeout_sec: float,
    max_concurrency: int,
) -> Dict[str, Any]:
    clean = _clean_symbols(symbols)
    if not clean:
        return {}

    engine = await _resolve_engine(request)
    if engine is None:
        return {s: _make_placeholder(s, dq="MISSING", err="Engine unavailable") for s in clean}

    chunks = _chunk(clean, batch_size)
    sem = asyncio.Semaphore(max(1, max_concurrency))

    async def _run_chunk(chunk_syms: List[str]) -> Tuple[List[str], Union[List[Any], Exception]]:
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
            logger.warning("[analysis] %s for chunk(size=%d)", msg, len(chunk_syms))
            for s in chunk_syms:
                out[s.upper()] = _make_placeholder(s, dq="MISSING", err=msg)
            continue

        returned = list(res or [])
        chunk_map: Dict[str, Any] = {}
        for q in returned:
            q2 = _finalize_quote(_unwrap_tuple_payload(q))
            for k in _index_keys_for_quote(q2):
                chunk_map.setdefault(k, q2)

        for s in chunk_syms:
            s_up = s.upper()
            out[s_up] = chunk_map.get(s_up) or _make_placeholder(s_up, dq="MISSING", err="No data returned")

    return out


# =============================================================================
# Transform
# =============================================================================
def _quote_to_analysis(requested_symbol: str, uq: Any) -> SingleAnalysisResponse:
    # best-effort scoring enrichment (optional)
    try:
        from core.scoring_engine import enrich_with_scores  # type: ignore
        uq = enrich_with_scores(uq)  # type: ignore
    except Exception:
        pass

    sym = (_safe_get(uq, "symbol") or _normalize_any(requested_symbol) or requested_symbol or "").upper()

    price = _safe_get(uq, "current_price", "last_price", "price")
    change_pct = _safe_get(uq, "percent_change", "change_pct", "change_percent")

    last_utc = _safe_get(uq, "last_updated_utc", "as_of_utc")
    last_utc_iso = _iso_or_none(last_utc)

    return SingleAnalysisResponse(
        symbol=sym,
        name=_safe_get(uq, "name", "company_name"),
        market_region=_safe_get(uq, "market", "market_region"),
        price=price,
        change_pct=change_pct,
        market_cap=_safe_get(uq, "market_cap"),
        pe_ttm=_safe_get(uq, "pe_ttm"),
        pb=_safe_get(uq, "pb"),
        dividend_yield=_ratio_to_percent(_safe_get(uq, "dividend_yield")),
        roe=_ratio_to_percent(_safe_get(uq, "roe")),
        roa=_ratio_to_percent(_safe_get(uq, "roa")),
        data_quality=str(_safe_get(uq, "data_quality") or "MISSING"),
        value_score=_safe_get(uq, "value_score"),
        quality_score=_safe_get(uq, "quality_score"),
        momentum_score=_safe_get(uq, "momentum_score"),
        opportunity_score=_safe_get(uq, "opportunity_score"),
        risk_score=_safe_get(uq, "risk_score"),
        overall_score=_safe_get(uq, "overall_score"),
        recommendation=_safe_get(uq, "recommendation"),
        confidence=_safe_get(uq, "confidence"),
        fair_value=_safe_get(uq, "fair_value"),
        upside_percent=_safe_get(uq, "upside_percent"),
        valuation_label=_safe_get(uq, "valuation_label"),
        last_updated_utc=last_utc_iso,
        sources=_extract_sources_from_any(_safe_get(uq, "data_source", "source")),
        error=_safe_get(uq, "error"),
    )


# =============================================================================
# Routes
# =============================================================================
@router.get("/health")
@router.get("/ping")
async def analysis_health(request: Request) -> Dict[str, Any]:
    cfg = _cfg()
    eng = await _resolve_engine(request)

    providers: List[str] = []
    engine_version: Optional[str] = None
    engine_name: str = "none"

    try:
        if eng is not None:
            engine_name = type(eng).__name__
            engine_version = getattr(eng, "ENGINE_VERSION", None) or getattr(eng, "engine_version", None) or getattr(eng, "version", None)
            # support multiple engine styles
            for attr in ("providers_global", "providers_ksa", "providers", "enabled_providers"):
                v = getattr(eng, attr, None)
                if isinstance(v, list) and v:
                    providers.extend([str(x) for x in v if str(x).strip()])
            # de-dup
            seen = set()
            providers = [p for p in providers if not (p in seen or seen.add(p))]
    except Exception:
        providers = []

    return {
        "status": "ok",
        "module": "routes.ai_analysis",
        "version": AI_ANALYSIS_VERSION,
        "engine": engine_name,
        "engine_version": engine_version,
        "providers": providers,
        "limits": {
            "batch_size": cfg["batch_size"],
            "batch_timeout_sec": cfg["timeout_sec"],
            "batch_concurrency": cfg["concurrency"],
            "max_tickers": cfg["max_tickers"],
        },
        "auth": "open" if not _allowed_tokens() else "token",
        "timestamp_utc": _now_utc_iso(),
    }


@router.get("/quote", response_model=SingleAnalysisResponse)
async def analyze_single_quote(
    request: Request,
    symbol: str = Query(..., description="Ticker symbol (e.g., 1120.SR, AAPL, ^GSPC)."),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> SingleAnalysisResponse:
    _require_token_or_401(x_app_token)

    t = (symbol or "").strip()
    if not t:
        return SingleAnalysisResponse(symbol="", data_quality="MISSING", error="Symbol is required")

    cfg = _cfg()
    m = await _get_quotes_chunked(
        request,
        [t],
        batch_size=1,
        timeout_sec=cfg["timeout_sec"],
        max_concurrency=1,
    )

    key = _normalize_any(t) or t.strip().upper()
    uq = m.get(key) or _make_placeholder(key, dq="MISSING", err="No data returned")
    return _quote_to_analysis(t, uq)


@router.post("/quotes", response_model=BatchAnalysisResponse)
async def analyze_batch_quotes(
    request: Request,
    body: BatchAnalysisRequest = Body(...),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> BatchAnalysisResponse:
    _require_token_or_401(x_app_token)

    tickers = _clean_symbols((body.tickers or []) + (body.symbols or []))
    if not tickers:
        return BatchAnalysisResponse(results=[])

    cfg = _cfg()
    if len(tickers) > cfg["max_tickers"]:
        tickers = tickers[: cfg["max_tickers"]]

    m = await _get_quotes_chunked(
        request,
        tickers,
        batch_size=cfg["batch_size"],
        timeout_sec=cfg["timeout_sec"],
        max_concurrency=cfg["concurrency"],
    )

    results: List[SingleAnalysisResponse] = []
    for t in tickers:
        uq = m.get(t) or _make_placeholder(t, dq="MISSING", err="No data returned")
        results.append(_quote_to_analysis(t, uq))

    return BatchAnalysisResponse(results=results)


@router.post("/sheet-rows", response_model=SheetAnalysisResponse)
async def analyze_for_sheet(
    request: Request,
    body: BatchAnalysisRequest = Body(...),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> SheetAnalysisResponse:
    """
    Sheets-safe: ALWAYS returns HTTP 200 with {headers, rows, status}.
    If auth fails -> returns error rows (no exception).
    """
    headers = _select_headers(body.sheet_name)
    tickers = _clean_symbols((body.tickers or []) + (body.symbols or []))

    if not tickers:
        return SheetAnalysisResponse(headers=headers, rows=[], status="skipped", error="No tickers provided")

    # Sheets-safe auth (never raise out)
    if not _auth_ok(x_app_token):
        err = "Unauthorized (invalid or missing X-APP-TOKEN)."
        rows = [_error_row(t, headers, err) for t in tickers]
        return SheetAnalysisResponse(headers=headers, rows=rows, status="error", error=err)

    cfg = _cfg()
    if len(tickers) > cfg["max_tickers"]:
        tickers = tickers[: cfg["max_tickers"]]

    try:
        m = await _get_quotes_chunked(
            request,
            tickers,
            batch_size=cfg["batch_size"],
            timeout_sec=cfg["timeout_sec"],
            max_concurrency=cfg["concurrency"],
        )

        rows: List[List[Any]] = []
        for t in tickers:
            uq = m.get(t) or _make_placeholder(t, dq="MISSING", err="No data returned")
            rows.append(_row_from_headers(headers, uq))

        status = _status_from_rows(headers, rows)
        return SheetAnalysisResponse(headers=headers, rows=rows, status=status)

    except Exception as exc:
        logger.exception("[analysis] exception in /sheet-rows: %s", exc)
        rows = [_error_row(t, headers, str(exc)) for t in tickers]
        return SheetAnalysisResponse(headers=headers, rows=rows, status="error", error=str(exc))


__all__ = [
    "router",
    "SingleAnalysisResponse",
    "BatchAnalysisRequest",
    "BatchAnalysisResponse",
    "SheetAnalysisResponse",
]
