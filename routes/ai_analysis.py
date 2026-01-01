# routes/ai_analysis.py  (FULL REPLACEMENT)
"""
AI & QUANT ANALYSIS ROUTES – GOOGLE SHEETS FRIENDLY (v4.2.0) – PROD SAFE / LOW-MISSING

Key goals
- Engine-driven only: prefer request.app.state.engine; fallback singleton (lazy).
- PROD SAFE: DO NOT import core.data_engine_v2 at module import-time.
- Defensive batching: chunking + timeout + bounded concurrency.
- Sheets-safe: /sheet-rows ALWAYS returns HTTP 200 with {headers, rows, status}.
- Canonical headers: core.schemas.get_headers_for_sheet (when available); fallback defaults.
- Token guard via X-APP-TOKEN (APP_TOKEN / BACKUP_APP_TOKEN). If no token is set => open.
- Header-driven row mapping + computed 52W position + Riyadh timestamp fill.
- Plan-aligned: Current + Historical + Forward expectations + Valuation + Overall + Recommendation.

v4.2.0 upgrades
- ✅ Fast path: cached schema imports + cached header plans (big speedup for large sheets)
- ✅ Robust mapping for tricky headers: P/E (TTM), P/B, P/S, Forward P/E
- ✅ Upside % + other ratio fields normalized to percent when values are in 0..1
- ✅ Volatility normalized to % when 0..1
- ✅ Recommendation enum enforced everywhere: BUY / HOLD / REDUCE / SELL
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request

# Pydantic v2 preferred, v1 fallback
try:
    from pydantic import BaseModel, ConfigDict, Field  # type: ignore

    _PYDANTIC_V2 = True
except Exception:  # pragma: no cover
    from pydantic import BaseModel, Field  # type: ignore

    ConfigDict = None  # type: ignore
    _PYDANTIC_V2 = False

logger = logging.getLogger("routes.ai_analysis")

AI_ANALYSIS_VERSION = "4.2.0"
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
# Schemas (lazy-safe + cached)
# =============================================================================
@lru_cache(maxsize=1)
def _try_import_schemas():
    try:
        from core import schemas as sch  # type: ignore

        return sch
    except Exception:
        return None


@lru_cache(maxsize=64)
def _schemas_get_headers(sheet_name: Optional[str]) -> Optional[Tuple[str, ...]]:
    sch = _try_import_schemas()
    if sch is None:
        return None
    fn = getattr(sch, "get_headers_for_sheet", None)
    if callable(fn):
        try:
            h = fn(sheet_name)
            if isinstance(h, list) and len(h) >= 10:
                return tuple(str(x) for x in h)
        except Exception:
            return None
    return None


@lru_cache(maxsize=2048)
def _schemas_header_to_field_cached(header: str) -> Optional[str]:
    sch = _try_import_schemas()
    if sch is None:
        return None

    fn = getattr(sch, "header_to_field", None)
    if callable(fn):
        try:
            out = fn(header)
            out = str(out or "").strip()
            return out or None
        except Exception:
            return None

    m = getattr(sch, "HEADER_TO_FIELD", None)
    if isinstance(m, dict):
        try:
            out = m.get(str(header).strip())
            out = str(out or "").strip()
            return out or None
        except Exception:
            return None

    return None


# =============================================================================
# Recommendation normalization (BUY/HOLD/REDUCE/SELL)
# =============================================================================
_RECO_ENUM = ("BUY", "HOLD", "REDUCE", "SELL")


def _normalize_recommendation(x: Any) -> str:
    """Canonical enum across all endpoints: BUY / HOLD / REDUCE / SELL."""
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

    return "HOLD"


# =============================================================================
# Lazy imports: V2 helpers (PROD SAFE) + cached normalize
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
def _try_import_v2_symbols() -> Tuple[Optional[Any], Optional[Any], Any]:
    """
    Returns (DataEngine, UnifiedQuote, normalize_symbol_callable). Never raises.
    Cached to avoid repeated imports per row/cell.
    """
    try:
        from core.data_engine_v2 import DataEngine as _DE  # type: ignore
        from core.data_engine_v2 import UnifiedQuote as _UQ  # type: ignore
        from core.data_engine_v2 import normalize_symbol as _NS  # type: ignore

        return _DE, _UQ, _NS
    except Exception:
        return None, None, _fallback_normalize


def _normalize_any(raw: str) -> str:
    try:
        _, _, ns = _try_import_v2_symbols()
        return (ns(raw) or "").strip().upper()
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
        logger.warning("[analysis] No APP_TOKEN configured -> endpoints are OPEN (no auth).")
    return out


def _auth_ok(x_app_token: Optional[str]) -> bool:
    allowed = _allowed_tokens()
    if not allowed:
        return True
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
                _ENGINE = None if DE is None else DE()
                if _ENGINE is not None:
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


def _safe_float_pos(x: Any, default: float) -> float:
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

    batch_size = _safe_int(getattr(s, "ai_batch_size", None), 25)
    timeout_sec = _safe_float_pos(getattr(s, "ai_batch_timeout_sec", None), 45.0)
    concurrency = _safe_int(getattr(s, "ai_batch_concurrency", None), 6)
    max_tickers = _safe_int(getattr(s, "ai_max_tickers", None), 800)

    batch_size = _safe_int(os.getenv("AI_BATCH_SIZE", batch_size), batch_size)
    timeout_sec = _safe_float_pos(os.getenv("AI_BATCH_TIMEOUT_SEC", timeout_sec), timeout_sec)
    concurrency = _safe_int(os.getenv("AI_BATCH_CONCURRENCY", concurrency), concurrency)
    max_tickers = _safe_int(os.getenv("AI_MAX_TICKERS", max_tickers), max_tickers)

    batch_size = max(5, min(250, batch_size))
    timeout_sec = max(5.0, min(180.0, timeout_sec))
    concurrency = max(1, min(30, concurrency))
    max_tickers = max(10, min(3000, max_tickers))

    return {"batch_size": batch_size, "timeout_sec": timeout_sec, "concurrency": concurrency, "max_tickers": max_tickers}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _now_utc_iso() -> str:
    return _now_utc().isoformat()


def _parse_iso_dt(x: Any) -> Optional[datetime]:
    if x is None or x == "":
        return None
    try:
        if isinstance(x, datetime):
            dt = x
        else:
            s = str(x).strip()
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _iso_or_none(x: Any) -> Optional[str]:
    dt = _parse_iso_dt(x)
    if dt is not None:
        return dt.isoformat()
    try:
        return str(x) if x is not None else None
    except Exception:
        return None


def _to_riyadh_iso(utc_any: Any) -> Optional[str]:
    if not utc_any:
        return None
    try:
        from zoneinfo import ZoneInfo  # py3.9+

        tz = ZoneInfo("Asia/Riyadh")
        dt = _parse_iso_dt(utc_any)
        if dt is None:
            return None
        return dt.astimezone(tz).isoformat()
    except Exception:
        return None


# =============================================================================
# Models
# =============================================================================
class _ExtraIgnoreBase(BaseModel):
    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")  # type: ignore
    else:  # pragma: no cover
        class Config:
            extra = "ignore"


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

    # History / expectations
    returns_1w: Optional[float] = None
    returns_1m: Optional[float] = None
    returns_3m: Optional[float] = None
    returns_6m: Optional[float] = None
    returns_12m: Optional[float] = None
    rsi14: Optional[float] = None
    vol_30d_ann: Optional[float] = None
    ma20: Optional[float] = None
    ma50: Optional[float] = None
    ma200: Optional[float] = None

    expected_return_1m: Optional[float] = None
    expected_return_3m: Optional[float] = None
    expected_return_12m: Optional[float] = None
    expected_price_1m: Optional[float] = None
    expected_price_3m: Optional[float] = None
    expected_price_12m: Optional[float] = None
    confidence_score: Optional[int] = None
    forecast_method: Optional[str] = None

    data_quality: str = "MISSING"

    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    risk_score: Optional[float] = None
    overall_score: Optional[float] = None
    recommendation: Optional[str] = None  # BUY/HOLD/REDUCE/SELL
    confidence: Optional[float] = None  # 0..1

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
        "sector": None,
        "sub_sector": None,
        "market": None,
        "currency": None,
        "listing_date": None,
        "current_price": None,
        "previous_close": None,
        "price_change": None,
        "percent_change": None,
        "day_high": None,
        "day_low": None,
        "week_52_high": None,
        "week_52_low": None,
        "position_52w_percent": None,
        "volume": None,
        "avg_volume_30d": None,
        "value_traded": None,
        "turnover_percent": None,
        "shares_outstanding": None,
        "free_float": None,
        "market_cap": None,
        "free_float_market_cap": None,
        "liquidity_score": None,
        "pe_ttm": None,
        "forward_pe": None,
        "pb": None,
        "ps": None,
        "dividend_yield": None,
        "roe": None,
        "roa": None,
        "value_score": None,
        "quality_score": None,
        "momentum_score": None,
        "opportunity_score": None,
        "risk_score": None,
        "overall_score": None,
        "fair_value": None,
        "upside_percent": None,
        "valuation_label": None,
        "recommendation": "HOLD",
        "confidence": None,
        "data_source": "none",
        "data_quality": dq,
        "error": err,
        "status": "error",
        "last_updated_utc": _now_utc_iso(),
        "last_updated_riyadh": "",
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


def _vol_to_percent(v: Any) -> Any:
    if v is None:
        return None
    try:
        f = float(v)
        return f * 100.0 if 0.0 <= f <= 1.0 else f
    except Exception:
        return v


def _hkey(h: str) -> str:
    s = str(h or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def _snake_guess(header: str) -> str:
    s = str(header or "").strip().lower()
    s = s.replace("%", " percent ")
    s = re.sub(r"[^\w]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
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


def _safe_float_or_none(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None


def _compute_fair_value_and_upside(uq: Any) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    price = _safe_float_or_none(_safe_get(uq, "current_price", "last_price", "price"))
    if price is None or price <= 0:
        return None, None, None

    fair = _safe_float_or_none(_safe_get(uq, "fair_value"))
    if fair is None:
        fair = _safe_float_or_none(_safe_get(uq, "expected_price_3m"))
    if fair is None:
        fair = _safe_float_or_none(_safe_get(uq, "expected_price_12m"))
    if fair is None:
        fair = _safe_float_or_none(_safe_get(uq, "ma200"))
    if fair is None:
        fair = _safe_float_or_none(_safe_get(uq, "ma50"))

    if fair is None or fair <= 0:
        return None, None, None

    upside = (fair / price - 1.0) * 100.0
    label = "Fairly Valued"
    if upside >= 15:
        label = "Undervalued"
    elif upside <= -15:
        label = "Overvalued"

    return fair, round(upside, 2), label


def _compute_overall_and_reco(uq: Any) -> Tuple[Optional[float], str, Optional[float]]:
    """
    Returns (overall_score_0_100, recommendation_enum, confidence_0_1).
    """
    opp = _safe_float_or_none(_safe_get(uq, "opportunity_score"))
    val = _safe_float_or_none(_safe_get(uq, "value_score"))
    qual = _safe_float_or_none(_safe_get(uq, "quality_score"))
    mom = _safe_float_or_none(_safe_get(uq, "momentum_score"))
    risk = _safe_float_or_none(_safe_get(uq, "risk_score"))

    conf_score = _safe_float_or_none(_safe_get(uq, "confidence_score"))
    if conf_score is None:
        conf_score = _safe_float_or_none(_safe_get(uq, "confidence"))

    conf01: Optional[float] = None
    conf100: Optional[float] = None
    if conf_score is not None:
        if 0.0 <= conf_score <= 1.0:
            conf01 = conf_score
            conf100 = conf_score * 100.0
        elif 1.0 < conf_score <= 100.0:
            conf01 = conf_score / 100.0
            conf100 = conf_score

    parts: List[float] = []
    if opp is not None:
        parts.append(0.45 * opp)
    if val is not None:
        parts.append(0.20 * val)
    if qual is not None:
        parts.append(0.20 * qual)
    if mom is not None:
        parts.append(0.15 * mom)

    if not parts:
        return None, "HOLD", conf01

    score = sum(parts)
    if risk is not None:
        score -= 0.15 * risk
    if conf100 is not None:
        score += 0.05 * (conf100 - 50.0)

    score = max(0.0, min(100.0, score))

    if score >= 65:
        reco = "BUY"
    elif score >= 50:
        reco = "HOLD"
    elif score >= 35:
        reco = "REDUCE"
    else:
        reco = "SELL"

    if risk is not None and risk >= 80:
        if reco == "BUY":
            reco = "HOLD"
        elif reco == "HOLD":
            reco = "REDUCE"
        elif reco == "REDUCE":
            reco = "SELL"

    return round(score, 2), reco, conf01


# =============================================================================
# Headers (BASE + EXTENDED)
# =============================================================================
_DEFAULT_HEADERS_BASE: List[str] = [
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

_DEFAULT_HEADERS_EXTENDED: List[str] = _DEFAULT_HEADERS_BASE + [
    "Returns 1W %",
    "Returns 1M %",
    "Returns 3M %",
    "Returns 6M %",
    "Returns 12M %",
    "MA20",
    "MA50",
    "MA200",
    "Expected Return 1M %",
    "Expected Return 3M %",
    "Expected Return 12M %",
    "Expected Price 1M",
    "Expected Price 3M",
    "Expected Price 12M",
    "Confidence Score",
    "Forecast Method",
    "History Points",
    "History Source",
    "History Last (UTC)",
]


def _select_headers(sheet_name: Optional[str], mode: Optional[str]) -> List[str]:
    """
    Preference order:
    1) core.schemas.get_headers_for_sheet(sheet_name)
    2) fallback defaults

    Enhancement:
    - if mode=extended and schemas returned a short/canonical list,
      append extended columns that are missing (no breaking reorder).
    """
    m = (mode or "").strip().lower()

    h = _schemas_get_headers(sheet_name)
    if h and len(h) >= 10:
        if m in ("ext", "extended", "full"):
            base_set = {str(x) for x in h}
            extras = [x for x in _DEFAULT_HEADERS_EXTENDED if x not in base_set]
            return list(h) + extras
        return list(h)

    if m in ("ext", "extended", "full"):
        return list(_DEFAULT_HEADERS_EXTENDED)

    sn = (sheet_name or "").strip().lower()
    if any(k in sn for k in ("global", "insight", "opportun", "advisor", "analysis")):
        return list(_DEFAULT_HEADERS_EXTENDED)

    return list(_DEFAULT_HEADERS_BASE)


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
    i_dq = _idx(headers, "Data Quality")
    if i_dq is not None:
        row[i_dq] = "MISSING"
    i_rec = _idx(headers, "Recommendation")
    if i_rec is not None:
        row[i_rec] = "HOLD"
    return row


def _status_from_rows(headers: List[str], rows: List[List[Any]]) -> str:
    i_err = _idx(headers, "Error")
    i_dq = _idx(headers, "Data Quality")

    any_err = False
    any_missing = False

    for r in rows:
        if i_err is not None and len(r) > i_err and str(r[i_err] or "").strip():
            any_err = True
        if i_dq is not None and len(r) > i_dq and str(r[i_dq] or "").strip().upper() == "MISSING":
            any_missing = True

    if not rows:
        return "skipped"
    if any_err and all((i_err is not None and len(r) > i_err and str(r[i_err] or "").strip()) for r in rows):
        return "error"
    if any_err or any_missing:
        return "partial"
    return "success"


# =============================================================================
# Header mapping plan (cached)
# =============================================================================
# Percent-like headers where a ratio (0..1) should become percent (0..100)
_RATIO_HEADERS = {
    "percent change",
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
    "upside %",
    "returns 1w %",
    "returns 1m %",
    "returns 3m %",
    "returns 6m %",
    "returns 12m %",
    "expected return 1m %",
    "expected return 3m %",
    "expected return 12m %",
}

_VOL_HEADERS = {"volatility (30d)"}


_LOCAL_HEADER_MAP: Dict[str, Tuple[Tuple[str, ...], Optional[Any]]] = {
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
    "percent change": (("percent_change", "change_pct", "change_percent"), _ratio_to_percent),
    "day high": (("day_high",), None),
    "day low": (("day_low",), None),
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
    "volatility (30d)": (("vol_30d_ann", "volatility_30d", "volatility_30d_ann"), _vol_to_percent),
    "rsi (14)": (("rsi14", "rsi_14"), None),
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
    "data source": (("data_source", "source", "provider"), None),
    "data quality": (("data_quality",), None),
    "last updated (utc)": (("last_updated_utc", "as_of_utc"), _iso_or_none),
    "last updated (riyadh)": (("last_updated_riyadh",), _iso_or_none),
    "error": (("error",), None),
    # Extended fields
    "returns 1w %": (("returns_1w",), _ratio_to_percent),
    "returns 1m %": (("returns_1m",), _ratio_to_percent),
    "returns 3m %": (("returns_3m",), _ratio_to_percent),
    "returns 6m %": (("returns_6m",), _ratio_to_percent),
    "returns 12m %": (("returns_12m",), _ratio_to_percent),
    "ma20": (("ma20",), None),
    "ma50": (("ma50",), None),
    "ma200": (("ma200",), None),
    "expected return 1m %": (("expected_return_1m",), _ratio_to_percent),
    "expected return 3m %": (("expected_return_3m",), _ratio_to_percent),
    "expected return 12m %": (("expected_return_12m",), _ratio_to_percent),
    "expected price 1m": (("expected_price_1m",), None),
    "expected price 3m": (("expected_price_3m",), None),
    "expected price 12m": (("expected_price_12m",), None),
    "confidence score": (("confidence_score",), None),
    "forecast method": (("forecast_method",), None),
    "history points": (("history_points",), None),
    "history source": (("history_source",), None),
    "history last (utc)": (("history_last_utc",), _iso_or_none),
}


@dataclass(frozen=True)
class _HeaderMeta:
    header: str
    hk: str
    schema_field: Optional[str]
    local_fields: Optional[Tuple[str, ...]]
    local_transform: Optional[Any]
    guess: str


@dataclass(frozen=True)
class _HeaderPlan:
    metas: Tuple[_HeaderMeta, ...]
    need_52w: bool
    need_val: bool
    need_overall: bool
    need_last_utc: bool
    need_last_riyadh: bool


@lru_cache(maxsize=64)
def _build_header_plan(headers: Tuple[str, ...]) -> _HeaderPlan:
    metas: List[_HeaderMeta] = []
    need_52w = False
    need_val = False
    need_overall = False
    need_last_utc = False
    need_last_riyadh = False

    for h in headers:
        hk = _hkey(h)
        if hk in ("52w position %", "52w position"):
            need_52w = True
        if hk in ("fair value", "upside %", "valuation label"):
            need_val = True
        if hk in ("recommendation", "overall score"):
            need_overall = True
        if hk == "last updated (utc)":
            need_last_utc = True
        if hk == "last updated (riyadh)":
            need_last_riyadh = True

        schema_field = _schemas_header_to_field_cached(h)
        local = _LOCAL_HEADER_MAP.get(hk)
        if local:
            lf, lt = local
        else:
            lf, lt = None, None

        metas.append(
            _HeaderMeta(
                header=h,
                hk=hk,
                schema_field=schema_field,
                local_fields=lf,
                local_transform=lt,
                guess=_snake_guess(h),
            )
        )

    return _HeaderPlan(
        metas=tuple(metas),
        need_52w=need_52w,
        need_val=need_val,
        need_overall=need_overall,
        need_last_utc=need_last_utc,
        need_last_riyadh=need_last_riyadh,
    )


def _value_for_meta(meta: _HeaderMeta, uq: Any, *, fair_pack: Optional[Tuple[Any, Any, Any]], overall_pack: Optional[Tuple[Any, Any, Any]]) -> Any:
    hk = meta.hk

    # 52W Position computed if missing
    if hk in ("52w position %", "52w position"):
        v = _safe_get(uq, "position_52w_percent", "position_52w")
        if v is not None:
            return v
        cp = _safe_get(uq, "current_price", "last_price", "price")
        lo = _safe_get(uq, "week_52_low", "low_52w")
        hi = _safe_get(uq, "week_52_high", "high_52w")
        return _compute_52w_position_pct(cp, lo, hi)

    # Timestamps fill
    if hk == "last updated (utc)":
        v = _safe_get(uq, "last_updated_utc", "as_of_utc") or _now_utc_iso()
        return _iso_or_none(v)

    if hk == "last updated (riyadh)":
        v = _safe_get(uq, "last_updated_riyadh")
        if not v:
            u = _safe_get(uq, "last_updated_utc", "as_of_utc") or _now_utc_iso()
            v = _to_riyadh_iso(u) or ""
        return _iso_or_none(v) or v

    # Valuation / overall / reco computed if missing
    if hk in ("fair value", "upside %", "valuation label"):
        fair, upside, label = fair_pack or (None, None, None)
        if hk == "fair value":
            v = _safe_get(uq, "fair_value")
            return v if v is not None else fair
        if hk == "upside %":
            v = _safe_get(uq, "upside_percent")
            vv = v if v is not None else upside
            return _ratio_to_percent(vv)
        if hk == "valuation label":
            v = _safe_get(uq, "valuation_label")
            return v if v is not None else label

    if hk in ("overall score", "recommendation"):
        overall_calc, reco_calc, _conf01 = overall_pack or (None, "HOLD", None)

        if hk == "overall score":
            v = _safe_get(uq, "overall_score")
            return v if v is not None else overall_calc

        if hk == "recommendation":
            v = _safe_get(uq, "recommendation")
            raw = v if v is not None else reco_calc
            return _normalize_recommendation(raw)

    # 1) schemas mapping if available
    if meta.schema_field:
        v = _safe_get(uq, meta.schema_field)
        if hk in _RATIO_HEADERS:
            v = _ratio_to_percent(v)
        if hk in _VOL_HEADERS:
            v = _vol_to_percent(v)
        if hk == "recommendation":
            return _normalize_recommendation(v)
        return v

    # 2) local mapping
    if meta.local_fields:
        val = _safe_get(uq, *meta.local_fields)
        if hk == "recommendation":
            return _normalize_recommendation(val)
        if hk in _RATIO_HEADERS:
            val = _ratio_to_percent(val)
        if hk in _VOL_HEADERS:
            val = _vol_to_percent(val)
        if meta.local_transform and val is not None:
            try:
                return meta.local_transform(val)
            except Exception:
                return val
        return val

    # 3) fallback snake-case guess
    val = _safe_get(uq, meta.guess)
    if hk == "recommendation":
        return _normalize_recommendation(val)
    if hk in _RATIO_HEADERS:
        return _ratio_to_percent(val)
    if hk in _VOL_HEADERS:
        return _vol_to_percent(val)
    return val


def _row_from_plan(plan: _HeaderPlan, uq: Any) -> List[Any]:
    fair_pack: Optional[Tuple[Any, Any, Any]] = None
    overall_pack: Optional[Tuple[Any, Any, Any]] = None

    if plan.need_val:
        fair_pack = _compute_fair_value_and_upside(uq)
    if plan.need_overall:
        overall_pack = _compute_overall_and_reco(uq)

    # Fill Riyadh on the object if present in headers (helps other consumers too)
    if plan.need_last_riyadh:
        last_utc = _safe_get(uq, "last_updated_utc", "as_of_utc") or _now_utc_iso()
        last_riy = _safe_get(uq, "last_updated_riyadh")
        if not last_riy:
            try:
                riy = _to_riyadh_iso(last_utc) or ""
                if isinstance(uq, dict):
                    uq["last_updated_riyadh"] = riy
                else:
                    setattr(uq, "last_updated_riyadh", riy)
            except Exception:
                pass

    row = [_value_for_meta(m, uq, fair_pack=fair_pack, overall_pack=overall_pack) for m in plan.metas]
    if len(row) < len(plan.metas):
        row += [None] * (len(plan.metas) - len(row))
    return row[: len(plan.metas)]


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


async def _engine_get_quotes_any(engine: Any, syms: List[str]) -> Union[List[Any], Dict[str, Any]]:
    fn = getattr(engine, "get_enriched_quotes", None)
    if callable(fn):
        res = _unwrap_tuple_payload(await _maybe_await(fn(syms)))
        if isinstance(res, (list, dict)):
            return res
        return list(res or [])

    fn2 = getattr(engine, "get_quotes", None)
    if callable(fn2):
        res = _unwrap_tuple_payload(await _maybe_await(fn2(syms)))
        if isinstance(res, (list, dict)):
            return res
        return list(res or [])

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

    async def _run_chunk(chunk_syms: List[str]) -> Tuple[List[str], Union[Union[List[Any], Dict[str, Any]], Exception]]:
        async with sem:
            try:
                res = await asyncio.wait_for(_engine_get_quotes_any(engine, chunk_syms), timeout=timeout_sec)
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

        chunk_map: Dict[str, Any] = {}

        if isinstance(res, dict):
            for k, v in res.items():
                q = _finalize_quote(_unwrap_tuple_payload(v))
                kk = str(k or "").strip().upper()
                if kk:
                    chunk_map.setdefault(kk, q)
                for alt in _index_keys_for_quote(q):
                    chunk_map.setdefault(alt, q)
        else:
            returned = list(res or [])
            if len(returned) == len(chunk_syms):
                for i, s in enumerate(chunk_syms):
                    q = _finalize_quote(_unwrap_tuple_payload(returned[i]))
                    chunk_map.setdefault(s.upper(), q)
                    for alt in _index_keys_for_quote(q):
                        chunk_map.setdefault(alt, q)
            else:
                for q0 in returned:
                    q = _finalize_quote(_unwrap_tuple_payload(q0))
                    for alt in _index_keys_for_quote(q):
                        chunk_map.setdefault(alt, q)

        for s in chunk_syms:
            s_up = s.upper()
            out[s_up] = chunk_map.get(s_up) or _make_placeholder(s_up, dq="MISSING", err="No data returned")

    return out


# =============================================================================
# Transform
# =============================================================================
def _quote_to_analysis(requested_symbol: str, uq: Any) -> SingleAnalysisResponse:
    try:
        from core.scoring_engine import enrich_with_scores  # type: ignore

        uq = enrich_with_scores(uq)  # type: ignore
    except Exception:
        pass

    sym = (_safe_get(uq, "symbol") or _normalize_any(requested_symbol) or requested_symbol or "").upper()

    price = _safe_get(uq, "current_price", "last_price", "price")
    change_pct = _ratio_to_percent(_safe_get(uq, "percent_change", "change_pct", "change_percent"))

    last_utc = _safe_get(uq, "last_updated_utc", "as_of_utc") or _now_utc_iso()
    last_utc_iso = _iso_or_none(last_utc)

    fair_calc, upside_calc, val_label_calc = _compute_fair_value_and_upside(uq)
    overall_calc, reco_calc, conf01_calc = _compute_overall_and_reco(uq)

    fair2 = _safe_get(uq, "fair_value")
    upside2 = _safe_get(uq, "upside_percent")
    val_label2 = _safe_get(uq, "valuation_label")
    reco2 = _safe_get(uq, "recommendation")
    overall2 = _safe_get(uq, "overall_score")

    reco_norm = _normalize_recommendation(reco2 if reco2 is not None else reco_calc)

    conf = _safe_get(uq, "confidence")
    if conf is None:
        conf = conf01_calc
    else:
        try:
            cf = float(conf)
            if 1.0 < cf <= 100.0:
                conf = cf / 100.0
        except Exception:
            pass

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
        returns_1w=_ratio_to_percent(_safe_get(uq, "returns_1w")),
        returns_1m=_ratio_to_percent(_safe_get(uq, "returns_1m")),
        returns_3m=_ratio_to_percent(_safe_get(uq, "returns_3m")),
        returns_6m=_ratio_to_percent(_safe_get(uq, "returns_6m")),
        returns_12m=_ratio_to_percent(_safe_get(uq, "returns_12m")),
        rsi14=_safe_get(uq, "rsi14", "rsi_14"),
        vol_30d_ann=_vol_to_percent(_safe_get(uq, "vol_30d_ann", "volatility_30d", "volatility_30d_ann")),
        ma20=_safe_get(uq, "ma20"),
        ma50=_safe_get(uq, "ma50"),
        ma200=_safe_get(uq, "ma200"),
        expected_return_1m=_ratio_to_percent(_safe_get(uq, "expected_return_1m")),
        expected_return_3m=_ratio_to_percent(_safe_get(uq, "expected_return_3m")),
        expected_return_12m=_ratio_to_percent(_safe_get(uq, "expected_return_12m")),
        expected_price_1m=_safe_get(uq, "expected_price_1m"),
        expected_price_3m=_safe_get(uq, "expected_price_3m"),
        expected_price_12m=_safe_get(uq, "expected_price_12m"),
        confidence_score=_safe_get(uq, "confidence_score"),
        forecast_method=_safe_get(uq, "forecast_method"),
        data_quality=str(_safe_get(uq, "data_quality") or "MISSING"),
        value_score=_safe_get(uq, "value_score"),
        quality_score=_safe_get(uq, "quality_score"),
        momentum_score=_safe_get(uq, "momentum_score"),
        opportunity_score=_safe_get(uq, "opportunity_score"),
        risk_score=_safe_get(uq, "risk_score"),
        overall_score=overall2 if overall2 is not None else overall_calc,
        recommendation=reco_norm,
        confidence=conf,
        fair_value=fair2 if fair2 is not None else fair_calc,
        upside_percent=_ratio_to_percent(upside2 if upside2 is not None else upside_calc),
        valuation_label=val_label2 if val_label2 is not None else val_label_calc,
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
            for attr in ("providers_global", "providers_ksa", "providers", "enabled_providers"):
                v = getattr(eng, attr, None)
                if isinstance(v, list) and v:
                    providers.extend([str(x) for x in v if str(x).strip()])
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
        "recommendation_enum": ["BUY", "HOLD", "REDUCE", "SELL"],
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
    m = await _get_quotes_chunked(request, [t], batch_size=1, timeout_sec=cfg["timeout_sec"], max_concurrency=1)

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
        request, tickers, batch_size=cfg["batch_size"], timeout_sec=cfg["timeout_sec"], max_concurrency=cfg["concurrency"]
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
    mode: Optional[str] = Query(default=None, description="Headers mode: base|extended (appends if schemas returns canonical)."),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> SheetAnalysisResponse:
    """
    Sheets-safe: ALWAYS returns HTTP 200 with {headers, rows, status}.
    If auth fails -> returns error rows (no exception).
    """
    headers = _select_headers(body.sheet_name, mode)
    tickers = _clean_symbols((body.tickers or []) + (body.symbols or []))

    if not tickers:
        return SheetAnalysisResponse(headers=headers, rows=[], status="skipped", error="No tickers provided")

    if not _auth_ok(x_app_token):
        err = "Unauthorized (invalid or missing X-APP-TOKEN)."
        rows = [_error_row(t, headers, err) for t in tickers]
        return SheetAnalysisResponse(headers=headers, rows=rows, status="error", error=err)

    cfg = _cfg()
    if len(tickers) > cfg["max_tickers"]:
        tickers = tickers[: cfg["max_tickers"]]

    plan = _build_header_plan(tuple(headers))

    try:
        m = await _get_quotes_chunked(
            request, tickers, batch_size=cfg["batch_size"], timeout_sec=cfg["timeout_sec"], max_concurrency=cfg["concurrency"]
        )

        rows: List[List[Any]] = []
        for t in tickers:
            uq = m.get(t) or _make_placeholder(t, dq="MISSING", err="No data returned")
            # enforce recommendation enum on object (helps any downstream too)
            try:
                if isinstance(uq, dict):
                    uq["recommendation"] = _normalize_recommendation(uq.get("recommendation"))
                else:
                    setattr(uq, "recommendation", _normalize_recommendation(getattr(uq, "recommendation", None)))
            except Exception:
                pass
            rows.append(_row_from_plan(plan, uq))

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
