# routes/ai_analysis.py  (FULL REPLACEMENT)
"""
AI & QUANT ANALYSIS ROUTES – GOOGLE SHEETS FRIENDLY (v4.4.4) – PROD SAFE / LOW-MISSING

Key goals
- Engine-driven only: prefer request.app.state.engine; fallback singleton (lazy).
- PROD SAFE: DO NOT import core.data_engine_v2 at module import-time.
- Defensive batching: chunking + timeout + bounded concurrency.
- Sheets-safe: /sheet-rows ALWAYS returns HTTP 200 with {headers, rows, status}.
- Canonical headers: core.schemas.get_headers_for_sheet (when available); fallback per-page defaults.
- Token guard via X-APP-TOKEN (APP_TOKEN / BACKUP_APP_TOKEN). If no token is set => open.
- Header-driven row mapping + computed 52W position + Riyadh timestamp fill.
- Plan-aligned: Current + Historical + Forecast/Expected + Valuation + Overall + Recommendation.

v4.4.4 changes (headers + forecast/reco clarity)
- ✅ Per-page customized fallback headers (MARKET_LEADERS / KSA_TADAWUL / GLOBAL_MARKETS / MUTUAL_FUNDS / COMMODITIES_FX / MY_PORTFOLIO / INSIGHTS_ANALYSIS).
- ✅ Forecast ROI/Expected Return coercion fixed: handles 0..1 ratios correctly (prevents wrong forecast prices).
- ✅ Badges are now auto-derived when engine doesn’t provide them (Rec/Momentum/Opportunity/Risk badges).
- ✅ Adds Risk Bucket / Confidence Bucket header aliases (common in your schema/tests).
- ✅ Minor completeness: adds Turnover % to fallback schema; improves market inference.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
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

AI_ANALYSIS_VERSION = "4.4.4"
router = APIRouter(prefix="/v1/analysis", tags=["AI & Analysis"])


# =============================================================================
# Settings shim (safe)
# =============================================================================
try:
    from core.config import get_settings  # type: ignore
except Exception:  # pragma: no cover
    try:
        from config import get_settings  # type: ignore
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
            if isinstance(h, (list, tuple)) and len(h) >= 10:
                return tuple(str(x) for x in h)
        except Exception:
            return None
    return None


@lru_cache(maxsize=4096)
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
    """Canonical enum: BUY / HOLD / REDUCE / SELL."""
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


def _ensure_reco_on_obj(uq: Any) -> None:
    """Force recommendation enum on dict-like or attribute objects. Never raises."""
    try:
        if uq is None:
            return
        if isinstance(uq, dict):
            uq["recommendation"] = _normalize_recommendation(uq.get("recommendation"))
        else:
            setattr(uq, "recommendation", _normalize_recommendation(getattr(uq, "recommendation", None)))
    except Exception:
        pass


# =============================================================================
# COMPAT PATCHES (fix: normalize_recommendation(default=...))
# =============================================================================
@lru_cache(maxsize=1)
def _apply_compat_patches() -> bool:
    """
    Fixes:
      normalize_recommendation() got an unexpected keyword argument 'default'

    We patch any found `normalize_recommendation` function in likely core modules to accept:
      (x, default="HOLD", *args, **kwargs)

    Safe: wrapped with try/except, never raises, and only patches if needed.
    """
    module_names = (
        "core.data_engine_v2",
        "core.scoring_engine",
        "core.utils",
        "core.recommendations",
        "core.recommendation",
        "core.providers.utils",
    )

    patched_any = False

    def _wrap_if_needed(fn: Any) -> Optional[Any]:
        try:
            sig = inspect.signature(fn)
            params = sig.parameters
            if "default" in params:
                return None
            if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
                return None
        except Exception:
            return None

        def _wrapped(x: Any, default: str = "HOLD", *args: Any, **kwargs: Any) -> Any:
            try:
                return fn(x)
            except TypeError:
                if x is None or str(x).strip() == "":
                    return default
                try:
                    return fn(x)
                except Exception:
                    return default
            except Exception:
                if x is None or str(x).strip() == "":
                    return default
                return "HOLD"

        try:
            _wrapped.__name__ = getattr(fn, "__name__", "normalize_recommendation")
        except Exception:
            pass
        return _wrapped

    for mn in module_names:
        try:
            mod = __import__(mn, fromlist=["*"])
        except Exception:
            continue

        try:
            fn = getattr(mod, "normalize_recommendation", None)
            if callable(fn):
                w = _wrap_if_needed(fn)
                if w is not None:
                    setattr(mod, "normalize_recommendation", w)
                    patched_any = True
        except Exception:
            pass

        try:
            for cls_name in ("DataEngine", "ScoringEngine"):
                cls = getattr(mod, cls_name, None)
                if cls and hasattr(cls, "normalize_recommendation"):
                    m = getattr(cls, "normalize_recommendation")
                    if callable(m):
                        w = _wrap_if_needed(m)
                        if w is not None:
                            setattr(cls, "normalize_recommendation", staticmethod(w))  # type: ignore
                            patched_any = True
        except Exception:
            pass

    if patched_any:
        logger.warning("[analysis] Applied compat patch for normalize_recommendation(default=...).")
    return patched_any


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
    if any(ch in s for ch in ("^", "=")):
        return s
    if s.isdigit():
        return f"{s}.SR"
    return s


@lru_cache(maxsize=1)
def _try_import_v2_symbols() -> Tuple[Optional[Any], Optional[Any], Any]:
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
        return _fallback_normalize(raw)


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

    # IMPORTANT: do NOT warn here; health checks may call often.
    # We report "auth": "open" in /health response instead.
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
                _apply_compat_patches()
                DE, _, _ = _try_import_v2_symbols()
                _ENGINE = None if DE is None else DE()
                if _ENGINE is not None:
                    logger.info("[analysis] DataEngine initialized (fallback singleton).")
            except Exception as exc:
                logger.exception("[analysis] Failed to init DataEngine: %s", exc)
                _ENGINE = None
    return _ENGINE


async def _resolve_engine(request: Optional[Request]) -> Optional[Any]:
    _apply_compat_patches()
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
        from zoneinfo import ZoneInfo

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
    if _PYDANTIC_V2 and ConfigDict is not None:
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
    sheetName: Optional[str] = None


class BatchAnalysisResponse(_ExtraIgnoreBase):
    results: List[SingleAnalysisResponse] = Field(default_factory=list)


class SheetAnalysisResponse(_ExtraIgnoreBase):
    headers: List[str] = Field(default_factory=list)
    rows: List[List[Any]] = Field(default_factory=list)
    status: str = "success"
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
            uq = fn()
    except Exception:
        pass
    _ensure_reco_on_obj(uq)
    return uq


def _make_placeholder(symbol: str, *, dq: str = "MISSING", err: str = "No data") -> Dict[str, Any]:
    sym = (symbol or "").strip().upper() or "UNKNOWN"
    last_utc = _now_utc_iso()
    last_riy = _to_riyadh_iso(last_utc) or ""
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
        "eps_ttm": None,
        "forward_eps": None,
        "pe_ttm": None,
        "forward_pe": None,
        "pb": None,
        "ps": None,
        "ev_ebitda": None,
        "dividend_yield": None,
        "dividend_rate": None,
        "payout_ratio": None,
        "roe": None,
        "roa": None,
        "net_margin": None,
        "ebitda_margin": None,
        "revenue_growth": None,
        "net_income_growth": None,
        "beta": None,
        "volatility_30d": None,
        "rsi14": None,
        "ma20": None,
        "ma50": None,
        "ma200": None,
        "returns_1w": None,
        "returns_1m": None,
        "returns_3m": None,
        "returns_6m": None,
        "returns_12m": None,
        "expected_return_1m": None,
        "expected_return_3m": None,
        "expected_return_12m": None,
        "expected_price_1m": None,
        "expected_price_3m": None,
        "expected_price_12m": None,
        "forecast_price_1m": None,
        "forecast_price_3m": None,
        "forecast_price_12m": None,
        "expected_roi_1m": None,
        "expected_roi_3m": None,
        "expected_roi_12m": None,
        "forecast_confidence": None,
        "forecast_updated_utc": None,
        "forecast_updated_riyadh": None,
        "confidence_score": None,
        "forecast_method": None,
        "history_points": None,
        "history_source": None,
        "history_last_utc": None,
        "value_score": None,
        "quality_score": None,
        "momentum_score": None,
        "opportunity_score": None,
        "risk_score": None,
        "overall_score": None,
        "fair_value": None,
        "upside_percent": None,
        "valuation_label": None,
        "rec_badge": None,
        "momentum_badge": None,
        "opportunity_badge": None,
        "risk_badge": None,
        "risk_bucket": None,
        "confidence_bucket": None,
        "recommendation": "HOLD",
        "confidence": None,
        "data_source": "none",
        "data_quality": dq,
        "error": err,
        "status": "error",
        "last_updated_utc": last_utc,
        "last_updated_riyadh": last_riy,
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


# =============================================================================
# Derived metrics from history_points (when available)
# =============================================================================
def _history_parse_points(hp: Any) -> Tuple[List[float], List[float], Optional[str]]:
    closes: List[Tuple[float, float]] = []
    vols: List[Tuple[float, float]] = []
    last_dt: Optional[datetime] = None

    if not hp:
        return [], [], None

    try:
        if isinstance(hp, dict):
            for k in ("points", "data", "bars", "history", "result"):
                if k in hp:
                    hp = hp[k]
                    break
    except Exception:
        pass

    if not isinstance(hp, list):
        return [], [], None

    for p in hp:
        try:
            t_epoch: Optional[float] = None
            c_val: Optional[float] = None
            v_val: Optional[float] = None

            if isinstance(p, dict):
                t_raw = p.get("t") or p.get("time") or p.get("timestamp") or p.get("date")
                if t_raw is not None:
                    if isinstance(t_raw, (int, float)):
                        t_epoch = float(t_raw)
                    else:
                        dt = _parse_iso_dt(t_raw)
                        if dt:
                            t_epoch = dt.timestamp()
                            if last_dt is None or dt > last_dt:
                                last_dt = dt

                for ck in ("close", "c", "adjClose", "adj_close", "close_adj", "cAdj", "Close"):
                    if ck in p and p[ck] is not None:
                        c_val = _safe_float_or_none(p[ck])
                        break

                for vk in ("volume", "v", "vol", "Volume"):
                    if vk in p and p[vk] is not None:
                        v_val = _safe_float_or_none(p[vk])
                        break

            elif isinstance(p, (list, tuple)) and len(p) >= 5:
                t_raw = p[0]
                if isinstance(t_raw, (int, float)):
                    t_epoch = float(t_raw)
                else:
                    dt = _parse_iso_dt(t_raw)
                    if dt:
                        t_epoch = dt.timestamp()
                        if last_dt is None or dt > last_dt:
                            last_dt = dt

                c_val = _safe_float_or_none(p[4])
                if len(p) >= 6:
                    v_val = _safe_float_or_none(p[5])

            if t_epoch is None:
                continue
            if c_val is not None:
                closes.append((t_epoch, c_val))
            if v_val is not None:
                vols.append((t_epoch, v_val))
        except Exception:
            continue

    closes.sort(key=lambda x: x[0])
    vols.sort(key=lambda x: x[0])

    closes_f = [c for _, c in closes]
    vols_f = [v for _, v in vols]

    if last_dt is None and closes:
        try:
            last_dt = datetime.fromtimestamp(closes[-1][0], tz=timezone.utc)
        except Exception:
            last_dt = None

    last_iso = last_dt.astimezone(timezone.utc).isoformat() if last_dt else None
    return closes_f, vols_f, last_iso


def _mean(xs: List[float]) -> Optional[float]:
    xs = [x for x in xs if x is not None and math.isfinite(x)]
    if not xs:
        return None
    return sum(xs) / float(len(xs))


def _sma(xs: List[float], n: int) -> Optional[float]:
    if n <= 0 or not xs or len(xs) < n:
        return None
    return _mean(xs[-n:])


def _returns_from_close(xs: List[float], days: int) -> Optional[float]:
    if not xs or len(xs) <= days:
        return None
    a = xs[-1]
    b = xs[-(days + 1)]
    if b is None or b == 0:
        return None
    try:
        return ((a / b) - 1.0) * 100.0
    except Exception:
        return None


def _vol_30d_ann(xs: List[float]) -> Optional[float]:
    if not xs or len(xs) < 31:
        return None
    closes = xs[-31:]
    rets: List[float] = []
    for i in range(1, len(closes)):
        a = closes[i - 1]
        b = closes[i]
        if a is None or a == 0:
            continue
        try:
            r = (b / a) - 1.0
            if math.isfinite(r):
                rets.append(r)
        except Exception:
            continue
    if len(rets) < 10:
        return None
    m = sum(rets) / float(len(rets))
    var = sum((r - m) ** 2 for r in rets) / float(max(1, len(rets) - 1))
    sd = math.sqrt(var)
    ann = sd * math.sqrt(252.0)
    return round(ann * 100.0, 4)


def _rsi_14(xs: List[float]) -> Optional[float]:
    if not xs or len(xs) < 15:
        return None
    closes = xs[-(14 + 1) :]
    gains = 0.0
    losses = 0.0
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        if diff > 0:
            gains += diff
        else:
            losses -= diff
    if gains + losses == 0:
        return None
    avg_gain = gains / 14.0
    avg_loss = losses / 14.0
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return round(rsi, 2)


def _derive_from_history(uq: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    hp = _safe_get(uq, "history_points", "history", "chart_points", "price_history", "bars")
    closes, vols, last_iso = _history_parse_points(hp)

    if closes:
        if len(closes) >= 2:
            out["prev close"] = closes[-2]
            out["previous close"] = closes[-2]

        out["ma20"] = _sma(closes, 20)
        out["ma50"] = _sma(closes, 50)
        out["ma200"] = _sma(closes, 200)

        out["returns 1w %"] = _returns_from_close(closes, 5)
        out["returns 1m %"] = _returns_from_close(closes, 21)
        out["returns 3m %"] = _returns_from_close(closes, 63)
        out["returns 6m %"] = _returns_from_close(closes, 126)
        out["returns 12m %"] = _returns_from_close(closes, 252)

        out["rsi (14)"] = _rsi_14(closes)
        out["rsi 14"] = out["rsi (14)"]  # alias
        out["volatility 30d"] = _vol_30d_ann(closes)
        out["volatility (30d)"] = out["volatility 30d"]

        look = closes[-252:] if len(closes) >= 252 else closes
        try:
            out["52w high"] = max(look) if look else None
            out["52w low"] = min(look) if look else None
        except Exception:
            pass

    if vols:
        take = vols[-30:] if len(vols) >= 30 else vols
        avg = _mean(take)
        if avg is not None:
            out["avg vol 30d"] = avg
            out["avg volume (30d)"] = avg
        out["volume"] = vols[-1]

    if isinstance(hp, list):
        out["history points"] = len(hp)
    elif isinstance(hp, dict):
        for k in ("points", "data", "bars", "history"):
            if isinstance(hp.get(k), list):
                out["history points"] = len(hp[k])
                break

    if last_iso:
        out["history last (utc)"] = last_iso

    if hp:
        out["history source"] = _safe_get(uq, "history_source") or "history_points"

    return out


# =============================================================================
# Cross-field derivations (fallbacks)
# =============================================================================
def _compute_change_and_pct(price: Any, prev_close: Any) -> Tuple[Optional[float], Optional[float]]:
    p = _safe_float_or_none(price)
    pc = _safe_float_or_none(prev_close)
    if p is None or pc is None:
        return None, None
    ch = p - pc
    if pc == 0:
        return round(ch, 6), None
    pct = (ch / pc) * 100.0
    return round(ch, 6), round(pct, 4)


def _compute_market_cap(price: Any, shares_out: Any) -> Optional[float]:
    p = _safe_float_or_none(price)
    so = _safe_float_or_none(shares_out)
    if p is None or so is None:
        return None
    if p <= 0 or so <= 0:
        return None
    return round(p * so, 2)


def _compute_free_float_mkt_cap(market_cap: Any, free_float_pct: Any) -> Optional[float]:
    mc = _safe_float_or_none(market_cap)
    ff = _safe_float_or_none(free_float_pct)
    if mc is None or ff is None:
        return None
    if 0.0 <= ff <= 1.0:
        return round(mc * ff, 2)
    if 1.0 < ff <= 100.0:
        return round(mc * (ff / 100.0), 2)
    return None


def _derive_cross_fields(uq: Any, derived: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    price = _safe_get(uq, "current_price", "last_price", "price")
    prev = _safe_get(uq, "previous_close", "prev_close") or derived.get("prev close") or derived.get("previous close")

    ch = _safe_get(uq, "price_change", "change")
    pct = _safe_get(uq, "percent_change", "change_pct", "change_percent")
    if ch is None or pct is None:
        ch2, pct2 = _compute_change_and_pct(price, prev)
        if ch is None and ch2 is not None:
            out["change"] = ch2
            out["price change"] = ch2
        if pct is None and pct2 is not None:
            out["change %"] = pct2
            out["percent change"] = pct2

    mc = _safe_get(uq, "market_cap")
    so = _safe_get(uq, "shares_outstanding", "shares", "shares_out")
    if mc is None:
        mc2 = _compute_market_cap(price, so)
        if mc2 is not None:
            out["market cap"] = mc2

    vt = _safe_get(uq, "value_traded")
    vol = _safe_get(uq, "volume") or derived.get("volume")
    if vt is None:
        p = _safe_float_or_none(price)
        v = _safe_float_or_none(vol)
        if p is not None and v is not None and p > 0 and v > 0:
            out["value traded"] = round(p * v, 2)

    ffmc = _safe_get(uq, "free_float_market_cap", "free_float_mkt_cap")
    ff = _safe_get(uq, "free_float", "free_float_percent")
    if ffmc is None:
        mc_src = mc if mc is not None else out.get("market cap")
        ffmc2 = _compute_free_float_mkt_cap(mc_src, ff)
        if ffmc2 is not None:
            out["free float mkt cap"] = ffmc2
            out["free float market cap"] = ffmc2

    pos = _safe_get(uq, "position_52w_percent", "position_52w")
    if pos is None:
        lo = _safe_get(uq, "week_52_low", "low_52w") or derived.get("52w low")
        hi = _safe_get(uq, "week_52_high", "high_52w") or derived.get("52w high")
        pos2 = _compute_52w_position_pct(price, lo, hi)
        if pos2 is not None:
            out["52w position %"] = pos2

    return out


# =============================================================================
# Forecast helpers
# =============================================================================
def _coerce_confidence_to_0_100(v: Any) -> Optional[float]:
    x = _safe_float_or_none(v)
    if x is None:
        return None
    if 0.0 <= x <= 1.0:
        return round(x * 100.0, 2)
    if 1.0 < x <= 100.0:
        return round(x, 2)
    return None


def _pct_float(x: Any) -> Optional[float]:
    """Coerce ratios or percents into a percent float (e.g., 0.14 -> 14.0; 14 -> 14.0)."""
    y = _ratio_to_percent(x)
    try:
        return None if y is None or y == "" else float(y)
    except Exception:
        return None


def _forecast_fill(uq: Any, derived: Dict[str, Any]) -> Dict[str, Any]:
    """
    Forecast columns for Sheets.
    IMPORTANT: ROI/Expected Return may arrive as ratios (0..1). We normalize to percent.
    """
    out: Dict[str, Any] = {}

    price = _safe_float_or_none(_safe_get(uq, "current_price", "last_price", "price"))
    if price is None:
        return out

    roi_1m = _pct_float(_safe_get(uq, "expected_roi_1m", "expected_return_1m"))
    roi_3m = _pct_float(_safe_get(uq, "expected_roi_3m", "expected_return_3m"))
    roi_12m = _pct_float(_safe_get(uq, "expected_roi_12m", "expected_return_12m"))

    # fallback to derived historical returns (already in %)
    if roi_1m is None:
        roi_1m = _safe_float_or_none(derived.get("returns 1m %"))
    if roi_3m is None:
        roi_3m = _safe_float_or_none(derived.get("returns 3m %"))
    if roi_12m is None:
        roi_12m = _safe_float_or_none(derived.get("returns 12m %"))

    fp1 = _safe_float_or_none(_safe_get(uq, "forecast_price_1m", "expected_price_1m"))
    fp3 = _safe_float_or_none(_safe_get(uq, "forecast_price_3m", "expected_price_3m"))
    fp12 = _safe_float_or_none(_safe_get(uq, "forecast_price_12m", "expected_price_12m"))

    # If no forecast prices, compute from ROI% (now correct even if ROI came as ratio)
    if fp1 is None and roi_1m is not None:
        fp1 = round(price * (1.0 + roi_1m / 100.0), 6)
    if fp3 is None and roi_3m is not None:
        fp3 = round(price * (1.0 + roi_3m / 100.0), 6)
    if fp12 is None and roi_12m is not None:
        fp12 = round(price * (1.0 + roi_12m / 100.0), 6)

    out["forecast price (1m)"] = fp1
    out["forecast price (3m)"] = fp3
    out["forecast price (12m)"] = fp12

    # Both “Expected ROI” and “Expected Return” are filled consistently (clarity)
    out["expected roi % (1m)"] = roi_1m
    out["expected roi % (3m)"] = roi_3m
    out["expected roi % (12m)"] = roi_12m

    out["expected return 1m %"] = roi_1m
    out["expected return 3m %"] = roi_3m
    out["expected return 12m %"] = roi_12m

    conf = _coerce_confidence_to_0_100(_safe_get(uq, "forecast_confidence", "confidence_score", "confidence"))
    out["forecast confidence"] = conf

    futc = (
        _safe_get(uq, "forecast_updated_utc")
        or _safe_get(uq, "last_updated_utc", "as_of_utc")
        or derived.get("history last (utc)")
        or _now_utc_iso()
    )
    out["forecast updated (utc)"] = _iso_or_none(futc) or futc

    friy = _safe_get(uq, "forecast_updated_riyadh")
    if not friy:
        friy = _to_riyadh_iso(futc) or ""
    out["forecast updated (riyadh)"] = _iso_or_none(friy) or friy

    return out


# =============================================================================
# Fair value / overall (simple)
# =============================================================================
def _compute_fair_value_and_upside(uq: Any) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    price = _safe_float_or_none(_safe_get(uq, "current_price", "last_price", "price"))
    if price is None or price <= 0:
        return None, None, None

    fair = _safe_float_or_none(_safe_get(uq, "fair_value"))
    if fair is None:
        fair = _safe_float_or_none(_safe_get(uq, "expected_price_3m", "forecast_price_3m"))
    if fair is None:
        fair = _safe_float_or_none(_safe_get(uq, "expected_price_12m", "forecast_price_12m"))
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


def _risk_bucket_from_score(risk_score: Optional[float]) -> Optional[str]:
    if risk_score is None:
        return None
    try:
        r = float(risk_score)
    except Exception:
        return None
    if r < 35:
        return "Low"
    if r < 70:
        return "Moderate"
    return "High"


def _confidence_bucket_from_conf(conf_score_any: Any) -> Optional[str]:
    if conf_score_any is None:
        return None
    try:
        x = float(conf_score_any)
        if 0.0 <= x <= 1.0:
            x = x * 100.0
        if x >= 70:
            return "High"
        if x >= 45:
            return "Moderate"
        return "Low"
    except Exception:
        return None


def _compute_badges(uq: Any, derived: Dict[str, Any], overall_pack: Optional[Tuple[Any, Any, Any]]) -> Dict[str, Any]:
    """
    Fill badges if engine didn’t provide them.
    Output keys are header-keys (lowercase) to match hk comparisons.
    """
    out: Dict[str, Any] = {}

    reco = _normalize_recommendation(_safe_get(uq, "recommendation")) or "HOLD"
    if overall_pack:
        _overall, reco_calc, _conf01 = overall_pack
        if not _safe_get(uq, "recommendation"):
            reco = _normalize_recommendation(reco_calc)

    # Rec badge
    if not _safe_get(uq, "rec_badge"):
        badge = {"BUY": "✅ BUY", "HOLD": "🟡 HOLD", "REDUCE": "🟠 REDUCE", "SELL": "🔴 SELL"}.get(reco, "🟡 HOLD")
        out["rec badge"] = badge

    # Momentum badge (prefer RSI if available; else momentum_score)
    if not _safe_get(uq, "momentum_badge"):
        rsi = _safe_float_or_none(_safe_get(uq, "rsi14", "rsi_14")) or _safe_float_or_none(derived.get("rsi (14)"))
        mom_score = _safe_float_or_none(_safe_get(uq, "momentum_score"))
        if rsi is not None:
            if rsi >= 70:
                out["momentum badge"] = "⚠️ Overbought"
            elif rsi <= 30:
                out["momentum badge"] = "✅ Oversold"
            else:
                out["momentum badge"] = "↔️ Neutral"
        elif mom_score is not None:
            if mom_score >= 70:
                out["momentum badge"] = "📈 Strong"
            elif mom_score >= 45:
                out["momentum badge"] = "↗️ Moderate"
            else:
                out["momentum badge"] = "↘️ Weak"

    # Opportunity badge
    if not _safe_get(uq, "opportunity_badge"):
        opp = _safe_float_or_none(_safe_get(uq, "opportunity_score"))
        if opp is not None:
            if opp >= 80:
                out["opportunity badge"] = "⭐ Top Pick"
            elif opp >= 60:
                out["opportunity badge"] = "👀 Watch"
            else:
                out["opportunity badge"] = "—"

    # Risk badge + bucket
    if not _safe_get(uq, "risk_badge"):
        risk = _safe_float_or_none(_safe_get(uq, "risk_score"))
        bucket = _risk_bucket_from_score(risk)
        if bucket:
            out["risk badge"] = {"Low": "🟢 Low", "Moderate": "🟠 Moderate", "High": "🔴 High"}.get(bucket, bucket)
            if not _safe_get(uq, "risk_bucket"):
                out["risk bucket"] = bucket

    # Confidence bucket
    if not _safe_get(uq, "confidence_bucket"):
        conf_any = _safe_get(uq, "confidence_score", "confidence")
        bucket = _confidence_bucket_from_conf(conf_any)
        if bucket:
            out["confidence bucket"] = bucket

    return out


# =============================================================================
# Headers – safe completion for missing “required” dashboard columns
# =============================================================================
_REQUIRED_DASHBOARD_COLS: Tuple[str, ...] = (
    "Forecast Price (1M)",
    "Expected ROI % (1M)",
    "Forecast Price (3M)",
    "Expected ROI % (3M)",
    "Forecast Price (12M)",
    "Expected ROI % (12M)",
    "Forecast Confidence",
    "Forecast Updated (UTC)",
    "Forecast Updated (Riyadh)",
    "Volatility 30D",
    "MA20",
    "MA50",
    "MA200",
    "Rec Badge",
    "Momentum Badge",
    "Opportunity Badge",
    "Risk Badge",
    "Rank",
    "Origin",
)

_DEFAULT_HEADERS_BASE: List[str] = [
    "Rank",
    "Symbol",
    "Origin",
    "Name",
    "Sector",
    "Sub Sector",
    "Market",
    "Currency",
    "Listing Date",
    "Price",
    "Prev Close",
    "Change",
    "Change %",
    "Day High",
    "Day Low",
    "52W High",
    "52W Low",
    "52W Position %",
    "Volume",
    "Avg Vol 30D",
    "Value Traded",
    "Turnover %",
    "Liquidity Score",
    "Shares Outstanding",
    "Free Float %",
    "Market Cap",
    "Free Float Mkt Cap",
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
    "RSI (14)",
    "Risk Bucket",
    "Confidence Bucket",
    "Fair Value",
    "Upside %",
    "Valuation Label",
    "Value Score",
    "Quality Score",
    "Momentum Score",
    "Opportunity Score",
    "Risk Score",
    "Overall Score",
    "Rec Badge",
    "Momentum Badge",
    "Opportunity Badge",
    "Risk Badge",
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
    "Volatility 30D",
    "Forecast Price (1M)",
    "Expected ROI % (1M)",
    "Forecast Price (3M)",
    "Expected ROI % (3M)",
    "Forecast Price (12M)",
    "Expected ROI % (12M)",
    "Forecast Confidence",
    "Forecast Updated (UTC)",
    "Forecast Updated (Riyadh)",
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

# Per-page customized fallback headers (used only if core.schemas is unavailable)
_HEADERS_BY_SHEET_FALLBACK: Dict[str, List[str]] = {
    "MARKET_LEADERS": list(_DEFAULT_HEADERS_EXTENDED),
    "KSA_TADAWUL": list(_DEFAULT_HEADERS_EXTENDED),
    "GLOBAL_MARKETS": list(_DEFAULT_HEADERS_EXTENDED),
    "MY_PORTFOLIO": list(_DEFAULT_HEADERS_EXTENDED),
    "INSIGHTS_ANALYSIS": list(_DEFAULT_HEADERS_EXTENDED),
    # Funds/Commodities are typically lighter (still include forecast block for your dashboard)
    "MUTUAL_FUNDS": _append_missing_columns(
        [
            "Rank",
            "Symbol",
            "Origin",
            "Name",
            "Market",
            "Currency",
            "Price",
            "Prev Close",
            "Change",
            "Change %",
            "Volume",
            "Value Traded",
            "Risk Bucket",
            "Confidence Bucket",
            "Rec Badge",
            "Recommendation",
            "Overall Score",
            "Forecast Price (1M)",
            "Expected ROI % (1M)",
            "Forecast Price (3M)",
            "Expected ROI % (3M)",
            "Forecast Price (12M)",
            "Expected ROI % (12M)",
            "Forecast Confidence",
            "Forecast Updated (UTC)",
            "Forecast Updated (Riyadh)",
            "Data Source",
            "Data Quality",
            "Last Updated (UTC)",
            "Last Updated (Riyadh)",
            "Error",
        ],
        _REQUIRED_DASHBOARD_COLS,
    ),
    "COMMODITIES_FX": _append_missing_columns(
        [
            "Rank",
            "Symbol",
            "Origin",
            "Name",
            "Market",
            "Currency",
            "Price",
            "Prev Close",
            "Change",
            "Change %",
            "Day High",
            "Day Low",
            "52W High",
            "52W Low",
            "52W Position %",
            "Volatility 30D",
            "MA20",
            "MA50",
            "MA200",
            "Rec Badge",
            "Recommendation",
            "Overall Score",
            "Forecast Price (1M)",
            "Expected ROI % (1M)",
            "Forecast Price (3M)",
            "Expected ROI % (3M)",
            "Forecast Price (12M)",
            "Expected ROI % (12M)",
            "Forecast Confidence",
            "Forecast Updated (UTC)",
            "Forecast Updated (Riyadh)",
            "Data Source",
            "Data Quality",
            "Last Updated (UTC)",
            "Last Updated (Riyadh)",
            "Error",
        ],
        _REQUIRED_DASHBOARD_COLS,
    ),
}


def _append_missing_columns(base: List[str], extras: Sequence[str]) -> List[str]:
    base_ci = {str(x).strip().lower() for x in base}
    out = list(base)
    for e in extras:
        if str(e).strip().lower() not in base_ci:
            out.append(e)
            base_ci.add(str(e).strip().lower())
    return out


def _sheet_key(sheet_name: Optional[str]) -> str:
    sn = (sheet_name or "").strip().upper()
    if not sn:
        return ""
    # normalize some common variants
    if sn in ("MARKET_LEADERS", "MARKETLEADERS"):
        return "MARKET_LEADERS"
    if sn in ("KSA_TADAWUL", "KSA", "TADAWUL", "KSA-TADAWUL"):
        return "KSA_TADAWUL"
    if sn in ("GLOBAL_MARKETS", "GLOBAL", "GLOBAL-MARKETS"):
        return "GLOBAL_MARKETS"
    if sn in ("MUTUAL_FUNDS", "FUNDS", "MUTUAL-FUNDS"):
        return "MUTUAL_FUNDS"
    if sn in ("COMMODITIES_FX", "COMMODITIES", "FX", "COMMODITIES-FX"):
        return "COMMODITIES_FX"
    if sn in ("MY_PORTFOLIO", "PORTFOLIO"):
        return "MY_PORTFOLIO"
    if sn in ("INSIGHTS_ANALYSIS", "INSIGHTS", "ANALYSIS"):
        return "INSIGHTS_ANALYSIS"
    return sn


def _select_headers(sheet_name: Optional[str], mode: Optional[str]) -> List[str]:
    m = (mode or "").strip().lower()

    # 1) Canonical schemas (preferred)
    h = _schemas_get_headers(sheet_name)
    if h and len(h) >= 10:
        hh = list(h)
        hh = _append_missing_columns(hh, _REQUIRED_DASHBOARD_COLS)
        if m in ("ext", "extended", "full"):
            hh = _append_missing_columns(hh, _DEFAULT_HEADERS_EXTENDED)
        return hh

    # 2) Per-page customized fallbacks (requested)
    sk = _sheet_key(sheet_name)
    if sk and sk in _HEADERS_BY_SHEET_FALLBACK:
        base = list(_HEADERS_BY_SHEET_FALLBACK[sk])
        if m in ("ext", "extended", "full"):
            base = _append_missing_columns(base, _DEFAULT_HEADERS_EXTENDED)
        return _append_missing_columns(base, _REQUIRED_DASHBOARD_COLS)

    # 3) Generic fallbacks
    if m in ("ext", "extended", "full"):
        return list(_DEFAULT_HEADERS_EXTENDED)

    sn = (sheet_name or "").strip().lower()
    if any(k in sn for k in ("global", "insight", "opportun", "advisor", "analysis", "portfolio", "ksa", "tadawul")):
        return list(_DEFAULT_HEADERS_EXTENDED)

    return _append_missing_columns(list(_DEFAULT_HEADERS_BASE), _REQUIRED_DASHBOARD_COLS)


def _idx(headers: List[str], name: str) -> Optional[int]:
    k = name.strip().lower()
    for i, h in enumerate(headers):
        if str(h).strip().lower() == k:
            return i
    return None


def _error_row(symbol: str, headers: List[str], err: str, *, rank: Optional[int] = None, origin: Optional[str] = None) -> List[Any]:
    row = [None] * len(headers)

    i_rank = _idx(headers, "Rank")
    if i_rank is not None and rank is not None:
        row[i_rank] = rank

    i_origin = _idx(headers, "Origin")
    if i_origin is not None and origin:
        row[i_origin] = origin

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

    nowu = _now_utc_iso()
    i_last_utc = _idx(headers, "Last Updated (UTC)")
    if i_last_utc is not None:
        row[i_last_utc] = nowu

    i_last_riy = _idx(headers, "Last Updated (Riyadh)")
    if i_last_riy is not None:
        row[i_last_riy] = _to_riyadh_iso(nowu)

    return row


def _status_from_rows(headers: List[str], rows: List[List[Any]]) -> str:
    i_err = _idx(headers, "Error")
    i_dq = _idx(headers, "Data Quality")

    any_err = False
    any_missing = False

    for r in rows:
        if i_err is not None and len(r) > i_err and str(r[i_err] or "").strip():
            any_err = True
        if i_dq is not None and len(r) > i_dq:
            dq = str(r[i_dq] or "").strip().upper()
            if dq in ("MISSING", "BAD", "ERROR"):
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
_RATIO_HEADERS = {
    "change %",
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
    "expected roi % (1m)",
    "expected roi % (3m)",
    "expected roi % (12m)",
    "52w position %",
    "turnover %",
}

_VOL_HEADERS = {"volatility 30d", "volatility (30d)"}

_LOCAL_HEADER_MAP: Dict[str, Tuple[Tuple[str, ...], Optional[Any]]] = {
    "rank": (("rank",), None),
    "origin": (("origin",), None),
    "symbol": (("symbol",), None),
    "name": (("name", "company_name"), None),
    "company name": (("name", "company_name"), None),
    "sector": (("sector", "sector_name"), None),
    "sub sector": (("sub_sector", "subsector", "sub_sector_name"), None),
    "market": (("market", "market_region"), None),
    "currency": (("currency",), None),
    "listing date": (("listing_date", "ipo", "ipo_date"), None),
    "price": (("current_price", "last_price", "price", "last"), None),
    "prev close": (("previous_close", "prev_close"), None),
    "change": (("price_change", "change"), None),
    "change %": (("percent_change", "change_pct", "change_percent"), _ratio_to_percent),
    "day high": (("day_high", "high"), None),
    "day low": (("day_low", "low"), None),
    "52w high": (("week_52_high", "high_52w", "52w_high"), None),
    "52w low": (("week_52_low", "low_52w", "52w_low"), None),
    "52w position %": (("position_52w_percent", "position_52w"), _ratio_to_percent),
    "volume": (("volume", "vol"), None),
    "avg vol 30d": (("avg_volume_30d", "avg_volume", "avg_vol_30d"), None),
    "value traded": (("value_traded", "value", "turnover_value"), None),
    "turnover %": (("turnover_percent", "turnover", "turnover_pct"), _ratio_to_percent),
    "liquidity score": (("liquidity_score",), None),
    "shares outstanding": (("shares_outstanding", "shares", "shares_out"), None),
    "free float %": (("free_float", "free_float_percent"), _ratio_to_percent),
    "market cap": (("market_cap",), None),
    "free float mkt cap": (("free_float_market_cap", "free_float_mkt_cap"), None),
    "eps (ttm)": (("eps_ttm", "eps"), None),
    "forward eps": (("forward_eps", "eps_forward"), None),
    "p/e (ttm)": (("pe_ttm", "pe"), None),
    "forward p/e": (("forward_pe", "pe_forward"), None),
    "p/b": (("pb",), None),
    "p/s": (("ps",), None),
    "ev/ebitda": (("ev_ebitda", "ev_to_ebitda"), None),
    "dividend yield %": (("dividend_yield", "div_yield"), _ratio_to_percent),
    "dividend yield": (("dividend_yield", "div_yield"), _ratio_to_percent),
    "roe %": (("roe",), _ratio_to_percent),
    "roe": (("roe",), _ratio_to_percent),
    "roa %": (("roa",), _ratio_to_percent),
    "roa": (("roa",), _ratio_to_percent),
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
    "rec badge": (("rec_badge",), None),
    "momentum badge": (("momentum_badge",), None),
    "opportunity badge": (("opportunity_badge",), None),
    "risk badge": (("risk_badge",), None),
    "risk bucket": (("risk_bucket",), None),
    "confidence bucket": (("confidence_bucket",), None),
    "data source": (("data_source", "source", "provider"), None),
    "data quality": (("data_quality",), None),
    "last updated (utc)": (("last_updated_utc", "as_of_utc"), _iso_or_none),
    "last updated (riyadh)": (("last_updated_riyadh",), _iso_or_none),
    "error": (("error",), None),
    "returns 1w %": (("returns_1w",), _ratio_to_percent),
    "returns 1m %": (("returns_1m",), _ratio_to_percent),
    "returns 3m %": (("returns_3m",), _ratio_to_percent),
    "returns 6m %": (("returns_6m",), _ratio_to_percent),
    "returns 12m %": (("returns_12m",), _ratio_to_percent),
    "rsi (14)": (("rsi14", "rsi_14"), None),
    "rsi 14": (("rsi14", "rsi_14"), None),
    "ma20": (("ma20",), None),
    "ma50": (("ma50",), None),
    "ma200": (("ma200",), None),
    "volatility 30d": (("vol_30d_ann", "volatility_30d", "volatility_30d_ann"), _vol_to_percent),
    "forecast price (1m)": (("forecast_price_1m", "expected_price_1m"), None),
    "forecast price (3m)": (("forecast_price_3m", "expected_price_3m"), None),
    "forecast price (12m)": (("forecast_price_12m", "expected_price_12m"), None),
    "expected roi % (1m)": (("expected_roi_1m", "expected_return_1m"), _ratio_to_percent),
    "expected roi % (3m)": (("expected_roi_3m", "expected_return_3m"), _ratio_to_percent),
    "expected roi % (12m)": (("expected_roi_12m", "expected_return_12m"), _ratio_to_percent),
    "expected return 1m %": (("expected_return_1m", "expected_roi_1m"), _ratio_to_percent),
    "expected return 3m %": (("expected_return_3m", "expected_roi_3m"), _ratio_to_percent),
    "expected return 12m %": (("expected_return_12m", "expected_roi_12m"), _ratio_to_percent),
    "expected price 1m": (("expected_price_1m", "forecast_price_1m"), None),
    "expected price 3m": (("expected_price_3m", "forecast_price_3m"), None),
    "expected price 12m": (("expected_price_12m", "forecast_price_12m"), None),
    "forecast confidence": (("forecast_confidence", "confidence_score", "confidence"), None),
    "forecast updated (utc)": (("forecast_updated_utc", "forecast_updated", "last_updated_utc", "as_of_utc", "history_last_utc"), _iso_or_none),
    "forecast updated (riyadh)": (("forecast_updated_riyadh",), _iso_or_none),
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
    need_val: bool
    need_overall: bool
    need_last_utc: bool
    need_last_riyadh: bool
    needs_history_deriv: bool
    needs_forecast_fill: bool
    needs_badges: bool
    needs_buckets: bool


@lru_cache(maxsize=64)
def _build_header_plan(headers: Tuple[str, ...]) -> _HeaderPlan:
    metas: List[_HeaderMeta] = []
    need_val = False
    need_overall = False
    need_last_utc = False
    need_last_riyadh = False
    needs_history_deriv = False
    needs_forecast_fill = False
    needs_badges = False
    needs_buckets = False

    for h in headers:
        hk = _hkey(h)

        if hk in ("fair value", "upside %", "valuation label"):
            need_val = True
        if hk in ("recommendation", "overall score"):
            need_overall = True
        if hk == "last updated (utc)":
            need_last_utc = True
        if hk == "last updated (riyadh)":
            need_last_riyadh = True

        if hk in (
            "avg vol 30d",
            "prev close",
            "returns 1w %",
            "returns 1m %",
            "returns 3m %",
            "returns 6m %",
            "returns 12m %",
            "ma20",
            "ma50",
            "ma200",
            "rsi (14)",
            "rsi 14",
            "volatility 30d",
            "52w high",
            "52w low",
            "history points",
            "history source",
            "history last (utc)",
        ):
            needs_history_deriv = True

        if hk in (
            "forecast price (1m)",
            "expected roi % (1m)",
            "forecast price (3m)",
            "expected roi % (3m)",
            "forecast price (12m)",
            "expected roi % (12m)",
            "forecast confidence",
            "forecast updated (utc)",
            "forecast updated (riyadh)",
            "expected return 1m %",
            "expected return 3m %",
            "expected return 12m %",
            "expected price 1m",
            "expected price 3m",
            "expected price 12m",
        ):
            needs_forecast_fill = True

        if hk in ("rec badge", "momentum badge", "opportunity badge", "risk badge"):
            needs_badges = True

        if hk in ("risk bucket", "confidence bucket"):
            needs_buckets = True

        schema_field = _schemas_header_to_field_cached(h)
        local = _LOCAL_HEADER_MAP.get(hk)
        lf, lt = (local[0], local[1]) if local else (None, None)

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
        need_val=need_val,
        need_overall=need_overall,
        need_last_utc=need_last_utc,
        need_last_riyadh=need_last_riyadh,
        needs_history_deriv=needs_history_deriv,
        needs_forecast_fill=needs_forecast_fill,
        needs_badges=needs_badges,
        needs_buckets=needs_buckets,
    )


def _is_blank(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str) and not v.strip():
        return True
    return False


def _apply_common_transforms(hk: str, v: Any) -> Any:
    if hk == "recommendation":
        return _normalize_recommendation(v)
    if hk in _RATIO_HEADERS:
        return _ratio_to_percent(v)
    if hk in _VOL_HEADERS:
        return _vol_to_percent(v)
    return v


def _value_for_meta(
    meta: _HeaderMeta,
    uq: Any,
    *,
    row_index_1based: int,
    origin: str,
    derived: Dict[str, Any],
    cross: Dict[str, Any],
    forecast: Dict[str, Any],
    badges: Dict[str, Any],
    fair_pack: Optional[Tuple[Any, Any, Any]],
    overall_pack: Optional[Tuple[Any, Any, Any]],
) -> Any:
    hk = meta.hk

    if hk == "rank":
        v = _safe_get(uq, "rank")
        return v if not _is_blank(v) else row_index_1based

    if hk == "origin":
        v = _safe_get(uq, "origin")
        return v if not _is_blank(v) else origin

    if hk in ("52w position %", "52w position"):
        v = _safe_get(uq, "position_52w_percent", "position_52w")
        if v is not None:
            return _ratio_to_percent(v)
        if hk in cross:
            return cross.get(hk)
        cp = _safe_get(uq, "current_price", "last_price", "price")
        lo = _safe_get(uq, "week_52_low", "low_52w") or derived.get("52w low")
        hi = _safe_get(uq, "week_52_high", "high_52w") or derived.get("52w high")
        return _compute_52w_position_pct(cp, lo, hi)

    if hk == "last updated (utc)":
        v = _safe_get(uq, "last_updated_utc", "as_of_utc") or _now_utc_iso()
        return _iso_or_none(v)

    if hk == "last updated (riyadh)":
        v = _safe_get(uq, "last_updated_riyadh")
        if not v:
            u = _safe_get(uq, "last_updated_utc", "as_of_utc") or _now_utc_iso()
            v = _to_riyadh_iso(u) or ""
        return _iso_or_none(v) or v

    # Forecast fill (derived)
    if hk in forecast and not _is_blank(forecast.get(hk)):
        return _apply_common_transforms(hk, forecast.get(hk))

    # Badges (derived)
    if hk in badges and not _is_blank(badges.get(hk)):
        return badges.get(hk)

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

    # schema mapping
    if meta.schema_field:
        v = _safe_get(uq, meta.schema_field)
        v = _apply_common_transforms(hk, v)
        if not _is_blank(v):
            return v

    # local mapping
    if meta.local_fields:
        val = _safe_get(uq, *meta.local_fields)
        if meta.local_transform and val is not None:
            try:
                val = meta.local_transform(val)
            except Exception:
                pass
        val = _apply_common_transforms(hk, val)
        if not _is_blank(val):
            return val

    if hk in cross and not _is_blank(cross.get(hk)):
        return _apply_common_transforms(hk, cross.get(hk))

    if hk in derived and not _is_blank(derived.get(hk)):
        return _apply_common_transforms(hk, derived.get(hk))

    # guess
    val = _safe_get(uq, meta.guess)
    val = _apply_common_transforms(hk, val)
    if not _is_blank(val):
        return val

    if hk == "data source":
        hs = _safe_get(uq, "history_source")
        if not _is_blank(hs):
            return str(hs)

    if hk == "data quality":
        p = _safe_get(uq, "current_price", "last_price", "price")
        if not _is_blank(p):
            return "OK"
        return "MISSING"

    return None


def _row_from_plan(plan: _HeaderPlan, uq: Any, *, row_index_1based: int, origin: str) -> List[Any]:
    fair_pack: Optional[Tuple[Any, Any, Any]] = None
    overall_pack: Optional[Tuple[Any, Any, Any]] = None

    if plan.need_val:
        fair_pack = _compute_fair_value_and_upside(uq)
    if plan.need_overall or plan.needs_badges or plan.needs_buckets:
        overall_pack = _compute_overall_and_reco(uq)

    derived: Dict[str, Any] = {}
    if plan.needs_history_deriv:
        try:
            derived = _derive_from_history(uq)
        except Exception:
            derived = {}

    cross: Dict[str, Any] = {}
    try:
        cross = _derive_cross_fields(uq, derived)
    except Exception:
        cross = {}

    forecast: Dict[str, Any] = {}
    if plan.needs_forecast_fill:
        try:
            forecast = _forecast_fill(uq, derived)
        except Exception:
            forecast = {}

    # ensure last_updated_riyadh on uq when requested
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

    # badges + buckets (derived) for clarity
    badges: Dict[str, Any] = {}
    if plan.needs_badges or plan.needs_buckets:
        try:
            badges = _compute_badges(uq, derived, overall_pack)
        except Exception:
            badges = {}

    row = [
        _value_for_meta(
            m,
            uq,
            row_index_1based=row_index_1based,
            origin=origin,
            derived=derived,
            cross=cross,
            forecast=forecast,
            badges=badges,
            fair_pack=fair_pack,
            overall_pack=overall_pack,
        )
        for m in plan.metas
    ]
    if len(row) < len(plan.metas):
        row += [None] * (len(plan.metas) - len(row))
    return row[: len(plan.metas)]


# =============================================================================
# Engine calls (defensive batching) + supported-kwargs filter
# =============================================================================
async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


def _unwrap_tuple_payload(x: Any) -> Any:
    if isinstance(x, tuple) and len(x) == 2:
        return x[0]
    return x


def _call_with_supported_kwargs(fn: Any, *args: Any, **kwargs: Any) -> Any:
    if not kwargs:
        return fn(*args)

    try:
        sig = inspect.signature(fn)
        params = sig.parameters
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
            return fn(*args, **kwargs)

        allowed = set(params.keys())
        filtered = {k: v for k, v in kwargs.items() if k in allowed}
        return fn(*args, **filtered)
    except Exception:
        return fn(*args, **kwargs)


def _call_batch_best_effort(engine: Any, fn_name: str, syms: List[str], *, ctx: Optional[Dict[str, Any]] = None) -> Any:
    fn = getattr(engine, fn_name, None)
    if not callable(fn):
        return None

    ctx = ctx or {}
    ctx_try = {
        "sheet_name": ctx.get("sheet_name"),
        "sheet": ctx.get("sheet_name"),
        "page_key": ctx.get("page_key"),
        "page": ctx.get("page_key") or ctx.get("sheet_name"),
        "market": ctx.get("market"),
        "region": ctx.get("market") or ctx.get("region"),
    }
    ctx_try = {k: v for k, v in ctx_try.items() if v not in (None, "", [])}

    kw_attempts = [
        {"refresh": False, "include_history": True, "include_fundamentals": True},
        {"refresh": False, "include_history": True},
        {"refresh": False},
        {},
    ]

    last_exc: Optional[Exception] = None
    for kw in kw_attempts:
        try:
            merged = dict(ctx_try)
            merged.update(kw)
            return _call_with_supported_kwargs(fn, syms, **merged)
        except TypeError as e:
            last_exc = e
            continue
        except Exception:
            raise

    try:
        return fn(syms)
    except Exception:
        if last_exc:
            raise last_exc
        raise


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


async def _engine_get_quotes_any(engine: Any, syms: List[str], *, ctx: Optional[Dict[str, Any]] = None) -> Union[List[Any], Dict[str, Any]]:
    fn = getattr(engine, "get_enriched_quotes", None)
    if callable(fn):
        try:
            res = _unwrap_tuple_payload(await _maybe_await(_call_batch_best_effort(engine, "get_enriched_quotes", syms, ctx=ctx)))
            if isinstance(res, (list, dict)):
                return res
            return list(res or [])
        except Exception as e:
            logger.warning("[analysis] get_enriched_quotes failed -> fallback to get_quotes. err=%s", e)

    fn2 = getattr(engine, "get_quotes", None)
    if callable(fn2):
        try:
            res = _unwrap_tuple_payload(await _maybe_await(_call_batch_best_effort(engine, "get_quotes", syms, ctx=ctx)))
            if isinstance(res, (list, dict)):
                return res
            return list(res or [])
        except Exception as e:
            logger.warning("[analysis] get_quotes failed -> fallback to per-symbol. err=%s", e)

    out: List[Any] = []
    for s in syms:
        fn3 = getattr(engine, "get_enriched_quote", None)
        if callable(fn3):
            try:
                out.append(_unwrap_tuple_payload(await _maybe_await(_call_with_supported_kwargs(fn3, s, **(ctx or {})))))
                continue
            except Exception:
                pass
        fn4 = getattr(engine, "get_quote", None)
        if callable(fn4):
            try:
                out.append(_unwrap_tuple_payload(await _maybe_await(_call_with_supported_kwargs(fn4, s, **(ctx or {})))))
                continue
            except Exception as e:
                out.append(_make_placeholder(s, dq="MISSING", err=f"Engine error: {e}"))
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
    ctx: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    _apply_compat_patches()

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
                res = await asyncio.wait_for(_engine_get_quotes_any(engine, chunk_syms, ctx=ctx), timeout=timeout_sec)
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
            uq = chunk_map.get(s_up) or _make_placeholder(s_up, dq="MISSING", err="No data returned")
            _ensure_reco_on_obj(uq)
            out[s_up] = uq

    return out


# =============================================================================
# Transform to single response
# =============================================================================
def _quote_to_analysis(requested_symbol: str, uq: Any) -> SingleAnalysisResponse:
    try:
        from core.scoring_engine import enrich_with_scores  # type: ignore

        uq = enrich_with_scores(uq)  # type: ignore
    except Exception:
        pass

    _ensure_reco_on_obj(uq)

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
            engine_version = (
                getattr(eng, "ENGINE_VERSION", None)
                or getattr(eng, "engine_version", None)
                or getattr(eng, "version", None)
            )
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
    m = await _get_quotes_chunked(
        request,
        [t],
        batch_size=1,
        timeout_sec=cfg["timeout_sec"],
        max_concurrency=1,
        ctx=None,
    )

    key = _normalize_any(t) or t.strip().upper()
    uq = m.get(key) or _make_placeholder(key, dq="MISSING", err="No data returned")
    _ensure_reco_on_obj(uq)
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
        ctx=None,
    )

    results: List[SingleAnalysisResponse] = []
    for t in tickers:
        uq = m.get(t) or _make_placeholder(t, dq="MISSING", err="No data returned")
        _ensure_reco_on_obj(uq)
        results.append(_quote_to_analysis(t, uq))

    return BatchAnalysisResponse(results=results)


def _infer_market_from_sheet(sheet_name: Optional[str]) -> Optional[str]:
    sn = (sheet_name or "").strip().upper()
    if not sn:
        return None
    if "KSA" in sn or "TADAWUL" in sn:
        return "KSA"
    if "MUTUAL" in sn or "FUND" in sn:
        return "GLOBAL"
    if sn.startswith("GLOBAL") or "GLOBAL" in sn:
        return "GLOBAL"
    if "COMMOD" in sn or "FX" in sn:
        return "GLOBAL"
    if "PORTFOLIO" in sn:
        return "MIXED"
    return None


@router.post("/sheet-rows", response_model=SheetAnalysisResponse)
async def analyze_for_sheet(
    request: Request,
    body: BatchAnalysisRequest = Body(...),
    mode: Optional[str] = Query(default=None, description="Headers mode: base|extended."),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> SheetAnalysisResponse:
    """
    Sheets-safe: ALWAYS returns HTTP 200 with {headers, rows, status}.
    If auth fails -> returns error rows (no exception).
    """
    sheet_name = (body.sheet_name or body.sheetName or "").strip() or None
    headers = _select_headers(sheet_name, mode)
    tickers = _clean_symbols((body.tickers or []) + (body.symbols or []))

    if not tickers:
        return SheetAnalysisResponse(headers=headers, rows=[], status="skipped", error="No tickers provided")

    origin = (sheet_name or "UNKNOWN").strip()

    if not _auth_ok(x_app_token):
        err = "Unauthorized (invalid or missing X-APP-TOKEN)."
        rows = [_error_row(t, headers, err, rank=i + 1, origin=origin) for i, t in enumerate(tickers)]
        return SheetAnalysisResponse(headers=headers, rows=rows, status="error", error=err)

    cfg = _cfg()
    if len(tickers) > cfg["max_tickers"]:
        tickers = tickers[: cfg["max_tickers"]]

    plan = _build_header_plan(tuple(headers))

    ctx = {
        "sheet_name": sheet_name,
        "page_key": sheet_name,
        "market": _infer_market_from_sheet(sheet_name),
    }

    try:
        m = await _get_quotes_chunked(
            request,
            tickers,
            batch_size=cfg["batch_size"],
            timeout_sec=cfg["timeout_sec"],
            max_concurrency=cfg["concurrency"],
            ctx=ctx,
        )

        rows: List[List[Any]] = []
        for i, t in enumerate(tickers, start=1):
            uq = m.get(t) or _make_placeholder(t, dq="MISSING", err="No data returned")
            _ensure_reco_on_obj(uq)
            rows.append(_row_from_plan(plan, uq, row_index_1based=i, origin=origin))

        status = _status_from_rows(headers, rows)
        return SheetAnalysisResponse(headers=headers, rows=rows, status=status)

    except Exception as exc:
        logger.exception("[analysis] exception in /sheet-rows: %s", exc)
        rows = [_error_row(t, headers, str(exc), rank=i + 1, origin=origin) for i, t in enumerate(tickers)]
        return SheetAnalysisResponse(headers=headers, rows=rows, status="error", error=str(exc))


__all__ = [
    "router",
    "SingleAnalysisResponse",
    "BatchAnalysisRequest",
    "BatchAnalysisResponse",
    "SheetAnalysisResponse",
]
