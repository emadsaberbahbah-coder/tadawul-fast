```python
# routes/routes_argaam.py  (FULL REPLACEMENT)
"""
routes/routes_argaam.py
------------------------------------------------------------
KSA / Argaam Gateway – v3.3.0 (Engine v2 aligned, Sheets-safe + YahooChart primary-ready)

What this router guarantees (alignment with your other routers like enriched_quote.py):
- ✅ Never crashes app startup: all imports are safe + optional.
- ✅ Prefer returning HTTP 200 + (status/error) payload over raising.
- ✅ Timestamps (last_updated_utc / last_updated_riyadh) are NEVER null.
- ✅ Best-effort schema fill: payload includes ALL EnrichedQuote fields (prevents missing columns in Sheets).
- ✅ KSA strict normalization: accepts 1120 or 1120.SR only; always responds as 1120.SR.
- ✅ Yahoo Chart is available as a robust provider (avoids yfinance currentTradingPeriod issues).
- ✅ yfinance fallback remains optional (OFF by default).

Provider strategy (KSA):
- Default flow:
    1) Engine (app.state.engine OR singleton DataEngine)
    2) Yahoo Chart fallback (ON by default)
    3) yfinance fallback (OFF by default; enable via env)
- If you want Yahoo Chart to be PRIMARY (before engine), set:
    KSA_PRIMARY_MODE=yahoo_chart

Auth:
- Token guard via X-APP-TOKEN (APP_TOKEN / BACKUP_APP_TOKEN). If no token set => OPEN mode.

Env knobs:
- KSA_PRIMARY_MODE = engine | yahoo_chart   (default: engine)
- KSA_YAHOO_CHART_FALLBACK = true/false     (default: true)
- KSA_YFINANCE_FALLBACK = true/false        (default: false)
- ARGAAM_TIMEOUT_SEC (default: 30)
- ARGAAM_MAX_TICKERS (default: 400)
- ARGAAM_BATCH_SIZE (default: 40)
- ARGAAM_CONCURRENCY (default: 6)
- DEBUG_ERRORS=1 enables traceback logging (keeps responses safe)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import traceback
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from importlib import import_module
from typing import Any, Dict, List, Optional, Sequence, Tuple

import httpx
from fastapi import APIRouter, Body, Header, Query, Request
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger("routes.argaam")

ROUTE_VERSION = "3.3.0"
router = APIRouter(prefix="/v1/argaam", tags=["KSA / Argaam"])

RIYADH_TZ = timezone(timedelta(hours=3))

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


# =============================================================================
# Small utils
# =============================================================================
def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY


def _clamp(s: Any, max_len: int = 2500) -> str:
    t = (str(s) if s is not None else "").strip()
    if not t:
        return ""
    if len(t) <= max_len:
        return t
    return t[: max_len - 12] + " ...TRUNC..."


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return default


def _env_int(name: str, default: int) -> int:
    try:
        v = int(str(os.getenv(name, str(default))).strip())
        return v if v > 0 else default
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        v = float(str(os.getenv(name, str(default))).strip())
        return v if v > 0 else default
    except Exception:
        return default


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_riyadh_iso() -> str:
    return datetime.now(RIYADH_TZ).isoformat()


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip().replace(",", "")
        if not s or s.lower() in ("nan", "none", "null", "n/a", "-"):
            return None
        return float(s)
    except Exception:
        return None


def _safe_import(path: str) -> Optional[Any]:
    try:
        return import_module(path)
    except Exception:
        return None


def _model_to_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    md = getattr(obj, "model_dump", None)
    if callable(md):
        try:
            return md(exclude_none=False)
        except Exception:
            pass
    dct = getattr(obj, "dict", None)
    if callable(dct):
        try:
            return dct()
        except Exception:
            pass
    od = getattr(obj, "__dict__", None)
    if isinstance(od, dict):
        return dict(od)
    return {}


def _as_payload(obj: Any) -> Dict[str, Any]:
    """
    JSON-safe dict without throwing.
    """
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return jsonable_encoder(obj)
    try:
        return jsonable_encoder(_model_to_dict(obj))
    except Exception:
        return {"value": str(obj)}


def _ensure_timestamps(payload_or_model: Any) -> Any:
    utc = _now_utc_iso()
    riy = _now_riyadh_iso()

    if isinstance(payload_or_model, dict):
        payload_or_model["last_updated_utc"] = payload_or_model.get("last_updated_utc") or utc
        payload_or_model["last_updated_riyadh"] = payload_or_model.get("last_updated_riyadh") or riy
        return payload_or_model

    # pydantic v2 model_copy/update if available
    try:
        lu = getattr(payload_or_model, "last_updated_utc", None)
        lr = getattr(payload_or_model, "last_updated_riyadh", None)
        upd = {"last_updated_utc": (lu or utc), "last_updated_riyadh": (lr or riy)}
        if hasattr(payload_or_model, "model_copy"):
            return payload_or_model.model_copy(update=upd)
        if hasattr(payload_or_model, "copy"):
            return payload_or_model.copy(update=upd)
    except Exception:
        pass

    # last resort: setattr
    try:
        if not getattr(payload_or_model, "last_updated_utc", None):
            setattr(payload_or_model, "last_updated_utc", utc)
        if not getattr(payload_or_model, "last_updated_riyadh", None):
            setattr(payload_or_model, "last_updated_riyadh", riy)
    except Exception:
        pass

    return payload_or_model


def _schema_fill_best_effort(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensure payload contains every EnrichedQuote field (best-effort),
    so Sheets never miss expected keys even if upstream returns partial dicts.
    """
    try:
        mf = getattr(EnrichedQuote, "model_fields", None)  # type: ignore[name-defined]
        if isinstance(mf, dict) and mf:
            for k in mf.keys():
                payload.setdefault(k, None)
    except Exception:
        pass

    # Always keep these keys stable (router-level contract)
    payload.setdefault("status", "success")
    payload.setdefault("error", "")
    payload.setdefault("data_quality", payload.get("data_quality") or "MISSING")
    payload.setdefault("data_source", payload.get("data_source") or "unknown")
    payload.setdefault("market", payload.get("market") or "KSA")
    payload.setdefault("currency", payload.get("currency") or "SAR")

    return payload


# =============================================================================
# Safe imports: settings + engine + schemas + models
# =============================================================================
_get_settings = None
for _p in ("core.config", "routes.config", "config"):
    _m = _safe_import(_p)
    if _m and hasattr(_m, "get_settings"):
        _get_settings = getattr(_m, "get_settings")
        break


def _settings_obj() -> Any:
    if _get_settings:
        try:
            return _get_settings()
        except Exception:
            return None
    return None


# Engine / UnifiedQuote / normalize_symbol (prefer v2)
DataEngine = None
UnifiedQuote = None
normalize_symbol = None

for _p in ("core.data_engine_v2", "core.data_engine"):
    _m = _safe_import(_p)
    if not _m:
        continue
    DataEngine = DataEngine or getattr(_m, "DataEngine", None)
    UnifiedQuote = UnifiedQuote or getattr(_m, "UnifiedQuote", None)
    normalize_symbol = normalize_symbol or getattr(_m, "normalize_symbol", None)
    if DataEngine and UnifiedQuote and normalize_symbol:
        break


# EnrichedQuote model (preferred)
EnrichedQuote = None
for _p in ("core.enriched_quote", "enriched_quote"):
    _m = _safe_import(_p)
    if _m and hasattr(_m, "EnrichedQuote"):
        EnrichedQuote = getattr(_m, "EnrichedQuote")
        break


# Sheets schema helper (preferred)
get_headers_for_sheet = None
_schemas = _safe_import("core.schemas")
if _schemas and hasattr(_schemas, "get_headers_for_sheet"):
    get_headers_for_sheet = getattr(_schemas, "get_headers_for_sheet")


# Optional yfinance fallback
_yf = _safe_import("yfinance")


# =============================================================================
# Fallback models (only used if imports are missing)
# =============================================================================
class _FallbackUnifiedQuote(BaseModel):
    model_config = ConfigDict(extra="ignore")
    symbol: str = ""
    market: Optional[str] = None
    currency: Optional[str] = None
    data_quality: str = "MISSING"
    data_source: Optional[str] = None
    error: Optional[str] = None


class _FallbackEnrichedQuote(BaseModel):
    model_config = ConfigDict(extra="ignore")
    # Identity
    symbol: str = ""
    name: Optional[str] = None
    market: Optional[str] = "KSA"
    currency: Optional[str] = "SAR"

    # Price
    current_price: Optional[float] = None
    previous_close: Optional[float] = None
    day_high: Optional[float] = None
    day_low: Optional[float] = None
    percent_change: Optional[float] = None
    volume: Optional[float] = None

    # Some fundamentals (best-effort)
    market_cap: Optional[float] = None
    pe_ttm: Optional[float] = None
    pb: Optional[float] = None
    dividend_yield: Optional[float] = None
    roe: Optional[float] = None

    # Scores
    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    risk_score: Optional[float] = None
    overall_score: Optional[float] = None
    recommendation: Optional[str] = None

    # Meta
    data_source: Optional[str] = None
    data_quality: str = "MISSING"
    last_updated_utc: Optional[str] = None
    last_updated_riyadh: Optional[str] = None
    error: Optional[str] = None
    status: Optional[str] = None

    @classmethod
    def from_unified(cls, uq: Any) -> "_FallbackEnrichedQuote":
        d = _model_to_dict(uq)
        return cls(
            symbol=str(d.get("symbol") or d.get("ticker") or ""),
            market=str(d.get("market") or "KSA"),
            currency=str(d.get("currency") or "SAR"),
            name=d.get("name") or d.get("company_name"),
            current_price=d.get("current_price") or d.get("last_price"),
            previous_close=d.get("previous_close"),
            day_high=d.get("day_high"),
            day_low=d.get("day_low"),
            percent_change=d.get("percent_change"),
            volume=d.get("volume"),
            market_cap=d.get("market_cap"),
            pe_ttm=d.get("pe_ttm"),
            pb=d.get("pb"),
            dividend_yield=d.get("dividend_yield"),
            roe=d.get("roe"),
            value_score=d.get("value_score"),
            quality_score=d.get("quality_score"),
            momentum_score=d.get("momentum_score"),
            opportunity_score=d.get("opportunity_score"),
            risk_score=d.get("risk_score"),
            overall_score=d.get("overall_score"),
            recommendation=d.get("recommendation"),
            data_source=d.get("data_source") or d.get("provider"),
            data_quality=str(d.get("data_quality") or "MISSING"),
            last_updated_utc=d.get("last_updated_utc"),
            last_updated_riyadh=d.get("last_updated_riyadh"),
            error=d.get("error"),
        )

    def to_row(self, headers: List[str]) -> List[Any]:
        d = self.model_dump(exclude_none=False)
        out: List[Any] = []
        for h in headers:
            key = str(h or "").strip().lower()
            if key in ("symbol", "ticker"):
                out.append(self.symbol)
            elif key in ("company name", "name"):
                out.append(self.name or "")
            elif key == "market":
                out.append(self.market or "")
            elif key == "currency":
                out.append(self.currency or "")
            elif key in ("last price", "current price"):
                out.append(self.current_price)
            elif key in ("previous close", "previous_close"):
                out.append(self.previous_close)
            elif key in ("day high", "high", "day_high"):
                out.append(self.day_high)
            elif key in ("day low", "low", "day_low"):
                out.append(self.day_low)
            elif key in ("change %", "percent change", "percent_change"):
                out.append(self.percent_change)
            elif key == "volume":
                out.append(self.volume)
            elif key in ("market cap", "market_cap"):
                out.append(self.market_cap)
            elif key in ("p/e (ttm)", "pe (ttm)", "pe_ttm"):
                out.append(self.pe_ttm)
            elif key in ("p/b", "pb"):
                out.append(self.pb)
            elif key in ("dividend yield %", "dividend_yield"):
                out.append(self.dividend_yield)
            elif key in ("roe %", "roe"):
                out.append(self.roe)
            elif key in ("opportunity score", "opportunity_score"):
                out.append(self.opportunity_score)
            elif key == "recommendation":
                out.append(self.recommendation or "")
            elif key in ("data source", "data_source"):
                out.append(self.data_source or "")
            elif key in ("data quality", "data_quality"):
                out.append(self.data_quality)
            elif key in ("last updated (utc)", "last_updated_utc"):
                out.append(self.last_updated_utc or "")
            elif key in ("last updated (riyadh)", "last_updated_riyadh"):
                out.append(self.last_updated_riyadh or "")
            elif key == "error":
                out.append(self.error or "")
            else:
                out.append(d.get(key))
        if len(out) < len(headers):
            out += [None] * (len(headers) - len(out))
        return out[: len(headers)]


if UnifiedQuote is None:
    UnifiedQuote = _FallbackUnifiedQuote  # type: ignore[assignment]

if EnrichedQuote is None:
    EnrichedQuote = _FallbackEnrichedQuote  # type: ignore[assignment]


# =============================================================================
# Headers helper
# =============================================================================
_DEFAULT_HEADERS: List[str] = [
    "Symbol",
    "Company Name",
    "Market",
    "Currency",
    "Last Price",
    "Previous Close",
    "Day High",
    "Day Low",
    "Percent Change",
    "Volume",
    "Market Cap",
    "P/E (TTM)",
    "P/B",
    "Dividend Yield %",
    "ROE %",
    "Opportunity Score",
    "Recommendation",
    "Data Source",
    "Data Quality",
    "Last Updated (UTC)",
    "Last Updated (Riyadh)",
    "Error",
]


def _safe_headers(sheet_name: Optional[str]) -> List[str]:
    headers: List[str] = []
    if get_headers_for_sheet:
        try:
            h = get_headers_for_sheet(sheet_name)
            if isinstance(h, list):
                headers = [str(x).strip() for x in h if x and str(x).strip()]
        except Exception:
            headers = []

    if not headers:
        headers = list(_DEFAULT_HEADERS)

    # enforce Symbol first, Error last
    headers = [h for h in headers if str(h).strip()]
    if not headers or str(headers[0]).strip().lower() != "symbol":
        headers = [h for h in headers if str(h).strip().lower() != "symbol"]
        headers.insert(0, "Symbol")
    if not any(str(h).strip().lower() == "error" for h in headers):
        headers.append("Error")

    return headers


# =============================================================================
# Auth (OPEN mode if no tokens configured)
# =============================================================================
def _settings_str_attr(s: Any, attr: str) -> Optional[str]:
    try:
        v = getattr(s, attr, None)
        if isinstance(v, str) and v.strip():
            return v.strip()
    except Exception:
        return None
    return None


@lru_cache(maxsize=1)
def _allowed_tokens() -> List[str]:
    tokens: List[str] = []
    s = _settings_obj()

    if s is not None:
        for a in ("app_token", "backup_app_token", "APP_TOKEN", "BACKUP_APP_TOKEN"):
            v = _settings_str_attr(s, a)
            if v:
                tokens.append(v)

    env_mod = _safe_import("env")
    if env_mod is not None:
        for a in ("APP_TOKEN", "BACKUP_APP_TOKEN"):
            try:
                v = getattr(env_mod, a, None)
                if isinstance(v, str) and v.strip():
                    tokens.append(v.strip())
            except Exception:
                pass

    for k in ("APP_TOKEN", "BACKUP_APP_TOKEN"):
        v = os.getenv(k)
        if v and v.strip():
            tokens.append(v.strip())

    out: List[str] = []
    seen = set()
    for t in tokens:
        if t not in seen:
            seen.add(t)
            out.append(t)

    if not out:
        logger.warning("[argaam] No APP_TOKEN configured -> endpoints are OPEN (no auth).")
    return out


def _auth_error(x_app_token: Optional[str]) -> Optional[str]:
    allowed = _allowed_tokens()
    if not allowed:
        return None
    if not x_app_token or x_app_token.strip() not in allowed:
        return "Unauthorized (invalid or missing X-APP-TOKEN)."
    return None


# =============================================================================
# Config
# =============================================================================
def _cfg() -> Dict[str, Any]:
    default_sheet = os.getenv("ARGAAM_DEFAULT_SHEET") or os.getenv("SHEET_KSA_TADAWUL") or "KSA_Tadawul_Market"
    primary_mode = (os.getenv("KSA_PRIMARY_MODE") or "engine").strip().lower()
    if primary_mode not in ("engine", "yahoo_chart"):
        primary_mode = "engine"

    return {
        "max_tickers": _env_int("ARGAAM_MAX_TICKERS", 400),
        "batch_size": _env_int("ARGAAM_BATCH_SIZE", 40),
        "concurrency": _env_int("ARGAAM_CONCURRENCY", 6),
        "timeout_sec": _env_float("ARGAAM_TIMEOUT_SEC", 30.0),
        "default_sheet": default_sheet,
        "primary_mode": primary_mode,  # engine | yahoo_chart
        "allow_yahoo_chart_fallback": _env_bool("KSA_YAHOO_CHART_FALLBACK", True),
        "yahoo_chart_concurrency": _env_int("KSA_YAHOO_CHART_CONCURRENCY", 6),
        "allow_yfinance_fallback": _env_bool("KSA_YFINANCE_FALLBACK", False),
        "yfinance_concurrency": _env_int("KSA_YFINANCE_CONCURRENCY", 3),
    }


# =============================================================================
# Engine resolution (prefer app.state.engine; else singleton)
# =============================================================================
_ENGINE: Optional[Any] = None
_ENGINE_LOCK = asyncio.Lock()


def _looks_like_engine(obj: Any) -> bool:
    if obj is None:
        return False
    return any(hasattr(obj, m) for m in ("get_enriched_quote", "get_enriched_quotes", "get_quote", "get_quotes"))


def _get_app_engine(request: Request) -> Optional[Any]:
    try:
        st = getattr(getattr(request, "app", None), "state", None)
        if not st:
            return None
        for attr in ("engine", "data_engine", "data_engine_v2"):
            eng = getattr(st, attr, None)
            if _looks_like_engine(eng):
                return eng
        return None
    except Exception:
        return None


async def _get_singleton_engine() -> Optional[Any]:
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE

    async with _ENGINE_LOCK:
        if _ENGINE is not None:
            return _ENGINE

        if DataEngine is None:
            logger.error("[argaam] DataEngine import missing -> engine unavailable.")
            _ENGINE = None
            return None

        try:
            _ENGINE = DataEngine()  # type: ignore[misc]
            logger.info("[argaam] DataEngine initialized (singleton).")
        except Exception as exc:
            dbg = _truthy(os.getenv("DEBUG_ERRORS", "0"))
            if dbg:
                logger.exception("[argaam] Failed to init DataEngine: %s", exc)
            else:
                logger.warning("[argaam] Failed to init DataEngine: %s", exc)
            _ENGINE = None

    return _ENGINE


async def _resolve_engine(request: Request) -> Optional[Any]:
    eng = _get_app_engine(request)
    if eng is not None:
        return eng
    return await _get_singleton_engine()


# =============================================================================
# KSA strict symbol normalization
# =============================================================================
def _normalize_ksa_symbol(symbol: Any) -> str:
    s = str(symbol or "").strip().upper()
    if not s:
        return ""

    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"):
        s = s[: -len(".TADAWUL")].strip()

    if s.endswith(".SR"):
        base = s[:-3]
        return f"{base}.SR" if base.isdigit() else ""

    if s.isdigit():
        return f"{s}.SR"

    return ""


def _dedupe_preserve_order(items: Sequence[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in items or []:
        k = (x or "").strip().upper()
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(k)
    return out


def _needs_ksa_fallback(uq: Any) -> bool:
    d = _model_to_dict(uq)
    dq = str(d.get("data_quality") or "").upper()
    err = str(d.get("error") or "")

    if dq == "MISSING":
        return True

    # common provider failure signatures
    sigs = (
        "tadawul:no_url",
        "argaam:no_row",
        "No KSA provider data",
        "currentTradingPeriod",
        "yfinance error",
        "yfinance blocked",
    )
    if any(s in err for s in sigs):
        return True

    cp = d.get("current_price")
    if cp in (None, "", "null"):
        return True

    return False


# =============================================================================
# Yahoo Chart client (stable KSA provider)
# =============================================================================
_YCH_CLIENT: Optional[httpx.AsyncClient] = None
_YCH_LOCK = asyncio.Lock()


async def _get_yahoo_chart_client() -> httpx.AsyncClient:
    global _YCH_CLIENT
    if _YCH_CLIENT is not None:
        return _YCH_CLIENT

    async with _YCH_LOCK:
        if _YCH_CLIENT is not None:
            return _YCH_CLIENT

        _YCH_CLIENT = httpx.AsyncClient(
            timeout=httpx.Timeout(12.0, connect=6.0),
            follow_redirects=True,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/json,text/plain,*/*",
                "Accept-Language": "en-US,en;q=0.8,ar;q=0.6",
                "Connection": "keep-alive",
            },
        )
        return _YCH_CLIENT


@router.on_event("shutdown")
async def _shutdown_clients() -> None:
    # best-effort close (never raise)
    global _YCH_CLIENT
    try:
        if _YCH_CLIENT is not None:
            await _YCH_CLIENT.aclose()
    except Exception:
        pass
    _YCH_CLIENT = None


async def _fetch_ksa_yahoo_chart(sym: str) -> Dict[str, Any]:
    try:
        client = await _get_yahoo_chart_client()
        url = f"https://query2.finance.yahoo.com/v8/finance/chart/{sym}"
        params = {"interval": "1d", "range": "5d", "includePrePost": "false", "events": "div,splits"}
        r = await client.get(url, params=params)

        if r.status_code >= 400:
            return _ensure_timestamps(
                {
                    "symbol": sym,
                    "market": "KSA",
                    "currency": "SAR",
                    "data_source": "yahoo_chart",
                    "data_quality": "MISSING",
                    "error": f"yahoo_chart http {r.status_code}",
                }
            )

        data = r.json() if r.content else {}
        chart = (data or {}).get("chart") or {}
        result = (chart.get("result") or [])
        if not result or not isinstance(result, list) or not isinstance(result[0], dict):
            err = chart.get("error") or {}
            msg = err.get("description") or err.get("message") or "yahoo_chart empty result"
            return _ensure_timestamps(
                {
                    "symbol": sym,
                    "market": "KSA",
                    "currency": "SAR",
                    "data_source": "yahoo_chart",
                    "data_quality": "MISSING",
                    "error": str(msg),
                }
            )

        res0 = result[0]
        meta = res0.get("meta") or {}
        ind = res0.get("indicators") or {}
        quotes = (ind.get("quote") or [])
        q0 = quotes[0] if quotes and isinstance(quotes[0], dict) else {}

        closes = q0.get("close") or []
        highs = q0.get("high") or []
        lows = q0.get("low") or []
        vols = q0.get("volume") or []

        def _last_num(arr: Any) -> Optional[float]:
            if not isinstance(arr, list):
                return _safe_float(arr)
            for v in reversed(arr):
                fv = _safe_float(v)
                if fv is not None:
                    return fv
            return None

        current = _safe_float(meta.get("regularMarketPrice")) or _last_num(closes)
        prev = _safe_float(meta.get("previousClose"))
        day_high = _last_num(highs)
        day_low = _last_num(lows)
        volume = _last_num(vols)

        pct = None
        try:
            if current is not None and prev not in (None, 0.0):
                pct = (float(current) - float(prev)) / float(prev) * 100.0
        except Exception:
            pct = None

        currency = meta.get("currency") or "SAR"
        dq = (
            "FULL"
            if (current is not None and prev is not None and day_high is not None and day_low is not None)
            else ("PARTIAL" if current is not None else "MISSING")
        )

        return _ensure_timestamps(
            {
                "symbol": sym,
                "market": "KSA",
                "currency": currency,
                "current_price": current,
                "previous_close": prev,
                "day_high": day_high,
                "day_low": day_low,
                "volume": volume,
                "percent_change": pct,
                "data_source": "yahoo_chart",
                "data_quality": dq,
                "error": "" if current is not None else "yahoo_chart returned no price",
            }
        )

    except Exception as exc:
        return _ensure_timestamps(
            {
                "symbol": sym,
                "market": "KSA",
                "currency": "SAR",
                "data_source": "yahoo_chart",
                "data_quality": "MISSING",
                "error": f"yahoo_chart error: {exc}",
            }
        )


# =============================================================================
# Optional yfinance fallback (LAST resort; off by default)
# =============================================================================
async def _fetch_ksa_yfinance(sym: str) -> Dict[str, Any]:
    if _yf is None:
        return _ensure_timestamps(
            {
                "symbol": sym,
                "market": "KSA",
                "currency": "SAR",
                "data_quality": "MISSING",
                "data_source": "yfinance",
                "error": "yfinance not available",
            }
        )

    def _work() -> Dict[str, Any]:
        try:
            t = _yf.Ticker(sym)  # type: ignore[attr-defined]
            info = {}
            try:
                info = t.info or {}
            except Exception:
                info = {}

            fast = {}
            try:
                fast = getattr(t, "fast_info", None) or {}
            except Exception:
                fast = {}

            price = fast.get("last_price") or fast.get("lastPrice") or fast.get("regularMarketPrice")
            prev = fast.get("previous_close") or fast.get("previousClose") or fast.get("regularMarketPreviousClose")
            vol = fast.get("volume") or fast.get("regularMarketVolume")

            if price is None:
                price = info.get("regularMarketPrice") or info.get("currentPrice")
            if prev is None:
                prev = info.get("regularMarketPreviousClose") or info.get("previousClose")
            if vol is None:
                vol = info.get("regularMarketVolume") or info.get("volume")

            pct = None
            try:
                if price is not None and prev not in (None, 0, 0.0):
                    pct = (float(price) - float(prev)) / float(prev) * 100.0
            except Exception:
                pct = None

            return {
                "symbol": sym,
                "market": "KSA",
                "currency": "SAR",
                "current_price": float(price) if price is not None else None,
                "previous_close": float(prev) if prev is not None else None,
                "percent_change": float(pct) if pct is not None else None,
                "volume": float(vol) if vol is not None else None,
                "data_source": "yfinance",
                "data_quality": "PARTIAL" if price is not None else "MISSING",
                "error": "" if price is not None else "yfinance returned no price",
            }
        except Exception as exc:
            return {
                "symbol": sym,
                "market": "KSA",
                "currency": "SAR",
                "data_quality": "MISSING",
                "data_source": "yfinance",
                "error": f"yfinance error: {exc}",
            }

    return _ensure_timestamps(await asyncio.to_thread(_work))


# =============================================================================
# Engine calls (timeout safe)
# =============================================================================
async def _engine_get_quote(engine: Any, sym: str, timeout_sec: float) -> Any:
    async def _call() -> Any:
        if hasattr(engine, "get_enriched_quote"):
            return await engine.get_enriched_quote(sym)  # type: ignore[misc]
        if hasattr(engine, "get_quote"):
            return await engine.get_quote(sym)  # type: ignore[misc]
        # fallback: batch of one
        res = await _engine_get_quotes(engine, [sym], timeout_sec=timeout_sec, concurrency=1)
        return res[0] if res else {"symbol": sym, "data_quality": "MISSING", "error": "No data returned"}

    try:
        return await asyncio.wait_for(_call(), timeout=timeout_sec)
    except Exception as exc:
        return {"symbol": sym, "data_quality": "MISSING", "error": f"Engine quote error: {exc}"}


async def _engine_get_quotes(engine: Any, syms: List[str], *, timeout_sec: float, concurrency: int) -> List[Any]:
    async def _call_batch() -> List[Any]:
        if hasattr(engine, "get_enriched_quotes"):
            return await engine.get_enriched_quotes(syms)  # type: ignore[misc]
        if hasattr(engine, "get_quotes"):
            return await engine.get_quotes(syms)  # type: ignore[misc]

        sem = asyncio.Semaphore(max(1, int(concurrency) if concurrency else 1))

        async def _one(s: str) -> Any:
            async with sem:
                return await _engine_get_quote(engine, s, timeout_sec=timeout_sec)

        return list(await asyncio.gather(*[_one(s) for s in syms], return_exceptions=False))

    try:
        return await asyncio.wait_for(_call_batch(), timeout=timeout_sec)
    except Exception as exc:
        return [{"symbol": s, "data_quality": "MISSING", "error": f"Engine batch error: {exc}"} for s in syms]


# =============================================================================
# Transform unified -> enriched + scoring
# =============================================================================
def _to_scored_enriched(uq: Any, requested_symbol: str) -> Any:
    """
    Convert to EnrichedQuote (preferred) and enrich with scores if available.
    Always forces symbol/market/currency and timestamps.
    """
    sym = _normalize_ksa_symbol(requested_symbol) or (str(requested_symbol or "").strip().upper() or "UNKNOWN")
    forced: Dict[str, Any] = {"symbol": sym, "market": "KSA", "currency": "SAR"}

    try:
        eq = EnrichedQuote.from_unified(uq)  # type: ignore[attr-defined]

        # force required keys if missing/blank
        try:
            upd = {k: v for k, v in forced.items() if getattr(eq, k, None) in (None, "", "UNKNOWN")}
            if upd:
                if hasattr(eq, "model_copy"):
                    eq = eq.model_copy(update=upd)
                else:
                    eq = eq.copy(update=upd)
        except Exception:
            pass

        # ensure data_source
        try:
            if not getattr(eq, "data_source", None):
                d = _model_to_dict(uq)
                ds = d.get("data_source") or d.get("provider") or "argaam_gateway"
                if hasattr(eq, "model_copy"):
                    eq = eq.model_copy(update={"data_source": ds})
                else:
                    eq = eq.copy(update={"data_source": ds})
        except Exception:
            pass

        # optional scoring
        try:
            from core.scoring_engine import enrich_with_scores  # type: ignore

            eq2 = enrich_with_scores(eq)  # type: ignore
            if eq2 is not None:
                eq = eq2
        except Exception:
            pass

        # timestamps
        eq = _ensure_timestamps(eq)

        # best-effort schema fill for client consistency
        payload = _as_payload(eq)
        payload = _schema_fill_best_effort(payload)
        return payload

    except Exception as exc:
        dbg = _truthy(os.getenv("DEBUG_ERRORS", "0"))
        if dbg:
            logger.exception("[argaam] transform error for %s: %s", sym, exc)
        else:
            logger.warning("[argaam] transform error for %s: %s", sym, exc)

        eq_payload = _ensure_timestamps(
            {
                "status": "error",
                "symbol": sym,
                "market": "KSA",
                "currency": "SAR",
                "data_quality": "MISSING",
                "data_source": "argaam_gateway",
                "error": f"Transform error: {exc}",
            }
        )
        return _schema_fill_best_effort(eq_payload)


# =============================================================================
# Apply fallback(s)
# =============================================================================
async def _apply_fallback_for_missing(targets: List[str], items: List[Any], cfg: Dict[str, Any]) -> List[Any]:
    allow_ych = bool(cfg.get("allow_yahoo_chart_fallback", True))
    allow_yf = bool(cfg.get("allow_yfinance_fallback", False))

    ych_sem = asyncio.Semaphore(max(1, int(cfg.get("yahoo_chart_concurrency", 6) or 6)))
    yf_sem = asyncio.Semaphore(max(1, int(cfg.get("yfinance_concurrency", 3) or 3)))

    async def _fix_one(i: int) -> Any:
        uq = items[i]
        if not _needs_ksa_fallback(uq):
            return uq

        sym = targets[i]

        if allow_ych:
            async with ych_sem:
                ych = await _fetch_ksa_yahoo_chart(sym)
            if str(ych.get("data_quality") or "").upper() != "MISSING":
                return ych
            # append error trace (still keep original)
            base = _model_to_dict(uq)
            base["error"] = (str(base.get("error") or "") + " | yahoo_chart_failed: " + str(ych.get("error") or "")).strip(
                " |"
            )
            uq = base

        if allow_yf:
            async with yf_sem:
                yf_uq = await _fetch_ksa_yfinance(sym)
            if str(yf_uq.get("data_quality") or "").upper() != "MISSING":
                return yf_uq
            base = _model_to_dict(uq)
            base["error"] = (str(base.get("error") or "") + " | yfinance_failed: " + str(yf_uq.get("error") or "")).strip(
                " |"
            )
            return base

        return uq

    return list(await asyncio.gather(*[_fix_one(i) for i in range(len(targets))], return_exceptions=False))


# =============================================================================
# Request / Response models
# =============================================================================
class ArgaamBatchRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    symbols: List[str] = Field(default_factory=list, description="KSA symbols (e.g. ['1120', '1180.SR'])")
    tickers: List[str] = Field(default_factory=list, description="Alias for symbols (legacy compat)")
    sheet_name: Optional[str] = Field(default=None, description="Sheet name for header selection")


class KSASheetResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")
    status: str = "success"  # success | partial | error | skipped
    error: Optional[str] = None
    headers: List[str] = Field(default_factory=list)
    rows: List[List[Any]] = Field(default_factory=list)
    meta: Dict[str, Any] = Field(default_factory=dict)


def _row_with_error(headers: List[str], symbol: str, error: str, data_quality: str = "MISSING") -> List[Any]:
    row: List[Any] = [None] * len(headers)

    def _idx(name: str) -> Optional[int]:
        target = name.strip().lower()
        for i, h in enumerate(headers):
            if str(h).strip().lower() == target:
                return i
        return None

    i_sym = _idx("symbol")
    i_err = _idx("error")
    i_dq = _idx("data quality") or _idx("data_quality")

    if i_sym is not None:
        row[i_sym] = symbol
    else:
        row[0] = symbol

    if i_dq is not None:
        row[i_dq] = data_quality

    if i_err is not None:
        row[i_err] = error
    else:
        row[-1] = error

    return row


# =============================================================================
# Routes
# =============================================================================
@router.get("/health")
async def argaam_health(request: Request) -> Dict[str, Any]:
    cfg = _cfg()
    eng = await _resolve_engine(request)

    providers: List[str] = []
    ksa_providers: List[str] = []
    flags: Dict[str, Any] = {}

    if eng:
        try:
            providers = list(getattr(eng, "enabled_providers", []) or [])
        except Exception:
            providers = []
        try:
            ksa_providers = list(getattr(eng, "enabled_ksa_providers", []) or [])
        except Exception:
            ksa_providers = []
        for attr in ("enable_yfinance", "enable_yfinance_ksa", "enable_yahoo_chart_ksa"):
            try:
                if hasattr(eng, attr):
                    flags[attr] = bool(getattr(eng, attr))
            except Exception:
                pass

    s = _settings_obj()
    app_version = None
    if s is not None:
        app_version = getattr(s, "app_version", None) or getattr(s, "version", None)

    return {
        "status": "ok",
        "module": "routes.routes_argaam",
        "route_version": ROUTE_VERSION,
        "app_version": app_version,
        "engine_available": bool(eng),
        "engine": "DataEngineV2" if bool(eng) else "unavailable",
        "providers": providers,
        "ksa_providers": ksa_providers,
        "engine_flags": flags,
        "ksa_mode": "STRICT",
        "auth": "open" if not _allowed_tokens() else "token",
        "ksa_primary_mode": cfg["primary_mode"],
        "fallbacks": {
            "yahoo_chart_available": True,
            "allow_yahoo_chart_fallback": bool(cfg["allow_yahoo_chart_fallback"]),
            "yahoo_chart_concurrency": int(cfg["yahoo_chart_concurrency"]),
            "yfinance_available": bool(_yf),
            "allow_yfinance_fallback": bool(cfg["allow_yfinance_fallback"]),
            "yfinance_concurrency": int(cfg["yfinance_concurrency"]),
        },
        "limits": {
            "ARGAAM_MAX_TICKERS": cfg["max_tickers"],
            "ARGAAM_BATCH_SIZE": cfg["batch_size"],
            "ARGAAM_CONCURRENCY": cfg["concurrency"],
            "ARGAAM_TIMEOUT_SEC": cfg["timeout_sec"],
        },
        "timestamp_utc": _now_utc_iso(),
        "default_sheet": cfg["default_sheet"],
    }


@router.get("/headers")
async def argaam_headers(
    sheet_name: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> Dict[str, Any]:
    headers = _safe_headers(sheet_name)
    err = _auth_error(x_app_token)
    if err:
        return {"status": "error", "error": err, "headers": headers, "count": len(headers), "sheet_name": sheet_name}
    return {"status": "success", "headers": headers, "count": len(headers), "sheet_name": sheet_name}


@router.get("/quote")
async def ksa_single_quote(
    request: Request,
    symbol: str = Query(..., description="KSA symbol (1120 or 1120.SR)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    debug: int = Query(default=0, description="debug=1 includes safe debug hint in error (best-effort)"),
) -> Dict[str, Any]:
    cfg = _cfg()
    dbg = _truthy(os.getenv("DEBUG_ERRORS", "0")) or bool(int(debug or 0))

    err = _auth_error(x_app_token)
    if err:
        payload = _ensure_timestamps(
            {
                "status": "error",
                "symbol": (symbol or "").strip().upper() or "UNKNOWN",
                "market": "KSA",
                "currency": "SAR",
                "data_quality": "MISSING",
                "data_source": "auth",
                "error": err,
            }
        )
        return _schema_fill_best_effort(payload)

    ksa_symbol = _normalize_ksa_symbol(symbol)
    if not ksa_symbol:
        payload = _ensure_timestamps(
            {
                "status": "error",
                "symbol": (symbol or "").strip().upper() or "UNKNOWN",
                "market": "KSA",
                "currency": "SAR",
                "data_quality": "MISSING",
                "data_source": "validation",
                "error": f"Invalid KSA symbol '{symbol}'. Must be numeric or end in .SR",
            }
        )
        return _schema_fill_best_effort(payload)

    # Primary mode (your requirement: allow Yahoo as PRIMARY for KSA)
    uq: Any = None
    primary_mode = cfg["primary_mode"]

    if primary_mode == "yahoo_chart":
        uq = await _fetch_ksa_yahoo_chart(ksa_symbol)
        if str(_model_to_dict(uq).get("data_quality") or "").upper() == "MISSING":
            uq = None  # continue to engine/fallback chain

    if uq is None:
        eng = await _resolve_engine(request)
        if eng is None:
            uq = {"symbol": ksa_symbol, "market": "KSA", "currency": "SAR", "data_quality": "MISSING", "error": "Engine unavailable"}
        else:
            uq = await _engine_get_quote(eng, ksa_symbol, timeout_sec=cfg["timeout_sec"])

    # Apply fallbacks if needed
    if _needs_ksa_fallback(uq):
        fixed = await _apply_fallback_for_missing([ksa_symbol], [uq], cfg)
        uq = fixed[0] if fixed else uq

    out = _to_scored_enriched(uq, ksa_symbol)

    # Safe debug hint (no stacktrace by default)
    if dbg:
        d = _model_to_dict(uq)
        hint = {
            "primary_mode": primary_mode,
            "engine_used": bool(await _resolve_engine(request)),
            "uq_data_source": d.get("data_source") or d.get("provider"),
            "uq_data_quality": d.get("data_quality"),
        }
        out = _as_payload(out)
        out["debug"] = hint

    out = _ensure_timestamps(out)
    return _schema_fill_best_effort(out)


@router.post("/quotes")
async def ksa_batch_quotes(
    request: Request,
    body: ArgaamBatchRequest = Body(...),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> List[Dict[str, Any]]:
    cfg = _cfg()
    err = _auth_error(x_app_token)

    raw_list = (body.symbols or []) + (body.tickers or [])
    if not raw_list:
        return []

    normalized: List[str] = []
    invalid: List[str] = []
    for s in raw_list:
        n = _normalize_ksa_symbol(s)
        if n:
            normalized.append(n)
        else:
            if s and str(s).strip():
                invalid.append(str(s).strip())

    targets = _dedupe_preserve_order(normalized)
    if len(targets) > cfg["max_tickers"]:
        targets = targets[: cfg["max_tickers"]]

    out: List[Dict[str, Any]] = []

    if err:
        for t in targets:
            payload = _ensure_timestamps(
                {
                    "status": "error",
                    "symbol": t,
                    "market": "KSA",
                    "currency": "SAR",
                    "data_quality": "MISSING",
                    "data_source": "auth",
                    "error": err,
                }
            )
            out.append(_schema_fill_best_effort(payload))
    else:
        # Primary mode batch: if yahoo_chart primary, we try it first per symbol, then fill missing via engine/fallback chain
        primary_mode = cfg["primary_mode"]

        # 1) Start with initial items list
        items: List[Any] = []
        if primary_mode == "yahoo_chart":
            sem = asyncio.Semaphore(max(1, int(cfg["yahoo_chart_concurrency"])))

            async def _ych_one(s: str) -> Any:
                async with sem:
                    return await _fetch_ksa_yahoo_chart(s)

            items = list(await asyncio.gather(*[_ych_one(t) for t in targets], return_exceptions=False))
        else:
            items = [None] * len(targets)

        # 2) For those missing (or if primary is engine), call engine in batches
        eng = await _resolve_engine(request)
        batch_size = max(1, int(cfg["batch_size"]))

        for i0 in range(0, len(targets), batch_size):
            part = targets[i0 : i0 + batch_size]

            # determine which indices need engine
            need_engine_idx: List[int] = []
            for j, sym in enumerate(part):
                idx = i0 + j
                if items[idx] is None or _needs_ksa_fallback(items[idx]):
                    need_engine_idx.append(idx)

            if need_engine_idx and eng is not None:
                got = await _engine_get_quotes(eng, [targets[i] for i in need_engine_idx], timeout_sec=cfg["timeout_sec"], concurrency=cfg["concurrency"])
                for k, idx in enumerate(need_engine_idx):
                    items[idx] = got[k] if k < len(got) else items[idx]

            # fill None if engine unavailable
            if eng is None:
                for idx in need_engine_idx:
                    items[idx] = items[idx] or {"symbol": targets[idx], "market": "KSA", "currency": "SAR", "data_quality": "MISSING", "error": "Engine unavailable"}

        # 3) Apply fallbacks for still-missing items
        items = await _apply_fallback_for_missing(targets, items, cfg)

        # 4) Transform -> enriched payloads
        for i, t in enumerate(targets):
            payload = _to_scored_enriched(items[i], t)
            payload = _ensure_timestamps(payload)
            out.append(_schema_fill_best_effort(_as_payload(payload)))

    # Append invalid symbols as error payloads (keeps client stable)
    for bad in invalid:
        payload = _ensure_timestamps(
            {
                "status": "error",
                "symbol": bad.upper(),
                "market": "KSA",
                "currency": "SAR",
                "data_quality": "MISSING",
                "data_source": "validation",
                "error": "Invalid KSA symbol (must be numeric or end in .SR)",
            }
        )
        out.append(_schema_fill_best_effort(payload))

    return out


@router.post("/sheet-rows", response_model=KSASheetResponse)
async def ksa_sheet_rows(
    request: Request,
    body: ArgaamBatchRequest = Body(...),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> KSASheetResponse:
    cfg = _cfg()
    sheet_name = body.sheet_name or cfg["default_sheet"]
    headers = _safe_headers(sheet_name)

    raw_list = (body.symbols or []) + (body.tickers or [])
    if not raw_list:
        return KSASheetResponse(
            status="skipped",
            error="No symbols provided",
            headers=headers,
            rows=[],
            meta={"sheet_name": sheet_name, "timestamp_utc": _now_utc_iso()},
        )

    err = _auth_error(x_app_token)
    if err:
        rows = [_row_with_error(headers, str(s or "").strip().upper() or "UNKNOWN", err) for s in raw_list]
        return KSASheetResponse(
            status="error",
            error=err,
            headers=headers,
            rows=rows,
            meta={"sheet_name": sheet_name, "timestamp_utc": _now_utc_iso()},
        )

    valid_targets: List[str] = []
    invalid: List[str] = []
    for s in raw_list:
        n = _normalize_ksa_symbol(s)
        if n:
            valid_targets.append(n)
        else:
            if s and str(s).strip():
                invalid.append(str(s).strip())

    valid_targets = _dedupe_preserve_order(valid_targets)
    if len(valid_targets) > cfg["max_tickers"]:
        valid_targets = valid_targets[: cfg["max_tickers"]]

    rows: List[List[Any]] = []
    for bad in invalid:
        rows.append(_row_with_error(headers, bad.upper(), "Invalid KSA symbol (must be numeric or end in .SR)"))

    if not valid_targets:
        return KSASheetResponse(
            status="partial" if rows else "skipped",
            error="No valid KSA symbols provided" if not rows else None,
            headers=headers,
            rows=rows,
            meta={"sheet_name": sheet_name, "requested": len(raw_list), "valid": 0, "invalid": len(invalid), "timestamp_utc": _now_utc_iso()},
        )

    # Get quotes using /quotes logic, then map to rows
    quotes_payloads = await ksa_batch_quotes(request, ArgaamBatchRequest(symbols=valid_targets, sheet_name=sheet_name), x_app_token)

    any_fail = False
    for p in quotes_payloads:
        # p already schema-filled dict
        sym = str(p.get("symbol") or "").strip().upper() or "UNKNOWN"
        dq = str(p.get("data_quality") or "MISSING")

        try:
            # Prefer model mapping when available
            try:
                eq_model = EnrichedQuote(**p)  # type: ignore[misc]
                eq_model = _ensure_timestamps(eq_model)
                row = eq_model.to_row(headers) if hasattr(eq_model, "to_row") else None  # type: ignore[attr-defined]
            except Exception:
                row = None

            if not isinstance(row, list):
                # fallback mapping using dict + fallback model
                eq_fallback = _FallbackEnrichedQuote(**p)
                eq_fallback = _ensure_timestamps(eq_fallback)
                row = eq_fallback.to_row(headers)

            if len(row) < len(headers):
                row += [None] * (len(headers) - len(row))
            rows.append(row[: len(headers)])

        except Exception as exc:
            any_fail = True
            rows.append(_row_with_error(headers, sym, f"Row mapping failed: {exc}", data_quality=dq))

    status = "success"
    if invalid or any_fail:
        status = "partial"

    # If any row has a non-empty error, mark partial
    try:
        err_idx = next(i for i, h in enumerate(headers) if str(h).strip().lower() == "error")
        if any((r[err_idx] not in (None, "", "null")) for r in rows if len(r) > err_idx):
            status = "partial"
    except Exception:
        pass

    return KSASheetResponse(
        status=status,
        headers=headers,
        rows=rows,
        meta={
            "sheet_name": sheet_name,
            "requested": len(raw_list),
            "valid": len(valid_targets),
            "invalid": len(invalid),
            "timestamp_utc": _now_utc_iso(),
            "ksa_primary_mode": cfg["primary_mode"],
            "fallback_yahoo_chart": bool(cfg["allow_yahoo_chart_fallback"]),
            "fallback_yfinance": bool(cfg["allow_yfinance_fallback"]),
        },
    )


__all__ = ["router"]
```
