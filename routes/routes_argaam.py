# routes/routes_argaam.py  (FULL REPLACEMENT)
"""
routes_argaam.py
------------------------------------------------------------
KSA / Argaam Gateway – v3.2.0 (Engine v2 aligned, Sheets-safe + YahooChart fallback)

Key upgrades vs v3.1.0:
- ✅ Force timestamps (last_updated_utc / last_updated_riyadh) to NEVER be null.
- ✅ Add KSA Yahoo Chart fallback (stable, avoids yfinance 'currentTradingPeriod' failures).
- ✅ Keep yfinance fallback optional (env-controlled) as LAST resort.
- ✅ Health endpoint exposes real KSA providers + fallback switches (yahoo_chart/yfinance).

Auth:
- Token guard via X-APP-TOKEN (APP_TOKEN / BACKUP_APP_TOKEN). If no token set => open mode.

Notes:
- This router is defensive: prefer returning 200 + error payload over raising.
- Strict KSA normalization: accepts 1120 or 1120.SR only; responds as 1120.SR.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone, timedelta
from functools import lru_cache
from importlib import import_module
from typing import Any, Dict, List, Optional, Sequence

import httpx
from fastapi import APIRouter, Body, Header, Query, Request
from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger("routes.argaam")

ROUTE_VERSION = "3.2.0"
router = APIRouter(prefix="/v1/argaam", tags=["KSA / Argaam"])

RIYADH_TZ = timezone(timedelta(hours=3))
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# =============================================================================
# Safe imports (never break router mount)
# =============================================================================
def _safe_import(path: str) -> Optional[Any]:
    try:
        return import_module(path)
    except Exception:
        return None


# Settings getter (prefer core.config, fallback config, then env vars)
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


# DataEngine + UnifiedQuote + normalize_symbol (prefer v2, fallback legacy)
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


# EnrichedQuote model (preferred) — but never crash if missing
EnrichedQuote = None
for _p in ("core.enriched_quote", "enriched_quote"):
    _m = _safe_import(_p)
    if _m and hasattr(_m, "EnrichedQuote"):
        EnrichedQuote = getattr(_m, "EnrichedQuote")
        break


# Schemas helper (preferred)
get_headers_for_sheet = None
_schemas = _safe_import("core.schemas")
if _schemas and hasattr(_schemas, "get_headers_for_sheet"):
    get_headers_for_sheet = getattr(_schemas, "get_headers_for_sheet")


# Optional: yfinance fallback (KSA-only)
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

    def finalize(self) -> "_FallbackUnifiedQuote":
        return self


class _FallbackEnrichedQuote(BaseModel):
    model_config = ConfigDict(extra="ignore")
    symbol: str = ""
    name: Optional[str] = None
    market: Optional[str] = "KSA"
    currency: Optional[str] = "SAR"

    current_price: Optional[float] = None
    previous_close: Optional[float] = None
    day_high: Optional[float] = None
    day_low: Optional[float] = None

    percent_change: Optional[float] = None
    volume: Optional[float] = None
    market_cap: Optional[float] = None
    pe_ttm: Optional[float] = None
    pb: Optional[float] = None
    dividend_yield: Optional[float] = None
    roe: Optional[float] = None

    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    risk_score: Optional[float] = None
    overall_score: Optional[float] = None
    recommendation: Optional[str] = None

    data_source: Optional[str] = None
    data_quality: str = "MISSING"
    last_updated_utc: Optional[str] = None
    last_updated_riyadh: Optional[str] = None
    error: Optional[str] = None

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
            opportunity_score=d.get("opportunity_score"),
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
            lk = str(h or "").strip().lower()
            if lk in ("symbol", "ticker"):
                out.append(self.symbol)
            elif lk in ("company name", "name"):
                out.append(self.name or "")
            elif lk == "market":
                out.append(self.market or "")
            elif lk == "currency":
                out.append(self.currency or "")
            elif lk in ("last price", "current price"):
                out.append(self.current_price)
            elif lk in ("previous close", "previous_close"):
                out.append(self.previous_close)
            elif lk in ("day high", "high", "day_high"):
                out.append(self.day_high)
            elif lk in ("day low", "low", "day_low"):
                out.append(self.day_low)
            elif lk in ("change %", "percent change", "percent_change"):
                out.append(self.percent_change)
            elif lk == "volume":
                out.append(self.volume)
            elif lk in ("market cap", "market_cap"):
                out.append(self.market_cap)
            elif lk in ("p/e (ttm)", "pe (ttm)", "pe_ttm"):
                out.append(self.pe_ttm)
            elif lk in ("p/b", "pb"):
                out.append(self.pb)
            elif lk in ("dividend yield %", "dividend_yield"):
                out.append(self.dividend_yield)
            elif lk in ("roe %", "roe"):
                out.append(self.roe)
            elif lk in ("opportunity score", "opportunity_score"):
                out.append(self.opportunity_score)
            elif lk == "recommendation":
                out.append(self.recommendation or "")
            elif lk in ("data source", "data_source"):
                out.append(self.data_source or "")
            elif lk in ("data quality", "data_quality"):
                out.append(self.data_quality)
            elif lk in ("last updated (utc)", "last_updated_utc"):
                out.append(self.last_updated_utc or "")
            elif lk in ("last updated (riyadh)", "last_updated_riyadh"):
                out.append(self.last_updated_riyadh or "")
            elif lk == "error":
                out.append(self.error or "")
            else:
                out.append(d.get(lk))
        if len(out) < len(headers):
            out += [None] * (len(headers) - len(out))
        return out[: len(headers)]


if UnifiedQuote is None:
    UnifiedQuote = _FallbackUnifiedQuote  # type: ignore

if EnrichedQuote is None:
    EnrichedQuote = _FallbackEnrichedQuote  # type: ignore


# =============================================================================
# Optional schema helper fallback
# =============================================================================
_DEFAULT_SHEETS_HEADERS_FALLBACK: List[str] = [
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
        headers = list(_DEFAULT_SHEETS_HEADERS_FALLBACK)

    if not headers or headers[0].strip().lower() != "symbol":
        headers = [h for h in headers if h.strip().lower() != "symbol"]
        headers.insert(0, "Symbol")

    if not any(h.strip().lower() == "error" for h in headers):
        headers.append("Error")

    return headers


# =============================================================================
# Settings / auth (X-APP-TOKEN) — open mode if no token configured
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
# Limits / config
# =============================================================================
def _safe_int_env(name: str, default: int) -> int:
    try:
        v = int(os.getenv(name, str(default)).strip())
        return v if v > 0 else default
    except Exception:
        return default


def _safe_float_env(name: str, default: float) -> float:
    try:
        v = float(os.getenv(name, str(default)).strip())
        return v if v > 0 else default
    except Exception:
        return default


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


def _cfg() -> Dict[str, Any]:
    default_sheet = os.getenv("ARGAAM_DEFAULT_SHEET") or os.getenv("SHEET_KSA_TADAWUL") or "KSA_Tadawul_Market"
    return {
        "max_tickers": _safe_int_env("ARGAAM_MAX_TICKERS", 400),
        "batch_size": _safe_int_env("ARGAAM_BATCH_SIZE", 40),
        "concurrency": _safe_int_env("ARGAAM_CONCURRENCY", 6),
        "timeout_sec": _safe_float_env("ARGAAM_TIMEOUT_SEC", 30.0),
        "default_sheet": default_sheet,
        # NEW: Yahoo Chart fallback (recommended default ON)
        "allow_yahoo_chart_fallback": _env_bool("KSA_YAHOO_CHART_FALLBACK", True),
        "yahoo_chart_concurrency": _safe_int_env("KSA_YAHOO_CHART_CONCURRENCY", 6),
        # Legacy: yfinance fallback (default OFF; keep optional)
        "allow_yfinance_fallback": _env_bool("KSA_YFINANCE_FALLBACK", False),
        "yf_concurrency": _safe_int_env("KSA_YFINANCE_CONCURRENCY", 3),
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
            _ENGINE = DataEngine()  # type: ignore
            logger.info("[argaam] DataEngine initialized (singleton).")
        except Exception as exc:
            logger.exception("[argaam] Failed to init DataEngine: %s", exc)
            _ENGINE = None

    return _ENGINE


async def _resolve_engine(request: Request) -> Optional[Any]:
    eng = _get_app_engine(request)
    if eng is not None:
        return eng
    return await _get_singleton_engine()


# =============================================================================
# Symbol logic (KSA strict)
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
        if not k:
            continue
        if k in seen:
            continue
        seen.add(k)
        out.append(k)
    return out


# =============================================================================
# Mapping helpers
# =============================================================================
def _model_to_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    try:
        return obj.model_dump(exclude_none=False)  # type: ignore
    except Exception:
        pass
    try:
        return obj.dict()  # type: ignore
    except Exception:
        pass
    try:
        return dict(getattr(obj, "__dict__", {}) or {})
    except Exception:
        return {}


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_riyadh_iso() -> str:
    return datetime.now(RIYADH_TZ).isoformat()


def _ensure_timestamps(obj: Any) -> Any:
    """
    Make last_updated_utc + last_updated_riyadh NEVER null/empty.
    Works for dict OR pydantic models.
    """
    utc = _now_utc_iso()
    riy = _now_riyadh_iso()

    if isinstance(obj, dict):
        obj["last_updated_utc"] = obj.get("last_updated_utc") or utc
        obj["last_updated_riyadh"] = obj.get("last_updated_riyadh") or riy
        return obj

    # pydantic v2
    try:
        lu = getattr(obj, "last_updated_utc", None)
        lr = getattr(obj, "last_updated_riyadh", None)
        upd = {
            "last_updated_utc": (lu or utc),
            "last_updated_riyadh": (lr or riy),
        }
        if hasattr(obj, "model_copy"):
            return obj.model_copy(update=upd)
        if hasattr(obj, "copy"):
            return obj.copy(update=upd)
    except Exception:
        pass

    # last-resort setattr
    try:
        if not getattr(obj, "last_updated_utc", None):
            setattr(obj, "last_updated_utc", utc)
        if not getattr(obj, "last_updated_riyadh", None):
            setattr(obj, "last_updated_riyadh", riy)
    except Exception:
        pass
    return obj


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


def _needs_ksa_fallback(uq: Any) -> bool:
    d = _model_to_dict(uq)
    dq = str(d.get("data_quality") or "").upper()
    err = str(d.get("error") or "")

    if dq == "MISSING":
        return True

    # signatures we know mean "no usable KSA data"
    sigs = (
        "tadawul:no_url",
        "argaam:no_row",
        "No KSA provider data",
        "currentTradingPeriod",
        "yfinance error",
        "yfinance blocked",
        "Unauthorized",
    )
    if any(s in err for s in sigs):
        return True

    # engine might return placeholders with no price
    cp = d.get("current_price")
    if cp in (None, "", "null"):
        return True

    return False


# =============================================================================
# KSA fallback: Yahoo Chart (stable)
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


async def _fetch_ksa_yahoo_chart(sym: str) -> Dict[str, Any]:
    """
    KSA fallback via Yahoo Finance chart endpoint (no yfinance dependency).
    """
    try:
        client = await _get_yahoo_chart_client()
        url = f"https://query2.finance.yahoo.com/v8/finance/chart/{sym}"
        params = {
            "interval": "1d",
            "range": "5d",
            "includePrePost": "false",
            "events": "div,splits",
        }
        r = await client.get(url, params=params)
        if r.status_code >= 400:
            return {
                "symbol": sym,
                "market": "KSA",
                "currency": "SAR",
                "data_source": "yahoo_chart",
                "data_quality": "MISSING",
                "error": f"yahoo_chart http {r.status_code}",
            }

        data = r.json() if r.content else {}
        chart = (data or {}).get("chart") or {}
        result = (chart.get("result") or [])
        if not result or not isinstance(result, list) or not isinstance(result[0], dict):
            err = chart.get("error") or {}
            msg = err.get("description") or err.get("message") or "yahoo_chart empty result"
            return {
                "symbol": sym,
                "market": "KSA",
                "currency": "SAR",
                "data_source": "yahoo_chart",
                "data_quality": "MISSING",
                "error": str(msg),
            }

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
        dq = "FULL" if (current is not None and prev is not None and day_high is not None and day_low is not None) else (
            "PARTIAL" if current is not None else "MISSING"
        )

        return {
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
            "error": None if current is not None else "yahoo_chart returned no price",
            "last_updated_utc": _now_utc_iso(),
            "last_updated_riyadh": _now_riyadh_iso(),
        }
    except Exception as exc:
        return {
            "symbol": sym,
            "market": "KSA",
            "currency": "SAR",
            "data_source": "yahoo_chart",
            "data_quality": "MISSING",
            "error": f"yahoo_chart error: {exc}",
        }


# =============================================================================
# Legacy fallback: yfinance (optional)
# =============================================================================
async def _fetch_ksa_yfinance(sym: str) -> Dict[str, Any]:
    """
    KSA fallback using yfinance (sync library -> run in thread).
    Returns a UnifiedQuote-like dict.
    """
    if _yf is None:
        return {
            "symbol": sym,
            "market": "KSA",
            "currency": "SAR",
            "data_quality": "MISSING",
            "data_source": "yfinance",
            "error": "yfinance not available",
        }

    def _work() -> Dict[str, Any]:
        try:
            t = _yf.Ticker(sym)  # type: ignore

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

            price = None
            prev = None
            vol = None

            for k in ("last_price", "lastPrice", "regularMarketPrice"):
                if k in fast and fast.get(k) not in (None, ""):
                    price = fast.get(k)
                    break
            for k in ("previous_close", "previousClose", "regularMarketPreviousClose"):
                if k in fast and fast.get(k) not in (None, ""):
                    prev = fast.get(k)
                    break
            for k in ("volume", "regularMarketVolume"):
                if k in fast and fast.get(k) not in (None, ""):
                    vol = fast.get(k)
                    break

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

            mc = info.get("marketCap")
            pe = info.get("trailingPE")
            pb = info.get("priceToBook")
            dy = info.get("dividendYield")
            name = info.get("shortName") or info.get("longName")

            try:
                if dy is not None and 0 < float(dy) < 1:
                    dy = float(dy) * 100.0
            except Exception:
                pass

            return {
                "symbol": sym,
                "name": name,
                "market": "KSA",
                "currency": "SAR",
                "current_price": float(price) if price is not None else None,
                "previous_close": float(prev) if prev is not None else None,
                "percent_change": float(pct) if pct is not None else None,
                "volume": float(vol) if vol is not None else None,
                "market_cap": float(mc) if mc is not None else None,
                "pe_ttm": float(pe) if pe is not None else None,
                "pb": float(pb) if pb is not None else None,
                "dividend_yield": float(dy) if dy is not None else None,
                "data_source": "yfinance",
                "data_quality": "PARTIAL" if price is not None else "MISSING",
                "error": None if price is not None else "yfinance returned no price",
                "last_updated_utc": _now_utc_iso(),
                "last_updated_riyadh": _now_riyadh_iso(),
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

    return await asyncio.to_thread(_work)


# =============================================================================
# Transformation + scoring
# =============================================================================
def _to_scored_enriched(uq: Any, requested_symbol: str) -> Any:
    sym = _normalize_ksa_symbol(requested_symbol) or (str(requested_symbol or "").strip().upper() or "UNKNOWN")
    forced: Dict[str, Any] = {"symbol": sym, "market": "KSA", "currency": "SAR"}

    try:
        eq = EnrichedQuote.from_unified(uq)  # type: ignore

        # force missing identity fields
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

        # scoring (best-effort)
        try:
            from core.scoring_engine import enrich_with_scores  # type: ignore

            try:
                eq2 = enrich_with_scores(eq)  # type: ignore
                if eq2 is not None:
                    eq = eq2
            except Exception:
                pass
        except Exception:
            pass

        # force symbol normalized
        try:
            if getattr(eq, "symbol", None) != sym:
                if hasattr(eq, "model_copy"):
                    eq = eq.model_copy(update={"symbol": sym})
                else:
                    eq = eq.copy(update={"symbol": sym})
        except Exception:
            pass

        # ✅ force timestamps (never null)
        eq = _ensure_timestamps(eq)

        return eq

    except Exception as exc:
        logger.exception("[argaam] transform error for %s", sym)
        eq = EnrichedQuote(  # type: ignore
            symbol=sym,
            market="KSA",
            currency="SAR",
            data_quality="MISSING",
            error=f"Transform error: {exc}",
        )
        return _ensure_timestamps(eq)


# =============================================================================
# Engine call shims (timeout + batching)
# =============================================================================
async def _engine_get_quote(engine: Any, sym: str, timeout_sec: float) -> Any:
    async def _call() -> Any:
        if hasattr(engine, "get_enriched_quote"):
            return await engine.get_enriched_quote(sym)  # type: ignore
        if hasattr(engine, "get_quote"):
            return await engine.get_quote(sym)  # type: ignore
        res = await _engine_get_quotes(engine, [sym], timeout_sec=timeout_sec, concurrency=1)
        return res[0] if res else {"symbol": sym, "data_quality": "MISSING", "error": "No data returned"}

    try:
        return await asyncio.wait_for(_call(), timeout=timeout_sec)
    except Exception as exc:
        return {"symbol": sym, "data_quality": "MISSING", "error": f"Engine quote error: {exc}"}


async def _engine_get_quotes(engine: Any, syms: List[str], *, timeout_sec: float, concurrency: int) -> List[Any]:
    async def _call_batch() -> List[Any]:
        if hasattr(engine, "get_enriched_quotes"):
            return await engine.get_enriched_quotes(syms)  # type: ignore
        if hasattr(engine, "get_quotes"):
            return await engine.get_quotes(syms)  # type: ignore

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
# Apply fallback(s) for missing KSA items
# =============================================================================
async def _apply_fallback_for_missing(targets: List[str], items: List[Any], cfg: Dict[str, Any]) -> List[Any]:
    """
    Given engine-returned items for targets (same order), replace missing ones using:
      1) yahoo_chart (if enabled)
      2) yfinance (if enabled)
    """
    allow_ych = bool(cfg.get("allow_yahoo_chart_fallback", True))
    allow_yf = bool(cfg.get("allow_yfinance_fallback", False))

    ych_sem = asyncio.Semaphore(max(1, int(cfg.get("yahoo_chart_concurrency", 6) or 6)))
    yf_sem = asyncio.Semaphore(max(1, int(cfg.get("yf_concurrency", 3) or 3)))

    async def _fix_one(i: int) -> Any:
        uq = items[i]
        if not _needs_ksa_fallback(uq):
            return uq

        sym = targets[i]

        # 1) Yahoo Chart
        if allow_ych:
            async with ych_sem:
                ych = await _fetch_ksa_yahoo_chart(sym)
            if str(ych.get("data_quality") or "").upper() != "MISSING":
                return ych
            # if still missing, keep error for debug chaining
            uq_d = _model_to_dict(uq)
            e0 = str(uq_d.get("error") or "")
            e1 = str(ych.get("error") or "")
            uq_d["error"] = (e0 + " | yahoo_chart_failed: " + e1).strip(" |")
            uq = uq_d

        # 2) yfinance
        if allow_yf:
            async with yf_sem:
                yf_uq = await _fetch_ksa_yfinance(sym)
            if str(yf_uq.get("data_quality") or "").upper() != "MISSING":
                return yf_uq
            uq_d = _model_to_dict(uq)
            e0 = str(uq_d.get("error") or "")
            e1 = str(yf_uq.get("error") or "")
            uq_d["error"] = (e0 + " | yfinance_failed: " + e1).strip(" |")
            return uq_d

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

        # expose engine flags if present
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

    # also reflect route fallback config
    fallbacks = {
        "yahoo_chart_available": True,  # route uses httpx, not optional
        "allow_yahoo_chart_fallback": bool(cfg.get("allow_yahoo_chart_fallback", True)),
        "yahoo_chart_concurrency": int(cfg.get("yahoo_chart_concurrency", 6) or 6),
        "yfinance_available": bool(_yf),
        "allow_yfinance_fallback": bool(cfg.get("allow_yfinance_fallback", False)),
        "yfinance_concurrency": int(cfg.get("yf_concurrency", 3) or 3),
    }

    return {
        "status": "ok",
        "module": "routes_argaam",
        "route_version": ROUTE_VERSION,
        "app_version": app_version,
        "engine_available": bool(eng),
        "engine": "DataEngineV2" if bool(eng) else "unavailable",
        "providers": providers,
        "ksa_providers": ksa_providers,
        "engine_flags": flags,
        "ksa_mode": "STRICT",
        "auth": "open" if not _allowed_tokens() else "token",
        "fallbacks": fallbacks,
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
    debug: int = Query(default=0, description="debug=1 appends fallback trace into error (best-effort)"),
) -> Any:
    err = _auth_error(x_app_token)
    if err:
        eq = EnrichedQuote(  # type: ignore
            symbol=(symbol or "").strip().upper() or "UNKNOWN",
            market="KSA",
            currency="SAR",
            data_quality="MISSING",
            error=err,
        )
        return _ensure_timestamps(eq)

    ksa_symbol = _normalize_ksa_symbol(symbol)
    if not ksa_symbol:
        eq = EnrichedQuote(  # type: ignore
            symbol=(symbol or "").strip().upper() or "UNKNOWN",
            market="KSA",
            currency="SAR",
            data_quality="MISSING",
            error=f"Invalid KSA symbol '{symbol}'. Must be numeric or end in .SR",
        )
        return _ensure_timestamps(eq)

    cfg = _cfg()
    eng = await _resolve_engine(request)

    if eng is None:
        uq: Any = {"symbol": ksa_symbol, "market": "KSA", "currency": "SAR", "data_quality": "MISSING", "error": "Engine unavailable"}
    else:
        uq = await _engine_get_quote(eng, ksa_symbol, timeout_sec=cfg["timeout_sec"])

    # Apply fallback(s) if needed (yahoo_chart first, then optional yfinance)
    if _needs_ksa_fallback(uq):
        fixed = await _apply_fallback_for_missing([ksa_symbol], [uq], cfg)
        uq = fixed[0] if fixed else uq
        if debug:
            # keep whatever error trace we built inside the fallback pipeline
            pass

    eq = _to_scored_enriched(uq, ksa_symbol)
    return _ensure_timestamps(eq)


@router.post("/quotes")
async def ksa_batch_quotes(
    request: Request,
    body: ArgaamBatchRequest = Body(...),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> List[Any]:
    err = _auth_error(x_app_token)
    raw_list = (body.symbols or []) + (body.tickers or [])
    if not raw_list:
        return []

    cfg = _cfg()

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

    out: List[Any] = []

    if err:
        for t in targets:
            eq = EnrichedQuote(  # type: ignore
                symbol=t,
                market="KSA",
                currency="SAR",
                data_quality="MISSING",
                error=err,
            )
            out.append(_ensure_timestamps(eq))
    else:
        eng = await _resolve_engine(request)

        if eng is None:
            # build missing shells then apply fallback(s)
            shells = [{"symbol": t, "market": "KSA", "currency": "SAR", "data_quality": "MISSING", "error": "Engine unavailable"} for t in targets]
            fixed = await _apply_fallback_for_missing(targets, shells, cfg)
            for i, t in enumerate(targets):
                out.append(_ensure_timestamps(_to_scored_enriched(fixed[i], t)))
        else:
            batch_size = max(1, int(cfg["batch_size"]))
            for part in [targets[i : i + batch_size] for i in range(0, len(targets), batch_size)]:
                got = await _engine_get_quotes(eng, part, timeout_sec=cfg["timeout_sec"], concurrency=cfg["concurrency"])

                # map by symbol
                m: Dict[str, Any] = {}
                for q in got or []:
                    d = _model_to_dict(q)
                    k = str(d.get("symbol") or d.get("ticker") or "").upper()
                    if k:
                        m[k] = q

                ordered = [m.get(t.upper()) or {"symbol": t, "market": "KSA", "currency": "SAR", "data_quality": "MISSING", "error": "No data returned"} for t in part]
                ordered = await _apply_fallback_for_missing(part, ordered, cfg)

                for i, t in enumerate(part):
                    out.append(_ensure_timestamps(_to_scored_enriched(ordered[i], t)))

    for bad in invalid:
        eq = EnrichedQuote(  # type: ignore
            symbol=bad.upper(),
            market="KSA",
            currency="SAR",
            data_quality="MISSING",
            error="Invalid KSA symbol (must be numeric or end in .SR)",
        )
        out.append(_ensure_timestamps(eq))

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
            meta={
                "sheet_name": sheet_name,
                "requested": len(raw_list),
                "valid": 0,
                "invalid": len(invalid),
                "timestamp_utc": _now_utc_iso(),
            },
        )

    eng = await _resolve_engine(request)

    batch_size = max(1, int(cfg["batch_size"]))
    any_fail = False

    for part in [valid_targets[i : i + batch_size] for i in range(0, len(valid_targets), batch_size)]:
        if eng is None:
            got = [{"symbol": t, "market": "KSA", "currency": "SAR", "data_quality": "MISSING", "error": "Engine unavailable"} for t in part]
        else:
            got = await _engine_get_quotes(eng, part, timeout_sec=cfg["timeout_sec"], concurrency=cfg["concurrency"])

        # map by symbol
        m: Dict[str, Any] = {}
        for q in got or []:
            d = _model_to_dict(q)
            k = str(d.get("symbol") or d.get("ticker") or "").upper()
            if k:
                m[k] = q

        ordered = [m.get(t.upper()) or {"symbol": t, "market": "KSA", "currency": "SAR", "data_quality": "MISSING", "error": "No data"} for t in part]
        ordered = await _apply_fallback_for_missing(part, ordered, cfg)

        for i, t in enumerate(part):
            uq = ordered[i]
            eq = _ensure_timestamps(_to_scored_enriched(uq, t))
            try:
                row = eq.to_row(headers)  # type: ignore
                if not isinstance(row, list):
                    raise ValueError("to_row did not return a list")
                if len(row) < len(headers):
                    row += [None] * (len(headers) - len(row))
                rows.append(row[: len(headers)])
            except Exception as exc:
                any_fail = True
                dq = str(getattr(eq, "data_quality", "MISSING") or "MISSING")
                rows.append(_row_with_error(headers, t, f"Row mapping failed: {exc}", data_quality=dq))

    status = "success"
    if invalid or any_fail:
        status = "partial"

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
            "fallback_yahoo_chart": bool(cfg.get("allow_yahoo_chart_fallback", True)),
            "fallback_yfinance": bool(cfg.get("allow_yfinance_fallback", False)),
        },
    )


__all__ = ["router"]
