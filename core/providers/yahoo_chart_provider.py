# core/providers/yahoo_chart_provider.py
"""
core/providers/yahoo_chart_provider.py
============================================================
Yahoo Quote/Chart Provider (KSA-safe) — v2.0.0
(PATCH-ALIGNED + CUSTOM HEADERS + TTL CACHE + HISTORY/FORECAST + CLIENT REUSE)

Primary goals
- Reliable quote source for KSA (.SR) without yfinance.
- Prefer Yahoo v7 quote endpoint (fast, rich fields).
- Fallback to Yahoo v8 chart endpoint (robust when quote is missing/blocked).
- Optional supplement mode: if quote is partial, enrich missing fields via chart.
- Optional history analytics + forecasting from chart candles.

✅ Engine alignment
- fetch_quote_patch / fetch_enriched_quote_patch / fetch_quote_and_enrichment_patch
  return Dict[str, Any] ONLY (NO tuples).

Exports (engine-compatible)
- fetch_quote_patch(symbol, debug=False) -> dict patch (fast)
- fetch_enriched_quote_patch(symbol, debug=False) -> dict patch (quote + (optional) history/forecast)
- fetch_quote_and_enrichment_patch(symbol, debug=False) -> dict patch
- fetch_quote(symbol, debug=False) -> dict (compat; includes status/data_quality/error)
- get_quote(symbol, debug=False)    (alias)
- aclose_yahoo_client()            (best-effort close hook)
- YahooChartProvider               (class wrapper)

Env vars (supported)
Core
- YAHOO_ENABLED (default true)                          # set false to disable silently
- YAHOO_UA                                           # user agent override
- YAHOO_ACCEPT_LANGUAGE (default: en-US,en;q=0.9)
- YAHOO_TIMEOUT_S (default 8.5)                       # connects fast, keep-alive reuse
- YAHOO_RETRY_MAX (default 2)                         # additional retries (0 => no retry)
- YAHOO_RETRY_BACKOFF_MS (default 250)
- YAHOO_ALT_HOSTS (comma-separated hosts or full base urls)

Headers (CUSTOMIZED)
- YAHOO_HEADERS_JSON              (base headers JSON dict)
- YAHOO_HEADERS_QUOTE_JSON        (quote endpoint override JSON dict)
- YAHOO_HEADERS_CHART_JSON        (chart endpoint override JSON dict)

Chart behavior
- ENABLE_YAHOO_CHART_SUPPLEMENT (default true)
- YAHOO_CHART_RANGE (default 5d)                       # for quote fallback/supplement
- YAHOO_CHART_INTERVAL (default 1d)

History/Forecast (per your “data forecasting” plan)
- YAHOO_ENABLE_HISTORY (default true)
- YAHOO_ENABLE_FORECAST (default true)
- YAHOO_HISTORY_RANGE (default 2y)                     # used by enriched mode
- YAHOO_HISTORY_INTERVAL (default 1d)
- YAHOO_HISTORY_POINTS_MAX (default 420, min 120)      # cap arrays for speed
- YAHOO_VERBOSE_WARNINGS (default false)               # adds _warn in patch

Caching (TTL)
- YAHOO_TTL_QUOTE_SEC   (default 10, min 3)
- YAHOO_TTL_CHART_SEC   (default 20, min 3)
- YAHOO_TTL_HISTORY_SEC (default 900, min 60)

Forecast outputs (best-effort; simple momentum-style)
- returns_1w/1m/3m/6m/12m (pct)
- ma20/ma50/ma200
- volatility_30d (annualized %, based on daily returns)
- rsi_14
- expected_return_1m/3m/12m (pct)
- expected_price_1m/3m/12m
- confidence_score (0..100)
- forecast_method
- history_points, history_last_ts

Notes
- Forecast is a simple quantitative estimate (NOT investment advice).
- Provider remains safe: no network at import-time, no exceptions to callers.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import random
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List

import httpx

PROVIDER_VERSION = "2.0.0"
PROVIDER_NAME = "yahoo_chart"

# ---------------------------
# Safe env parsing
# ---------------------------
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}


def _env_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in _FALSY:
        return False
    if raw in _TRUTHY:
        return True
    return default


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        f = float(str(v).strip())
        return f if f > 0 else default
    except Exception:
        return default


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(str(v).strip())
    except Exception:
        return default


def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip()
    return s if s else default


def _configured() -> bool:
    return _env_bool("YAHOO_ENABLED", True)


# ---------------------------
# HTTP config
# ---------------------------
UA = _env_str(
    "YAHOO_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
)

_ACCEPT_LANGUAGE = _env_str("YAHOO_ACCEPT_LANGUAGE", "en-US,en;q=0.9")

_TIMEOUT_S = _env_float("YAHOO_TIMEOUT_S", 8.5)
TIMEOUT = httpx.Timeout(timeout=_TIMEOUT_S, connect=min(5.0, _TIMEOUT_S))

# Retries for transient Yahoo responses
_RETRY_MAX = int(_env_str("YAHOO_RETRY_MAX", "2") or "2")  # additional retries
_RETRY_BACKOFF_MS = _env_float("YAHOO_RETRY_BACKOFF_MS", 250.0)
_RETRY_STATUSES = {429, 500, 502, 503, 504}

# If quote is partial, supplement missing fields from chart (default ON)
ENABLE_SUPPLEMENT = _env_bool("ENABLE_YAHOO_CHART_SUPPLEMENT", True)

# History / forecast
ENABLE_HISTORY = _env_bool("YAHOO_ENABLE_HISTORY", True)
ENABLE_FORECAST = _env_bool("YAHOO_ENABLE_FORECAST", True)
VERBOSE_WARN = _env_bool("YAHOO_VERBOSE_WARNINGS", False)

CHART_RANGE_FAST = _env_str("YAHOO_CHART_RANGE", "5d")
CHART_INTERVAL_FAST = _env_str("YAHOO_CHART_INTERVAL", "1d")

HISTORY_RANGE = _env_str("YAHOO_HISTORY_RANGE", "2y")
HISTORY_INTERVAL = _env_str("YAHOO_HISTORY_INTERVAL", "1d")
HISTORY_POINTS_MAX = max(120, _env_int("YAHOO_HISTORY_POINTS_MAX", 420))

# Allow alternative hosts (comma-separated): query2.finance.yahoo.com, etc.
_DEFAULT_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
_DEFAULT_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

_ALT_HOSTS = [h.strip() for h in (_env_str("YAHOO_ALT_HOSTS", "")).split(",") if h.strip()]

QUOTE_URLS: List[str] = [_DEFAULT_QUOTE_URL]
CHART_URLS: List[str] = [_DEFAULT_CHART_URL]

for host in _ALT_HOSTS:
    if "://" in host:
        base = host.rstrip("/")
        QUOTE_URLS.append(f"{base}/v7/finance/quote")
        CHART_URLS.append(f"{base}/v8/finance/chart/{{symbol}}")
    else:
        QUOTE_URLS.append(f"https://{host}/v7/finance/quote")
        CHART_URLS.append(f"https://{host}/v8/finance/chart/{{symbol}}")


# ---------------------------
# TTL cache (import-safe if cachetools missing)
# ---------------------------
try:
    from cachetools import TTLCache  # type: ignore

    _HAS_CACHETOOLS = True
except Exception:  # pragma: no cover
    _HAS_CACHETOOLS = False

    class TTLCache(dict):  # type: ignore
        def __init__(self, maxsize: int = 1024, ttl: float = 60.0) -> None:
            super().__init__()
            self._maxsize = max(1, int(maxsize))
            self._ttl = max(1.0, float(ttl))
            self._exp: Dict[str, float] = {}

        def get(self, key: str, default: Any = None) -> Any:  # type: ignore
            now = time.time()
            exp = self._exp.get(key)
            if exp is not None and exp < now:
                try:
                    super().pop(key, None)
                except Exception:
                    pass
                self._exp.pop(key, None)
                return default
            return super().get(key, default)

        def __setitem__(self, key: str, value: Any) -> None:  # type: ignore
            if len(self) >= self._maxsize:
                try:
                    oldest_key = next(iter(self.keys()))
                    super().pop(oldest_key, None)
                    self._exp.pop(oldest_key, None)
                except Exception:
                    pass
            super().__setitem__(key, value)
            self._exp[key] = time.time() + self._ttl


def _ttl_quote() -> float:
    return max(3.0, _env_float("YAHOO_TTL_QUOTE_SEC", 10.0))


def _ttl_chart() -> float:
    return max(3.0, _env_float("YAHOO_TTL_CHART_SEC", 20.0))


def _ttl_history() -> float:
    return max(60.0, _env_float("YAHOO_TTL_HISTORY_SEC", 900.0))


_Q_CACHE: TTLCache = TTLCache(maxsize=9000, ttl=_ttl_quote())
_C_CACHE: TTLCache = TTLCache(maxsize=5000, ttl=_ttl_chart())
_H_CACHE: TTLCache = TTLCache(maxsize=2500, ttl=_ttl_history())


# ---------------------------
# Client reuse (keep-alive)
# ---------------------------
_CLIENT: Optional[httpx.AsyncClient] = None
_CLIENT_LOCK = asyncio.Lock()


def _json_headers(env_key: str) -> Dict[str, str]:
    raw = _env_str(env_key, "")
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return {str(k): str(v) for k, v in obj.items()}
    except Exception:
        return {}
    return {}


_BASE_HEADERS = {
    "User-Agent": UA,
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": _ACCEPT_LANGUAGE,
    "Connection": "keep-alive",
}
_BASE_HEADERS.update(_json_headers("YAHOO_HEADERS_JSON"))


def _headers_for(kind: str, symbol: str) -> Dict[str, str]:
    """
    Merges:
      base headers (YAHOO_HEADERS_JSON)
      + endpoint overrides (YAHOO_HEADERS_QUOTE_JSON / YAHOO_HEADERS_CHART_JSON)
      + dynamic Referer/Origin (helps Yahoo behave)
    """
    sym = (symbol or "").strip().upper()
    ref = f"https://finance.yahoo.com/quote/{sym}"
    h = dict(_BASE_HEADERS)

    if kind == "quote":
        h.update(_json_headers("YAHOO_HEADERS_QUOTE_JSON"))
    elif kind == "chart":
        h.update(_json_headers("YAHOO_HEADERS_CHART_JSON"))

    # Always keep these dynamic (unless user explicitly overrides via headers JSON)
    h.setdefault("Referer", ref)
    h.setdefault("Origin", "https://finance.yahoo.com")
    return h


async def _get_client() -> httpx.AsyncClient:
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT
    async with _CLIENT_LOCK:
        if _CLIENT is None:
            _CLIENT = httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=True)
    return _CLIENT


async def aclose_yahoo_client() -> None:
    global _CLIENT
    c = _CLIENT
    _CLIENT = None
    if c is not None:
        try:
            await c.aclose()
        except Exception:
            pass


# ---------------------------
# Helpers
# ---------------------------
_KSA_CODE_RE = re.compile(r"^\d{3,6}$")
_KSA_RE = re.compile(r"^\d{3,6}(\.SR)?$", re.IGNORECASE)


def _is_nan(x: Any) -> bool:
    try:
        return x is None or (isinstance(x, float) and math.isnan(x))
    except Exception:
        return False


def _to_float(x: Any) -> Optional[float]:
    try:
        if _is_nan(x) or x is None:
            return None
        if isinstance(x, bool):
            return None
        return float(x)
    except Exception:
        return None


def _safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s or None


def _last_non_null(vals: Any) -> Optional[float]:
    try:
        if not isinstance(vals, list) or not vals:
            return None
        for v in reversed(vals):
            f = _to_float(v)
            if f is not None:
                return f
        return None
    except Exception:
        return None


def _position_52w(cur: Optional[float], lo: Optional[float], hi: Optional[float]) -> Optional[float]:
    if cur is None or lo is None or hi is None:
        return None
    if hi == lo:
        return None
    try:
        return (cur - lo) / (hi - lo) * 100.0
    except Exception:
        return None


def _normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", "").strip()
    # numeric Tadawul code -> .SR
    if _KSA_CODE_RE.match(s):
        return f"{s}.SR"
    return s


def _base_patch(symbol: str) -> Dict[str, Any]:
    sym = (symbol or "").strip().upper()
    is_ksa = sym.endswith(".SR")
    return {
        "symbol": sym,
        "market": "KSA" if is_ksa else "GLOBAL",
        "currency": "SAR" if is_ksa else None,
        "data_source": PROVIDER_NAME,
        "provider_version": PROVIDER_VERSION,
        "error": "",
        # identity
        "name": None,
        "exchange": None,
        # price / trading
        "current_price": None,
        "previous_close": None,
        "open": None,
        "day_high": None,
        "day_low": None,
        "week_52_high": None,
        "week_52_low": None,
        "position_52w_percent": None,
        "volume": None,
        "avg_volume_30d": None,
        "market_cap": None,
        "value_traded": None,
        "price_change": None,
        "percent_change": None,
        # history/forecast extras (enriched mode)
        "returns_1w": None,
        "returns_1m": None,
        "returns_3m": None,
        "returns_6m": None,
        "returns_12m": None,
        "ma20": None,
        "ma50": None,
        "ma200": None,
        "volatility_30d": None,
        "rsi_14": None,
        "expected_return_1m": None,
        "expected_return_3m": None,
        "expected_return_12m": None,
        "expected_price_1m": None,
        "expected_price_3m": None,
        "expected_price_12m": None,
        "confidence_score": None,
        "forecast_method": None,
        "history_points": None,
        "history_last_ts": None,
    }


def _clean_patch(p: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (p or {}).items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        out[k] = v
    return out


def _fill_derived(p: Dict[str, Any]) -> None:
    cur = _to_float(p.get("current_price"))
    prev = _to_float(p.get("previous_close"))
    vol = _to_float(p.get("volume"))

    if p.get("price_change") is None and cur is not None and prev is not None:
        p["price_change"] = cur - prev

    if p.get("percent_change") is None and cur is not None and prev not in (None, 0.0):
        try:
            p["percent_change"] = (cur - prev) / prev * 100.0
        except Exception:
            pass

    if p.get("value_traded") is None and cur is not None and vol is not None:
        p["value_traded"] = cur * vol

    if p.get("position_52w_percent") is None:
        hi = _to_float(p.get("week_52_high"))
        lo = _to_float(p.get("week_52_low"))
        p["position_52w_percent"] = _position_52w(cur, lo, hi)


def _is_patch_partial(p: Dict[str, Any]) -> bool:
    need = ("day_high", "day_low", "volume", "previous_close")
    for k in need:
        if _to_float(p.get(k)) is None:
            return True
    return False


def _guard_52w(p: Dict[str, Any]) -> None:
    """
    Yahoo sometimes returns fiftyTwoWeekLow = 0.0 for KSA.
    If low==0 while price is normal, treat as missing.
    """
    cur = _to_float(p.get("current_price"))
    lo = _to_float(p.get("week_52_low"))
    if lo is not None and lo == 0.0 and cur is not None and cur > 1.0:
        p["week_52_low"] = None
    if _to_float(p.get("week_52_low")) is None:
        p["position_52w_percent"] = None


# ---------------------------
# Forecast helpers
# ---------------------------
def _stddev(xs: List[float]) -> Optional[float]:
    try:
        n = len(xs)
        if n < 2:
            return None
        mu = sum(xs) / n
        var = sum((x - mu) ** 2 for x in xs) / (n - 1)
        return math.sqrt(var)
    except Exception:
        return None


def _return_pct(last: float, prior: float) -> Optional[float]:
    try:
        if prior == 0:
            return None
        return (last / prior - 1.0) * 100.0
    except Exception:
        return None


def _compute_rsi_14(closes: List[float]) -> Optional[float]:
    try:
        if len(closes) < 15:
            return None
        gains: List[float] = []
        losses: List[float] = []
        for i in range(-14, 0):
            d = closes[i] - closes[i - 1]
            if d >= 0:
                gains.append(d)
                losses.append(0.0)
            else:
                gains.append(0.0)
                losses.append(-d)
        avg_gain = sum(gains) / 14.0
        avg_loss = sum(losses) / 14.0
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1.0 + rs))
        if math.isnan(rsi) or math.isinf(rsi):
            return None
        return float(max(0.0, min(100.0, rsi)))
    except Exception:
        return None


def _history_analytics(closes: List[float]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not closes:
        return out

    closes = closes[-HISTORY_POINTS_MAX:]
    last = closes[-1]

    # returns (trading-day approximations)
    idx = {
        "returns_1w": 5,
        "returns_1m": 21,
        "returns_3m": 63,
        "returns_6m": 126,
        "returns_12m": 252,
    }
    for k, n in idx.items():
        if len(closes) > n:
            out[k] = _return_pct(last, closes[-(n + 1)])

    def ma(n: int) -> Optional[float]:
        if len(closes) < n:
            return None
        return sum(closes[-n:]) / float(n)

    out["ma20"] = ma(20)
    out["ma50"] = ma(50)
    out["ma200"] = ma(200)

    # volatility 30d annualized %
    if len(closes) >= 31:
        window = closes[-31:]
        rets: List[float] = []
        for i in range(1, len(window)):
            p0 = window[i - 1]
            p1 = window[i]
            if p0 != 0:
                rets.append((p1 / p0) - 1.0)
        sd = _stddev(rets)
        if sd is not None:
            out["volatility_30d"] = float(sd * math.sqrt(252.0) * 100.0)

    out["rsi_14"] = _compute_rsi_14(closes)

    def mean_daily_return(n_days: int) -> Optional[float]:
        if len(closes) < (n_days + 2):
            return None
        start = closes[-(n_days + 1)]
        if start == 0:
            return None
        total = (last / start) - 1.0
        return total / float(n_days)

    r1m_d = mean_daily_return(21)
    r3m_d = mean_daily_return(63)
    r12m_d = mean_daily_return(252)

    if ENABLE_FORECAST:
        if r1m_d is not None:
            out["expected_return_1m"] = float(r1m_d * 21.0 * 100.0)
            out["expected_price_1m"] = float(last * (1.0 + (out["expected_return_1m"] / 100.0)))
        if r3m_d is not None:
            out["expected_return_3m"] = float(r3m_d * 63.0 * 100.0)
            out["expected_price_3m"] = float(last * (1.0 + (out["expected_return_3m"] / 100.0)))
        if r12m_d is not None:
            out["expected_return_12m"] = float(r12m_d * 252.0 * 100.0)
            out["expected_price_12m"] = float(last * (1.0 + (out["expected_return_12m"] / 100.0)))
        out["forecast_method"] = "yahoo_history_momentum_v1"
    else:
        out["forecast_method"] = "history_only"

    # confidence_score (0..100)
    pts = len(closes)
    base = max(0.0, min(100.0, (pts / 252.0) * 100.0))
    vol = _to_float(out.get("volatility_30d"))
    if vol is None:
        conf = base * 0.75
    else:
        penalty = min(60.0, max(0.0, (vol / 100.0) * 35.0))
        conf = max(0.0, min(100.0, base - penalty))
    out["confidence_score"] = float(conf)

    return out


# ---------------------------
# HTTP
# ---------------------------
async def _http_get_json(
    url: str,
    symbol: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    kind: str,
) -> Tuple[Optional[dict], Optional[str]]:
    client = await _get_client()
    headers = _headers_for(kind, symbol)

    last_err: Optional[str] = None
    attempts = 1 + max(0, _RETRY_MAX)

    for i in range(attempts):
        try:
            r = await client.get(url, params=params, headers=headers)

            if r.status_code == 200:
                try:
                    js = r.json()
                    if isinstance(js, dict):
                        return js, None
                    last_err = "unexpected JSON type"
                except Exception as je:
                    last_err = f"JSON decode failed: {je.__class__.__name__}"
            else:
                if r.status_code in _RETRY_STATUSES and i < attempts - 1:
                    last_err = f"HTTP {r.status_code}"
                else:
                    return None, f"HTTP {r.status_code}"

        except Exception as e:
            last_err = f"{e.__class__.__name__}: {e}"

        if i < attempts - 1:
            delay = (_RETRY_BACKOFF_MS / 1000.0) * (2**i)
            jitter = random.random() * 0.25
            await asyncio.sleep(min(2.5, delay + jitter))

    return None, last_err or "request failed"


async def _http_get_json_multi(
    urls: List[str],
    symbol: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    kind: str,
) -> Tuple[Optional[dict], Optional[str], Optional[str]]:
    last_err = None
    for u in urls:
        js, err = await _http_get_json(u, symbol, params=params, kind=kind)
        if js is not None:
            return js, None, u
        last_err = err
    return None, last_err or "all hosts failed", None


# ---------------------------
# Internal fetchers (return (patch, warn))
# ---------------------------
async def _fetch_from_quote(symbol: str, debug: bool = False) -> Tuple[Dict[str, Any], Optional[str]]:
    patch = _base_patch(symbol)
    params = {
        "symbols": symbol,
        "formatted": "false",
        "region": "SA" if symbol.endswith(".SR") else "US",
        "lang": "en-US",
    }

    js, err, used = await _http_get_json_multi(QUOTE_URLS, symbol, params=params, kind="quote")
    if js is None:
        return patch, f"yahoo_quote failed: {err}"

    try:
        res = (js.get("quoteResponse") or {}).get("result") or []
        if not res:
            return patch, "yahoo_quote empty result"
        q = res[0] or {}
    except Exception:
        return patch, "yahoo_quote parse error"

    patch["name"] = _safe_str(q.get("longName") or q.get("shortName") or q.get("displayName"))
    patch["currency"] = _safe_str(q.get("currency")) or patch.get("currency")
    patch["exchange"] = _safe_str(q.get("fullExchangeName") or q.get("exchange"))

    patch["current_price"] = _to_float(q.get("regularMarketPrice"))
    patch["previous_close"] = _to_float(q.get("regularMarketPreviousClose"))
    patch["open"] = _to_float(q.get("regularMarketOpen"))
    patch["day_high"] = _to_float(q.get("regularMarketDayHigh"))
    patch["day_low"] = _to_float(q.get("regularMarketDayLow"))

    patch["week_52_high"] = _to_float(q.get("fiftyTwoWeekHigh"))
    patch["week_52_low"] = _to_float(q.get("fiftyTwoWeekLow"))

    patch["volume"] = _to_float(q.get("regularMarketVolume"))
    patch["avg_volume_30d"] = _to_float(q.get("averageDailyVolume3Month") or q.get("averageDailyVolume10Day"))
    patch["market_cap"] = _to_float(q.get("marketCap"))

    if debug:
        patch["_debug_quote"] = {
            "used_url": used,
            "quoteType": q.get("quoteType"),
            "exchange": q.get("exchange"),
            "fullExchangeName": q.get("fullExchangeName"),
            "marketState": q.get("marketState"),
        }

    _guard_52w(patch)
    _fill_derived(patch)
    return patch, None


def _parse_chart_result(js: dict) -> Tuple[Optional[dict], Optional[str]]:
    try:
        chart = js.get("chart") or {}
        if chart.get("error"):
            e = chart.get("error") or {}
            msg = str(e.get("description") or e.get("message") or "chart error").strip()
            return None, msg
        res = (chart.get("result") or [None])[0] or None
        if not isinstance(res, dict):
            return None, "chart result missing"
        return res, None
    except Exception:
        return None, "chart parse error"


async def _fetch_from_chart(
    symbol: str,
    *,
    range_: str,
    interval: str,
    debug: bool = False,
    cache_kind: str = "chart",
) -> Tuple[Dict[str, Any], Optional[str]]:
    patch = _base_patch(symbol)

    params = {
        "range": range_,
        "interval": interval,
        "includePrePost": "false",
        "events": "div|split|earn",
        "corsDomain": "finance.yahoo.com",
    }

    cache_key = f"{cache_kind}::{symbol}::{range_}::{interval}"
    if cache_kind == "chart":
        hit = _C_CACHE.get(cache_key)
    else:
        hit = _H_CACHE.get(cache_key)

    if isinstance(hit, dict) and hit:
        return dict(hit), None

    # chart URL is templated
    last_err = None
    used_url = None
    js = None

    for templ in CHART_URLS:
        url = templ.format(symbol=symbol)
        js, err, used = await _http_get_json_multi([url], symbol, params=params, kind="chart")
        used_url = used or url
        if js is not None:
            last_err = None
            break
        last_err = err

    if js is None:
        return patch, f"yahoo_chart failed: {last_err}"

    res, perr = _parse_chart_result(js)
    if res is None:
        return patch, f"yahoo_chart error: {perr}"

    meta = res.get("meta") or {}
    ind = (res.get("indicators") or {}).get("quote") or []
    q0 = ind[0] if ind else {}

    opens = q0.get("open") or []
    highs = q0.get("high") or []
    lows = q0.get("low") or []
    closes = q0.get("close") or []
    vols = q0.get("volume") or []
    ts = res.get("timestamp") or []

    patch["currency"] = _safe_str(meta.get("currency")) or patch.get("currency")
    patch["exchange"] = _safe_str(meta.get("exchangeName") or meta.get("fullExchangeName")) or patch.get("exchange")
    patch["name"] = _safe_str(meta.get("longName") or meta.get("shortName")) or patch.get("name")

    patch["current_price"] = _to_float(meta.get("regularMarketPrice")) or _last_non_null(closes)
    patch["previous_close"] = _to_float(meta.get("previousClose")) or _to_float(meta.get("chartPreviousClose"))
    patch["open"] = _to_float(meta.get("regularMarketOpen")) or _last_non_null(opens)
    patch["day_high"] = _to_float(meta.get("regularMarketDayHigh")) or _last_non_null(highs)
    patch["day_low"] = _to_float(meta.get("regularMarketDayLow")) or _last_non_null(lows)

    patch["week_52_high"] = _to_float(meta.get("fiftyTwoWeekHigh"))
    patch["week_52_low"] = _to_float(meta.get("fiftyTwoWeekLow"))

    patch["volume"] = _to_float(meta.get("regularMarketVolume")) or _last_non_null(vols)

    # If meta lacks 52w hi/lo, compute from the series (best-effort)
    try:
        if _to_float(patch.get("week_52_high")) is None and isinstance(highs, list) and highs:
            hs = [h for h in (_to_float(x) for x in highs) if h is not None]
            if hs:
                patch["week_52_high"] = max(hs)
        if _to_float(patch.get("week_52_low")) is None and isinstance(lows, list) and lows:
            ls = [l for l in (_to_float(x) for x in lows) if l is not None]
            if ls:
                patch["week_52_low"] = min(ls)
    except Exception:
        pass

    # history info
    try:
        if isinstance(ts, list) and ts:
            patch["history_last_ts"] = int(ts[-1])
            patch["history_points"] = int(len([x for x in closes if _to_float(x) is not None]))
    except Exception:
        pass

    if debug:
        patch["_debug_chart"] = {
            "used_url": used_url,
            "range": range_,
            "interval": interval,
            "meta_keys_sample": sorted(list(meta.keys()))[:80],
        }

    _guard_52w(patch)
    _fill_derived(patch)

    patch2 = _clean_patch(patch)
    if cache_kind == "chart":
        _C_CACHE[cache_key] = dict(patch2)
    else:
        _H_CACHE[cache_key] = dict(patch2)

    return patch2, None


async def _fetch_history_forecast_patch(symbol: str, debug: bool = False) -> Tuple[Dict[str, Any], Optional[str]]:
    if not (_configured() and ENABLE_HISTORY):
        return {}, None

    p, err = await _fetch_from_chart(
        symbol,
        range_=HISTORY_RANGE,
        interval=HISTORY_INTERVAL,
        debug=debug,
        cache_kind="history",
    )
    if err:
        return {}, err

    # Pull closes again from chart for analytics (requires re-fetching JSON arrays).
    # We do one more direct request but cache it separately for history analytics.
    # (Keeps quote fallback fast and avoids storing huge arrays in cache.)
    params = {
        "range": HISTORY_RANGE,
        "interval": HISTORY_INTERVAL,
        "includePrePost": "false",
        "events": "div|split|earn",
        "corsDomain": "finance.yahoo.com",
    }

    cache_key = f"hanalytics::{symbol}::{HISTORY_RANGE}::{HISTORY_INTERVAL}"
    hit = _H_CACHE.get(cache_key)
    if isinstance(hit, dict) and hit:
        return dict(hit), None

    js = None
    last_err = None
    for templ in CHART_URLS:
        url = templ.format(symbol=symbol)
        js, e, _ = await _http_get_json_multi([url], symbol, params=params, kind="chart")
        if js is not None:
            last_err = None
            break
        last_err = e

    if js is None:
        return {}, f"history fetch failed: {last_err}"

    res, perr = _parse_chart_result(js)
    if res is None:
        return {}, f"history parse failed: {perr}"

    ind = (res.get("indicators") or {}).get("quote") or []
    q0 = ind[0] if ind else {}
    closes_raw = q0.get("close") or []
    closes: List[float] = []
    for x in closes_raw if isinstance(closes_raw, list) else []:
        f = _to_float(x)
        if f is not None:
            closes.append(float(f))

    if len(closes) < 25:
        return {}, "history too short"

    analytics = _history_analytics(closes)

    patch: Dict[str, Any] = {}
    patch.update(analytics)

    # keep last_ts + points from p (already best-effort)
    if p.get("history_last_ts") is not None:
        patch["history_last_ts"] = p.get("history_last_ts")
    patch["history_points"] = len(closes[-HISTORY_POINTS_MAX:])

    patch = _clean_patch(patch)
    _H_CACHE[cache_key] = dict(patch)
    return patch, None


# ---------------------------
# Engine-facing PATCH API (DICT RETURNS)
# ---------------------------
async def fetch_quote_patch(symbol: str, debug: bool = False) -> Dict[str, Any]:
    """
    FAST path: quote endpoint first; chart fallback if needed.
    """
    if not _configured():
        return {}

    sym = _normalize_symbol(symbol)
    if not sym:
        return {"error": f"{PROVIDER_NAME}: empty symbol"}

    ck = f"quote_patch::{sym}"
    hit = _Q_CACHE.get(ck)
    if isinstance(hit, dict) and hit:
        return dict(hit)

    warns: List[str] = []

    # 1) quote endpoint
    p1, e1 = await _fetch_from_quote(sym, debug=debug)

    # 2) fallback to chart if quote fails or missing price
    if e1 or _to_float(p1.get("current_price")) is None:
        p2, e2 = await _fetch_from_chart(sym, range_=CHART_RANGE_FAST, interval=CHART_INTERVAL_FAST, debug=debug)
        if not e2 and _to_float(p2.get("current_price")) is not None:
            p2["error"] = ""
            out = _clean_patch(p2)
            _Q_CACHE[ck] = dict(out)
            return out

        # return best-effort patch with warning (allows engine fallback)
        warns.append(e1 or e2 or "yahoo quote+chart failed")
        p1["error"] = "warning: " + (warns[0] if warns else "yahoo quote+chart failed")
        out = _clean_patch(p1)
        _Q_CACHE[ck] = dict(out)
        return out

    # 3) optional supplement (quote partial -> fill missing from chart)
    if ENABLE_SUPPLEMENT and _is_patch_partial(p1):
        p2, e2 = await _fetch_from_chart(sym, range_=CHART_RANGE_FAST, interval=CHART_INTERVAL_FAST, debug=debug)
        if not e2:
            for k, v in (p2 or {}).items():
                if v is None:
                    continue
                if p1.get(k) is None or p1.get(k) == "":
                    p1[k] = v
            _guard_52w(p1)
            _fill_derived(p1)
        else:
            warns.append(f"supplement: {e2}")

    p1["error"] = ""
    if warns and VERBOSE_WARN:
        p1["_warn"] = " | ".join(warns)

    out = _clean_patch(p1)
    _Q_CACHE[ck] = dict(out)
    return out


async def fetch_enriched_quote_patch(symbol: str, debug: bool = False) -> Dict[str, Any]:
    """
    ENRICHED path:
    - Get quote patch (fast)
    - Add history/forecast fields (best-effort)
    """
    if not _configured():
        return {}

    base = await fetch_quote_patch(symbol, debug=debug)

    # If quote failed (only error), keep it as-is.
    if isinstance(base, dict) and base.get("error") and _to_float(base.get("current_price")) is None:
        return dict(base)

    if not ENABLE_HISTORY:
        return dict(base)

    sym = _normalize_symbol(symbol)
    if not sym:
        return dict(base)

    h_patch, h_err = await _fetch_history_forecast_patch(sym, debug=debug)
    if h_patch:
        # fill only if missing (don’t overwrite quote fields)
        for k, v in h_patch.items():
            if v is None:
                continue
            if base.get(k) in (None, ""):
                base[k] = v

    if h_err and VERBOSE_WARN:
        w = str(base.get("_warn") or "").strip()
        base["_warn"] = (w + " | " if w else "") + f"history: {h_err}"

    return _clean_patch(base)


async def fetch_quote_and_enrichment_patch(symbol: str, debug: bool = False) -> Dict[str, Any]:
    return await fetch_enriched_quote_patch(symbol, debug=debug)


# Back-compat helpers (return a full payload dict)
async def fetch_quote(symbol: str, debug: bool = False) -> Dict[str, Any]:
    patch = await fetch_quote_patch(symbol, debug=debug)
    out = dict(patch or {})
    has_price = _to_float(out.get("current_price")) is not None
    out.setdefault("data_quality", "OK" if has_price else "BAD")
    out["status"] = "success" if has_price else "error"
    out["error"] = str(out.get("error") or "")
    return out


async def get_quote(symbol: str, debug: bool = False) -> Dict[str, Any]:
    return await fetch_quote(symbol, debug=debug)


@dataclass
class YahooChartProvider:
    name: str = PROVIDER_NAME

    async def fetch_quote(self, symbol: str, debug: bool = False) -> Dict[str, Any]:
        return await fetch_quote(symbol, debug=debug)


__all__ = [
    "PROVIDER_VERSION",
    "PROVIDER_NAME",
    "fetch_quote_patch",
    "fetch_quote_and_enrichment_patch",
    "fetch_enriched_quote_patch",
    "fetch_quote",
    "get_quote",
    "aclose_yahoo_client",
    "YahooChartProvider",
]
