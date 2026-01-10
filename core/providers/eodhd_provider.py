# core/providers/eodhd_provider.py
"""
core/providers/eodhd_provider.py
============================================================
EODHD Provider — v2.0.0 (HARDENED + ENGINE-PATCH + CUSTOM HEADERS + HISTORY/FORECAST)

Key points
- ✅ Uses EODHD_API_KEY (preferred in Render)
- ✅ Also accepts EODHD_API_TOKEN / EODHD_TOKEN (legacy)
- ✅ Default base URL: https://eodhistoricaldata.com/api
- ✅ No network calls at import-time
- ✅ Lazy singleton AsyncClient (reuse connections)
- ✅ Retry/backoff on 429 + transient 5xx
- ✅ Clean PATCH outputs (no empty-string / None noise)
- ✅ Optional History + Forecast (momentum/vol/RSI/MA) from EOD endpoint

KSA handling (IMPORTANT)
- Default: KSA is BLOCKED (safe) to avoid routing confusion.
- Enable KSA via: ALLOW_EODHD_KSA=true  (also accepts EODHD_ALLOW_KSA=true)

Customized Headers (NEW)
- EODHD_HEADERS_JSON                     (base headers)
- EODHD_HEADERS_REALTIME_JSON            (realtime endpoint override)
- EODHD_HEADERS_FUNDAMENTALS_JSON        (fundamentals endpoint override)
- EODHD_HEADERS_HISTORY_JSON             (history endpoint override)
All are JSON dict strings.

History/Forecast (NEW)
- Uses /eod/{symbol} daily candles
- Adds (best-effort):
  returns_1w/1m/3m/6m/12m, ma20/ma50/ma200, volatility_30d (% annualized), rsi_14
  expected_return_1m/3m/12m (%), expected_price_1m/3m/12m
  confidence_score, forecast_method, history_points, history_last_date

Exports (engine discovery)
- fetch_quote_patch
- fetch_enriched_quote_patch
- fetch_quote_and_enrichment_patch
- fetch_quote_and_fundamentals_patch      (alias)
- fetch_enriched_patch                    (alias)
- aclose_eodhd_client

Env vars (supported)
Auth/Base
- EODHD_API_KEY (preferred)
- EODHD_API_TOKEN / EODHD_TOKEN (legacy)
- EODHD_BASE_URL (default: https://eodhistoricaldata.com/api)

Behavior
- EODHD_ENABLE_FUNDAMENTALS (default: true)
- EODHD_ENABLE_HISTORY (default: true)
- EODHD_ENABLE_FORECAST (default: true)
- EODHD_VERBOSE_WARNINGS (default: false)  # if true attaches _warn

KSA guard
- ALLOW_EODHD_KSA / EODHD_ALLOW_KSA (default: false)

Timeout/Retry
- EODHD_TIMEOUT_S (default: 8.5)  (fallback to HTTP_TIMEOUT_SEC / HTTP_TIMEOUT)
- EODHD_RETRY_ATTEMPTS (default: 2) (min 1)
- EODHD_RETRY_DELAY_SEC (default: 0.25)

Caching
- EODHD_QUOTE_TTL_SEC (default: 12, min 5)
- EODHD_FUND_TTL_SEC (default: 21600, min 120)   # 6 hours
- EODHD_HISTORY_TTL_SEC (default: 1200, min 60)  # 20 min

History window
- EODHD_HISTORY_DAYS (default: 400, min 60)
- EODHD_HISTORY_POINTS_MAX (default: 400, min 100)

Notes
- Forecast here is a simple momentum-style estimate (non-investment advice).
- This provider is meant for GLOBAL symbols primarily; KSA is off by default.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

import httpx

logger = logging.getLogger("core.providers.eodhd_provider")

PROVIDER_VERSION = "2.0.0"
PROVIDER_NAME = "eodhd"

DEFAULT_BASE_URL = "https://eodhistoricaldata.com/api"

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

USER_AGENT_DEFAULT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# Env helpers (safe)
# ---------------------------------------------------------------------------
def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip()
    return s if s else default


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(str(v).strip())
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return float(str(v).strip())
    except Exception:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in _FALSY:
        return False
    if raw in _TRUTHY:
        return True
    return default


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _token() -> Optional[str]:
    for k in ("EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_TOKEN"):
        v = (os.getenv(k) or "").strip()
        if v:
            return v
    return None


def _base_url() -> str:
    return _env_str("EODHD_BASE_URL", DEFAULT_BASE_URL).rstrip("/")


def _ua() -> str:
    return _env_str("EODHD_UA", USER_AGENT_DEFAULT)


def _timeout_default() -> float:
    for k in ("EODHD_TIMEOUT_S", "HTTP_TIMEOUT_SEC", "HTTP_TIMEOUT"):
        v = (os.getenv(k) or "").strip()
        if v:
            try:
                t = float(v)
                if t > 0:
                    return t
            except Exception:
                pass
    return 8.5


def _retry_attempts() -> int:
    r = _env_int("EODHD_RETRY_ATTEMPTS", _env_int("MAX_RETRIES", 2))
    return max(1, int(r))


def _retry_delay_sec() -> float:
    d = _env_float("EODHD_RETRY_DELAY_SEC", 0.25)
    return d if d > 0 else 0.25


def _enable_fundamentals() -> bool:
    return _env_bool("EODHD_ENABLE_FUNDAMENTALS", True)


def _enable_history() -> bool:
    return _env_bool("EODHD_ENABLE_HISTORY", True)


def _enable_forecast() -> bool:
    return _env_bool("EODHD_ENABLE_FORECAST", True)


def _verbose_warn() -> bool:
    return _env_bool("EODHD_VERBOSE_WARNINGS", False)


def _allow_ksa() -> bool:
    return _env_bool("ALLOW_EODHD_KSA", False) or _env_bool("EODHD_ALLOW_KSA", False)


def _quote_ttl_sec() -> float:
    ttl = _env_float("EODHD_QUOTE_TTL_SEC", 12.0)
    return max(5.0, float(ttl if ttl > 0 else 12.0))


def _fund_ttl_sec() -> float:
    ttl = _env_float("EODHD_FUND_TTL_SEC", 21600.0)
    return max(120.0, float(ttl if ttl > 0 else 21600.0))


def _history_ttl_sec() -> float:
    ttl = _env_float("EODHD_HISTORY_TTL_SEC", 1200.0)
    return max(60.0, float(ttl if ttl > 0 else 1200.0))


def _history_days() -> int:
    d = _env_int("EODHD_HISTORY_DAYS", 400)
    return max(60, int(d))


def _history_points_max() -> int:
    n = _env_int("EODHD_HISTORY_POINTS_MAX", 400)
    return max(100, int(n))


# ---------------------------------------------------------------------------
# TTL cache (import-safe if cachetools missing)
# ---------------------------------------------------------------------------
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


# Provider-level caches (no network at import time)
_QUOTE_CACHE: TTLCache = TTLCache(maxsize=8000, ttl=_quote_ttl_sec())
_FUND_CACHE: TTLCache = TTLCache(maxsize=4000, ttl=_fund_ttl_sec())
_HIST_CACHE: TTLCache = TTLCache(maxsize=2500, ttl=_history_ttl_sec())


# ---------------------------------------------------------------------------
# Customized headers
# ---------------------------------------------------------------------------
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


def _base_headers() -> Dict[str, str]:
    h = {
        "User-Agent": _ua(),
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.8",
    }
    h.update(_json_headers("EODHD_HEADERS_JSON"))
    return h


def _endpoint_headers(kind: str) -> Dict[str, str]:
    key = {
        "realtime": "EODHD_HEADERS_REALTIME_JSON",
        "fundamentals": "EODHD_HEADERS_FUNDAMENTALS_JSON",
        "history": "EODHD_HEADERS_HISTORY_JSON",
    }.get(kind, "")
    return _json_headers(key) if key else {}


# ---------------------------------------------------------------------------
# Symbol guards
# ---------------------------------------------------------------------------
_KSA_RE = re.compile(r"^\d{3,6}(\.SR)?$", re.IGNORECASE)


def _looks_like_ksa(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    if not s:
        return False
    if s.endswith(".SR"):
        return True
    if s.isdigit():
        return True
    return bool(_KSA_RE.match(s))


# ---------------------------------------------------------------------------
# Numeric helpers
# ---------------------------------------------------------------------------
def _is_nan(x: Any) -> bool:
    try:
        return x is None or (isinstance(x, float) and math.isnan(x))
    except Exception:
        return False


def _to_float(x: Any) -> Optional[float]:
    try:
        if _is_nan(x) or x is None:
            return None
        s = str(x).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def _pick(d: Any, *keys: str) -> Any:
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d:
            return d.get(k)
    return None


def _pos_52w(cur: Optional[float], lo: Optional[float], hi: Optional[float]) -> Optional[float]:
    if cur is None or lo is None or hi is None:
        return None
    if hi == lo:
        return None
    try:
        return (cur - lo) / (hi - lo) * 100.0
    except Exception:
        return None


def _clean_patch(patch: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (patch or {}).items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        out[k] = v
    return out


def _merge_into(dst: Dict[str, Any], src: Dict[str, Any], *, force_keys: Sequence[str] = ()) -> None:
    """
    Merge src into dst:
    - Fill blanks/missing in dst
    - Force overwrite for keys in force_keys
    """
    fset = set(force_keys or ())
    for k, v in (src or {}).items():
        if v is None:
            continue
        if k in fset:
            dst[k] = v
            continue
        if k not in dst:
            dst[k] = v
            continue
        cur = dst.get(k)
        if cur is None:
            dst[k] = v
        elif isinstance(cur, str) and not cur.strip():
            dst[k] = v


def _fill_derived(patch: Dict[str, Any]) -> None:
    cur = _to_float(patch.get("current_price"))
    prev = _to_float(patch.get("previous_close"))
    vol = _to_float(patch.get("volume"))

    if patch.get("price_change") is None and cur is not None and prev is not None:
        patch["price_change"] = cur - prev

    if patch.get("percent_change") is None and cur is not None and prev not in (None, 0.0):
        try:
            patch["percent_change"] = (cur - prev) / prev * 100.0
        except Exception:
            pass

    if patch.get("value_traded") is None and cur is not None and vol is not None:
        patch["value_traded"] = cur * vol

    if patch.get("position_52w_percent") is None:
        patch["position_52w_percent"] = _pos_52w(
            cur,
            _to_float(patch.get("week_52_low")),
            _to_float(patch.get("week_52_high")),
        )

    mc = _to_float(patch.get("market_cap"))
    ff = _to_float(patch.get("free_float"))
    if patch.get("free_float_market_cap") is None and mc is not None and ff is not None:
        patch["free_float_market_cap"] = mc * (ff / 100.0)


# ---------------------------------------------------------------------------
# History analytics (forecast)
# ---------------------------------------------------------------------------
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


def _return_pct(last: float, prior: float) -> Optional[float]:
    try:
        if prior == 0:
            return None
        return (last / prior - 1.0) * 100.0
    except Exception:
        return None


def _compute_history_analytics(closes: List[float]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not closes:
        return out

    last = closes[-1]

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
        xs = closes[-n:]
        return sum(xs) / float(n)

    out["ma20"] = ma(20)
    out["ma50"] = ma(50)
    out["ma200"] = ma(200)

    # volatility_30d from last 31 closes
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

    # momentum forecast
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

    if _enable_forecast():
        if r1m_d is not None:
            out["expected_return_1m"] = float(r1m_d * 21.0 * 100.0)
            out["expected_price_1m"] = float(last * (1.0 + (out["expected_return_1m"] / 100.0)))
        if r3m_d is not None:
            out["expected_return_3m"] = float(r3m_d * 63.0 * 100.0)
            out["expected_price_3m"] = float(last * (1.0 + (out["expected_return_3m"] / 100.0)))
        if r12m_d is not None:
            out["expected_return_12m"] = float(r12m_d * 252.0 * 100.0)
            out["expected_price_12m"] = float(last * (1.0 + (out["expected_return_12m"] / 100.0)))
        out["forecast_method"] = "eodhd_history_momentum_v1"
    else:
        out["forecast_method"] = "history_only"

    # confidence score (0..100): more points + lower vol => higher
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


# ---------------------------------------------------------------------------
# Client singleton
# ---------------------------------------------------------------------------
_CLIENT: Optional[httpx.AsyncClient] = None
_LOCK = asyncio.Lock()


async def _get_client() -> httpx.AsyncClient:
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT
    async with _LOCK:
        if _CLIENT is None:
            t = _timeout_default()
            timeout = httpx.Timeout(t, connect=min(10.0, t))
            _CLIENT = httpx.AsyncClient(timeout=timeout, headers=_base_headers(), follow_redirects=True)
            logger.info(
                "EODHD client init v%s | base=%s | timeout=%.1fs | cachetools=%s",
                PROVIDER_VERSION,
                _base_url(),
                t,
                _HAS_CACHETOOLS,
            )
    return _CLIENT


async def aclose_eodhd_client() -> None:
    global _CLIENT
    c = _CLIENT
    _CLIENT = None
    if c is not None:
        try:
            await c.aclose()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
async def _get_json_dict(url: str, params: Dict[str, Any], *, kind: str) -> Tuple[Optional[dict], Optional[str]]:
    tok = _token()
    if not tok:
        return None, "not configured (EODHD_API_KEY)"

    p = dict(params or {})
    p.setdefault("api_token", tok)
    p.setdefault("fmt", "json")

    client = await _get_client()
    retries = _retry_attempts()
    base_delay = _retry_delay_sec()
    req_headers = _endpoint_headers(kind)

    last_err: Optional[str] = None

    for attempt in range(retries):
        try:
            r = await client.get(url, params=p, headers=req_headers if req_headers else None)
            sc = int(r.status_code)

            if sc == 429 or 500 <= sc < 600:
                last_err = f"HTTP {sc}"
                if attempt < retries - 1:
                    await asyncio.sleep(base_delay * (2**attempt) + random.random() * 0.25)
                    continue
                return None, last_err

            if sc != 200:
                hint = ""
                if sc in (401, 403):
                    hint = " (auth failed: check EODHD_API_KEY)"
                msg = ""
                try:
                    js = r.json()
                    if isinstance(js, dict):
                        msg = str(js.get("message") or js.get("error") or "").strip()
                except Exception:
                    msg = ""
                if msg:
                    return None, f"HTTP {sc}{hint}: {msg}"
                return None, f"HTTP {sc}{hint}"

            try:
                js = r.json()
            except Exception:
                return None, "invalid JSON response"

            if not isinstance(js, dict):
                return None, "unexpected JSON type"
            return js, None

        except Exception as e:
            last_err = f"{e.__class__.__name__}: {e}"
            if attempt < retries - 1:
                await asyncio.sleep(base_delay * (2**attempt) + random.random() * 0.25)
                continue
            return None, last_err

    return None, last_err or "request failed"


async def _get_json_list(url: str, params: Dict[str, Any], *, kind: str) -> Tuple[Optional[list], Optional[str]]:
    tok = _token()
    if not tok:
        return None, "not configured (EODHD_API_KEY)"

    p = dict(params or {})
    p.setdefault("api_token", tok)
    p.setdefault("fmt", "json")

    client = await _get_client()
    retries = _retry_attempts()
    base_delay = _retry_delay_sec()
    req_headers = _endpoint_headers(kind)

    last_err: Optional[str] = None

    for attempt in range(retries):
        try:
            r = await client.get(url, params=p, headers=req_headers if req_headers else None)
            sc = int(r.status_code)

            if sc == 429 or 500 <= sc < 600:
                last_err = f"HTTP {sc}"
                if attempt < retries - 1:
                    await asyncio.sleep(base_delay * (2**attempt) + random.random() * 0.25)
                    continue
                return None, last_err

            if sc != 200:
                hint = ""
                if sc in (401, 403):
                    hint = " (auth failed: check EODHD_API_KEY)"
                msg = ""
                try:
                    js = r.json()
                    if isinstance(js, dict):
                        msg = str(js.get("message") or js.get("error") or "").strip()
                except Exception:
                    msg = ""
                if msg:
                    return None, f"HTTP {sc}{hint}: {msg}"
                return None, f"HTTP {sc}{hint}"

            try:
                js = r.json()
            except Exception:
                return None, "invalid JSON response"

            if not isinstance(js, list):
                return None, "unexpected JSON type"
            return js, None

        except Exception as e:
            last_err = f"{e.__class__.__name__}: {e}"
            if attempt < retries - 1:
                await asyncio.sleep(base_delay * (2**attempt) + random.random() * 0.25)
                continue
            return None, last_err

    return None, last_err or "request failed"


# ---------------------------------------------------------------------------
# Endpoint fetchers
# ---------------------------------------------------------------------------
async def _fetch_realtime_patch(symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    if not _token():
        return {}, "not configured (EODHD_API_KEY)"

    url = f"{_base_url()}/real-time/{symbol}"
    js, err = await _get_json_dict(url, {}, kind="realtime")
    if js is None:
        return {}, f"realtime failed: {err}"

    close = _pick(js, "close", "Close")
    prev = _pick(js, "previous_close", "previousClose", "PreviousClose", "previousClosePrice")
    opn = _pick(js, "open", "Open")
    high = _pick(js, "high", "High")
    low = _pick(js, "low", "Low")
    vol = _pick(js, "volume", "Volume")

    chg = _pick(js, "change", "Change")
    chg_p = _pick(js, "change_p", "ChangePercent", "changePercent", "changePercentages")

    patch: Dict[str, Any] = {
        "current_price": _to_float(close),
        "previous_close": _to_float(prev),
        "open": _to_float(opn),
        "day_high": _to_float(high),
        "day_low": _to_float(low),
        "volume": _to_float(vol),
        "price_change": _to_float(chg),
        "percent_change": _to_float(chg_p),
    }
    return _clean_patch(patch), None


async def _fetch_fundamentals_patch(symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    if not _enable_fundamentals():
        return {}, None
    if not _token():
        return {}, "not configured (EODHD_API_KEY)"

    url = f"{_base_url()}/fundamentals/{symbol}"
    js, err = await _get_json_dict(url, {}, kind="fundamentals")
    if js is None:
        return {}, f"fundamentals failed: {err}"

    general = js.get("General") or {}
    highlights = js.get("Highlights") or {}
    valuation = js.get("Valuation") or {}
    technicals = js.get("Technicals") or {}
    shares = js.get("SharesStats") or {}

    patch: Dict[str, Any] = {}

    patch["name"] = (general.get("Name") or general.get("LongName") or "") or ""
    patch["sector"] = (general.get("Sector") or "") or ""
    patch["industry"] = (general.get("Industry") or "") or ""
    patch["sub_sector"] = (general.get("GicSector") or general.get("GicIndustry") or "") or ""
    patch["currency"] = (general.get("CurrencyCode") or "") or ""
    patch["listing_date"] = (general.get("IPODate") or "") or ""
    patch["exchange"] = (general.get("Exchange") or general.get("ExchangeName") or "") or ""

    mc = _to_float(highlights.get("MarketCapitalization"))
    mc_mln = _to_float(highlights.get("MarketCapitalizationMln"))
    if mc is None and mc_mln is not None:
        mc = mc_mln * 1_000_000.0
    patch["market_cap"] = mc

    patch["eps_ttm"] = _to_float(highlights.get("EarningsShare"))
    patch["pe_ttm"] = _to_float(highlights.get("PERatio"))
    patch["pb"] = _to_float(valuation.get("PriceBookMRQ") or highlights.get("PriceBook"))
    patch["ps"] = _to_float(valuation.get("PriceSalesTTM") or highlights.get("PriceSalesTTM"))
    patch["ev_ebitda"] = _to_float(valuation.get("EnterpriseValueEbitda") or highlights.get("EVToEBITDA"))

    patch["dividend_yield"] = _to_float(highlights.get("DividendYield"))
    patch["roe"] = _to_float(highlights.get("ReturnOnEquityTTM"))
    patch["roa"] = _to_float(highlights.get("ReturnOnAssetsTTM"))
    patch["net_margin"] = _to_float(highlights.get("ProfitMargin"))
    patch["beta"] = _to_float(technicals.get("Beta"))

    patch["week_52_high"] = _to_float(technicals.get("52WeekHigh"))
    patch["week_52_low"] = _to_float(technicals.get("52WeekLow"))

    patch["ma50"] = _to_float(technicals.get("50DayMA"))
    patch["ma20"] = _to_float(technicals.get("20DayMA") or technicals.get("10DayMA"))
    patch["avg_volume_30d"] = _to_float(technicals.get("AverageVolume"))

    so = _to_float(
        _pick(shares, "SharesOutstanding", "SharesOutstandingFloat", "SharesOutstandingEOD")
        or general.get("SharesOutstanding")
    )
    patch["shares_outstanding"] = so

    float_shares = _to_float(_pick(shares, "SharesFloat", "FloatShares", "SharesOutstandingFloat"))
    if so and float_shares and so > 0:
        patch["free_float"] = (float_shares / so) * 100.0

    patch["debt_to_equity"] = _to_float(highlights.get("DebtToEquity"))
    patch["current_ratio"] = _to_float(highlights.get("CurrentRatio"))
    patch["quick_ratio"] = _to_float(highlights.get("QuickRatio"))

    return _clean_patch(patch), None


async def _fetch_history_patch(symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    if not _enable_history():
        return {}, None
    if not _token():
        return {}, "not configured (EODHD_API_KEY)"

    days = _history_days()
    to_d = date.today()
    from_d = to_d - timedelta(days=days)

    url = f"{_base_url()}/eod/{symbol}"
    js_list, err = await _get_json_list(
        url,
        {"from": from_d.isoformat(), "to": to_d.isoformat(), "period": "d"},
        kind="history",
    )
    if js_list is None:
        return {}, f"history failed: {err}"

    closes: List[float] = []
    last_date: Optional[str] = None

    for it in js_list:
        if not isinstance(it, dict):
            continue
        c = _to_float(it.get("close"))
        if c is None:
            continue
        closes.append(float(c))
        d = it.get("date")
        if isinstance(d, str) and d.strip():
            last_date = d.strip()

    closes = closes[-_history_points_max():]

    if len(closes) < 25:
        return {}, "history returned too few points"

    analytics = _compute_history_analytics(closes)
    patch: Dict[str, Any] = {
        "history_points": len(closes),
        "history_last_date": last_date or "",
        "forecast_source": "eodhd_eod",
    }
    patch.update(analytics)
    return _clean_patch(patch), None


# ---------------------------------------------------------------------------
# Main fetch
# ---------------------------------------------------------------------------
async def _fetch(symbol: str, *, want_fundamentals: bool, want_history: bool) -> Dict[str, Any]:
    sym = (symbol or "").strip()
    if not sym:
        return {"error": f"{PROVIDER_NAME}: empty symbol"}

    # Controlled KSA enablement
    if _looks_like_ksa(sym) and not _allow_ksa():
        p = {
            "symbol": sym,
            "data_source": PROVIDER_NAME,
            "provider_version": PROVIDER_VERSION,
            "error": "warning: KSA blocked for EODHD (set ALLOW_EODHD_KSA=true to enable)",
            "last_updated_utc": _utc_iso(),
        }
        return _clean_patch(p)

    if not _token():
        p = {
            "symbol": sym,
            "data_source": PROVIDER_NAME,
            "provider_version": PROVIDER_VERSION,
            "error": "warning: not configured (EODHD_API_KEY)",
            "last_updated_utc": _utc_iso(),
        }
        return _clean_patch(p)

    warns: List[str] = []

    # --- realtime (cached) ---
    ck_q = f"q::{sym}"
    hit = _QUOTE_CACHE.get(ck_q)
    if isinstance(hit, dict) and hit:
        rt_patch = dict(hit)
        rt_err = None
    else:
        rt_patch, rt_err = await _fetch_realtime_patch(sym)
        if rt_patch:
            _QUOTE_CACHE[ck_q] = dict(rt_patch)

    if rt_err:
        # If realtime fails, return warning patch (engine can fallback)
        p = {
            "symbol": sym,
            "data_source": PROVIDER_NAME,
            "provider_version": PROVIDER_VERSION,
            "error": f"warning: {PROVIDER_NAME}: {rt_err}",
            "last_updated_utc": _utc_iso(),
        }
        return _clean_patch(p)

    out: Dict[str, Any] = {
        "symbol": sym,
        "data_source": PROVIDER_NAME,
        "provider_version": PROVIDER_VERSION,
        "last_updated_utc": _utc_iso(),
    }
    _merge_into(out, rt_patch)

    # --- fundamentals (cached, best-effort) ---
    if want_fundamentals and _enable_fundamentals():
        ck_f = f"f::{sym}"
        hitf = _FUND_CACHE.get(ck_f)
        if isinstance(hitf, dict) and hitf:
            f_patch = dict(hitf)
            f_err = None
        else:
            f_patch, f_err = await _fetch_fundamentals_patch(sym)
            if f_patch:
                _FUND_CACHE[ck_f] = dict(f_patch)

        if f_err:
            warns.append(f"fundamentals: {f_err}")
        else:
            _merge_into(
                out,
                f_patch,
                force_keys=(
                    "market_cap",
                    "pe_ttm",
                    "eps_ttm",
                    "week_52_high",
                    "week_52_low",
                    "shares_outstanding",
                    "free_float",
                    "ma20",
                    "ma50",
                    "avg_volume_30d",
                ),
            )

    # --- history/forecast (cached, best-effort) ---
    if want_history and _enable_history():
        ck_h = f"h::{sym}::{_history_days()}::{_history_points_max()}"
        hith = _HIST_CACHE.get(ck_h)
        if isinstance(hith, dict) and hith:
            h_patch = dict(hith)
            h_err = None
        else:
            h_patch, h_err = await _fetch_history_patch(sym)
            if h_patch:
                _HIST_CACHE[ck_h] = dict(h_patch)

        if h_err:
            warns.append(f"history: {h_err}")
        else:
            # analytics shouldn't overwrite quote/fundamentals
            for k, v in (h_patch or {}).items():
                if k not in out and v is not None:
                    out[k] = v

    if warns and _verbose_warn():
        out["_warn"] = " | ".join([w for w in warns if w])

    _fill_derived(out)
    return _clean_patch(out)


# ---------------------------------------------------------------------------
# Engine-compatible exported callables (name-based discovery)
# ---------------------------------------------------------------------------
async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    # fast path: realtime only
    return await _fetch(symbol, want_fundamentals=False, want_history=False)


async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    # enriched: realtime + fundamentals + history/forecast (best-effort)
    return await _fetch(symbol, want_fundamentals=True, want_history=True)


async def fetch_quote_and_enrichment_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await fetch_enriched_quote_patch(symbol, *args, **kwargs)


# Compatibility aliases (some engines try these names)
async def fetch_quote_and_fundamentals_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _fetch(symbol, want_fundamentals=True, want_history=False)


async def fetch_enriched_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await fetch_enriched_quote_patch(symbol, *args, **kwargs)


@dataclass
class EodhdProvider:
    name: str = PROVIDER_NAME

    async def fetch_quote(self, symbol: str, debug: bool = False) -> Dict[str, Any]:
        # keep signature compatible with other providers
        return await fetch_enriched_quote_patch(symbol)


__all__ = [
    "fetch_quote_patch",
    "fetch_enriched_quote_patch",
    "fetch_quote_and_enrichment_patch",
    "fetch_quote_and_fundamentals_patch",
    "fetch_enriched_patch",
    "aclose_eodhd_client",
    "PROVIDER_VERSION",
    "PROVIDER_NAME",
    "EodhdProvider",
]
