#!/usr/bin/env python3
# core/providers/eodhd_provider.py
"""
================================================================================
EODHD Provider — v4.6.0 (GLOBAL PRIMARY / HISTORY+FUNDAMENTALS / SCHEMA-ALIGNED)
================================================================================

Goal (Phase D + EODHD Global Primary)
- Global pages should use EODHD for: price + history + fundamentals (where available).
- Only fall back to Yahoo providers when EODHD doesn't have a field.
- International symbols should be supported across all pages (Global_Markets, Market_Leaders,
  Mutual_Funds, My_Portfolio, Top_10_Investments, etc.)

What this revision improves
- ✅ Keeps EODHD as the primary provider for global shares.
- ✅ Expands fundamentals extraction from nested Financials / Technicals / SplitsDividends.
- ✅ Adds TTM rollups and derived ratios:
      revenue_ttm, free_cash_flow_ttm, gross/operating/profit margins,
      debt_to_equity, float_shares, avg_volume_10d.
- ✅ Adds stronger history-derived fallback for quote fields when real-time is sparse.
- ✅ Computes RSI14 from history and strengthens 52W high/low using daily highs/lows.
- ✅ Adds more compatibility wrappers for engines/routes discovering common provider methods.
- ✅ Still Render-safe: no pandas / numpy / scipy.

Canonical patch orientation
- Returns schema-aligned snake_case fields for engine merge.
- Also includes selected backward-compatible aliases for older callers.

================================================================================
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import httpx

logger = logging.getLogger("core.providers.eodhd_provider")
logger.addHandler(logging.NullHandler())

PROVIDER_NAME            = "eodhd"
PROVIDER_VERSION         = "4.8.0"
VERSION                  = PROVIDER_VERSION
PROVIDER_BATCH_SUPPORTED = True   # ENH v4.8.0

DEFAULT_BASE_URL = "https://eodhistoricaldata.com/api"

UA_DEFAULT = "TFB-EODHD/4.8.0 (Render)"

# FIX v4.7.0: Exchange suffix remapping for normalize_eodhd_symbol().
_EODHD_SUFFIX_REMAP: Dict[str, str] = {
    "XETRA": "F",    # Deutsche Borse XETRA -> Frankfurt (.F in EODHD)
    "XLON":  "LSE",  # London XLON notation -> LSE
}

# FIX v4.7.0: Exchange name by suffix for identity fallback.
_SUFFIX_EXCHANGE_MAP: Dict[str, str] = {
    "US":    "NYSE/NASDAQ",
    "MC":    "Bolsa de Madrid",
    "LSE":   "London Stock Exchange",
    "L":     "London Stock Exchange",
    "PA":    "Euronext Paris",
    "F":     "Deutsche Borse XETRA",
    "XETRA": "Deutsche Borse XETRA",
    "ST":    "Nasdaq Stockholm",
    "OL":    "Oslo Bors",
    "HE":    "Nasdaq Helsinki",
    "CO":    "Nasdaq Copenhagen",
    "JK":    "Jakarta Stock Exchange",
    "TW":    "Taiwan Stock Exchange",
    "HK":    "HKEX",
    "TO":    "Toronto Stock Exchange",
    "AU":    "ASX",
    "SI":    "Singapore Exchange",
    "SR":    "Tadawul (Saudi Exchange)",
    "MX":    "Bolsa Mexicana de Valores",
    "BR":    "B3 Brazil",
    "SZ":    "Shenzhen Stock Exchange",
    "SS":    "Shanghai Stock Exchange",
    "BO":    "BSE India",
    "NS":    "NSE India",
    "KS":    "Korea Stock Exchange",
    "T":     "Tokyo Stock Exchange",
    "IL":    "Tel Aviv Stock Exchange",
    "WA":    "Warsaw Stock Exchange",
    "VI":    "Vienna Stock Exchange",
    "SW":    "SIX Swiss Exchange",
    "AS":    "Euronext Amsterdam",
    "LI":    "Lisbon Exchange",
    "IR":    "Euronext Dublin",
}

# FIX v4.7.0: Country by suffix for identity fallback.
_SUFFIX_COUNTRY_MAP: Dict[str, str] = {
    "US":    "USA",
    "MC":    "Spain",
    "LSE":   "United Kingdom",
    "L":     "United Kingdom",
    "PA":    "France",
    "F":     "Germany",
    "XETRA": "Germany",
    "ST":    "Sweden",
    "OL":    "Norway",
    "HE":    "Finland",
    "CO":    "Denmark",
    "JK":    "Indonesia",
    "TW":    "Taiwan",
    "HK":    "Hong Kong",
    "TO":    "Canada",
    "AU":    "Australia",
    "SI":    "Singapore",
    "SR":    "Saudi Arabia",
    "MX":    "Mexico",
    "BR":    "Brazil",
    "SZ":    "China",
    "SS":    "China",
    "BO":    "India",
    "NS":    "India",
    "KS":    "South Korea",
    "T":     "Japan",
    "IL":    "Israel",
    "WA":    "Poland",
    "VI":    "Austria",
    "SW":    "Switzerland",
    "AS":    "Netherlands",
    "LI":    "Portugal",
    "IR":    "Ireland",
}


def _exchange_from_suffix(symbol_norm: str) -> Optional[str]:
    """Derive exchange name from the EODHD-normalized symbol suffix."""
    if "." in symbol_norm:
        suf = symbol_norm.rsplit(".", 1)[1].upper()
        return _SUFFIX_EXCHANGE_MAP.get(suf)
    return None


def _country_from_suffix(symbol_norm: str) -> Optional[str]:
    """Derive country from the EODHD-normalized symbol suffix."""
    if "." in symbol_norm:
        suf = symbol_norm.rsplit(".", 1)[1].upper()
        return _SUFFIX_COUNTRY_MAP.get(suf)
    return None



try:
    import orjson  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)

except Exception:
    import json  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, bytes):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}
_US_LIKE_RE = re.compile(r"^[A-Z0-9][A-Z0-9\-_]{0,16}$")
_KSA_RE = re.compile(r"^\d{3,6}\.SR$", re.IGNORECASE)


# =============================================================================
# Env helpers
# =============================================================================
def _env_str(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in _TRUTHY:
        return True
    if raw in _FALSY:
        return False
    return default


def _env_int(name: str, default: int, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    try:
        v = int(float((os.getenv(name) or str(default)).strip()))
    except Exception:
        v = default
    if lo is not None and v < lo:
        v = lo
    if hi is not None and v > hi:
        v = hi
    return v


def _env_float(name: str, default: float, lo: Optional[float] = None, hi: Optional[float] = None) -> float:
    try:
        v = float((os.getenv(name) or str(default)).strip())
    except Exception:
        v = default
    if lo is not None and v < lo:
        v = lo
    if hi is not None and v > hi:
        v = hi
    return v


def _token() -> str:
    return _env_str("EODHD_API_KEY") or _env_str("EODHD_API_TOKEN") or _env_str("EODHD_KEY")


def _base_url() -> str:
    return _env_str("EODHD_BASE_URL", DEFAULT_BASE_URL).rstrip("/")


def _default_exchange() -> str:
    ex = _env_str("EODHD_DEFAULT_EXCHANGE") or _env_str("EODHD_SYMBOL_SUFFIX_DEFAULT") or "US"
    return ex.strip().upper() or "US"


def _append_exchange_suffix() -> bool:
    return _env_bool("EODHD_APPEND_EXCHANGE_SUFFIX", True)


def _ksa_blocked_by_default() -> bool:
    return _env_bool("KSA_DISALLOW_EODHD", False)


def _allow_ksa_override() -> bool:
    return _env_bool("ALLOW_EODHD_KSA", False) or _env_bool("EODHD_ALLOW_KSA", False)


def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(timezone.utc)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).isoformat()


def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(timezone.utc)
    tz = timezone(timedelta(hours=3))
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(tz).isoformat()


# =============================================================================
# Coercion helpers
# =============================================================================
def safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            x = float(v)
        else:
            s = str(v).strip().replace(",", "")
            if not s:
                return None
            if s.endswith("%"):
                s = s[:-1].strip()
                x = float(s) / 100.0
            else:
                x = float(s)
        if math.isnan(x) or math.isinf(x):
            return None
        return x
    except Exception:
        return None


def safe_int(v: Any) -> Optional[int]:
    f = safe_float(v)
    if f is None:
        return None
    try:
        return int(round(f))
    except Exception:
        return None


def safe_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    try:
        s = str(v).strip()
        return s if s else None
    except Exception:
        return None


def _clean_patch(p: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in (p or {}).items() if v is not None and v != ""}


def _frac_from_percentish(v: Any) -> Optional[float]:
    f = safe_float(v)
    if f is None:
        return None
    if abs(f) > 1.5:
        return f / 100.0
    return f



def _frac_from_change_pct(v: Any) -> Optional[float]:
    """
    FIX v4.7.0: Convert price change percentage to fraction with tight threshold.
    Threshold: abs(f) > 1.0 -> divide by 100.
    EODHD returns change_p=1.4247 meaning 1.4247% daily change.
    With old threshold > 1.5, 1.4247 was NOT divided, causing Momentum Score = 100.
    DO NOT use for fundamentals (ROE/ROA can legitimately be > 1.0 as fractions).
    """
    f = safe_float(v)
    if f is None:
        return None
    if abs(f) > 1.0:
        return f / 100.0
    return f

def _safe_div(a: Any, b: Any) -> Optional[float]:
    x = safe_float(a)
    y = safe_float(b)
    if x is None or y in (None, 0.0):
        return None
    try:
        return x / y
    except Exception:
        return None


def _first_present(*vals: Any) -> Any:
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return v
    return None


def _sum_present(values: Iterable[Any]) -> Optional[float]:
    total = 0.0
    seen = False
    for v in values:
        f = safe_float(v)
        if f is None:
            continue
        total += f
        seen = True
    return total if seen else None


# =============================================================================
# Symbol normalization (GLOBAL, incl. international)
# =============================================================================
def normalize_eodhd_symbol(symbol: str) -> str:
    s = (symbol or "").strip()
    if not s:
        return ""
    s_up = s.upper()

    try:
        from core.symbols.normalize import to_eodhd_symbol as _to_eodhd_symbol  # type: ignore

        if callable(_to_eodhd_symbol):
            out = _to_eodhd_symbol(s_up, default_exchange=_default_exchange())  # type: ignore[arg-type]
            if isinstance(out, str) and out.strip():
                return out.strip().upper()
    except Exception:
        pass

    if "=" in s_up or "^" in s_up or "/" in s_up:
        return s_up

    if s_up.endswith(".SR") and _KSA_RE.match(s_up):
        return s_up

    if "." in s_up:
        base, suf = s_up.rsplit(".", 1)

        # FIX v4.7.0: Remap non-EODHD exchange suffixes to EODHD equivalents.
        if suf in _EODHD_SUFFIX_REMAP:
            remapped = _EODHD_SUFFIX_REMAP[suf]
            logger.debug("Symbol suffix remap: %s.%s -> %s.%s", base, suf, base, remapped)
            return f"{base}.{remapped}"

        if len(suf) >= 2:
            return s_up
        if not _append_exchange_suffix():
            return s_up
        return f"{s_up}.{_default_exchange()}"

    if not _append_exchange_suffix():
        return s_up

    if _US_LIKE_RE.match(s_up):
        return f"{s_up}.{_default_exchange()}"

    return s_up


# =============================================================================
# Nested-data helpers
# =============================================================================
def _listify_rows(node: Any) -> List[Dict[str, Any]]:
    if isinstance(node, list):
        rows = [r for r in node if isinstance(r, dict)]
    elif isinstance(node, dict):
        rows = []
        for k, v in node.items():
            if isinstance(v, dict):
                row = dict(v)
                row.setdefault("date", k)
                rows.append(row)
    else:
        rows = []

    def _sort_key(row: Dict[str, Any]) -> str:
        return safe_str(row.get("date")) or safe_str(row.get("filing_date")) or ""

    rows.sort(key=_sort_key, reverse=True)
    return rows


def _statement_rows(financials: Dict[str, Any], section: str, periodicity: str) -> List[Dict[str, Any]]:
    sec = (financials or {}).get(section) or {}
    if not isinstance(sec, dict):
        return []
    rows = sec.get(periodicity)
    if rows is None and periodicity == "quarterly":
        rows = sec.get("quarter")
    if rows is None and periodicity == "yearly":
        rows = sec.get("annual") or sec.get("year")
    return _listify_rows(rows)


def _pick_numeric(row: Dict[str, Any], *keys: str) -> Optional[float]:
    for k in keys:
        if k in row:
            f = safe_float(row.get(k))
            if f is not None:
                return f
    lower_map = {str(k).lower(): v for k, v in row.items()}
    for k in keys:
        f = safe_float(lower_map.get(str(k).lower()))
        if f is not None:
            return f
    return None


def _latest_numeric(rows: List[Dict[str, Any]], *keys: str) -> Optional[float]:
    for row in rows:
        v = _pick_numeric(row, *keys)
        if v is not None:
            return v
    return None


def _sum_latest_n(rows: List[Dict[str, Any]], n: int, *keys: str) -> Optional[float]:
    if not rows:
        return None
    vals: List[float] = []
    for row in rows[: max(1, n)]:
        v = _pick_numeric(row, *keys)
        if v is not None:
            vals.append(v)
    if not vals:
        return None
    return sum(vals)


def _infer_asset_class(general: Dict[str, Any], etf_data: Dict[str, Any]) -> Optional[str]:
    t = (safe_str(general.get("Type")) or safe_str(general.get("Category")) or "").strip().lower()
    if not t and etf_data:
        return "ETF"
    mapping = {
        "common stock": "Equity",
        "stock": "Equity",
        "equity": "Equity",
        "preferred stock": "Equity",
        "etf": "ETF",
        "exchange traded fund": "ETF",
        "fund": "Fund",
        "mutual fund": "Mutual Fund",
        "adr": "ADR",
        "reit": "REIT",
        "index": "Index",
        "currency": "FX",
        "forex": "FX",
        "futures": "Future",
        "future": "Future",
        "commodity": "Commodity",
        "bond": "Bond",
    }
    if t in mapping:
        return mapping[t]
    if "fund" in t and "mutual" in t:
        return "Mutual Fund"
    if "etf" in t:
        return "ETF"
    if "fund" in t:
        return "Fund"
    if "stock" in t or "equity" in t:
        return "Equity"
    if t:
        return t.title()
    return None


# =============================================================================
# Async primitives: SingleFlight + TTL Cache + TokenBucket
# =============================================================================
class _SingleFlight:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._calls: Dict[str, asyncio.Future] = {}

    async def do(self, key: str, coro_factory: Callable[[], Any]) -> Any:
        async with self._lock:
            fut = self._calls.get(key)
            if fut is None:
                fut = asyncio.get_running_loop().create_future()
                self._calls[key] = fut
                owner = True
            else:
                owner = False

        if not owner:
            return await fut  # type: ignore

        try:
            res = await coro_factory()
            if not fut.done():
                fut.set_result(res)
            return res
        except Exception as e:
            if not fut.done():
                fut.set_exception(e)
            raise
        finally:
            async with self._lock:
                self._calls.pop(key, None)


@dataclass
class _CacheItem:
    exp: float
    val: Any


class _TTLCache:
    def __init__(self, maxsize: int, ttl_sec: float):
        self.maxsize = max(128, int(maxsize))
        self.ttl_sec = max(1.0, float(ttl_sec))
        self._d: Dict[str, _CacheItem] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Any:
        now = time.monotonic()
        async with self._lock:
            it = self._d.get(key)
            if not it:
                return None
            if it.exp > now:
                return it.val
            self._d.pop(key, None)
            return None

    async def set(self, key: str, val: Any, ttl_sec: Optional[float] = None) -> None:
        now = time.monotonic()
        ttl = self.ttl_sec if ttl_sec is None else max(1.0, float(ttl_sec))
        async with self._lock:
            if len(self._d) >= self.maxsize and key not in self._d:
                self._d.pop(next(iter(self._d.keys())), None)
            self._d[key] = _CacheItem(exp=now + ttl, val=val)


class _TokenBucket:
    def __init__(self, rate_per_sec: float, burst: float):
        self.rate = max(0.0, float(rate_per_sec))
        self.capacity = max(1.0, float(burst))
        self.tokens = self.capacity
        self.last = time.monotonic()
        self._lock = asyncio.Lock()

    async def wait(self, amount: float = 1.0) -> None:
        if self.rate <= 0:
            return
        amount = max(0.0001, float(amount))
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = max(0.0, now - self.last)
                self.last = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                if self.tokens >= amount:
                    self.tokens -= amount
                    return
                deficit = amount - self.tokens
                sleep_s = deficit / self.rate if self.rate > 0 else 0.25
            await asyncio.sleep(min(1.0, max(0.05, sleep_s)))


# =============================================================================
# Stats calculations from close series (no numpy)
# =============================================================================
def _daily_returns(closes: List[float]) -> List[float]:
    rets: List[float] = []
    for i in range(1, len(closes)):
        p0 = closes[i - 1]
        p1 = closes[i]
        if p0 and p0 > 0 and p1 and p1 > 0:
            rets.append((p1 / p0) - 1.0)
    return rets


def _stdev(x: List[float]) -> Optional[float]:
    if len(x) < 2:
        return None
    m = sum(x) / len(x)
    var = sum((v - m) ** 2 for v in x) / max(1, len(x) - 1)
    return math.sqrt(max(0.0, var))


def _max_drawdown(closes: List[float]) -> Optional[float]:
    if len(closes) < 2:
        return None
    peak = closes[0]
    mdd = 0.0
    for p in closes:
        if p > peak:
            peak = p
        if peak > 0:
            dd = (p / peak) - 1.0
            if dd < mdd:
                mdd = dd
    return mdd


def _var_95_1d(returns: List[float]) -> Optional[float]:
    if len(returns) < 20:
        return None
    xs = sorted(returns)
    idx = int(round(0.05 * (len(xs) - 1)))
    q = xs[max(0, min(len(xs) - 1, idx))]
    return abs(q) if q < 0 else 0.0


def _sharpe_1y(returns: List[float], rf_annual: float) -> Optional[float]:
    if len(returns) < 60:
        return None
    mu = sum(returns) / len(returns)
    sd = _stdev(returns)
    if sd is None or sd == 0:
        return None
    rf_daily = float(rf_annual) / 252.0
    ex = mu - rf_daily
    return (ex / sd) * math.sqrt(252.0)


def _annualized_vol(returns: List[float]) -> Optional[float]:
    sd = _stdev(returns)
    if sd is None:
        return None
    return sd * math.sqrt(252.0)


def _rsi14(closes: List[float]) -> Optional[float]:
    if len(closes) < 15:
        return None
    gains: List[float] = []
    losses: List[float] = []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(abs(min(delta, 0.0)))
    if len(gains) < 14:
        return None
    avg_gain = sum(gains[:14]) / 14.0
    avg_loss = sum(losses[:14]) / 14.0
    for i in range(14, len(gains)):
        avg_gain = ((avg_gain * 13.0) + gains[i]) / 14.0
        avg_loss = ((avg_loss * 13.0) + losses[i]) / 14.0
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


# =============================================================================
# EODHD Client
# =============================================================================
class EODHDClient:
    def __init__(self) -> None:
        self.api_key = _token()
        self.base_url = _base_url()

        self.timeout_sec = _env_float("EODHD_TIMEOUT_SEC", 15.0, lo=3.0, hi=120.0)
        self.retry_attempts = _env_int("EODHD_RETRY_ATTEMPTS", 4, lo=0, hi=10)
        self.retry_base_delay = _env_float("EODHD_RETRY_BASE_DELAY", 0.6, lo=0.0, hi=10.0)

        self.max_concurrency = _env_int("EODHD_MAX_CONCURRENCY", 20, lo=1, hi=100)
        self._sem = asyncio.Semaphore(self.max_concurrency)

        rps = _env_float("EODHD_RATE_LIMIT_RPS", 4.0, lo=0.0, hi=50.0)
        burst = _env_float("EODHD_RATE_LIMIT_BURST", 8.0, lo=1.0, hi=200.0)
        self._bucket = _TokenBucket(rate_per_sec=rps, burst=burst)

        self._sf = _SingleFlight()

        self.quote_cache = _TTLCache(maxsize=6000, ttl_sec=_env_float("EODHD_QUOTE_TTL_SEC", 12.0, lo=1.0, hi=600.0))
        self.fund_cache = _TTLCache(maxsize=3000, ttl_sec=_env_float("EODHD_FUND_TTL_SEC", 21600.0, lo=60.0, hi=86400.0))
        self.hist_cache = _TTLCache(maxsize=2000, ttl_sec=_env_float("EODHD_HISTORY_TTL_SEC", 1800.0, lo=60.0, hi=86400.0))

        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.timeout_sec),
            headers={"User-Agent": _env_str("EODHD_USER_AGENT", UA_DEFAULT)},
            limits=httpx.Limits(max_keepalive_connections=30, max_connections=60),
            http2=True,
        )

    def _base_params(self) -> Dict[str, str]:
        return {"api_token": self.api_key, "fmt": "json"}

    async def _request_json(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Tuple[Optional[Any], Optional[str]]:
        if not self.api_key:
            return None, "EODHD_API_KEY missing"

        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        p = {**self._base_params(), **(params or {})}
        last_err: Optional[str] = None

        async with self._sem:
            for attempt in range(max(1, self.retry_attempts + 1)):
                await self._bucket.wait(1.0)
                try:
                    r = await self._client.get(url, params=p)
                    sc = int(r.status_code)

                    if sc == 429:
                        ra = r.headers.get("Retry-After")
                        wait = float(ra) if ra and ra.replace(".", "", 1).isdigit() else min(15.0, 1.0 + attempt)
                        last_err = "HTTP 429"
                        await asyncio.sleep(wait)
                        continue

                    if sc in (401, 403):
                        body_hint = ""
                        try:
                            body_hint = (r.text or "")[:160]
                        except Exception:
                            body_hint = ""
                        return None, f"HTTP {sc} auth_error {body_hint}".strip()

                    if 500 <= sc < 600:
                        last_err = f"HTTP {sc}"
                        base = self.retry_base_delay * (2 ** (attempt - 1))
                        await asyncio.sleep(min(12.0, random.uniform(0, base + 0.25)))
                        continue

                    if sc == 404:
                        return None, "HTTP 404 not_found"

                    if sc >= 400:
                        return None, f"HTTP {sc}"

                    try:
                        data = json_loads(r.content)
                        return data, None
                    except Exception:
                        return None, "invalid_json_payload"

                except httpx.RequestError as e:
                    last_err = f"network_error:{e.__class__.__name__}"
                    base = self.retry_base_delay * (2 ** (attempt - 1))
                    await asyncio.sleep(min(12.0, random.uniform(0, base + 0.25)))
                except Exception as e:
                    last_err = f"unexpected_error:{e.__class__.__name__}"
                    break

        return None, last_err or "request_failed"

    async def fetch_quote(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        sym_raw = (symbol or "").strip().upper()
        sym = normalize_eodhd_symbol(sym_raw)
        if not sym:
            return {}, "invalid_symbol"

        if _KSA_RE.match(sym) and _ksa_blocked_by_default() and not _allow_ksa_override():
            return {}, "ksa_blocked"

        ck = f"q:{sym}"
        cached = await self.quote_cache.get(ck)
        if cached:
            return cached, None

        async def _do() -> Tuple[Dict[str, Any], Optional[str]]:
            data, err = await self._request_json(f"real-time/{sym}", params={})
            if err or not isinstance(data, dict):
                return {}, err or "bad_payload"

            close = safe_float(_first_present(data.get("close"), data.get("adjusted_close"), data.get("last"), data.get("price")))
            prev = safe_float(_first_present(data.get("previousClose"), data.get("previous_close"), data.get("prev_close")))
            open_px = safe_float(_first_present(data.get("open"), data.get("dayOpen"), data.get("day_open")))
            high_px = safe_float(_first_present(data.get("high"), data.get("dayHigh"), data.get("day_high")))
            low_px = safe_float(_first_present(data.get("low"), data.get("dayLow"), data.get("day_low")))
            volume = safe_float(_first_present(data.get("volume"), data.get("shareVolume"), data.get("avgVolume")))
            chg = safe_float(_first_present(data.get("change"), data.get("price_change")))

            if chg is None and close is not None and prev not in (None, 0.0):
                chg = close - prev
            change_frac = _frac_from_percentish(_first_present(data.get("change_p"), data.get("percent_change"), data.get("changePercent")))
            if change_frac is None and close is not None and prev not in (None, 0.0):
                change_frac = (close / prev) - 1.0

            exchange = safe_str(_first_present(data.get("exchange"), data.get("fullExchangeName"), data.get("primaryExchange")))
            currency = safe_str(_first_present(data.get("currency"), data.get("currency_code")))
            market_cap = safe_float(_first_present(data.get("market_cap"), data.get("marketCapitalization"), data.get("marketCapitalisation")))

            patch = _clean_patch(
                {
                    "symbol": sym_raw,
                    "symbol_normalized": sym,
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    "name": safe_str(_first_present(data.get("name"), data.get("shortName"), data.get("longName"))),
                    "exchange": exchange,
                    "currency": currency,
                    "current_price": close,
                    "price": close,
                    "previous_close": prev,
                    "prev_close": prev,
                    "day_open": open_px,
                    "open": open_px,
                    "day_high": high_px,
                    "day_low": low_px,
                    "volume": volume,
                    "market_cap": market_cap,
                    "price_change": chg,
                    "change": chg,
                    "percent_change": change_frac,
                    "change_pct": change_frac,
                    "timestamp": safe_str(_first_present(data.get("timestamp"), data.get("date"))),
                    "last_updated_utc": _utc_iso(),
                    "last_updated_riyadh": _riyadh_iso(),
                }
            )
            await self.quote_cache.set(ck, patch)
            return patch, None

        return await self._sf.do(ck, _do)

    async def fetch_fundamentals(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        sym_raw = (symbol or "").strip().upper()
        sym = normalize_eodhd_symbol(sym_raw)
        if not sym:
            return {}, "invalid_symbol"

        if _KSA_RE.match(sym) and _ksa_blocked_by_default() and not _allow_ksa_override():
            return {}, "ksa_blocked"

        ck = f"f:{sym}"
        cached = await self.fund_cache.get(ck)
        if cached:
            return cached, None

        async def _do() -> Tuple[Dict[str, Any], Optional[str]]:
            data, err = await self._request_json(f"fundamentals/{sym}", params={})
            if err or not isinstance(data, dict):
                return {}, err or "bad_payload"

            general = data.get("General") or {}
            highlights = data.get("Highlights") or {}
            valuation = data.get("Valuation") or {}
            shares = data.get("SharesStats") or {}
            tech = data.get("Technicals") or {}
            splits = data.get("SplitsDividends") or {}
            etf_data = data.get("ETF_Data") or {}
            financials = data.get("Financials") or {}

            income_q = _statement_rows(financials, "Income_Statement", "quarterly")
            income_y = _statement_rows(financials, "Income_Statement", "yearly")
            cash_q = _statement_rows(financials, "Cash_Flow", "quarterly")
            cash_y = _statement_rows(financials, "Cash_Flow", "yearly")
            balance_q = _statement_rows(financials, "Balance_Sheet", "quarterly")
            balance_y = _statement_rows(financials, "Balance_Sheet", "yearly")

            revenue_ttm = _sum_latest_n(income_q, 4, "totalRevenue", "revenue", "Revenue")
            gross_profit_ttm = _sum_latest_n(income_q, 4, "grossProfit", "GrossProfit")
            operating_income_ttm = _sum_latest_n(income_q, 4, "operatingIncome", "OperatingIncome")
            net_income_ttm = _sum_latest_n(income_q, 4, "netIncome", "NetIncome")
            cfo_ttm = _sum_latest_n(cash_q, 4, "totalCashFromOperatingActivities", "operatingCashFlow", "OperatingCashFlow")
            capex_ttm = _sum_latest_n(cash_q, 4, "capitalExpenditures", "CapitalExpenditure", "capitalExpenditure")
            fcf_ttm = None
            if cfo_ttm is not None and capex_ttm is not None:
                fcf_ttm = cfo_ttm - abs(capex_ttm)
            elif cfo_ttm is not None:
                fcf_ttm = cfo_ttm

            latest_rev_q = _latest_numeric(income_q, "totalRevenue", "revenue", "Revenue")
            prev_year_rev_q = None
            if len(income_q) >= 5:
                prev_year_rev_q = _pick_numeric(income_q[4], "totalRevenue", "revenue", "Revenue")
            revenue_growth_yoy = None
            if latest_rev_q is not None and prev_year_rev_q not in (None, 0.0):
                revenue_growth_yoy = (latest_rev_q / prev_year_rev_q) - 1.0
            if revenue_growth_yoy is None:
                latest_rev_y = _latest_numeric(income_y, "totalRevenue", "revenue", "Revenue")
                prev_rev_y = _pick_numeric(income_y[1], "totalRevenue", "revenue", "Revenue") if len(income_y) >= 2 else None
                if latest_rev_y is not None and prev_rev_y not in (None, 0.0):
                    revenue_growth_yoy = (latest_rev_y / prev_rev_y) - 1.0

            latest_net_q = _latest_numeric(income_q, "netIncome", "NetIncome")
            prev_year_net_q = _pick_numeric(income_q[4], "netIncome", "NetIncome") if len(income_q) >= 5 else None
            earnings_growth = None
            if latest_net_q is not None and prev_year_net_q not in (None, 0.0):
                earnings_growth = (latest_net_q / prev_year_net_q) - 1.0

            gross_margin = _safe_div(gross_profit_ttm, revenue_ttm)
            operating_margin = _safe_div(operating_income_ttm, revenue_ttm)
            profit_margin = _safe_div(net_income_ttm, revenue_ttm)

            latest_bs = balance_q[0] if balance_q else (balance_y[0] if balance_y else {})
            total_assets = _pick_numeric(latest_bs, "totalAssets", "TotalAssets")
            total_equity = _pick_numeric(
                latest_bs,
                "totalStockholderEquity",
                "totalShareholderEquity",
                "shareholdersEquity",
                "totalEquity",
                "TotalStockholderEquity",
            )
            total_debt = _first_present(
                _pick_numeric(latest_bs, "shortLongTermDebtTotal", "ShortLongTermDebtTotal"),
                _pick_numeric(latest_bs, "totalDebt", "TotalDebt"),
                _sum_present(
                    [
                        _pick_numeric(latest_bs, "shortTermDebt", "ShortTermDebt"),
                        _pick_numeric(latest_bs, "longTermDebt", "LongTermDebt"),
                    ]
                ),
            )
            debt_to_equity = _safe_div(total_debt, total_equity)
            roe = _frac_from_percentish(_first_present(highlights.get("ROE"), _safe_div(net_income_ttm, total_equity)))
            roa = _frac_from_percentish(_first_present(highlights.get("ROA"), _safe_div(net_income_ttm, total_assets)))

            shares_outstanding = safe_float(_first_present(shares.get("SharesOutstanding"), highlights.get("SharesOutstanding")))
            float_shares = safe_float(_first_present(shares.get("SharesFloat"), shares.get("FloatShares"), highlights.get("SharesFloat")))
            market_cap = safe_float(_first_present(highlights.get("MarketCapitalization"), general.get("MarketCapitalization")))

            week_52_high = safe_float(_first_present(tech.get("52WeekHigh"), tech.get("WeekHigh52"), highlights.get("52WeekHigh")))
            week_52_low = safe_float(_first_present(tech.get("52WeekLow"), tech.get("WeekLow52"), highlights.get("52WeekLow")))

            asset_class = _infer_asset_class(general, etf_data)
            country = safe_str(_first_present(general.get("CountryISO"), general.get("CountryName"), general.get("Country")))
            exchange = safe_str(_first_present(general.get("Exchange"), general.get("PrimaryExchange")))
            currency = safe_str(_first_present(general.get("CurrencyCode"), general.get("Currency"), highlights.get("Currency")))

            dividend_yield = _frac_from_percentish(
                _first_present(
                    splits.get("ForwardAnnualDividendYield"),
                    highlights.get("DividendYield"),
                    splits.get("TrailingAnnualDividendYield"),
                )
            )
            payout_ratio = _frac_from_percentish(_first_present(splits.get("PayoutRatio"), highlights.get("PayoutRatio")))

            patch = _clean_patch(
                {
                    "symbol": sym_raw,
                    "symbol_normalized": sym,
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    "name": safe_str(_first_present(general.get("Name"), general.get("ShortName"), general.get("LongName"))),
                    "exchange": exchange,
                    "currency": currency,
                    "country": country,
                    "sector": safe_str(general.get("Sector")),
                    "industry": safe_str(general.get("Industry")),
                    "asset_class": asset_class,
                    "asset_type": asset_class,
                    "market_cap": market_cap,
                    "enterprise_value": safe_float(_first_present(highlights.get("EnterpriseValue"), valuation.get("EnterpriseValue"))),
                    "shares_outstanding": shares_outstanding,
                    "float_shares": float_shares,
                    "shares_float": float_shares,
                    "eps_ttm": safe_float(_first_present(highlights.get("EarningsShare"), highlights.get("DilutedEpsTTM"), highlights.get("EPSEstimateCurrentYear"))),
                    "pe_ttm": safe_float(_first_present(valuation.get("TrailingPE"), highlights.get("PERatio"))),
                    "pe_forward": safe_float(_first_present(valuation.get("ForwardPE"), valuation.get("PriceEarnings"), highlights.get("ForwardPE"))),  # FIX v4.8.0: was "forward_pe"
                    "forward_pe": safe_float(_first_present(valuation.get("ForwardPE"), highlights.get("ForwardPE"))),  # kept for backward compat
                    "pb_ratio": safe_float(_first_present(valuation.get("PriceBookMRQ"), valuation.get("PriceBook"))),
                    "ps_ratio": safe_float(_first_present(valuation.get("PriceSalesTTM"), valuation.get("PriceSales"))),
                    "peg_ratio": safe_float(_first_present(valuation.get("PEGRatio"), valuation.get("PegRatio"))),
                    "ev_ebitda": safe_float(_first_present(valuation.get("EnterpriseValueEbitda"), valuation.get("EnterpriseValueEBITDA"))),
                    "pb": safe_float(_first_present(valuation.get("PriceBookMRQ"), valuation.get("PriceBook"))),
                    "ps": safe_float(_first_present(valuation.get("PriceSalesTTM"), valuation.get("PriceSales"))),
                    "peg": safe_float(_first_present(valuation.get("PEGRatio"), valuation.get("PegRatio"))),
                    "dividend_yield": dividend_yield,
                    "payout_ratio": payout_ratio,
                    "roe": roe,
                    "roa": roa,
                    "net_margin": _frac_from_percentish(_first_present(highlights.get("ProfitMargin"), profit_margin)),
                    "revenue_growth": _frac_from_percentish(_first_present(highlights.get("RevenueGrowth"), revenue_growth_yoy)),
                    "revenue_growth_yoy": _frac_from_percentish(_first_present(highlights.get("RevenueGrowth"), revenue_growth_yoy)),
                    "earnings_growth": _frac_from_percentish(_first_present(highlights.get("EarningsGrowth"), earnings_growth)),
                    "beta": safe_float(_first_present(tech.get("Beta"), highlights.get("Beta"))),
                    "beta_5y": safe_float(_first_present(tech.get("Beta"), highlights.get("Beta"))),
                    "week_52_high": week_52_high,
                    "week_52_low": week_52_low,
                    "revenue_ttm": revenue_ttm,
                    "revenue": revenue_ttm,
                    "gross_margin": _frac_from_percentish(_first_present(highlights.get("GrossMargin"), gross_margin)),
                    "operating_margin": _frac_from_percentish(_first_present(highlights.get("OperatingMargin"), operating_margin)),
                    "profit_margin": _frac_from_percentish(_first_present(highlights.get("ProfitMargin"), profit_margin)),
                    "free_cash_flow_ttm": fcf_ttm,
                    "fcf_ttm": fcf_ttm,
                    "debt_to_equity": debt_to_equity,
                    "d_e_ratio": debt_to_equity,
                    "last_updated_utc": _utc_iso(),
                    "last_updated_riyadh": _riyadh_iso(),
                }
            )
            await self.fund_cache.set(ck, patch)
            return patch, None

        return await self._sf.do(ck, _do)

    async def fetch_history_stats(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        sym_raw = (symbol or "").strip().upper()
        sym = normalize_eodhd_symbol(sym_raw)
        if not sym:
            return {}, "invalid_symbol"

        if _KSA_RE.match(sym) and _ksa_blocked_by_default() and not _allow_ksa_override():
            return {}, "ksa_blocked"

        ck = f"h:{sym}"
        cached = await self.hist_cache.get(ck)
        if cached:
            return cached, None

        async def _do() -> Tuple[Dict[str, Any], Optional[str]]:
            days = _env_int("EODHD_HISTORY_DAYS", 420, lo=60, hi=5000)
            from_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
            data, err = await self._request_json(f"eod/{sym}", params={"from": from_date})
            if err or not isinstance(data, list):
                return {}, err or "bad_payload"

            rows = [r for r in data if isinstance(r, dict)]
            rows = [r for r in rows if safe_float(r.get("close")) is not None]
            rows.sort(key=lambda r: safe_str(r.get("date")) or "")

            closes: List[float] = []
            highs: List[float] = []
            lows: List[float] = []
            opens: List[float] = []
            vols: List[float] = []
            last_hist_dt: Optional[str] = None

            for row in rows:
                c = safe_float(row.get("close"))
                if c is not None:
                    closes.append(c)
                    last_hist_dt = safe_str(row.get("date")) or last_hist_dt
                h = safe_float(row.get("high"))
                if h is not None:
                    highs.append(h)
                l = safe_float(row.get("low"))
                if l is not None:
                    lows.append(l)
                o = safe_float(row.get("open"))
                if o is not None:
                    opens.append(o)
                v = safe_float(row.get("volume"))
                if v is not None:
                    vols.append(v)

            n = len(closes)
            if n < 2:
                patch = _clean_patch(
                    {
                        "symbol": sym_raw,
                        "symbol_normalized": sym,
                        "provider": PROVIDER_NAME,
                        "data_source": PROVIDER_NAME,
                        "history_points": n,
                        "history_source": PROVIDER_NAME,
                        "history_last_utc": last_hist_dt,
                        "last_updated_utc": _utc_iso(),
                        "last_updated_riyadh": _riyadh_iso(),
                    }
                )
                await self.hist_cache.set(ck, patch)
                return patch, None

            latest_row = rows[-1]
            prev_row = rows[-2] if len(rows) >= 2 else {}
            last = closes[-1]
            prev = safe_float(prev_row.get("close"))
            latest_open = safe_float(latest_row.get("open"))
            latest_high = safe_float(latest_row.get("high"))
            latest_low = safe_float(latest_row.get("low"))
            latest_vol = safe_float(latest_row.get("volume"))
            history_price_change = (last - prev) if (last is not None and prev not in (None, 0.0)) else None
            history_percent_change = ((last / prev) - 1.0) if (last is not None and prev not in (None, 0.0)) else None

            win_52 = _env_int("EODHD_HISTORY_WINDOW_52W", 252, lo=60, hi=800)
            close_win = closes[-min(win_52, len(closes)):]
            high_win = highs[-min(win_52, len(highs)):] if highs else []
            low_win = lows[-min(win_52, len(lows)):] if lows else []
            high_52 = max(high_win) if high_win else max(close_win)
            low_52 = min(low_win) if low_win else min(close_win)
            pos_52_frac = None
            if high_52 != low_52:
                pos_52_frac = (last - low_52) / (high_52 - low_52)

            avg_vol_10 = sum(vols[-10:]) / 10.0 if len(vols) >= 10 else (sum(vols) / len(vols) if vols else None)
            avg_vol_30 = sum(vols[-30:]) / 30.0 if len(vols) >= 30 else (sum(vols) / len(vols) if vols else None)

            rets_all = _daily_returns(closes)
            rets_1y = rets_all[-min(len(rets_all), 252):] if rets_all else []
            rets_90 = rets_all[-min(len(rets_all), 90):] if rets_all else []
            rets_30 = rets_all[-min(len(rets_all), 30):] if rets_all else []

            vol_30 = _annualized_vol(rets_30)
            vol_90 = _annualized_vol(rets_90)
            vol_1y = _annualized_vol(rets_1y)
            mdd_1y = _max_drawdown(closes[-min(len(closes), 252):])
            var95 = _var_95_1d(rets_1y)
            rf = _env_float("EODHD_RISK_FREE_RATE", 0.03, lo=0.0, hi=0.20)
            sharpe = _sharpe_1y(rets_1y, rf_annual=rf)
            rsi14 = _rsi14(closes[-100:])

            def _ret(k: int) -> Optional[float]:
                if n <= k:
                    return None
                base = closes[-1 - k]
                if not base:
                    return None
                return (last / base) - 1.0

            patch = _clean_patch(
                {
                    "symbol": sym_raw,
                    "symbol_normalized": sym,
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    # quote fallback from latest history row
                    "current_price": last,
                    "price": last,
                    "previous_close": prev,
                    "prev_close": prev,
                    "day_open": latest_open,
                    "open": latest_open,
                    "day_high": latest_high,
                    "day_low": latest_low,
                    "volume": latest_vol,
                    "price_change": history_price_change,
                    "change": history_price_change,
                    "percent_change": history_percent_change,
                    "change_pct": history_percent_change,
                    # 52W / liquidity / risk
                    "week_52_low": low_52,
                    "week_52_high": high_52,
                    "week_52_position_pct": pos_52_frac,
                    "avg_vol_10d": avg_vol_10,
                    "avg_volume_10d": avg_vol_10,
                    "avg_vol_30d": avg_vol_30,
                    "avg_volume_30d": avg_vol_30,
                    "rsi_14": rsi14,
                    "volatility_30d": vol_30,
                    "volatility_90d": vol_90,
                    "volatility_365d": vol_1y,
                    "max_drawdown_1y": mdd_1y,
                    "var_95_1d": var95,
                    "sharpe_1y": sharpe,
                    "returns_1w": _ret(5),
                    "returns_1m": _ret(21),
                    "returns_3m": _ret(63),
                    "returns_6m": _ret(126),
                    "returns_12m": _ret(252),
                    "history_points": n,
                    "history_source": PROVIDER_NAME,
                    "history_last_utc": last_hist_dt,
                    "last_updated_utc": _utc_iso(),
                    "last_updated_riyadh": _riyadh_iso(),
                }
            )
            patch["52w_high"] = patch.get("week_52_high")
            patch["52w_low"] = patch.get("week_52_low")
            patch["position_52w_pct"] = patch.get("week_52_position_pct")

            await self.hist_cache.set(ck, patch)
            return patch, None

        return await self._sf.do(ck, _do)

    async def fetch_enriched_quote_patch(self, symbol: str) -> Dict[str, Any]:
        now_utc = _utc_iso()
        now_riy = _riyadh_iso()
        sym_raw = (symbol or "").strip().upper()
        sym_norm = normalize_eodhd_symbol(sym_raw)

        if not sym_norm:
            return _clean_patch(
                {
                    "symbol": sym_raw,
                    "symbol_normalized": "",
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    "data_quality": "MISSING",
                    "error": "invalid_symbol",
                    "error_detail": "normalize_eodhd_symbol returned empty",
                    "last_updated_utc": now_utc,
                    "last_updated_riyadh": now_riy,
                }
            )

        if not self.api_key:
            return _clean_patch(
                {
                    "symbol": sym_raw,
                    "symbol_normalized": sym_norm,
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    "data_quality": "MISSING",
                    "error": "missing_api_key",
                    "error_detail": "EODHD_API_KEY (or EODHD_API_TOKEN/EODHD_KEY) is not set",
                    "last_updated_utc": now_utc,
                    "last_updated_riyadh": now_riy,
                }
            )

        if _KSA_RE.match(sym_norm) and _ksa_blocked_by_default() and not _allow_ksa_override():
            return _clean_patch(
                {
                    "symbol": sym_raw,
                    "symbol_normalized": sym_norm,
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    "data_quality": "BLOCKED",
                    "error": "ksa_blocked",
                    "error_detail": "KSA_DISALLOW_EODHD=true (override with ALLOW_EODHD_KSA=1)",
                    "last_updated_utc": now_utc,
                    "last_updated_riyadh": now_riy,
                }
            )

        # FIX v4.7.0: check both FUNDAMENTALS_ENABLED and EODHD_ENABLE_FUNDAMENTALS
        enable_fund = _env_bool("FUNDAMENTALS_ENABLED", True) or _env_bool("EODHD_ENABLE_FUNDAMENTALS", True)
        enable_hist = _env_bool("EODHD_ENABLE_HISTORY", True)

        tasks: List[asyncio.Task] = [asyncio.create_task(self.fetch_quote(sym_raw))]
        if enable_fund:
            tasks.append(asyncio.create_task(self.fetch_fundamentals(sym_raw)))
        if enable_hist:
            tasks.append(asyncio.create_task(self.fetch_history_stats(sym_raw)))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        merged: Dict[str, Any] = {
            "symbol": sym_raw,
            "symbol_normalized": sym_norm,
            "provider": PROVIDER_NAME,
            "data_source": PROVIDER_NAME,
            "data_sources": [PROVIDER_NAME],
            "provider_role": "primary_global",
            "last_updated_utc": now_utc,
            "last_updated_riyadh": now_riy,
        }
        errors: List[str] = []

        for r in results:
            if isinstance(r, Exception):
                errors.append(f"exception:{r.__class__.__name__}")
                continue
            try:
                patch, err = r  # type: ignore[misc]
            except Exception:
                errors.append("bad_task_result")
                continue
            if err:
                errors.append(str(err))
            if isinstance(patch, dict) and patch:
                for k, v in patch.items():
                    if v is None:
                        continue
                    if k not in merged or merged.get(k) in (None, "", [], {}):
                        merged[k] = v

        if merged.get("current_price") is None and merged.get("price") is not None:
            merged["current_price"] = merged.get("price")
        if merged.get("price") is None and merged.get("current_price") is not None:
            merged["price"] = merged.get("current_price")
        if merged.get("previous_close") is None and merged.get("prev_close") is not None:
            merged["previous_close"] = merged.get("prev_close")
        if merged.get("prev_close") is None and merged.get("previous_close") is not None:
            merged["prev_close"] = merged.get("previous_close")
        if merged.get("open") is None and merged.get("day_open") is not None:
            merged["open"] = merged.get("day_open")
        if merged.get("day_open") is None and merged.get("open") is not None:
            merged["day_open"] = merged.get("open")

        if merged.get("market_cap") is None and merged.get("shares_outstanding") is not None and merged.get("current_price") is not None:
            try:
                merged["market_cap"] = float(merged["shares_outstanding"]) * float(merged["current_price"])
            except Exception:
                pass

        if merged.get("percent_change") is None and merged.get("current_price") is not None and merged.get("previous_close") not in (None, 0.0):
            try:
                merged["percent_change"] = (float(merged["current_price"]) / float(merged["previous_close"])) - 1.0
            except Exception:
                pass
        if merged.get("price_change") is None and merged.get("current_price") is not None and merged.get("previous_close") is not None:
            try:
                merged["price_change"] = float(merged["current_price"]) - float(merged["previous_close"])
            except Exception:
                pass
        if merged.get("week_52_position_pct") is None:
            pos = _safe_div(
                (safe_float(merged.get("current_price")) or 0.0) - (safe_float(merged.get("week_52_low")) or 0.0),
                (safe_float(merged.get("week_52_high")) or 0.0) - (safe_float(merged.get("week_52_low")) or 0.0),
            )
            if pos is not None:
                merged["week_52_position_pct"] = pos

        if merged.get("current_price") is None:
            merged["data_quality"] = "MISSING"
            merged["error"] = "fetch_failed"
            merged["error_detail"] = ",".join(sorted(set(errors))) if errors else "no_data"
        else:
            merged["data_quality"] = "OK"
            if errors:
                merged["warning"] = "partial_sources"
                merged["info"] = {"warnings": sorted(set(errors))[:6]}


        # FIX v4.7.0: Backfill exchange/country from symbol suffix when
        # not populated by quote or fundamentals (common for ADRs/foreign listings).
        if not merged.get("exchange"):
            suffix_exchange = _exchange_from_suffix(sym_norm)
            if suffix_exchange:
                merged["exchange"] = suffix_exchange
        if not merged.get("country"):
            suffix_country = _country_from_suffix(sym_norm)
            if suffix_country:
                merged["country"] = suffix_country

        merged["change"] = merged.get("price_change")
        merged["change_pct"] = merged.get("percent_change")
        merged["52w_high"] = merged.get("week_52_high")
        merged["52w_low"] = merged.get("week_52_low")
        merged["beta_5y"] = merged.get("beta_5y") or merged.get("beta")
        merged["avg_volume_10d"] = merged.get("avg_volume_10d") or merged.get("avg_vol_10d")
        merged["avg_volume_30d"] = merged.get("avg_volume_30d") or merged.get("avg_vol_30d")
        merged["fcf_ttm"] = merged.get("fcf_ttm") or merged.get("free_cash_flow_ttm")
        merged["d_e_ratio"] = merged.get("d_e_ratio") or merged.get("debt_to_equity")

        return _clean_patch(merged)


    async def get_enriched_quotes_batch(
        self,
        symbols: List[str],
        *,
        mode: str = "",
    ) -> Dict[str, Any]:
        """
        ENH v4.8.0: Batch fetch enriched quote patches for multiple symbols.
        Returns {requested_symbol: patch_dict}.
        Uses EODHDClient.fetch_enriched_quote_patch() per symbol with semaphore.
        """
        if not symbols:
            return {}
        sem = asyncio.Semaphore(max(1, _env_int("EODHD_BATCH_CONCURRENCY", 8, lo=1, hi=32)))

        async def _one(sym: str) -> tuple:
            async with sem:
                try:
                    patch = await self.fetch_enriched_quote_patch(sym)
                    return sym, patch
                except Exception as e:
                    return sym, _clean_patch({
                        "symbol": sym, "provider": PROVIDER_NAME,
                        "data_quality": "MISSING",
                        "error": f"batch_error:{e.__class__.__name__}",
                        "last_updated_utc": _utc_iso(),
                    })

        results = await asyncio.gather(*[_one(s) for s in symbols], return_exceptions=True)
        out: Dict[str, Any] = {}
        for r in results:
            if isinstance(r, Exception):
                continue
            sym, patch = r
            key = (patch.get("symbol_normalized") or patch.get("symbol") or sym) if isinstance(patch, dict) else sym
            out[key] = patch
        return out

    async def fetch_price_history(self, symbol: str, days: Optional[int] = None, *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
        sym = normalize_eodhd_symbol((symbol or "").strip().upper())
        if not sym:
            return []
        if _KSA_RE.match(sym) and _ksa_blocked_by_default() and not _allow_ksa_override():
            return []
        hist_days = max(30, int(days or _env_int("EODHD_HISTORY_DAYS", 420, lo=60, hi=5000)))
        from_date = (datetime.now(timezone.utc) - timedelta(days=hist_days)).strftime("%Y-%m-%d")
        data, err = await self._request_json(f"eod/{sym}", params={"from": from_date})
        if err or not isinstance(data, list):
            return []
        return [r for r in data if isinstance(r, dict)]

    async def fetch_history(self, symbol: str, days: Optional[int] = None, *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
        return await self.fetch_price_history(symbol, days=days, *args, **kwargs)

    async def fetch_ohlc_history(self, symbol: str, days: Optional[int] = None, *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
        return await self.fetch_price_history(symbol, days=days, *args, **kwargs)

    async def fetch_quote_patch(self, symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        patch, err = await self.fetch_quote(symbol)
        if patch:
            return patch
        return _clean_patch(
            {
                "symbol": (symbol or "").strip().upper(),
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                "error": err or "fetch_quote_failed",
                "data_quality": "MISSING",
                "last_updated_utc": _utc_iso(),
                "last_updated_riyadh": _riyadh_iso(),
            }
        )

    async def fetch_enriched_quote(self, symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.fetch_enriched_quote_patch(symbol)

    async def aclose(self) -> None:
        try:
            await self._client.aclose()
        except Exception:
            pass


_INSTANCE: Optional[EODHDClient] = None
_INSTANCE_LOCK = asyncio.Lock()


async def get_client() -> EODHDClient:
    global _INSTANCE
    if _INSTANCE is None:
        async with _INSTANCE_LOCK:
            if _INSTANCE is None:
                _INSTANCE = EODHDClient()
    return _INSTANCE


async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    client = await get_client()
    return await client.fetch_enriched_quote_patch(symbol)


async def fetch_enriched_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    client = await get_client()
    return await client.fetch_enriched_quote_patch(symbol)


async def enriched_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await fetch_enriched_quote_patch(symbol, *args, **kwargs)


async def quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    client = await get_client()
    patch, err = await client.fetch_quote(symbol)
    if err:
        raise RuntimeError(err)
    return patch


async def get_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await quote(symbol, *args, **kwargs)


async def fetch_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await quote(symbol, *args, **kwargs)


async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    client = await get_client()
    return await client.fetch_quote_patch(symbol, *args, **kwargs)


async def fetch_history(symbol: str, days: Optional[int] = None, *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
    client = await get_client()
    return await client.fetch_history(symbol, days=days, *args, **kwargs)


async def fetch_price_history(symbol: str, days: Optional[int] = None, *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
    client = await get_client()
    return await client.fetch_price_history(symbol, days=days, *args, **kwargs)


async def fetch_ohlc_history(symbol: str, days: Optional[int] = None, *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
    client = await get_client()
    return await client.fetch_ohlc_history(symbol, days=days, *args, **kwargs)



async def fetch_enriched_quotes_batch(
    symbols: List[str], *, mode: str = "", **kwargs: Any
) -> Dict[str, Any]:
    """ENH v4.8.0: Batch fetch enriched patches for multiple symbols."""
    client = await get_client()
    return await client.get_enriched_quotes_batch(symbols, mode=mode)

async def fetch_quotes(symbols: List[str], *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
    client = await get_client()
    out: List[Dict[str, Any]] = []
    sem = asyncio.Semaphore(max(1, _env_int("EODHD_BATCH_CONCURRENCY", 8, lo=1, hi=32)))

    async def _one(s: str) -> Optional[Dict[str, Any]]:
        async with sem:
            try:
                return await client.fetch_enriched_quote_patch(s)
            except Exception:
                return None

    results = await asyncio.gather(*[_one(s) for s in (symbols or [])], return_exceptions=True)
    for r in results:
        if isinstance(r, dict) and r:
            out.append(r)
    return out


__all__ = [
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    "VERSION",
    "EODHDClient",
    "get_client",
    "normalize_eodhd_symbol",
    "fetch_enriched_quote_patch",
    "fetch_enriched_quotes_batch",
    "PROVIDER_BATCH_SUPPORTED",
    "fetch_enriched_quote",
    "enriched_quote",
    "quote",
    "get_quote",
    "fetch_quote",
    "fetch_quote_patch",
    "fetch_history",
    "fetch_price_history",
    "fetch_ohlc_history",
    "fetch_quotes",
]
