#!/usr/bin/env python3
# core/providers/tadawul_provider.py
"""
core/providers/tadawul_provider.py
===============================================================
Tadawul Provider / Client (KSA quote + fundamentals + optional history) — v2.7.1
PROD SAFE + ASYNC + ENGINE PATCH-STYLE + ALIGNED ROI KEYS + RIYADH TIME

FULL REPLACEMENT (v2.7.1) — What’s improved vs v2.6.0
- ✅ Singleflight (anti-stampede): concurrent requests for same symbol share one HTTP call
- ✅ Error backoff cache (short TTL): avoids hammering endpoints on repeated failures
- ✅ Confidence alignment: forecast_confidence is 0..1 (keeps confidence_score 0..100)
- ✅ Better candle parsing: supports {c,h,l,t} arrays OR list-of-candles objects
- ✅ Adds computed helpers when possible:
    - week_52_position_pct
    - day_range_pct
    - atr_14 (if highs/lows available)
- ✅ Keeps patch-style: only meaningful fields returned; silent if not configured
- ✅ Provenance always: provider/data_source/provider_version + last_updated_utc/riyadh

Environment variables
- TADAWUL_ENABLED=true/false (default true)

Endpoints (templates support {symbol} and {code} and optional {days})
- TADAWUL_QUOTE_URL            (required for quote)
- TADAWUL_FUNDAMENTALS_URL      (optional)
- TADAWUL_HISTORY_URL          (optional; {days} supported)
- TADAWUL_CANDLES_URL          (optional alias if HISTORY_URL missing)

HTTP controls
- TADAWUL_TIMEOUT_SEC (default 25)
- TADAWUL_RETRY_ATTEMPTS (default 3)
- TADAWUL_RETRY_DELAY_SEC (default 0.25)
- TADAWUL_USER_AGENT
- TADAWUL_HEADERS_JSON (optional JSON string of extra headers)

Caches
- TADAWUL_QUOTE_TTL_SEC (default 15)
- TADAWUL_FUNDAMENTALS_TTL_SEC (default 6 hours)
- TADAWUL_HISTORY_TTL_SEC (default 1200)
- TADAWUL_ERROR_TTL_SEC (default 6)

Toggles
- TADAWUL_ENABLE_FUNDAMENTALS=true/false (default true)
- TADAWUL_ENABLE_HISTORY=true/false (default true)
- TADAWUL_ENABLE_FORECAST=true/false (default true)
- TADAWUL_VERBOSE_WARNINGS=true/false (default false)

Advanced controls
- TADAWUL_MAX_CONCURRENCY (default 25)
- TADAWUL_RATE_LIMIT_PER_SEC (0 disables; default 0)
- TADAWUL_CIRCUIT_BREAKER=true/false (default true)
- TADAWUL_CB_FAIL_THRESHOLD (default 6)
- TADAWUL_CB_COOLDOWN_SEC (default 30)
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
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import httpx

logger = logging.getLogger("core.providers.tadawul_provider")

PROVIDER_NAME = "tadawul"
PROVIDER_VERSION = "2.7.1"

USER_AGENT_DEFAULT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

DEFAULT_TIMEOUT_SEC = 25.0
DEFAULT_RETRY_ATTEMPTS = 3

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

_KSA_CODE_RE = re.compile(r"^\d{3,6}$", re.IGNORECASE)
_KSA_SYMBOL_RE = re.compile(r"^\d{3,6}(\.SR)?$", re.IGNORECASE)

# ---------------------------------------------------------------------------
# Optional shared KSA normalizer
# ---------------------------------------------------------------------------
def _try_import_shared_ksa_helpers() -> Tuple[Optional[Any], Optional[Any]]:
    try:
        from core.symbols.normalize import normalize_ksa_symbol as _nksa  # type: ignore
        from core.symbols.normalize import looks_like_ksa as _lk  # type: ignore
        return _nksa, _lk
    except Exception:
        return None, None


_SHARED_NORM_KSA, _SHARED_LOOKS_KSA = _try_import_shared_ksa_helpers()

# ---------------------------------------------------------------------------
# Env helpers
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


def _safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s or None


def _configured() -> bool:
    if not _env_bool("TADAWUL_ENABLED", True):
        return False
    return bool(_safe_str(_env_str("TADAWUL_QUOTE_URL", "")))


def _emit_warnings() -> bool:
    return _env_bool("TADAWUL_VERBOSE_WARNINGS", False)


def _enable_fundamentals() -> bool:
    return _env_bool("TADAWUL_ENABLE_FUNDAMENTALS", True)


def _enable_history() -> bool:
    return _env_bool("TADAWUL_ENABLE_HISTORY", True)


def _enable_forecast() -> bool:
    return _env_bool("TADAWUL_ENABLE_FORECAST", True)


def _timeout_sec() -> float:
    t = _env_float("TADAWUL_TIMEOUT_SEC", DEFAULT_TIMEOUT_SEC)
    return max(5.0, float(t))


def _retry_attempts() -> int:
    r = _env_int("TADAWUL_RETRY_ATTEMPTS", DEFAULT_RETRY_ATTEMPTS)
    return max(1, int(r))


def _retry_delay_sec() -> float:
    d = _env_float("TADAWUL_RETRY_DELAY_SEC", 0.25)
    return max(0.05, float(d))


def _quote_ttl_sec() -> float:
    ttl = _env_float("TADAWUL_QUOTE_TTL_SEC", 15.0)
    return max(5.0, float(ttl))


def _fund_ttl_sec() -> float:
    ttl = _env_float("TADAWUL_FUNDAMENTALS_TTL_SEC", 21600.0)
    return max(60.0, float(ttl))


def _hist_ttl_sec() -> float:
    ttl = _env_float("TADAWUL_HISTORY_TTL_SEC", 1200.0)
    return max(60.0, float(ttl))


def _err_ttl_sec() -> float:
    ttl = _env_float("TADAWUL_ERROR_TTL_SEC", 6.0)
    return max(2.0, float(ttl))


def _history_days() -> int:
    d = _env_int("TADAWUL_HISTORY_DAYS", 400)
    return max(60, int(d))


def _history_points_max() -> int:
    n = _env_int("TADAWUL_HISTORY_POINTS_MAX", 400)
    return max(100, int(n))


def _max_concurrency() -> int:
    return max(2, _env_int("TADAWUL_MAX_CONCURRENCY", 25))


def _rate_limit_per_sec() -> float:
    return max(0.0, _env_float("TADAWUL_RATE_LIMIT_PER_SEC", 0.0))


def _cb_enabled() -> bool:
    return _env_bool("TADAWUL_CIRCUIT_BREAKER", True)


def _cb_fail_threshold() -> int:
    return max(2, _env_int("TADAWUL_CB_FAIL_THRESHOLD", 6))


def _cb_cooldown_sec() -> float:
    return max(5.0, _env_float("TADAWUL_CB_COOLDOWN_SEC", 30.0))


def _extra_headers() -> Dict[str, str]:
    """
    Optional JSON: {"Authorization":"Bearer ...","X-KEY":"..."}
    """
    raw = _env_str("TADAWUL_HEADERS_JSON", "")
    if not raw:
        return {}
    try:
        js = json.loads(raw)
        if isinstance(js, dict):
            out: Dict[str, str] = {}
            for k, v in js.items():
                ks = _safe_str(k)
                vs = _safe_str(v)
                if ks and vs:
                    out[ks] = vs
            return out
    except Exception:
        pass
    return {}


def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(timezone.utc)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).isoformat()


def _to_riyadh_iso(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    tz = timezone(timedelta(hours=3))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(tz).isoformat()


def _riyadh_now_iso() -> str:
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).isoformat()


# ---------------------------------------------------------------------------
# Caching (TTLCache fallback)
# ---------------------------------------------------------------------------
try:
    from cachetools import TTLCache  # type: ignore

    _HAS_CACHETOOLS = True
except Exception:
    _HAS_CACHETOOLS = False

    class TTLCache(dict):  # type: ignore
        def __init__(self, maxsize: int, ttl: float):
            super().__init__()
            self._maxsize = max(1, int(maxsize))
            self._ttl = max(1.0, float(ttl))
            self._exp: Dict[str, float] = {}

        def get(self, key, default=None):  # type: ignore
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

        def __setitem__(self, key, value):  # type: ignore
            if len(self) >= self._maxsize:
                try:
                    oldest_key = next(iter(self.keys()))
                    super().pop(oldest_key, None)
                    self._exp.pop(oldest_key, None)
                except Exception:
                    pass
            super().__setitem__(key, value)
            self._exp[key] = time.time() + self._ttl


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------
def _to_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        if isinstance(val, (int, float)):
            f = float(val)
            if math.isnan(f) or math.isinf(f):
                return None
            return f

        s = str(val).strip()
        if not s:
            return None
        s = s.translate(_ARABIC_DIGITS)
        s = s.replace("٬", ",").replace("٫", ".")
        s = s.replace("SAR", "").replace("ريال", "").strip()
        s = s.replace("%", "").replace(",", "").replace("+", "").strip()
        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()
        f = float(s)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def _clean_patch(p: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (p or {}).items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        out[k] = v
    return out


def _unwrap_common_envelopes(js: Union[dict, list]) -> Union[dict, list]:
    cur: Any = js
    for _ in range(3):
        if isinstance(cur, dict):
            for k in ("data", "result", "payload", "quote", "profile", "response"):
                if k in cur and isinstance(cur[k], (dict, list)):
                    cur = cur[k]
                    break
            else:
                break
        else:
            break
    return cur


def _coerce_dict(data: Union[dict, list]) -> dict:
    if isinstance(data, dict):
        return data
    if isinstance(data, list):
        for it in data:
            if isinstance(it, dict):
                return it
    return {}


def _find_first_value(
    obj: Any,
    keys: Sequence[str],
    *,
    max_depth: int = 7,
    max_nodes: int = 3000,
) -> Any:
    if obj is None:
        return None
    keyset = {str(k).strip().lower() for k in keys if k}
    if not keyset:
        return None

    q: List[Tuple[Any, int]] = [(obj, 0)]
    seen: set[int] = set()
    nodes = 0

    while q:
        x, d = q.pop(0)
        if x is None:
            continue
        xid = id(x)
        if xid in seen:
            continue
        seen.add(xid)

        nodes += 1
        if nodes > max_nodes:
            return None
        if d > max_depth:
            continue

        if isinstance(x, dict):
            for k, v in x.items():
                if str(k).strip().lower() in keyset:
                    return v
            for v in x.values():
                q.append((v, d + 1))
            continue

        if isinstance(x, list):
            for it in x:
                q.append((it, d + 1))
            continue

    return None


def _pick_num(obj: Any, *keys: str) -> Optional[float]:
    return _to_float(_find_first_value(obj, keys))


def _pick_str(obj: Any, *keys: str) -> Optional[str]:
    return _safe_str(_find_first_value(obj, keys))


def _pick_pct(obj: Any, *keys: str) -> Optional[float]:
    v = _to_float(_find_first_value(obj, keys))
    if v is None:
        return None
    return v * 100.0 if abs(v) <= 1.0 else v


# ---------------------------------------------------------------------------
# KSA symbol normalization (strict)
# ---------------------------------------------------------------------------
def _normalize_ksa_symbol(symbol: str) -> str:
    raw = (symbol or "").strip()
    if not raw:
        return ""

    raw = raw.translate(_ARABIC_DIGITS).strip()

    if callable(_SHARED_NORM_KSA):
        try:
            s = (_SHARED_NORM_KSA(raw) or "").strip().upper()
            if s.endswith(".SR"):
                code = s[:-3].strip()
                return f"{code}.SR" if _KSA_CODE_RE.match(code) else ""
            return ""
        except Exception:
            pass

    s = raw.strip().upper()
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()

    if s.endswith(".SR"):
        code = s[:-3].strip()
        return f"{code}.SR" if _KSA_CODE_RE.match(code) else ""

    if _KSA_CODE_RE.match(s):
        return f"{s}.SR"

    return ""


def _format_url(tpl: str, symbol: str, *, days: Optional[int] = None) -> str:
    s = (symbol or "").strip().upper()
    code = s[:-3] if s.endswith(".SR") else s
    u = (tpl or "").replace("{symbol}", s).replace("{code}", code)
    if days is not None:
        u = u.replace("{days}", str(int(days)))
    return u


# ---------------------------------------------------------------------------
# Rate limiter + circuit breaker
# ---------------------------------------------------------------------------
class _TokenBucket:
    def __init__(self, rate_per_sec: float) -> None:
        self.rate = max(0.0, float(rate_per_sec))
        self.capacity = max(1.0, self.rate) if self.rate > 0 else 0.0
        self.tokens = self.capacity
        self.last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        if self.rate <= 0:
            return

        async with self._lock:
            now = time.monotonic()
            elapsed = max(0.0, now - self.last)
            self.last = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            need = 1.0 - self.tokens
            wait = need / self.rate if self.rate > 0 else 0.0

        if wait > 0:
            await asyncio.sleep(min(2.0, wait + random.random() * 0.05))

        async with self._lock:
            self.tokens = max(0.0, self.tokens - 1.0)


class _CircuitBreaker:
    def __init__(self, enabled: bool, fail_threshold: int, cooldown_sec: float) -> None:
        self.enabled = bool(enabled)
        self.fail_threshold = max(2, int(fail_threshold))
        self.cooldown_sec = max(5.0, float(cooldown_sec))
        self._fail_count = 0
        self._open_until = 0.0
        self._lock = asyncio.Lock()

    async def allow(self) -> bool:
        if not self.enabled:
            return True
        async with self._lock:
            return not (self._open_until > time.monotonic())

    async def on_success(self) -> None:
        if not self.enabled:
            return
        async with self._lock:
            self._fail_count = 0
            self._open_until = 0.0

    async def on_failure(self) -> None:
        if not self.enabled:
            return
        async with self._lock:
            self._fail_count += 1
            if self._fail_count >= self.fail_threshold:
                self._open_until = time.monotonic() + self.cooldown_sec


# ---------------------------------------------------------------------------
# History analytics + forecast (log regression)
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


def _max_drawdown_pct(closes: List[float]) -> Optional[float]:
    try:
        if len(closes) < 2:
            return None
        peak = closes[0]
        mdd = 0.0
        for p in closes[1:]:
            if p > peak:
                peak = p
            if peak > 0:
                dd = (p / peak) - 1.0
                if dd < mdd:
                    mdd = dd
        return float(mdd * 100.0)
    except Exception:
        return None


def _log_regression(prices: List[float]) -> Optional[Dict[str, float]]:
    try:
        n = len(prices)
        if n < 12:
            return None
        ys = []
        for p in prices:
            if p is None or p <= 0:
                return None
            ys.append(math.log(float(p)))

        xs = list(range(n))
        x_mean = (n - 1) / 2.0
        y_mean = sum(ys) / n

        sxx = sum((x - x_mean) ** 2 for x in xs)
        if sxx == 0:
            return None
        sxy = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
        b = sxy / sxx
        a = y_mean - b * x_mean

        y_hat = [a + b * x for x in xs]
        ss_tot = sum((y - y_mean) ** 2 for y in ys)
        ss_res = sum((y - yh) ** 2 for y, yh in zip(ys, y_hat))
        r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

        if any(map(lambda z: math.isnan(z) or math.isinf(z), [b, r2])):
            return None
        return {"slope": float(b), "r2": float(max(0.0, min(1.0, r2))), "n": float(n)}
    except Exception:
        return None


def _roi_from_reg(reg: Optional[Dict[str, float]], horizon_days: int) -> Optional[float]:
    if not reg:
        return None
    b = reg.get("slope")
    if b is None:
        return None
    try:
        r = math.exp(float(b) * float(horizon_days)) - 1.0
        return float(r * 100.0)
    except Exception:
        return None


def _compute_atr_14(highs: List[float], lows: List[float], closes: List[float]) -> Optional[float]:
    try:
        n = min(len(highs), len(lows), len(closes))
        if n < 15:
            return None
        trs: List[float] = []
        # use last 14 TRs
        for i in range(n - 14, n):
            h = float(highs[i])
            l = float(lows[i])
            prev_c = float(closes[i - 1])
            tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
            trs.append(tr)
        return float(sum(trs) / 14.0)
    except Exception:
        return None


def _compute_history_analytics(closes: List[float], highs: Optional[List[float]] = None, lows: Optional[List[float]] = None) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not closes:
        return out

    last = float(closes[-1])

    idx = {"returns_1w": 5, "returns_1m": 21, "returns_3m": 63, "returns_6m": 126, "returns_12m": 252}
    for k, n in idx.items():
        if len(closes) > n:
            prior = float(closes[-(n + 1)])
            if prior != 0:
                out[k] = float(((last / prior) - 1.0) * 100.0)

    def ma(n: int) -> Optional[float]:
        if len(closes) < n:
            return None
        xs = closes[-n:]
        return float(sum(xs) / float(n))

    out["ma20"] = ma(20)
    out["ma50"] = ma(50)
    out["ma200"] = ma(200)

    if len(closes) >= 31:
        rets: List[float] = []
        window = closes[-31:]
        for i in range(1, len(window)):
            p0 = float(window[i - 1])
            p1 = float(window[i])
            if p0 != 0:
                rets.append((p1 / p0) - 1.0)
        sd = _stddev(rets)
        if sd is not None:
            out["volatility_30d"] = float(sd * math.sqrt(252.0) * 100.0)

    out["rsi_14"] = _compute_rsi_14(closes)
    out["max_drawdown_pct"] = _max_drawdown_pct(closes)

    if highs and lows:
        atr = _compute_atr_14(highs, lows, closes)
        if atr is not None:
            out["atr_14"] = float(atr)

    if _enable_forecast():
        reg_1m = _log_regression([float(x) for x in closes[-min(len(closes), 84):]])
        reg_3m = _log_regression([float(x) for x in closes[-min(len(closes), 252):]])
        reg_12m = _log_regression([float(x) for x in closes[-min(len(closes), 400):]])

        roi_1m = _roi_from_reg(reg_1m, 21)
        roi_3m = _roi_from_reg(reg_3m, 63)
        roi_12m = _roi_from_reg(reg_12m, 252)

        if roi_1m is not None:
            out["expected_roi_1m"] = float(roi_1m)
            out["forecast_price_1m"] = float(last * (1.0 + roi_1m / 100.0))
        if roi_3m is not None:
            out["expected_roi_3m"] = float(roi_3m)
            out["forecast_price_3m"] = float(last * (1.0 + roi_3m / 100.0))
        if roi_12m is not None:
            out["expected_roi_12m"] = float(roi_12m)
            out["forecast_price_12m"] = float(last * (1.0 + roi_12m / 100.0))

        pts = len(closes)
        base = max(0.0, min(100.0, (pts / 252.0) * 100.0))

        r2 = None
        for reg in (reg_3m, reg_1m, reg_12m):
            if reg and isinstance(reg.get("r2"), (int, float)):
                r2 = float(reg["r2"])
                break
        if r2 is None:
            r2 = 0.25

        vol = _to_float(out.get("volatility_30d"))
        vol_pen = 0.0 if vol is None else min(55.0, max(0.0, (vol / 100.0) * 35.0))

        conf_pct = (0.55 * base) + (0.45 * (r2 * 100.0)) - vol_pen
        conf_pct = max(0.0, min(100.0, conf_pct))

        out["confidence_score"] = float(conf_pct)                             # 0..100 (human readable)
        out["forecast_confidence"] = float(conf_pct / 100.0)          # 0..1 (engine-friendly)
        out["forecast_method"] = "tadawul_history_logreg_v3"
    else:
        out["forecast_method"] = "history_only"

    return out


# ---------------------------------------------------------------------------
# Singleflight helper (avoid stampede)
# ---------------------------------------------------------------------------
class _SingleFlight:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._futs: Dict[str, asyncio.Future] = {}

    async def run(self, key: str, coro_fn):
        async with self._lock:
            fut = self._futs.get(key)
            if fut is not None:
                return await fut
            fut = asyncio.get_event_loop().create_future()
            self._futs[key] = fut

        try:
            val = await coro_fn()
            if not fut.done():
                fut.set_result(val)
            return val
        except Exception as e:
            if not fut.done():
                fut.set_exception(e)
            raise
        finally:
            async with self._lock:
                self._futs.pop(key, None)


# ---------------------------------------------------------------------------
# Tadawul Client (lazy singleton)
# ---------------------------------------------------------------------------
class TadawulClient:
    def __init__(self) -> None:
        self.timeout_sec = _timeout_sec()
        self.retry_attempts = _retry_attempts()
        self.base_delay = _retry_delay_sec()

        timeout = httpx.Timeout(self.timeout_sec, connect=min(10.0, self.timeout_sec))

        headers = {
            "User-Agent": _env_str("TADAWUL_USER_AGENT", USER_AGENT_DEFAULT),
            "Accept": "application/json,text/plain,*/*",
        }
        headers.update(_extra_headers())

        self._client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers=headers,
            limits=httpx.Limits(max_keepalive_connections=25, max_connections=50),
        )

        self.quote_url = _safe_str(_env_str("TADAWUL_QUOTE_URL", ""))
        self.fundamentals_url = _safe_str(_env_str("TADAWUL_FUNDAMENTALS_URL", ""))
        self.history_url = _safe_str(_env_str("TADAWUL_HISTORY_URL", "")) or _safe_str(_env_str("TADAWUL_CANDLES_URL", ""))

        self._q_cache: TTLCache = TTLCache(maxsize=7000, ttl=_quote_ttl_sec())
        self._f_cache: TTLCache = TTLCache(maxsize=4000, ttl=_fund_ttl_sec())
        self._h_cache: TTLCache = TTLCache(maxsize=2500, ttl=_hist_ttl_sec())
        self._e_cache: TTLCache = TTLCache(maxsize=4000, ttl=_err_ttl_sec())

        self._sem = asyncio.Semaphore(_max_concurrency())
        self._bucket = _TokenBucket(_rate_limit_per_sec())
        self._cb = _CircuitBreaker(_cb_enabled(), _cb_fail_threshold(), _cb_cooldown_sec())
        self._sf = _SingleFlight()

        logger.info(
            "Tadawul client init v%s | quote=%s | fund=%s | hist=%s | timeout=%.1fs | retries=%s | cachetools=%s | max_conc=%s | rate/s=%.2f | cb=%s",
            PROVIDER_VERSION,
            bool(self.quote_url),
            bool(self.fundamentals_url),
            bool(self.history_url),
            self.timeout_sec,
            self.retry_attempts,
            _HAS_CACHETOOLS,
            _max_concurrency(),
            _rate_limit_per_sec(),
            _cb_enabled(),
        )

    async def aclose(self) -> None:
        try:
            await self._client.aclose()
        except Exception:
            pass

    async def _get_json(self, url: str) -> Tuple[Optional[Union[dict, list]], Optional[str]]:
        if not await self._cb.allow():
            return None, "circuit_open"

        retries = max(1, int(self.retry_attempts))
        last_err: Optional[str] = None

        async with self._sem:
            await self._bucket.acquire()

            for attempt in range(retries):
                try:
                    r = await self._client.get(url)
                    sc = int(r.status_code)

                    if sc == 429 or 500 <= sc < 600:
                        last_err = f"HTTP {sc}"
                        if attempt < retries - 1:
                            await asyncio.sleep(self.base_delay * (2**attempt) + random.random() * 0.25)
                            continue
                        await self._cb.on_failure()
                        return None, last_err

                    if sc >= 400:
                        await self._cb.on_failure()
                        return None, f"HTTP {sc}"

                    try:
                        js = r.json()
                    except Exception:
                        txt = (r.text or "").strip()
                        if txt.startswith("{") or txt.startswith("["):
                            try:
                                js = json.loads(txt)
                            except Exception:
                                await self._cb.on_failure()
                                return None, "invalid JSON"
                        else:
                            await self._cb.on_failure()
                            return None, "non-JSON response"

                    if not isinstance(js, (dict, list)):
                        await self._cb.on_failure()
                        return None, "unexpected JSON type"

                    js = _unwrap_common_envelopes(js)
                    await self._cb.on_success()
                    return js, None

                except Exception as e:
                    last_err = f"{e.__class__.__name__}: {e}"
                    if attempt < retries - 1:
                        await asyncio.sleep(self.base_delay * (2**attempt) + random.random() * 0.25)
                        continue
                    await self._cb.on_failure()
                    return None, last_err

        await self._cb.on_failure()
        return None, last_err or "request_failed"

    # ---------------------------
    # Mapping
    # ---------------------------
    def _map_quote(self, root: Any) -> Dict[str, Any]:
        patch: Dict[str, Any] = {}

        patch["current_price"] = _pick_num(root, "last", "last_price", "price", "close", "c", "tradingPrice", "regularMarketPrice")
        patch["previous_close"] = _pick_num(root, "previous_close", "prev_close", "pc", "PreviousClose", "prevClose")
        patch["open"] = _pick_num(root, "open", "o", "Open", "openPrice")
        patch["day_high"] = _pick_num(root, "high", "day_high", "h", "High", "dayHigh", "sessionHigh")
        patch["day_low"] = _pick_num(root, "low", "day_low", "l", "Low", "dayLow", "sessionLow")
        patch["volume"] = _pick_num(root, "volume", "v", "Volume", "tradedVolume", "qty", "quantity")
        patch["value_traded"] = _pick_num(root, "value_traded", "tradedValue", "turnover", "value", "tradeValue")

        patch["week_52_high"] = _pick_num(root, "week_52_high", "fiftyTwoWeekHigh", "52w_high", "yearHigh")
        patch["week_52_low"] = _pick_num(root, "week_52_low", "fiftyTwoWeekLow", "52w_low", "yearLow")

        patch["price_change"] = _pick_num(root, "change", "d", "price_change", "Change", "diff", "delta")
        patch["percent_change"] = _pick_pct(root, "change_pct", "change_percent", "dp", "percent_change", "pctChange", "changePercent")

        cp = _to_float(patch.get("current_price"))
        pc = _to_float(patch.get("previous_close"))

        if patch.get("price_change") is None and cp is not None and pc is not None:
            patch["price_change"] = float(cp - pc)
        if patch.get("percent_change") is None and cp is not None and pc is not None and pc != 0:
            patch["percent_change"] = float(((cp / pc) - 1.0) * 100.0)

        hi = _to_float(patch.get("day_high"))
        lo = _to_float(patch.get("day_low"))
        if cp is not None and hi is not None and lo is not None and cp != 0:
            patch["day_range_pct"] = float(((hi - lo) / cp) * 100.0)

        w52h = _to_float(patch.get("week_52_high"))
        w52l = _to_float(patch.get("week_52_low"))
        if cp is not None and w52h is not None and w52l is not None and w52h != w52l:
            patch["week_52_position_pct"] = float(((cp - w52l) / (w52h - w52l)) * 100.0)

        patch["currency"] = patch.get("currency") or "SAR"
        return _clean_patch(patch)

    def _map_fundamentals(self, root: Any) -> Dict[str, Any]:
        patch: Dict[str, Any] = {}

        name = _pick_str(root, "name", "company", "company_name", "CompanyName", "shortName", "longName")
        if name:
            patch["name"] = name

        sector = _pick_str(root, "sector", "Sector", "sectorName")
        if sector:
            patch["sector"] = sector

        industry = _pick_str(root, "industry", "Industry", "industryName")
        if industry:
            patch["industry"] = industry

        sub_sector = _pick_str(root, "sub_sector", "subSector", "SubSector", "subSectorName", "subIndustry")
        if sub_sector:
            patch["sub_sector"] = sub_sector

        patch["market_cap"] = _pick_num(root, "market_cap", "marketCap", "marketCapitalization", "MarketCap")
        patch["shares_outstanding"] = _pick_num(root, "shares", "shares_outstanding", "shareOutstanding", "SharesOutstanding")

        patch["pe_ttm"] = _pick_num(root, "pe", "pe_ttm", "trailingPE", "PE", "priceEarnings")
        patch["pb"] = _pick_num(root, "pb", "priceToBook", "PBR", "price_book")
        patch["eps_ttm"] = _pick_num(root, "eps", "eps_ttm", "trailingEps", "EPS")
        patch["dividend_yield"] = _pick_pct(root, "dividend_yield", "divYield", "yield", "DividendYield")
        patch["beta"] = _pick_num(root, "beta", "Beta")

        if patch.get("currency") is None:
            patch["currency"] = "SAR"

        return _clean_patch(patch)

    # ---------------------------
    # Candle parsing (supports arrays or list-of-objects)
    # ---------------------------
    def _parse_candles(self, js: Any) -> Tuple[List[float], List[float], List[float], Optional[datetime]]:
        closes: List[float] = []
        highs: List[float] = []
        lows: List[float] = []
        last_dt: Optional[datetime] = None

        if isinstance(js, dict):
            c_raw = js.get("c") or js.get("close") or js.get("closes")
            h_raw = js.get("h") or js.get("high") or js.get("highs")
            l_raw = js.get("l") or js.get("low") or js.get("lows")
            t_raw = js.get("t") or js.get("time") or js.get("timestamp") or js.get("timestamps")

            if isinstance(c_raw, list):
                for x in c_raw:
                    fx = _to_float(x)
                    if fx is not None:
                        closes.append(float(fx))
            if isinstance(h_raw, list):
                for x in h_raw:
                    fx = _to_float(x)
                    if fx is not None:
                        highs.append(float(fx))
            if isinstance(l_raw, list):
                for x in l_raw:
                    fx = _to_float(x)
                    if fx is not None:
                        lows.append(float(fx))

            if isinstance(t_raw, list) and t_raw:
                t_last = t_raw[-1]
                try:
                    if isinstance(t_last, (int, float)):
                        last_dt = datetime.fromtimestamp(float(t_last), tz=timezone.utc)
                except Exception:
                    last_dt = None

            # list-of-candles embedded
            if not closes:
                candles = js.get("candles") or js.get("data") or js.get("items") or js.get("prices")
                if isinstance(candles, list) and candles and isinstance(candles[0], dict):
                    for it in candles:
                        c = _to_float(it.get("c") or it.get("close") or it.get("price"))
                        h = _to_float(it.get("h") or it.get("high"))
                        l = _to_float(it.get("l") or it.get("low"))
                        t = it.get("t") or it.get("time") or it.get("timestamp") or it.get("date")
                        if c is not None:
                            closes.append(float(c))
                        if h is not None:
                            highs.append(float(h))
                        if l is not None:
                            lows.append(float(l))
                        if last_dt is None and t is not None:
                            try:
                                if isinstance(t, (int, float)):
                                    last_dt = datetime.fromtimestamp(float(t), tz=timezone.utc)
                            except Exception:
                                pass
                    if candles:
                        t_last2 = candles[-1].get("t") or candles[-1].get("time") or candles[-1].get("timestamp")
                        try:
                            if isinstance(t_last2, (int, float)):
                                last_dt = datetime.fromtimestamp(float(t_last2), tz=timezone.utc)
                        except Exception:
                            pass

        # clamp all lists to same max points
        maxp = _history_points_max()
        closes = closes[-maxp:]
        highs = highs[-maxp:]
        lows = lows[-maxp:]

        return closes, highs, lows, last_dt

    # ---------------------------
    # Fetch methods
    # ---------------------------
    async def fetch_quote_patch(self, symbol: str) -> Dict[str, Any]:
        if not _configured():
            return {}

        sym = _normalize_ksa_symbol(symbol)
        if not sym:
            return {}

        if not self.quote_url:
            return {} if not _emit_warnings() else {"_warn": "tadawul quote url missing"}

        ck = f"q::{sym}"
        hit = self._q_cache.get(ck)
        if isinstance(hit, dict) and hit:
            return dict(hit)

        # short error backoff
        if self._e_cache.get(ck):
            return {} if not _emit_warnings() else {"_warn": "tadawul quote temporarily backed off"}

        async def _do():
            url = _format_url(self.quote_url, sym)
            js, err = await self._get_json(url)
            if js is None:
                if err:
                    self._e_cache[ck] = err
                return {} if not _emit_warnings() else {"_warn": f"tadawul quote failed: {err}"}

            root = _coerce_dict(js)
            mapped = self._map_quote(root)

            if _to_float(mapped.get("current_price")) is None:
                self._e_cache[ck] = "no_price"
                return {} if not _emit_warnings() else {"_warn": "tadawul quote returned no price"}

            patch = dict(mapped)
            patch.update(
                {
                    "requested_symbol": symbol,
                    "symbol": sym,
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    "provider_version": PROVIDER_VERSION,
                    "last_updated_utc": _utc_iso(),
                    "last_updated_riyadh": _riyadh_now_iso(),
                }
            )
            patch = _clean_patch(patch)
            self._q_cache[ck] = dict(patch)
            return patch

        return await self._sf.run(ck, _do)

    async def fetch_fundamentals_patch(self, symbol: str) -> Dict[str, Any]:
        if not _configured() or not _enable_fundamentals():
            return {}

        sym = _normalize_ksa_symbol(symbol)
        if not sym:
            return {}

        if not self.fundamentals_url:
            return {}

        ck = f"f::{sym}"
        hit = self._f_cache.get(ck)
        if isinstance(hit, dict) and hit:
            return dict(hit)

        if self._e_cache.get(ck):
            return {}

        async def _do():
            url = _format_url(self.fundamentals_url, sym)
            js, err = await self._get_json(url)
            if js is None:
                if err:
                    self._e_cache[ck] = err
                return {} if not _emit_warnings() else {"_warn": f"tadawul fundamentals failed: {err}"}

            root = _coerce_dict(js)
            mapped = self._map_fundamentals(root)
            if not mapped:
                self._e_cache[ck] = "no_useful_fields"
                return {} if not _emit_warnings() else {"_warn": "tadawul fundamentals had no useful fields"}

            patch = dict(mapped)
            patch.update(
                {
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    "provider_version": PROVIDER_VERSION,
                }
            )
            patch = _clean_patch(patch)
            self._f_cache[ck] = dict(patch)
            return patch

        return await self._sf.run(ck, _do)

    async def fetch_history_patch(self, symbol: str) -> Dict[str, Any]:
        if not _configured() or not _enable_history():
            return {}

        sym = _normalize_ksa_symbol(symbol)
        if not sym:
            return {}

        if not self.history_url:
            return {}

        days = _history_days()
        ck = f"h::{sym}::{days}"
        hit = self._h_cache.get(ck)
        if isinstance(hit, dict) and hit:
            return dict(hit)

        if self._e_cache.get(ck):
            return {}

        async def _do():
            url = _format_url(self.history_url, sym, days=days)
            js, err = await self._get_json(url)
            if js is None:
                if err:
                    self._e_cache[ck] = err
                return {} if not _emit_warnings() else {"_warn": f"tadawul history failed: {err}"}

            closes, highs, lows, last_dt = self._parse_candles(js)
            if not closes:
                self._e_cache[ck] = "no_closes"
                return {} if not _emit_warnings() else {"_warn": "tadawul history parsed but no closes found"}

            analytics = _compute_history_analytics(closes, highs if highs else None, lows if lows else None)

            patch: Dict[str, Any] = {
                "history_points": len(closes),
                "history_last_utc": _utc_iso(last_dt) if last_dt else _utc_iso(),
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                "provider_version": PROVIDER_VERSION,
            }

            # forecast timestamps (only if forecast outputs exist)
            if any(k in analytics for k in ("expected_roi_1m", "expected_roi_3m", "expected_roi_12m", "forecast_method")):
                patch["forecast_updated_utc"] = _utc_iso(last_dt) if last_dt else _utc_iso()
                riyadh = _to_riyadh_iso(last_dt)
                if riyadh:
                    patch["forecast_updated_riyadh"] = riyadh
                patch["forecast_source"] = "tadawul_history"

            patch.update(analytics)
            patch = _clean_patch(patch)

            self._h_cache[ck] = dict(patch)
            return patch

        return await self._sf.run(ck, _do)

    async def fetch_enriched_quote_patch(self, symbol: str) -> Dict[str, Any]:
        """
        Quote first, then fill fundamentals, then add history analytics (without overwriting quote fields).
        """
        if not _configured():
            return {}

        q = await self.fetch_quote_patch(symbol)
        if not q:
            return {}

        f = await self.fetch_fundamentals_patch(symbol)
        h = await self.fetch_history_patch(symbol)

        out = dict(q)

        # Fill fundamentals only if missing
        if f:
            for k, v in f.items():
                if k == "_warn":
                    continue
                if (k not in out or out.get(k) in (None, "")) and v is not None:
                    out[k] = v

        # Add history analytics only if missing
        if h:
            for k, v in h.items():
                if k == "_warn":
                    continue
                if k not in out and v is not None:
                    out[k] = v

        out.setdefault("provider", PROVIDER_NAME)
        out.setdefault("data_source", PROVIDER_NAME)
        out.setdefault("provider_version", PROVIDER_VERSION)

        # Collect warnings if enabled
        if _emit_warnings():
            warns: List[str] = []
            for src in (q, f, h):
                w = src.get("_warn") if isinstance(src, dict) else None
                if isinstance(w, str) and w.strip():
                    warns.append(w.strip())
            if warns:
                out["_warn"] = " | ".join(warns)

        return _clean_patch(out)


# ---------------------------------------------------------------------------
# Lazy singleton access
# ---------------------------------------------------------------------------
_CLIENT_SINGLETON: Optional[TadawulClient] = None
_CLIENT_LOCK = asyncio.Lock()


async def get_tadawul_client() -> TadawulClient:
    global _CLIENT_SINGLETON
    if _CLIENT_SINGLETON is None:
        async with _CLIENT_LOCK:
            if _CLIENT_SINGLETON is None:
                _CLIENT_SINGLETON = TadawulClient()
    return _CLIENT_SINGLETON


async def aclose_tadawul_client() -> None:
    global _CLIENT_SINGLETON
    c = _CLIENT_SINGLETON
    _CLIENT_SINGLETON = None
    if c is not None:
        await c.aclose()


# ---------------------------------------------------------------------------
# Engine-compatible exported callables
# ---------------------------------------------------------------------------
async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    c = await get_tadawul_client()
    return await c.fetch_enriched_quote_patch(symbol)


async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    c = await get_tadawul_client()
    return await c.fetch_quote_patch(symbol)


async def fetch_fundamentals_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    c = await get_tadawul_client()
    return await c.fetch_fundamentals_patch(symbol)


async def fetch_history_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    c = await get_tadawul_client()
    return await c.fetch_history_patch(symbol)


__all__ = [
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    "get_tadawul_client",
    "fetch_enriched_quote_patch",
    "fetch_quote_patch",
    "fetch_fundamentals_patch",
    "fetch_history_patch",
    "aclose_tadawul_client",
]
