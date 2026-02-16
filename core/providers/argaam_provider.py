#!/usr/bin/env python3
# core/providers/argaam_provider.py
"""
core/providers/argaam_provider.py
===============================================================
Argaam Provider (KSA optional enrichment + optional history/forecast) — v2.2.0
PROD SAFE + ASYNC + ENGINE PATCH-STYLE + SILENT WHEN NOT CONFIGURED

What’s improved in v2.2.0 (vs your v1.9.0)
- ✅ Stronger KSA-only normalization (Arabic digits, Tadawul prefixes, .SR enforcement)
- ✅ Adds requested_symbol + normalized_symbol for perfect traceability
- ✅ Safe async concurrency controls + optional rate limiting (token bucket)
- ✅ Circuit breaker (optional) to avoid hammering a failing upstream
- ✅ More robust JSON unwrapping + history parsing (dict/list/arrays best-effort)
- ✅ Better derived fields: compute change / change% if missing but price+prev_close exist
- ✅ Advanced history analytics:
    - returns (1W/1M/3M/6M/12M), MA20/50/200, RSI(14), volatility(30D annualized),
      max_drawdown, trend slopes, R²
    - Forecast uses log-price regression (more stable than mean-daily-return)
    - Confidence uses points + R² + volatility penalty
- ✅ Riyadh timestamp conversion based on history_last_utc when available (not "now")
- ✅ Keeps key guarantees:
    - import-safe (cachetools optional)
    - no network at import-time
    - silent {} when not configured
    - strict KSA-only (non-KSA ignored)
    - patch returns only useful fields + optional _warn when enabled
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

logger = logging.getLogger("core.providers.argaam_provider")

PROVIDER_NAME = "argaam"
PROVIDER_VERSION = "2.2.0"

DEFAULT_TIMEOUT_SEC = 20.0
DEFAULT_RETRY_ATTEMPTS = 2

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

# KSA code: 3-6 digits
_KSA_CODE_RE = re.compile(r"^\d{3,6}$", re.IGNORECASE)


# ------------------------------------------------------------
# Optional shared normalizer (PROD SAFE)
# ------------------------------------------------------------
def _try_import_shared_normalizer() -> Tuple[Optional[Any], Optional[Any]]:
    """
    Returns (normalize_symbol, looks_like_ksa) if available; else (None, None).
    Never raises.
    """
    try:
        from core.symbols.normalize import normalize_symbol as _ns  # type: ignore
        from core.symbols.normalize import looks_like_ksa as _lk  # type: ignore

        return _ns, _lk
    except Exception:
        return None, None


_SHARED_NORMALIZE, _SHARED_LOOKS_KSA = _try_import_shared_normalizer()


# ------------------------------------------------------------
# Env + safe helpers
# ------------------------------------------------------------
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


def _safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s or None


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


def _env_bool(name: str, default: bool = True) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in _FALSY:
        return False
    if raw in _TRUTHY:
        return True
    return default


def _timeout_seconds() -> float:
    for key in ("ARGAAM_TIMEOUT_SEC", "HTTP_TIMEOUT_SEC", "HTTP_TIMEOUT"):
        t = _env_float(key, 0.0)
        if t and t > 0:
            return float(t)
    return DEFAULT_TIMEOUT_SEC


def _retry_attempts() -> int:
    r = _env_int("ARGAAM_RETRY_ATTEMPTS", 0)
    if r > 0:
        return max(1, r)
    r = _env_int("HTTP_RETRY_ATTEMPTS", 0)
    if r > 0:
        return max(1, r)
    r = _env_int("MAX_RETRIES", 0)
    if r > 0:
        return max(1, r)
    return DEFAULT_RETRY_ATTEMPTS


def _retry_delay_sec() -> float:
    d = _env_float("ARGAAM_RETRY_DELAY_SEC", 0.0)
    return d if d > 0 else 0.25


def _quote_ttl_sec() -> float:
    ttl = _env_float("ARGAAM_TTL_SEC", 15.0)
    if ttl <= 0:
        ttl = 15.0
    return max(5.0, float(ttl))


def _profile_ttl_sec() -> float:
    ttl = _env_float("ARGAAM_PROFILE_TTL_SEC", 3600.0)
    if ttl <= 0:
        ttl = 3600.0
    return max(60.0, float(ttl))


def _history_ttl_sec() -> float:
    ttl = _env_float("ARGAAM_HISTORY_TTL_SEC", 1200.0)
    if ttl <= 0:
        ttl = 1200.0
    return max(60.0, float(ttl))


def _history_days() -> int:
    d = _env_int("ARGAAM_HISTORY_DAYS", 400)
    return max(60, int(d))


def _history_points_max() -> int:
    n = _env_int("ARGAAM_HISTORY_POINTS_MAX", 0)
    return max(100, n) if n > 0 else 400


def _configured() -> bool:
    if not _env_bool("ARGAAM_ENABLED", True):
        return False

    return bool(
        _safe_str(os.getenv("ARGAAM_QUOTE_URL", ""))
        or _safe_str(os.getenv("ARGAAM_PROFILE_URL", ""))
        or _safe_str(os.getenv("ARGAAM_HISTORY_URL", ""))
        or _safe_str(os.getenv("ARGAAM_CANDLES_URL", ""))
    )


def _emit_warnings() -> bool:
    return _env_bool("ARGAAM_VERBOSE_WARNINGS", False)


def _history_enabled() -> bool:
    return _env_bool("ARGAAM_ENABLE_HISTORY", True)


def _forecast_enabled() -> bool:
    return _env_bool("ARGAAM_ENABLE_FORECAST", True)


def _max_concurrency() -> int:
    return max(2, _env_int("ARGAAM_MAX_CONCURRENCY", 20))


def _rate_limit_per_sec() -> float:
    # 0 => disabled
    return max(0.0, _env_float("ARGAAM_RATE_LIMIT_PER_SEC", 0.0))


def _cb_enabled() -> bool:
    return _env_bool("ARGAAM_CIRCUIT_BREAKER", True)


def _cb_fail_threshold() -> int:
    return max(2, _env_int("ARGAAM_CB_FAIL_THRESHOLD", 6))


def _cb_cooldown_sec() -> float:
    return max(5.0, _env_float("ARGAAM_CB_COOLDOWN_SEC", 30.0))


# ------------------------------------------------------------
# Symbol helpers (KSA strict)
# ------------------------------------------------------------
def _normalize_ksa_symbol(symbol: str) -> str:
    """
    Strict KSA normalization:
    - Accepts: "1234", "1234.SR", "TADAWUL:1234", "TADAWUL:1234.SR", "1234.TADAWUL"
    - Translates Arabic digits to ASCII
    - Returns canonical "####.SR" OR "" when non-KSA/invalid
    """
    raw = (symbol or "").strip()
    if not raw:
        return ""

    raw = raw.translate(_ARABIC_DIGITS).strip()

    # Prefer shared normalizer if available
    if callable(_SHARED_NORMALIZE) and callable(_SHARED_LOOKS_KSA):
        try:
            if not _SHARED_LOOKS_KSA(raw):
                return ""
            s2 = (_SHARED_NORMALIZE(raw) or "").strip().upper()
            if s2.endswith(".SR"):
                code = s2[:-3].strip()
                return f"{code}.SR" if _KSA_CODE_RE.match(code) else ""
            return ""
        except Exception:
            pass

    s = raw.upper().strip()

    # common prefixes/suffixes
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", "").strip()

    # enforce SR
    if s.endswith(".SR"):
        code = s[:-3].strip()
        return f"{code}.SR" if _KSA_CODE_RE.match(code) else ""

    if _KSA_CODE_RE.match(s):
        return f"{s}.SR"

    return ""


def _format_url(tpl: str, symbol: str, *, days: Optional[int] = None) -> str:
    """
    Supports templates with {symbol} and/or {code} and optional {days}.
    symbol must be normalized "####.SR".
    """
    s = (symbol or "").strip().upper()
    code = s[:-3] if s.endswith(".SR") else s
    u = (tpl or "").replace("{symbol}", s).replace("{code}", code)
    if days is not None:
        u = u.replace("{days}", str(int(days)))
    return u


# ------------------------------------------------------------
# Tiny TTL cache fallback if cachetools missing (import-safe)
# ------------------------------------------------------------
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


# ------------------------------------------------------------
# Parsing helpers
# ------------------------------------------------------------
def _safe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        if isinstance(val, (int, float)):
            f = float(val)
            if math.isnan(f) or math.isinf(f):
                return None
            return f

        s = str(val).strip()
        if not s or s in {"-", "—", "N/A", "NA", "null", "None"}:
            return None

        s = s.translate(_ARABIC_DIGITS)
        s = s.replace("٬", ",").replace("٫", ".")
        s = s.replace("−", "-")  # unicode minus
        s = s.replace("SAR", "").replace("ريال", "").replace("USD", "").strip()
        s = s.replace("%", "").replace(",", "").replace("+", "").strip()

        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()

        # K/M/B suffix
        m = re.match(r"^(-?\d+(\.\d+)?)([KMB])$", s, re.IGNORECASE)
        mult = 1.0
        if m:
            num = m.group(1)
            suf = m.group(3).upper()
            mult = 1_000.0 if suf == "K" else 1_000_000.0 if suf == "M" else 1_000_000_000.0
            s = num

        f = float(s) * mult
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def _pct_if_fraction(x: Any) -> Optional[float]:
    v = _safe_float(x)
    if v is None:
        return None
    return v * 100.0 if abs(v) <= 1.0 else v


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
    """
    Unwraps a few common wrappers like {"data": {...}} / {"result": {...}}.
    Bounded (prevents loops).
    """
    cur: Any = js
    for _ in range(4):
        if isinstance(cur, dict):
            for k in ("data", "result", "payload", "quote", "profile", "response", "items"):
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


def _find_first_value(obj: Any, keys: Sequence[str], *, max_depth: int = 7, max_nodes: int = 3500) -> Any:
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
    return _safe_float(_find_first_value(obj, keys))


def _pick_pct(obj: Any, *keys: str) -> Optional[float]:
    return _pct_if_fraction(_find_first_value(obj, keys))


def _pick_str(obj: Any, *keys: str) -> Optional[str]:
    return _safe_str(_find_first_value(obj, keys))


# ------------------------------------------------------------
# History parsing + analytics
# ------------------------------------------------------------
def _safe_dt(x: Any) -> Optional[datetime]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            v = float(x)
            if v > 10_000_000_000:  # ms
                v = v / 1000.0
            if v > 0:
                return datetime.fromtimestamp(v, tz=timezone.utc)
            return None
        s = str(x).strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _find_first_list_of_dicts(
    obj: Any, *, required_keys: Sequence[str], max_depth: int = 7, max_nodes: int = 6000
) -> Optional[List[Dict[str, Any]]]:
    if obj is None:
        return None
    req = {str(k).strip().lower() for k in required_keys if k}
    if not req:
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

        if isinstance(x, list) and x:
            if isinstance(x[0], dict):
                keys0 = {str(k).strip().lower() for k in x[0].keys()}
                if keys0.intersection(req):
                    out = [it for it in x if isinstance(it, dict)]
                    return out if out else None
            for it in x:
                q.append((it, d + 1))
            continue

        if isinstance(x, dict):
            for v in x.values():
                q.append((v, d + 1))
            continue

    return None


def _extract_close_series(history_json: Any, *, max_points: int) -> Tuple[List[float], Optional[datetime], Optional[str]]:
    """
    Returns (closes, last_dt_utc, source_hint).

    Supports:
    - list of dicts with close/c/price/last/value
    - dict containing such a list under common keys
    - list of arrays [t,o,h,l,c,v] (best-effort)
    """
    try:
        # 1) list of dicts
        items = _find_first_list_of_dicts(
            history_json,
            required_keys=("close", "c", "price", "last", "tradingPrice", "value", "date", "timestamp", "t"),
        )
        if items:
            rows: List[Tuple[Optional[datetime], float]] = []
            last_dt: Optional[datetime] = None

            for it in items:
                close = (
                    _safe_float(it.get("close"))
                    or _safe_float(it.get("c"))
                    or _safe_float(it.get("price"))
                    or _safe_float(it.get("last"))
                    or _safe_float(it.get("tradingPrice"))
                    or _safe_float(it.get("value"))
                )
                if close is None:
                    continue

                dt = (
                    _safe_dt(it.get("date"))
                    or _safe_dt(it.get("datetime"))
                    or _safe_dt(it.get("time"))
                    or _safe_dt(it.get("timestamp"))
                    or _safe_dt(it.get("t"))
                )

                if dt and (last_dt is None or dt > last_dt):
                    last_dt = dt

                rows.append((dt, float(close)))

            if not rows:
                return [], None, None

            have_dt = any(dt is not None for dt, _ in rows)
            if have_dt:
                rows = sorted(rows, key=lambda x: (x[0] or datetime(1970, 1, 1, tzinfo=timezone.utc)))

            closes = [c for _, c in rows][-max_points:]
            return closes, last_dt, "dict_series"

        # 2) list of arrays: [t,o,h,l,c,v] or [t,c]
        if isinstance(history_json, list) and history_json and isinstance(history_json[0], (list, tuple)):
            rows2: List[Tuple[Optional[datetime], float]] = []
            last_dt2: Optional[datetime] = None

            for arr in history_json:
                if not isinstance(arr, (list, tuple)) or len(arr) < 2:
                    continue
                dt = _safe_dt(arr[0])
                c = _safe_float(arr[4] if len(arr) >= 5 else arr[1])
                if c is None:
                    continue
                if dt and (last_dt2 is None or dt > last_dt2):
                    last_dt2 = dt
                rows2.append((dt, float(c)))

            if not rows2:
                return [], None, None

            have_dt = any(dt is not None for dt, _ in rows2)
            if have_dt:
                rows2 = sorted(rows2, key=lambda x: (x[0] or datetime(1970, 1, 1, tzinfo=timezone.utc)))

            closes2 = [c for _, c in rows2][-max_points:]
            return closes2, last_dt2, "array_series"

        return [], None, None
    except Exception:
        return [], None, None


def _return_pct(last: float, prior: float) -> Optional[float]:
    try:
        if prior == 0:
            return None
        return (last / prior - 1.0) * 100.0
    except Exception:
        return None


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
    """
    Fits y = a + b*x on y=log(price) with x=0..n-1.
    Returns slope b (per-step log return), r2, and n.
    """
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

        # r2
        y_hat = [a + b * x for x in xs]
        ss_tot = sum((y - y_mean) ** 2 for y in ys)
        ss_res = sum((y - yh) ** 2 for y, yh in zip(ys, y_hat))
        r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

        if math.isnan(b) or math.isinf(b) or math.isnan(r2) or math.isinf(r2):
            return None

        return {"slope": float(b), "r2": float(max(0.0, min(1.0, r2))), "n": float(n)}
    except Exception:
        return None


def _compute_history_analytics(closes: List[float]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not closes:
        return out

    last = float(closes[-1])

    # Returns
    idx = {"returns_1w": 5, "returns_1m": 21, "returns_3m": 63, "returns_6m": 126, "returns_12m": 252}
    for k, n in idx.items():
        if len(closes) > n:
            prior = float(closes[-(n + 1)])
            out[k] = _return_pct(last, prior)

    # MAs
    def ma(n: int) -> Optional[float]:
        if len(closes) < n:
            return None
        xs = closes[-n:]
        return float(sum(xs) / float(n))

    out["ma20"] = ma(20)
    out["ma50"] = ma(50)
    out["ma200"] = ma(200)

    # Volatility (30D annualized)
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

    # RSI + Max Drawdown
    out["rsi_14"] = _compute_rsi_14(closes)
    out["max_drawdown_pct"] = _max_drawdown_pct(closes)

    # Forecast via log-regression on different windows
    def reg_on_last(n: int) -> Optional[Dict[str, float]]:
        if len(closes) < n:
            return None
        return _log_regression([float(x) for x in closes[-n:]])

    reg_1m = reg_on_last(min(len(closes), 84))    # ~4 months cap for stability
    reg_3m = reg_on_last(min(len(closes), 252))   # up to 12 months
    reg_12m = reg_on_last(min(len(closes), 400))  # cap

    def roi_from_reg(reg: Optional[Dict[str, float]], horizon_days: int) -> Optional[float]:
        if not reg:
            return None
        b = reg.get("slope")
        if b is None:
            return None
        # expected multiplicative change over horizon
        r = math.exp(float(b) * float(horizon_days)) - 1.0
        return float(r * 100.0)

    roi_1m = roi_from_reg(reg_1m, 21)
    roi_3m = roi_from_reg(reg_3m, 63)
    roi_12m = roi_from_reg(reg_12m, 252)

    # Respect forecast toggle
    if _forecast_enabled():
        if roi_1m is not None:
            out["expected_roi_1m"] = roi_1m
            out["expected_price_1m"] = float(last * (1.0 + (roi_1m / 100.0)))
        if roi_3m is not None:
            out["expected_roi_3m"] = roi_3m
            out["expected_price_3m"] = float(last * (1.0 + (roi_3m / 100.0)))
        if roi_12m is not None:
            out["expected_roi_12m"] = roi_12m
            out["expected_price_12m"] = float(last * (1.0 + (roi_12m / 100.0)))

        # Trend diagnostics (helpful for debugging dashboards)
        if reg_1m:
            out["trend_slope_1m"] = float(reg_1m.get("slope", 0.0))
            out["trend_r2_1m"] = float(reg_1m.get("r2", 0.0))
        if reg_3m:
            out["trend_slope_3m"] = float(reg_3m.get("slope", 0.0))
            out["trend_r2_3m"] = float(reg_3m.get("r2", 0.0))
        if reg_12m:
            out["trend_slope_12m"] = float(reg_12m.get("slope", 0.0))
            out["trend_r2_12m"] = float(reg_12m.get("r2", 0.0))

        # Confidence: points + r2 - volatility penalty
        pts = len(closes)
        base = max(0.0, min(100.0, (pts / 252.0) * 100.0))

        # choose the most relevant r2 (3m if present else 1m else 12m)
        r2 = None
        for reg in (reg_3m, reg_1m, reg_12m):
            if reg and isinstance(reg.get("r2"), (int, float)):
                r2 = float(reg["r2"])
                break
        if r2 is None:
            r2 = 0.25

        vol = _safe_float(out.get("volatility_30d"))
        vol_pen = 0.0 if vol is None else min(55.0, max(0.0, (vol / 100.0) * 35.0))

        conf = (0.55 * base) + (0.45 * (r2 * 100.0)) - vol_pen
        conf = max(0.0, min(100.0, conf))
        out["confidence_score"] = float(conf)
        out["forecast_method"] = "argaam_history_logreg_v2"
    else:
        out["forecast_method"] = "history_only"

    return out


def _add_forecast_aliases(p: Dict[str, Any], *, history_last_dt: Optional[datetime] = None) -> Dict[str, Any]:
    """
    Adds alias fields for Sheets readability while keeping the original keys.
    Does not overwrite existing values.
    """
    out = dict(p or {})

    # price aliases
    if "expected_price_1m" in out and "forecast_price_1m" not in out:
        out["forecast_price_1m"] = out.get("expected_price_1m")
    if "expected_price_3m" in out and "forecast_price_3m" not in out:
        out["forecast_price_3m"] = out.get("expected_price_3m")
    if "expected_price_12m" in out and "forecast_price_12m" not in out:
        out["forecast_price_12m"] = out.get("expected_price_12m")

    # ROI aliases
    if "expected_roi_1m" in out and "expected_roi_pct_1m" not in out:
        out["expected_roi_pct_1m"] = out.get("expected_roi_1m")
    if "expected_roi_3m" in out and "expected_roi_pct_3m" not in out:
        out["expected_roi_pct_3m"] = out.get("expected_roi_3m")
    if "expected_roi_12m" in out and "expected_roi_pct_12m" not in out:
        out["expected_roi_pct_12m"] = out.get("expected_roi_12m")

    # confidence aliases
    if "confidence_score" in out and "forecast_confidence" not in out:
        out["forecast_confidence"] = out.get("confidence_score")

    # updated timestamps based on actual history last dt (better than "now")
    if "history_last_utc" in out and "forecast_updated_utc" not in out:
        out["forecast_updated_utc"] = out.get("history_last_utc")

    if "forecast_updated_riyadh" not in out:
        r = _to_riyadh_iso(history_last_dt)
        if r:
            out["forecast_updated_riyadh"] = r

    return out


# ------------------------------------------------------------
# Header builder
# ------------------------------------------------------------
def _base_headers() -> Dict[str, str]:
    h = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json,text/plain,text/html;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.8,ar;q=0.6",
    }
    raw = _safe_str(os.getenv("ARGAAM_HEADERS_JSON", ""))
    if raw:
        try:
            obj = json.loads(raw)
            if isinstance(obj, dict):
                for k, v in obj.items():
                    h[str(k)] = str(v)
        except Exception:
            pass
    return h


def _endpoint_headers(kind: str) -> Dict[str, str]:
    """
    kind: "quote" | "profile" | "history"
    """
    key = {
        "quote": "ARGAAM_HEADERS_QUOTE_JSON",
        "profile": "ARGAAM_HEADERS_PROFILE_JSON",
        "history": "ARGAAM_HEADERS_HISTORY_JSON",
    }.get(kind, "")

    if not key:
        return {}

    raw = _safe_str(os.getenv(key, ""))
    if not raw:
        return {}

    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return {str(k): str(v) for k, v in obj.items()}
    except Exception:
        return {}

    return {}


# ------------------------------------------------------------
# Rate limiter + circuit breaker (async-safe)
# ------------------------------------------------------------
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
            if self._open_until > time.monotonic():
                return False
            return True

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


# ------------------------------------------------------------
# HTTP client (lazy singleton)
# ------------------------------------------------------------
class ArgaamClient:
    def __init__(self) -> None:
        self.timeout_sec = _timeout_seconds()
        self.retry_attempts = _retry_attempts()
        timeout = httpx.Timeout(self.timeout_sec, connect=min(10.0, self.timeout_sec))

        self._client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers=_base_headers(),
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=40),
        )

        self.quote_url = _safe_str(_env_str("ARGAAM_QUOTE_URL", ""))
        self.profile_url = _safe_str(_env_str("ARGAAM_PROFILE_URL", ""))
        self.history_url = _safe_str(_env_str("ARGAAM_HISTORY_URL", "")) or _safe_str(_env_str("ARGAAM_CANDLES_URL", ""))

        self._quote_cache: TTLCache = TTLCache(maxsize=5000, ttl=_quote_ttl_sec())
        self._profile_cache: TTLCache = TTLCache(maxsize=3000, ttl=_profile_ttl_sec())
        self._hist_cache: TTLCache = TTLCache(maxsize=2000, ttl=_history_ttl_sec())

        self._sem = asyncio.Semaphore(_max_concurrency())
        self._bucket = _TokenBucket(_rate_limit_per_sec())
        self._cb = _CircuitBreaker(_cb_enabled(), _cb_fail_threshold(), _cb_cooldown_sec())

        logger.info(
            "Argaam client init v%s | quote_url_set=%s | profile_url_set=%s | hist_url_set=%s | timeout=%.1fs | retries=%s | cachetools=%s | max_conc=%s | rate/s=%.2f | cb=%s",
            PROVIDER_VERSION,
            bool(self.quote_url),
            bool(self.profile_url),
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

    async def _get_json(self, url: str, *, kind: str) -> Tuple[Optional[Union[dict, list]], Optional[str]]:
        # circuit breaker
        if not await self._cb.allow():
            return None, "circuit_open"

        retries = max(1, int(self.retry_attempts))
        base_delay = _retry_delay_sec()
        req_headers = _endpoint_headers(kind)

        last_err: Optional[str] = None

        # concurrency + rate limit guard
        async with self._sem:
            await self._bucket.acquire()

            for attempt in range(retries):
                try:
                    r = await self._client.get(url, headers=req_headers if req_headers else None)
                    sc = int(r.status_code)

                    if sc == 429 or 500 <= sc < 600:
                        last_err = f"HTTP {sc}"
                        if attempt < retries - 1:
                            await asyncio.sleep(base_delay * (2**attempt) + random.random() * 0.25)
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
                        await asyncio.sleep(base_delay * (2**attempt) + random.random() * 0.25)
                        continue
                    await self._cb.on_failure()
                    return None, last_err

        return None, last_err or "request failed"


# ------------------------------------------------------------
# Mapping (best-effort)
# ------------------------------------------------------------
def _map_payload(root: Any) -> Dict[str, Any]:
    """
    Best-effort mapping for common JSON payload shapes.
    Returns ONLY useful fields.
    """
    patch: Dict[str, Any] = {}

    # Quote-ish
    patch["current_price"] = _pick_num(
        root,
        "last",
        "last_price",
        "price",
        "close",
        "c",
        "LastPrice",
        "tradingPrice",
        "regularMarketPrice",
    )
    patch["previous_close"] = _pick_num(root, "previous_close", "prev_close", "pc", "PreviousClose", "prevClose")
    patch["open"] = _pick_num(root, "open", "o", "Open", "openPrice")
    patch["day_high"] = _pick_num(root, "high", "day_high", "h", "High", "dayHigh", "sessionHigh")
    patch["day_low"] = _pick_num(root, "low", "day_low", "l", "Low", "dayLow", "sessionLow")
    patch["volume"] = _pick_num(root, "volume", "v", "Volume", "tradedVolume", "qty", "quantity")

    # Optional extras
    patch["week_52_high"] = _pick_num(root, "week_52_high", "fiftyTwoWeekHigh", "52w_high", "yearHigh")
    patch["week_52_low"] = _pick_num(root, "week_52_low", "fiftyTwoWeekLow", "52w_low", "yearLow")
    patch["value_traded"] = _pick_num(root, "value_traded", "tradedValue", "turnover", "value", "tradeValue")

    patch["price_change"] = _pick_num(root, "change", "d", "price_change", "Change", "diff", "delta")
    patch["percent_change"] = _pick_pct(
        root,
        "change_pct",
        "change_percent",
        "dp",
        "percent_change",
        "ChangePercent",
        "pctChange",
        "changePercent",
    )

    # Identity-ish
    name = _pick_str(
        root,
        "name",
        "company",
        "company_name",
        "CompanyName",
        "shortName",
        "longName",
        "securityName",
        "issuerName",
    )
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

    # Currency hint (KSA)
    patch.setdefault("currency", "SAR")

    # Derive change / change% if missing but price + prev_close exist
    cp = _safe_float(patch.get("current_price"))
    pc = _safe_float(patch.get("previous_close"))
    if cp is not None and pc is not None and pc != 0:
        if _safe_float(patch.get("price_change")) is None:
            patch["price_change"] = float(cp - pc)
        if _safe_float(patch.get("percent_change")) is None:
            patch["percent_change"] = float(((cp / pc) - 1.0) * 100.0)

    return _clean_patch(patch)


def _identity_only(mapped: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k in ("name", "sector", "industry", "sub_sector"):
        v = mapped.get(k)
        if isinstance(v, str) and v.strip():
            out[k] = v
    return out


def _with_provenance(patch: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(patch or {})
    out.setdefault("provider", PROVIDER_NAME)
    out.setdefault("data_source", PROVIDER_NAME)
    return out


# ------------------------------------------------------------
# Lazy singleton
# ------------------------------------------------------------
_CLIENT_SINGLETON: Optional[ArgaamClient] = None
_CLIENT_LOCK = asyncio.Lock()


async def get_argaam_client() -> ArgaamClient:
    global _CLIENT_SINGLETON
    if _CLIENT_SINGLETON is None:
        async with _CLIENT_LOCK:
            if _CLIENT_SINGLETON is None:
                _CLIENT_SINGLETON = ArgaamClient()
    return _CLIENT_SINGLETON


async def aclose_argaam_client() -> None:
    global _CLIENT_SINGLETON
    c = _CLIENT_SINGLETON
    _CLIENT_SINGLETON = None
    if c is not None:
        await c.aclose()


# ------------------------------------------------------------
# Fetchers (DICT PATCH RETURNS)
# ------------------------------------------------------------
async def _fetch_quote_patch(symbol: str) -> Dict[str, Any]:
    if not _configured():
        return {}

    sym = _normalize_ksa_symbol(symbol)
    if not sym:
        return {}  # strict KSA-only, silent

    c = await get_argaam_client()

    ck = f"quote::{sym}"
    hit = c._quote_cache.get(ck)
    if isinstance(hit, dict) and hit:
        return dict(hit)

    quote_tpl = c.quote_url
    profile_tpl = c.profile_url

    warns: List[str] = []

    # 1) Quote endpoint
    if quote_tpl:
        url = _format_url(quote_tpl, sym)
        js, err = await c._get_json(url, kind="quote")
        if js is None:
            if _emit_warnings():
                return {"_warn": f"argaam quote failed: {err}"}
            return {}

        root = _coerce_dict(js)
        mapped = _map_payload(root)

        if _safe_float(mapped.get("current_price")) is None:
            if _emit_warnings():
                return {"_warn": "argaam quote returned no price"}
            return {}

        patch = _with_provenance(dict(mapped))
        patch["requested_symbol"] = symbol
        patch["normalized_symbol"] = sym
        patch = _clean_patch(patch)

        c._quote_cache[ck] = dict(patch)
        return patch

    # 2) Fallback: profile endpoint used as quote
    if profile_tpl:
        url = _format_url(profile_tpl, sym)
        js, err = await c._get_json(url, kind="profile")
        if js is None:
            if _emit_warnings():
                return {"_warn": f"argaam profile-as-quote failed: {err}"}
            return {}

        root = _coerce_dict(js)
        mapped = _map_payload(root)

        if _safe_float(mapped.get("current_price")) is None:
            if _emit_warnings():
                return {"_warn": "argaam profile-as-quote returned no price"}
            return {}

        patch = _with_provenance(dict(mapped))
        patch["requested_symbol"] = symbol
        patch["normalized_symbol"] = sym
        if _emit_warnings():
            warns.append("argaam: used profile endpoint as quote fallback")
            patch["_warn"] = " | ".join(warns)

        patch = _clean_patch(patch)
        c._quote_cache[ck] = dict(patch)
        return patch

    return {}


async def _fetch_profile_identity_patch(symbol: str) -> Dict[str, Any]:
    if not _configured():
        return {}

    sym = _normalize_ksa_symbol(symbol)
    if not sym:
        return {}

    c = await get_argaam_client()
    if not c.profile_url:
        return {}

    ck = f"profile::{sym}"
    hit = c._profile_cache.get(ck)
    if isinstance(hit, dict) and hit:
        return dict(hit)

    url = _format_url(c.profile_url, sym)
    js, err = await c._get_json(url, kind="profile")
    if js is None:
        if _emit_warnings():
            return {"_warn": f"argaam profile failed: {err}"}
        return {}

    root = _coerce_dict(js)
    mapped = _map_payload(root)
    identity = _identity_only(mapped)

    if not identity:
        if _emit_warnings():
            return {"_warn": "argaam profile had no identity fields"}
        return {}

    identity = _with_provenance(identity)
    identity["requested_symbol"] = symbol
    identity["normalized_symbol"] = sym
    identity = _clean_patch(identity)

    c._profile_cache[ck] = dict(identity)
    return identity


async def _fetch_history_patch(symbol: str) -> Dict[str, Any]:
    if not _configured():
        return {}

    if not _history_enabled():
        return {}

    sym = _normalize_ksa_symbol(symbol)
    if not sym:
        return {}

    c = await get_argaam_client()
    if not c.history_url:
        return {}

    days = _history_days()
    ck = f"hist::{sym}::{days}"
    hit = c._hist_cache.get(ck)
    if isinstance(hit, dict) and hit:
        return dict(hit)

    url = _format_url(c.history_url, sym, days=days)
    js, err = await c._get_json(url, kind="history")
    if js is None:
        if _emit_warnings():
            return {"_warn": f"argaam history failed: {err}"}
        return {}

    closes, last_dt, source_hint = _extract_close_series(js, max_points=_history_points_max())
    if not closes:
        if _emit_warnings():
            return {"_warn": "argaam history parsed but no close series found"}
        return {}

    analytics = _compute_history_analytics(closes)

    patch: Dict[str, Any] = {
        "requested_symbol": symbol,
        "normalized_symbol": sym,
        "history_points": len(closes),
        "history_last_utc": _utc_iso(last_dt) if last_dt else _utc_iso(),
        "history_source": source_hint or "unknown",
        "forecast_source": "argaam_history",
    }
    patch.update(analytics)

    patch = _with_provenance(patch)
    patch = _add_forecast_aliases(patch, history_last_dt=last_dt)
    patch = _clean_patch(patch)

    c._hist_cache[ck] = dict(patch)
    return patch


# ------------------------------------------------------------
# Engine-compatible exported callables (DICT RETURNS)
# ------------------------------------------------------------
async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _fetch_quote_patch(symbol)


async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """
    Quote first; if quote exists, merge profile identity into it (fill blanks only),
    then merge optional history analytics (does not overwrite quote fields).
    Silent {} when not configured.
    """
    if not _configured():
        return {}

    # Run fetches concurrently for performance
    q_patch, p_patch, h_patch = await asyncio.gather(
        _fetch_quote_patch(symbol),
        _fetch_profile_identity_patch(symbol),
        _fetch_history_patch(symbol),
    )

    out: Dict[str, Any] = {}
    warns: List[str] = []

    if q_patch:
        out.update({k: v for k, v in q_patch.items() if k != "_warn"})

        # fill identity gaps only
        if p_patch:
            for k, v in p_patch.items():
                if k == "_warn":
                    continue
                if (k not in out or out.get(k) in (None, "")) and v is not None:
                    out[k] = v

        # merge history analytics only if missing
        if h_patch:
            for k, v in h_patch.items():
                if k == "_warn":
                    continue
                if k not in out and v is not None:
                    out[k] = v
    else:
        # no quote -> still provide identity/history if available
        for src in (p_patch, h_patch):
            if src:
                for k, v in src.items():
                    if k != "_warn" and v is not None:
                        out[k] = v

    # Ensure provenance + forecast aliases on final output
    out = _with_provenance(out)
    out = _add_forecast_aliases(out)

    # Collect warnings if enabled
    if _emit_warnings():
        for src in (q_patch, p_patch, h_patch):
            w = src.get("_warn") if isinstance(src, dict) else None
            if isinstance(w, str) and w.strip():
                warns.append(w.strip())
        if warns:
            out["_warn"] = " | ".join(warns)

    return _clean_patch(out)


async def fetch_quote_and_enrichment_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await fetch_enriched_quote_patch(symbol)


# Extra aliases for compatibility with engines that probe these names
async def fetch_enriched_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await fetch_enriched_quote_patch(symbol)


async def fetch_quote_and_fundamentals_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    # Argaam doesn't provide true fundamentals here; alias to enriched quote patch
    return await fetch_enriched_quote_patch(symbol)


__all__ = [
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    "get_argaam_client",
    "fetch_quote_patch",
    "fetch_enriched_quote_patch",
    "fetch_quote_and_enrichment_patch",
    "fetch_enriched_patch",
    "fetch_quote_and_fundamentals_patch",
    "aclose_argaam_client",
]
