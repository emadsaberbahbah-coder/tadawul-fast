# core/providers/argaam_provider.py
"""
core/providers/argaam_provider.py
===============================================================
Argaam Provider (KSA optional enrichment + optional history/forecast) — v1.7.0
PROD SAFE + ASYNC + ENGINE PATCH-STYLE + SILENT WHEN NOT CONFIGURED

What’s improved in v1.7.0
- ✅ Customized headers (global + per-endpoint overrides):
    • ARGAAM_HEADERS_JSON
    • ARGAAM_HEADERS_QUOTE_JSON / ARGAAM_HEADERS_PROFILE_JSON / ARGAAM_HEADERS_HISTORY_JSON
- ✅ Optional History + Forecast analytics (if ARGAAM_HISTORY_URL is provided):
    • returns_1w/1m/3m/6m/12m
    • ma20/ma50/ma200
    • volatility_30d (annualized %, last ~30 trading days)
    • rsi_14
    • expected_return_1m/3m/12m + expected_price_1m/3m/12m (simple momentum estimate)
    • confidence_score + forecast_method
- ✅ Better symbol normalization:
    Accepts "1234", "1234.SR", "TADAWUL:1234", "TADAWUL:1234.SR", "1234.TADAWUL"
- ✅ Profile fallback for quote:
    If ARGAAM_QUOTE_URL is missing but ARGAAM_PROFILE_URL exists, it tries profile for quote-like fields.
- ✅ Still SILENT when not configured:
    If ARGAAM_ENABLED=false OR (ARGAAM_QUOTE_URL/ARGAAM_PROFILE_URL/ARGAAM_HISTORY_URL all missing),
    returns {} with no warnings (prevents noise in DataEngineV2).

Key guarantees
- Import-safe even if cachetools is missing (tiny TTL cache fallback).
- Strict KSA-only: non-KSA symbols are silently ignored.
- No network calls at import-time; AsyncClient is created lazily.
- Patch is CLEAN (only useful fields + optional _warn if enabled).
- Enriched call merges profile identity into quote patch (fills blanks only).

Supported env vars (optional)
Core enablement
- ARGAAM_ENABLED                  default true (set false to disable silently)
- ARGAAM_VERBOSE_WARNINGS         default false (if true, returns _warn strings)

Endpoints
- ARGAAM_QUOTE_URL                e.g. https://.../quote?symbol={symbol} or .../{code}
- ARGAAM_PROFILE_URL              e.g. https://.../profile?symbol={symbol}
- ARGAAM_HISTORY_URL              e.g. https://.../history?symbol={symbol}&days={days}
  (also accepts ARGAAM_CANDLES_URL as fallback for history)

Headers
- ARGAAM_HEADERS_JSON             JSON dict for base headers (optional)
- ARGAAM_HEADERS_QUOTE_JSON       JSON dict merged only for quote requests (optional)
- ARGAAM_HEADERS_PROFILE_JSON     JSON dict merged only for profile requests (optional)
- ARGAAM_HEADERS_HISTORY_JSON     JSON dict merged only for history requests (optional)

Timeout/Retry/TTL
- ARGAAM_TIMEOUT_SEC              fallback to HTTP_TIMEOUT_SEC / HTTP_TIMEOUT then 20
- ARGAAM_RETRY_ATTEMPTS           default 2 (min 1)
- ARGAAM_RETRY_DELAY_SEC          default 0.25
- ARGAAM_TTL_SEC                  quote TTL default 15 (min 5)
- ARGAAM_PROFILE_TTL_SEC          default 3600 (min 60)
- ARGAAM_HISTORY_TTL_SEC          default 1200 (min 60)

History/Forecast toggles
- ARGAAM_ENABLE_HISTORY           default true
- ARGAAM_ENABLE_FORECAST          default true
- ARGAAM_HISTORY_DAYS             default 400
- ARGAAM_HISTORY_POINTS_MAX       default 400 (cap stored points)

Exports (DICT RETURNS)
- fetch_quote_patch(symbol) -> Dict[str, Any]
- fetch_enriched_quote_patch(symbol) -> Dict[str, Any]
- fetch_quote_and_enrichment_patch(symbol) -> Dict[str, Any]      # alias
- fetch_enriched_patch(symbol) -> Dict[str, Any]                  # alias
- fetch_quote_and_fundamentals_patch(symbol) -> Dict[str, Any]    # alias (Argaam has no true fundamentals)
- aclose_argaam_client()
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
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import httpx

logger = logging.getLogger("core.providers.argaam_provider")

PROVIDER_NAME = "argaam"
PROVIDER_VERSION = "1.7.0"

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


# -----------------------------------------------------------------------------
# Env + safe helpers
# -----------------------------------------------------------------------------
def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


# -----------------------------------------------------------------------------
# Symbol helpers (KSA strict)
# -----------------------------------------------------------------------------
def _normalize_ksa_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        return ""

    # common wrappers
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()

    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", "").strip()

    # normalize ".SR"
    if s.endswith(".SR"):
        code = s[:-3].strip()
        return f"{code}.SR" if _KSA_CODE_RE.match(code) else ""

    # raw digits
    if _KSA_CODE_RE.match(s):
        return f"{s}.SR"

    return ""  # strict


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


# -----------------------------------------------------------------------------
# Tiny TTL cache fallback if cachetools missing (import-safe)
# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
# Parsing helpers
# -----------------------------------------------------------------------------
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


def _find_first_value(obj: Any, keys: Sequence[str], *, max_depth: int = 7, max_nodes: int = 3000) -> Any:
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


# -----------------------------------------------------------------------------
# History analytics (same style as Tadawul provider)
# -----------------------------------------------------------------------------
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
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _find_first_list_of_dicts(
    obj: Any, *, required_keys: Sequence[str], max_depth: int = 7, max_nodes: int = 5000
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


def _extract_close_series(history_json: Any, *, max_points: int) -> Tuple[List[float], Optional[str]]:
    try:
        items = _find_first_list_of_dicts(
            history_json,
            required_keys=("close", "c", "price", "last", "tradingPrice", "value"),
        )
        if not items:
            return [], None

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
            return [], None

        have_dt = any(dt is not None for dt, _ in rows)
        if have_dt:
            rows = sorted(rows, key=lambda x: (x[0] or datetime(1970, 1, 1, tzinfo=timezone.utc)))

        closes = [c for _, c in rows][-max_points:]
        last_iso = (last_dt.isoformat() if last_dt else None)
        return closes, last_iso
    except Exception:
        return [], None


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
            prior = closes[-(n + 1)]
            out[k] = _return_pct(last, prior)

    def ma(n: int) -> Optional[float]:
        if len(closes) < n:
            return None
        xs = closes[-n:]
        return sum(xs) / float(n)

    out["ma20"] = ma(20)
    out["ma50"] = ma(50)
    out["ma200"] = ma(200)

    if len(closes) >= 31:
        rets: List[float] = []
        window = closes[-31:]
        for i in range(1, len(window)):
            p0 = window[i - 1]
            p1 = window[i]
            if p0 != 0:
                rets.append((p1 / p0) - 1.0)
        sd = _stddev(rets)
        if sd is not None:
            out["volatility_30d"] = float(sd * math.sqrt(252.0) * 100.0)

    out["rsi_14"] = _compute_rsi_14(closes)

    # Forecast (momentum-style)
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

    if r1m_d is not None:
        out["expected_return_1m"] = float(r1m_d * 21.0 * 100.0)
        out["expected_price_1m"] = float(last * (1.0 + (out["expected_return_1m"] / 100.0)))
    if r3m_d is not None:
        out["expected_return_3m"] = float(r3m_d * 63.0 * 100.0)
        out["expected_price_3m"] = float(last * (1.0 + (out["expected_return_3m"] / 100.0)))
    if r12m_d is not None:
        out["expected_return_12m"] = float(r12m_d * 252.0 * 100.0)
        out["expected_price_12m"] = float(last * (1.0 + (out["expected_return_12m"] / 100.0)))

    pts = len(closes)
    base = max(0.0, min(100.0, (pts / 252.0) * 100.0))
    vol = _safe_float(out.get("volatility_30d"))
    if vol is None:
        conf = base * 0.75
    else:
        penalty = min(60.0, max(0.0, (vol / 100.0) * 35.0))
        conf = max(0.0, min(100.0, base - penalty))

    out["confidence_score"] = float(conf)
    out["forecast_method"] = "argaam_history_momentum_v1"

    return out


# -----------------------------------------------------------------------------
# Header builder
# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
# HTTP client (lazy singleton)
# -----------------------------------------------------------------------------
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

        logger.info(
            "Argaam client init v%s | quote_url_set=%s | profile_url_set=%s | hist_url_set=%s | timeout=%.1fs | retries=%s | cachetools=%s",
            PROVIDER_VERSION,
            bool(self.quote_url),
            bool(self.profile_url),
            bool(self.history_url),
            self.timeout_sec,
            self.retry_attempts,
            _HAS_CACHETOOLS,
        )

    async def aclose(self) -> None:
        try:
            await self._client.aclose()
        except Exception:
            pass

    async def _get_json(self, url: str, *, kind: str) -> Tuple[Optional[Union[dict, list]], Optional[str]]:
        retries = max(1, int(self.retry_attempts))
        base_delay = _retry_delay_sec()
        last_err: Optional[str] = None

        # Per-request header overrides (merged by httpx on request)
        req_headers = _endpoint_headers(kind)

        for attempt in range(retries):
            try:
                r = await self._client.get(url, headers=req_headers if req_headers else None)
                sc = int(r.status_code)

                if sc == 429 or 500 <= sc < 600:
                    last_err = f"HTTP {sc}"
                    if attempt < retries - 1:
                        await asyncio.sleep(base_delay * (2**attempt) + random.random() * 0.25)
                        continue
                    return None, last_err

                if sc >= 400:
                    return None, f"HTTP {sc}"

                try:
                    js = r.json()
                except Exception:
                    txt = (r.text or "").strip()
                    if txt.startswith("{") or txt.startswith("["):
                        try:
                            js = json.loads(txt)
                        except Exception:
                            return None, "invalid JSON"
                    else:
                        return None, "non-JSON response"

                if not isinstance(js, (dict, list)):
                    return None, "unexpected JSON type"

                js = _unwrap_common_envelopes(js)
                return js, None

            except Exception as e:
                last_err = f"{e.__class__.__name__}: {e}"
                if attempt < retries - 1:
                    await asyncio.sleep(base_delay * (2**attempt) + random.random() * 0.25)
                    continue
                return None, last_err

        return None, last_err or "request failed"


# -----------------------------------------------------------------------------
# Mapping (best-effort)
# -----------------------------------------------------------------------------
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

    # Currency hint
    if patch.get("currency") is None:
        patch["currency"] = "SAR"

    return _clean_patch(patch)


def _identity_only(mapped: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k in ("name", "sector", "industry", "sub_sector"):
        v = mapped.get(k)
        if isinstance(v, str) and v.strip():
            out[k] = v
    return out


# -----------------------------------------------------------------------------
# Lazy singleton
# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
# Fetchers (DICT PATCH RETURNS)
# -----------------------------------------------------------------------------
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
        patch = dict(mapped)
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
        patch = dict(mapped)
        if _emit_warnings():
            warns.append("argaam: used profile endpoint as quote fallback")
            patch["_warn"] = " | ".join(warns)
        c._quote_cache[ck] = dict(_clean_patch(patch))
        return _clean_patch(patch)

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

    closes, last_iso = _extract_close_series(js, max_points=_history_points_max())
    if not closes:
        if _emit_warnings():
            return {"_warn": "argaam history parsed but no close series found"}
        return {}

    analytics = _compute_history_analytics(closes)

    # Respect forecast toggle
    if not _forecast_enabled():
        for k in (
            "expected_return_1m",
            "expected_return_3m",
            "expected_return_12m",
            "expected_price_1m",
            "expected_price_3m",
            "expected_price_12m",
        ):
            analytics.pop(k, None)
        analytics["forecast_method"] = "history_only"

    patch: Dict[str, Any] = {
        "history_points": len(closes),
        "history_last_utc": last_iso or _utc_iso(),
        "forecast_source": "argaam_history",
    }
    patch.update(analytics)

    patch = _clean_patch(patch)
    c._hist_cache[ck] = dict(patch)
    return patch


# -----------------------------------------------------------------------------
# Engine-compatible exported callables (DICT RETURNS)
# -----------------------------------------------------------------------------
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

    q_patch = await _fetch_quote_patch(symbol)
    p_patch = await _fetch_profile_identity_patch(symbol)
    h_patch = await _fetch_history_patch(symbol)

    # If quote exists, enrich it with identity (fill blanks only)
    out: Dict[str, Any] = {}
    warns: List[str] = []

    if q_patch:
        # keep quote fields
        out.update({k: v for k, v in q_patch.items() if k != "_warn"})

        # fill identity gaps
        if p_patch:
            for k, v in p_patch.items():
                if k == "_warn":
                    continue
                if (k not in out or out.get(k) in (None, "")) and v is not None:
                    out[k] = v

        # merge history analytics (only if not already present)
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
    # Argaam doesn’t provide true fundamentals here; alias to enriched quote patch
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
