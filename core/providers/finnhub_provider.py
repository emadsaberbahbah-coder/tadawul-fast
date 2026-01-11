# core/providers/finnhub_provider.py
"""
core/providers/finnhub_provider.py
============================================================
Finnhub Provider (GLOBAL enrichment fallback) — v2.1.0
(PROD SAFE + ENGINE PATCH + CUSTOM HEADERS + HISTORY/FORECAST + CLIENT REUSE)

Role
- GLOBAL fallback/enrichment (quote + optional profile + optional history analytics)
- KSA symbols are explicitly rejected to prevent wrong routing.
- Yahoo-special symbols (^GSPC, GC=F, EURUSD=X, etc.) are rejected per your routing design.

What’s improved vs v2.0.0
- ✅ Uses shared canonical symbol normalizer when available (core.symbols.normalize)
    - Accepts canonical inputs like AAPL.US and converts to Finnhub-required "AAPL"
    - Keeps non-US exchange suffixes (e.g., VOD.L) as-is
- ✅ Adds stable provenance fields: provider="finnhub", data_source="finnhub", provider_version
- ✅ Adds Sheets-friendly forecast alias fields (keeps originals too):
    • forecast_price_1m/3m/12m  (alias of expected_price_*)
    • expected_roi_pct_1m/3m/12m (alias of expected_return_*)
    • forecast_confidence       (alias of confidence_score)
    • forecast_updated_utc      (ISO UTC derived from history_last_ts when available)
- ✅ Avoids treating numeric 0 as “empty” when merging patches
- ✅ Adds history_last_utc + forecast_source="finnhub_candle" when history is present

Env vars (supported)
Auth/Base
- FINNHUB_API_KEY (preferred)
- FINNHUB_API_TOKEN / FINNHUB_TOKEN (legacy)
- FINNHUB_BASE_URL (default: https://finnhub.io/api/v1)

Timeout/Retry
- FINNHUB_TIMEOUT_SEC (default: falls back to HTTP_TIMEOUT_SEC then 8.0)
- FINNHUB_RETRY_ATTEMPTS (default: 2, min 1)
- FINNHUB_RETRY_DELAY_SEC (default: 0.25)

Features
- FINNHUB_ENABLE_PROFILE (default: true)
- FINNHUB_ENABLE_HISTORY (default: true)
- FINNHUB_ENABLE_FORECAST (default: true)
- FINNHUB_VERBOSE_WARNINGS (default: false)  # adds _warn to patch

Caching
- FINNHUB_QUOTE_TTL_SEC   (default: 10, min 3)
- FINNHUB_PROFILE_TTL_SEC (default: 6 hours, min 120)
- FINNHUB_HISTORY_TTL_SEC (default: 20 min, min 60)

History window
- FINNHUB_HISTORY_DAYS (default: 400, min 60)
- FINNHUB_HISTORY_POINTS_MAX (default: 400, min 100)

Headers
- FINNHUB_UA (optional override)
- FINNHUB_HEADERS_JSON (base headers JSON dict)
- FINNHUB_HEADERS_QUOTE_JSON (quote override JSON dict)
- FINNHUB_HEADERS_PROFILE_JSON (profile override JSON dict)
- FINNHUB_HEADERS_HISTORY_JSON (history override JSON dict)

Exports
- fetch_quote_patch(symbol) -> Dict[str, Any]
- fetch_enriched_quote_patch(symbol) -> Dict[str, Any]
- fetch_quote_and_enrichment_patch(symbol) -> Dict[str, Any]
- aclose_finnhub_client()

Notes
- Forecast is a simple momentum-style estimate (non-investment advice).
- Returns {"error": "..."} for routing/fallback logic; never crashes on missing env.
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
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger("core.providers.finnhub_provider")

PROVIDER_VERSION = "2.1.0"
PROVIDER_NAME = "finnhub"

DEFAULT_BASE_URL = "https://finnhub.io/api/v1"
DEFAULT_TIMEOUT_SEC = 8.0
DEFAULT_RETRY_ATTEMPTS = 2

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

USER_AGENT_DEFAULT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Reuse one client
_CLIENT: Optional[httpx.AsyncClient] = None
_LOCK = asyncio.Lock()

# ---------------------------------------------------------------------------
# Optional shared normalizer (PROD SAFE)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Safe env parsing (never crash on bad env values)
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
    for k in ("FINNHUB_API_KEY", "FINNHUB_API_TOKEN", "FINNHUB_TOKEN"):
        v = (os.getenv(k) or "").strip()
        if v:
            return v
    return None


def _base_url() -> str:
    return _env_str("FINNHUB_BASE_URL", DEFAULT_BASE_URL).rstrip("/")


def _timeout_sec() -> float:
    t = _env_float("FINNHUB_TIMEOUT_SEC", _env_float("HTTP_TIMEOUT_SEC", DEFAULT_TIMEOUT_SEC))
    return t if t > 0 else DEFAULT_TIMEOUT_SEC


def _retry_attempts() -> int:
    r = _env_int("FINNHUB_RETRY_ATTEMPTS", _env_int("MAX_RETRIES", DEFAULT_RETRY_ATTEMPTS))
    return max(1, int(r))


def _retry_delay_sec() -> float:
    d = _env_float("FINNHUB_RETRY_DELAY_SEC", 0.25)
    return d if d > 0 else 0.25


def _enable_profile() -> bool:
    return _env_bool("FINNHUB_ENABLE_PROFILE", True)


def _enable_history() -> bool:
    return _env_bool("FINNHUB_ENABLE_HISTORY", True)


def _enable_forecast() -> bool:
    return _env_bool("FINNHUB_ENABLE_FORECAST", True)


def _verbose_warn() -> bool:
    return _env_bool("FINNHUB_VERBOSE_WARNINGS", False)


def _ua() -> str:
    return _env_str("FINNHUB_UA", USER_AGENT_DEFAULT)


def _quote_ttl_sec() -> float:
    ttl = _env_float("FINNHUB_QUOTE_TTL_SEC", 10.0)
    return max(3.0, ttl if ttl > 0 else 10.0)


def _profile_ttl_sec() -> float:
    ttl = _env_float("FINNHUB_PROFILE_TTL_SEC", 21600.0)  # 6 hours
    return max(120.0, ttl if ttl > 0 else 21600.0)


def _history_ttl_sec() -> float:
    ttl = _env_float("FINNHUB_HISTORY_TTL_SEC", 1200.0)  # 20 min
    return max(60.0, ttl if ttl > 0 else 1200.0)


def _history_days() -> int:
    d = _env_int("FINNHUB_HISTORY_DAYS", 400)
    return max(60, int(d))


def _history_points_max() -> int:
    n = _env_int("FINNHUB_HISTORY_POINTS_MAX", 400)
    return max(100, int(n))


# Separate caches (different TTLs)
_Q_CACHE: TTLCache = TTLCache(maxsize=8000, ttl=_quote_ttl_sec())
_P_CACHE: TTLCache = TTLCache(maxsize=4000, ttl=_profile_ttl_sec())
_H_CACHE: TTLCache = TTLCache(maxsize=2500, ttl=_history_ttl_sec())


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
    h.update(_json_headers("FINNHUB_HEADERS_JSON"))
    return h


def _endpoint_headers(kind: str) -> Dict[str, str]:
    key = {
        "quote": "FINNHUB_HEADERS_QUOTE_JSON",
        "profile": "FINNHUB_HEADERS_PROFILE_JSON",
        "history": "FINNHUB_HEADERS_HISTORY_JSON",
    }.get(kind, "")
    return _json_headers(key) if key else {}


async def _get_client() -> httpx.AsyncClient:
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT
    async with _LOCK:
        if _CLIENT is None:
            t = _timeout_sec()
            timeout = httpx.Timeout(t, connect=min(10.0, t))
            _CLIENT = httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=_base_headers())
            logger.info(
                "Finnhub client init v%s | base=%s | timeout=%.1fs | cachetools=%s",
                PROVIDER_VERSION,
                _base_url(),
                t,
                _HAS_CACHETOOLS,
            )
    return _CLIENT


async def aclose_finnhub_client() -> None:
    global _CLIENT
    c = _CLIENT
    _CLIENT = None
    if c is not None:
        try:
            await c.aclose()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Symbol rules / normalization
# ---------------------------------------------------------------------------
_KSA_RE = re.compile(r"^\d{3,6}(\.SR)?$", re.IGNORECASE)


def _looks_like_ksa(symbol: str) -> bool:
    s = (symbol or "").strip()
    if not s:
        return False
    if callable(_SHARED_LOOKS_KSA):
        try:
            return bool(_SHARED_LOOKS_KSA(s))
        except Exception:
            pass
    u = s.strip().upper()
    if u.endswith(".SR"):
        return True
    return bool(_KSA_RE.match(u))


def _is_yahoo_special(symbol: str) -> bool:
    u = (symbol or "").strip().upper()
    if not u:
        return False
    # Indices/FX/futures and other Yahoo "special" forms
    if "^" in u:
        return True
    if "=" in u:
        return True
    if "/" in u:
        return True
    if u.endswith("=X"):
        return True
    return False


def _strip_prefix_wrapper(s: str) -> str:
    """
    Accepts e.g. "NASDAQ:AAPL" -> "AAPL"
    Keep it conservative: only strip a single "WORD:" prefix.
    """
    u = (s or "").strip()
    if not u:
        return ""
    if ":" in u:
        left, right = u.split(":", 1)
        if left and right and left.replace("_", "").replace("-", "").isalnum():
            return right.strip()
    return u.strip()


def _to_finnhub_symbol(raw: str) -> str:
    """
    Finnhub typically expects:
      - US equities: "AAPL" (NOT "AAPL.US")
      - Other exchanges often: "VOD.L", "VOW3.DE", etc. (keep suffix)
    """
    s = _strip_prefix_wrapper(raw).strip()
    if not s:
        return ""

    # Prefer shared canonical normalize if available (accept AAPL -> AAPL.US)
    if callable(_SHARED_NORMALIZE):
        try:
            s = str(_SHARED_NORMALIZE(s) or s).strip()
        except Exception:
            s = s.strip()

    u = s.strip().upper()

    # Never allow KSA routed here
    if _looks_like_ksa(u):
        return ""

    # Strip only .US (canonical default exchange) for Finnhub
    if u.endswith(".US"):
        u = u[:-3].strip()

    return u


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


def _clean_patch(p: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (p or {}).items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        out[k] = v
    return out


def _fill_derived(out: Dict[str, Any]) -> None:
    cur = _to_float(out.get("current_price"))
    prev = _to_float(out.get("previous_close"))
    vol = _to_float(out.get("volume"))

    if out.get("price_change") is None and cur is not None and prev is not None:
        out["price_change"] = cur - prev

    if out.get("percent_change") is None and cur is not None and prev not in (None, 0.0):
        try:
            out["percent_change"] = (cur - prev) / prev * 100.0
        except Exception:
            pass

    if out.get("value_traded") is None and cur is not None and vol is not None:
        out["value_traded"] = cur * vol


def _quote_looks_empty(js: Dict[str, Any]) -> bool:
    # Finnhub quote keys: c,h,l,o,pc (and sometimes d,dp)
    for k in ("c", "h", "l", "o", "pc"):
        f = _to_float(js.get(k))
        if f not in (None, 0.0):
            return False
    return True


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
        out["forecast_method"] = "finnhub_history_momentum_v1"
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


def _add_forecast_aliases(p: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(p or {})

    # price aliases
    if "expected_price_1m" in out and "forecast_price_1m" not in out:
        out["forecast_price_1m"] = out.get("expected_price_1m")
    if "expected_price_3m" in out and "forecast_price_3m" not in out:
        out["forecast_price_3m"] = out.get("expected_price_3m")
    if "expected_price_12m" in out and "forecast_price_12m" not in out:
        out["forecast_price_12m"] = out.get("expected_price_12m")

    # ROI aliases
    if "expected_return_1m" in out and "expected_roi_pct_1m" not in out:
        out["expected_roi_pct_1m"] = out.get("expected_return_1m")
    if "expected_return_3m" in out and "expected_roi_pct_3m" not in out:
        out["expected_roi_pct_3m"] = out.get("expected_return_3m")
    if "expected_return_12m" in out and "expected_roi_pct_12m" not in out:
        out["expected_roi_pct_12m"] = out.get("expected_return_12m")

    # confidence alias
    if "confidence_score" in out and "forecast_confidence" not in out:
        out["forecast_confidence"] = out.get("confidence_score")

    # updated alias (prefer history_last_utc)
    if "history_last_utc" in out and "forecast_updated_utc" not in out:
        out["forecast_updated_utc"] = out.get("history_last_utc")

    return out


def _with_provenance(p: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(p or {})
    out.setdefault("provider", PROVIDER_NAME)
    out.setdefault("data_source", PROVIDER_NAME)
    out.setdefault("provider_version", PROVIDER_VERSION)
    return out


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
async def _get_json(path: str, params: Dict[str, Any], *, kind: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    tok = _token()
    if not tok:
        return None, "not configured (FINNHUB_API_KEY)"

    base = _base_url()
    url = f"{base}{path}"

    q = dict(params or {})
    q["token"] = tok

    retries = _retry_attempts()
    client = await _get_client()
    last_err: Optional[str] = None
    base_delay = _retry_delay_sec()
    hdrs = _endpoint_headers(kind)

    for attempt in range(retries):
        try:
            r = await client.get(url, params=q, headers=hdrs if hdrs else None)

            if r.status_code == 429 or 500 <= r.status_code < 600:
                last_err = f"HTTP {r.status_code}"
                if attempt < (retries - 1):
                    await asyncio.sleep(base_delay * (2**attempt) + random.random() * 0.25)
                    continue
                return None, last_err

            if r.status_code != 200:
                msg = ""
                try:
                    js2 = r.json()
                    if isinstance(js2, dict):
                        msg = str(js2.get("error") or js2.get("message") or "").strip()
                except Exception:
                    msg = ""
                return None, f"HTTP {r.status_code}" + (f": {msg}" if msg else "")

            try:
                js = r.json()
            except Exception:
                return None, "invalid JSON response"

            if not isinstance(js, dict):
                return None, "unexpected JSON type"

            return js, None

        except Exception as e:
            last_err = f"{e.__class__.__name__}: {e}"
            if attempt < (retries - 1):
                await asyncio.sleep(base_delay * (2**attempt) + random.random() * 0.25)
                continue
            return None, last_err

    return None, last_err or "request failed"


# ---------------------------------------------------------------------------
# Finnhub calls
# ---------------------------------------------------------------------------
async def _fetch_quote_patch(symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    js, err = await _get_json("/quote", {"symbol": symbol}, kind="quote")
    if js is None:
        return {}, err

    if _quote_looks_empty(js):
        return {}, "empty quote"

    patch: Dict[str, Any] = {
        "current_price": _to_float(js.get("c")),
        "previous_close": _to_float(js.get("pc")),
        "open": _to_float(js.get("o")),
        "day_high": _to_float(js.get("h")),
        "day_low": _to_float(js.get("l")),
    }

    d = _to_float(js.get("d"))
    dp = _to_float(js.get("dp"))
    if d is not None:
        patch["price_change"] = d
    if dp is not None:
        patch["percent_change"] = dp

    return _clean_patch(patch), None


async def _fetch_profile_patch(symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    js, err = await _get_json("/stock/profile2", {"symbol": symbol}, kind="profile")
    if js is None:
        return {}, err

    patch: Dict[str, Any] = {
        "name": (js.get("name") or "") or "",
        "currency": (js.get("currency") or "") or "",
        "industry": (js.get("finnhubIndustry") or "") or "",
        "sector": (js.get("gsector") or "") or "",
        "sub_sector": (js.get("gsubind") or "") or "",
        "listing_date": (js.get("ipo") or "") or "",
        "exchange": (js.get("exchange") or "") or "",
        "market_cap": _to_float(js.get("marketCapitalization")),
        "shares_outstanding": _to_float(js.get("shareOutstanding")),
        "country": (js.get("country") or "") or "",
        "weburl": (js.get("weburl") or "") or "",
        "logo": (js.get("logo") or "") or "",
    }
    return _clean_patch(patch), None


async def _fetch_history_patch(symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    if not _enable_history():
        return {}, None

    days = _history_days()
    now = datetime.now(timezone.utc)
    frm = now - timedelta(days=days)
    to_ts = int(now.timestamp())
    from_ts = int(frm.timestamp())

    js, err = await _get_json(
        "/stock/candle",
        {"symbol": symbol, "resolution": "D", "from": from_ts, "to": to_ts},
        kind="history",
    )
    if js is None:
        return {}, err

    if str(js.get("s") or "").lower() != "ok":
        return {}, f"history status not ok ({js.get('s')})"

    closes_raw = js.get("c")
    times_raw = js.get("t")

    if not isinstance(closes_raw, list) or not closes_raw:
        return {}, "history empty"

    closes: List[float] = []
    for x in closes_raw:
        f = _to_float(x)
        if f is not None:
            closes.append(float(f))

    closes = closes[-_history_points_max():]
    if len(closes) < 25:
        return {}, "history too short"

    analytics = _compute_history_analytics(closes)

    last_ts: Optional[int] = None
    last_iso: Optional[str] = None
    if isinstance(times_raw, list) and times_raw:
        try:
            last_ts = int(times_raw[-1])
            last_iso = datetime.fromtimestamp(last_ts, tz=timezone.utc).isoformat()
        except Exception:
            last_ts = None
            last_iso = None

    patch: Dict[str, Any] = {
        "history_points": len(closes),
        "history_last_ts": int(last_ts) if last_ts else None,
        "history_last_utc": last_iso,
        "forecast_source": "finnhub_candle",
    }
    patch.update(analytics)

    return _clean_patch(patch), None


# ---------------------------------------------------------------------------
# Main fetch
# ---------------------------------------------------------------------------
def _base_patch(symbol: str) -> Dict[str, Any]:
    # Keep compact (clean_patch will remove Nones/empty strings)
    return {
        "symbol": symbol,
        "market": "GLOBAL",
        "provider": PROVIDER_NAME,
        "data_source": PROVIDER_NAME,
        "provider_version": PROVIDER_VERSION,
        "error": "",
        # identity/profile
        "name": None,
        "exchange": None,
        "sector": None,
        "sub_sector": None,
        "industry": None,
        "listing_date": None,
        "currency": None,
        "country": None,
        "weburl": None,
        "logo": None,
        "market_cap": None,
        "shares_outstanding": None,
        # price
        "current_price": None,
        "previous_close": None,
        "open": None,
        "day_high": None,
        "day_low": None,
        "price_change": None,
        "percent_change": None,
        "volume": None,
        "value_traded": None,
        # history/forecast
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
        "history_last_utc": None,
        "forecast_source": None,
    }


async def _fetch(symbol_raw: str, *, want_profile: bool, want_history: bool) -> Dict[str, Any]:
    sym_in = (symbol_raw or "").strip()
    if not sym_in:
        return {"error": f"{PROVIDER_NAME}: empty symbol"}

    # Hard guard: avoid misrouting KSA to Finnhub
    if _looks_like_ksa(sym_in):
        return {"error": f"{PROVIDER_NAME}: KSA symbol not supported"}

    # Guard: avoid Yahoo-special symbols in this provider
    if _is_yahoo_special(sym_in):
        return {"error": f"{PROVIDER_NAME}: symbol format not supported"}

    if not _token():
        return {"error": f"{PROVIDER_NAME}: not configured (FINNHUB_API_KEY)"}

    sym = _to_finnhub_symbol(sym_in)
    if not sym:
        return {"error": f"{PROVIDER_NAME}: symbol not supported"}

    warns: List[str] = []
    out: Dict[str, Any] = _base_patch(sym)

    # Quote required
    ckq = f"q::{sym}"
    hitq = _Q_CACHE.get(ckq)
    if isinstance(hitq, dict) and hitq:
        q_patch = dict(hitq)
        q_err = None
    else:
        q_patch, q_err = await _fetch_quote_patch(sym)
        if q_patch:
            _Q_CACHE[ckq] = dict(q_patch)

    if not q_patch or _to_float(q_patch.get("current_price")) is None:
        return {"error": f"{PROVIDER_NAME}: quote failed ({q_err or 'empty'})"}

    for k, v in q_patch.items():
        if v is not None:
            out[k] = v
    _fill_derived(out)

    # Optional profile
    if want_profile and _enable_profile():
        ckp = f"p::{sym}"
        hitp = _P_CACHE.get(ckp)
        if isinstance(hitp, dict) and hitp:
            p_patch = dict(hitp)
            p_err = None
        else:
            p_patch, p_err = await _fetch_profile_patch(sym)
            if p_patch:
                _P_CACHE[ckp] = dict(p_patch)

        if p_err:
            warns.append(f"profile: {p_err}")
        else:
            for k, v in p_patch.items():
                if (out.get(k) is None or (isinstance(out.get(k), str) and not str(out.get(k)).strip())) and v not in (
                    None,
                    "",
                ):
                    out[k] = v

    # Optional history/forecast
    if want_history and _enable_history():
        ckh = f"h::{sym}::{_history_days()}::{_history_points_max()}"
        hith = _H_CACHE.get(ckh)
        if isinstance(hith, dict) and hith:
            h_patch = dict(hith)
            h_err = None
        else:
            h_patch, h_err = await _fetch_history_patch(sym)
            if h_patch:
                _H_CACHE[ckh] = dict(h_patch)

        if h_err:
            warns.append(f"history: {h_err}")
        else:
            for k, v in h_patch.items():
                if out.get(k) is None and v is not None:
                    out[k] = v

    out["last_updated_utc"] = _utc_iso()

    # Add aliases + provenance reinforcement
    out = _with_provenance(out)
    out = _add_forecast_aliases(out)

    if warns and _verbose_warn():
        out["_warn"] = " | ".join([w for w in warns if w])

    return _clean_patch(out)


# ---------------------------------------------------------------------------
# Engine discovery callables (return Dict patch, no tuples)
# ---------------------------------------------------------------------------
async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    # fast: quote only
    return await _fetch(symbol, want_profile=False, want_history=False)


async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    # enriched: quote + profile + history/forecast (best-effort)
    return await _fetch(symbol, want_profile=True, want_history=True)


async def fetch_quote_and_enrichment_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await fetch_enriched_quote_patch(symbol, *args, **kwargs)


@dataclass
class FinnhubProvider:
    name: str = PROVIDER_NAME

    async def fetch_quote(self, symbol: str, debug: bool = False) -> Dict[str, Any]:
        # keep signature compatible with other providers
        return await fetch_enriched_quote_patch(symbol)


__all__ = [
    "fetch_quote_patch",
    "fetch_enriched_quote_patch",
    "fetch_quote_and_enrichment_patch",
    "aclose_finnhub_client",
    "PROVIDER_VERSION",
    "PROVIDER_NAME",
    "FinnhubProvider",
]
