# core/providers/tadawul_provider.py
"""
core/providers/tadawul_provider.py
===============================================================
Tadawul Provider / Client (KSA quote + fundamentals + optional history) — v1.13.0
PROD SAFE + Async + ENGINE-ALIGNED (PATCH DICT RETURNS)

What this revision guarantees (v1.13.0)
- ✅ Engine-aligned exports (ALL return Dict[str, Any] — NO tuples):
    fetch_quote_patch
    fetch_fundamentals_patch
    fetch_enriched_quote_patch
    fetch_quote_and_enrichment_patch
    fetch_quote_and_fundamentals_patch
- ✅ Import-safe: no network at import-time, never crashes on missing env / cachetools
- ✅ Strict KSA-only guard:
    - Non-KSA symbols return {} (silent) to prevent misrouting/pollution
- ✅ Clean "not configured" behavior:
    - For KSA symbols: returns {"error": "warning: ..."} (no crash, allows engine fallback)
- ✅ Defensive parsing + bounded deep-find (safe for unknown JSON shapes)
- ✅ Retry/backoff on 429 + transient 5xx
- ✅ TTL cache for quote + fundamentals + history (env-configurable)
- ✅ OPTIONAL history + derived analytics:
    - returns_1w/1m/3m/6m/12m
    - ma20/ma50/ma200
    - volatility_30d (annualized %, last ~30 trading days)
    - rsi_14
    - expected_return_1m/3m/12m + expected_price_1m/3m/12m (simple momentum estimate)
    - confidence_score + forecast_method + history_points/source/last_utc
- ✅ Adds Sheets-friendly forecast alias fields (keeps originals too):
    • forecast_price_1m/3m/12m  (alias of expected_price_*)
    • expected_roi_pct_1m/3m/12m (alias of expected_return_*)
    • forecast_confidence       (alias of confidence_score)
    • forecast_updated_utc      (alias of history_last_utc)
- ✅ Fixes merge behavior: does NOT treat numeric 0 as “empty” when merging
- ✅ Adds stable provenance fields: provider="tadawul", data_source="tadawul", provider_version

Supported env vars (optional)
- TADAWUL_MARKET_ENABLED          true/false (default true)

Quote/Fundamentals endpoints (required if you want Tadawul data):
- TADAWUL_QUOTE_URL               template with {symbol} and/or {code}
- TADAWUL_FUNDAMENTALS_URL        template with {symbol} and/or {code}
  (also accepts TADAWUL_PROFILE_URL as fallback for fundamentals URL)

Optional history endpoint (for returns/MAs/forecast):
- TADAWUL_HISTORY_URL             template with {symbol}/{code} and optional {days}
  (also accepts TADAWUL_CANDLES_URL)

Optional headers and HTTP tuning:
- TADAWUL_HEADERS_JSON            JSON dict of headers (optional)
- TADAWUL_TIMEOUT_SEC             default falls back to HTTP_TIMEOUT/HTTP_TIMEOUT_SEC then 25
- TADAWUL_RETRY_ATTEMPTS          default 3 (min 1)
- TADAWUL_RETRY_DELAY_SEC         default 0.35 (supports RETRY_DELAY/RETRY_DELAY_SEC)

Cache tuning:
- TADAWUL_REFRESH_INTERVAL        used as quote cache TTL if > 0 (seconds)
- TADAWUL_QUOTE_TTL_SEC           explicit quote TTL override
- TADAWUL_FUND_TTL_SEC            fundamentals TTL (default 6 hours)
- TADAWUL_HISTORY_TTL_SEC         history TTL (default 20 minutes)
- TADAWUL_HISTORY_POINTS_MAX      default 400 (cap stored points for safety)

History/forecast feature toggles:
- TADAWUL_ENABLE_HISTORY          true/false (default true)
- TADAWUL_ENABLE_FORECAST         true/false (default true)

Notes
- This provider expects you to supply your own Tadawul endpoints via env URLs.
- Endpoint JSON schema can vary; parsing is best-effort via key search.
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
PROVIDER_VERSION = "1.13.0"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

DEFAULT_TIMEOUT_SEC = 25.0
DEFAULT_RETRY_ATTEMPTS = 3

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

# KSA symbol: 3-6 digits, optionally .SR
_KSA_CODE_RE = re.compile(r"^\d{3,6}$")
_KSA_SYMBOL_RE = re.compile(r"^\d{3,6}(\.SR)?$", re.IGNORECASE)

# ---------------------------------------------------------------------------
# Optional shared normalizer (PROD SAFE)
# ---------------------------------------------------------------------------


def _try_import_shared_ksa_helpers() -> Tuple[Optional[Any], Optional[Any]]:
    """
    Returns (normalize_ksa, looks_like_ksa) if available; else (None, None).
    Never raises.
    """
    try:
        from core.symbols.normalize import normalize_ksa_symbol as _nksa  # type: ignore
        from core.symbols.normalize import looks_like_ksa as _lk  # type: ignore

        return _nksa, _lk
    except Exception:
        return None, None


_SHARED_NORM_KSA, _SHARED_LOOKS_KSA = _try_import_shared_ksa_helpers()


# ---------------------------------------------------------------------------
# Env + safe helpers
# ---------------------------------------------------------------------------
def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


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
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default


def _timeout_sec() -> float:
    t = _env_float("TADAWUL_TIMEOUT_SEC", 0.0)
    if t > 0:
        return t
    t = _env_float("HTTP_TIMEOUT", 0.0)
    if t > 0:
        return t
    t = _env_float("HTTP_TIMEOUT_SEC", 0.0)
    if t > 0:
        return t
    return DEFAULT_TIMEOUT_SEC


def _retry_attempts() -> int:
    r = _env_int("TADAWUL_RETRY_ATTEMPTS", 0)
    if r > 0:
        return r
    r = _env_int("HTTP_RETRY_ATTEMPTS", 0)
    if r > 0:
        return r
    r = _env_int("MAX_RETRIES", 0)
    if r > 0:
        return r
    return DEFAULT_RETRY_ATTEMPTS


def _retry_delay_sec() -> float:
    d = _env_float("TADAWUL_RETRY_DELAY_SEC", _env_float("RETRY_DELAY", _env_float("RETRY_DELAY_SEC", 0.35)))
    return d if d > 0 else 0.35


def _quote_ttl_sec() -> float:
    ttl = _env_float("TADAWUL_QUOTE_TTL_SEC", 0.0)
    if ttl > 0:
        return max(5.0, ttl)
    ri = _env_float("TADAWUL_REFRESH_INTERVAL", 0.0)
    if ri > 0:
        return max(5.0, ri)
    return 15.0


def _fund_ttl_sec() -> float:
    ttl = _env_float("TADAWUL_FUND_TTL_SEC", 0.0)
    if ttl > 0:
        return max(120.0, ttl)
    return 21600.0  # 6 hours


def _history_ttl_sec() -> float:
    ttl = _env_float("TADAWUL_HISTORY_TTL_SEC", 0.0)
    if ttl > 0:
        return max(60.0, ttl)
    return 1200.0  # 20 minutes


def _history_points_max() -> int:
    n = _env_int("TADAWUL_HISTORY_POINTS_MAX", 0)
    return max(100, n) if n > 0 else 400


def _enabled() -> bool:
    return _env_bool("TADAWUL_MARKET_ENABLED", True)


def _history_enabled() -> bool:
    return _env_bool("TADAWUL_ENABLE_HISTORY", True)


def _forecast_enabled() -> bool:
    return _env_bool("TADAWUL_ENABLE_FORECAST", True)


def _origin_key() -> str:
    return _env_str("TADAWUL_ORIGIN_KEY", "KSA_TADAWUL").strip() or "KSA_TADAWUL"


# ---------------------------------------------------------------------------
# Symbol helpers (STRICT KSA)
# ---------------------------------------------------------------------------
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
    return bool(_KSA_SYMBOL_RE.match(u) or _KSA_CODE_RE.match(u))


def _ksa_code(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    if s.endswith(".SR"):
        s = s[:-3]
    if "." in s:
        s = s.split(".", 1)[0]
    return s


def _normalize_ksa_symbol(symbol: str) -> str:
    """
    STRICT:
    - "1234"   -> "1234.SR"
    - "1234.SR"-> "1234.SR"
    - everything else -> ""
    """
    s = (symbol or "").strip()
    if not s:
        return ""

    if callable(_SHARED_NORM_KSA):
        try:
            out = str(_SHARED_NORM_KSA(s) or "").strip().upper()
            if out and out.endswith(".SR") and _KSA_CODE_RE.match(out[:-3]):
                return out
            # fall through if shared normalizer returns something unexpected
        except Exception:
            pass

    u = s.strip().upper()
    if u.endswith(".SR"):
        code = u[:-3]
        return f"{code}.SR" if _KSA_CODE_RE.match(code) else ""
    if _KSA_CODE_RE.match(u):
        return f"{u}.SR"
    return ""


def _format_url(template: str, symbol: str, *, days: Optional[int] = None) -> str:
    code = _ksa_code(symbol)
    u = (template or "").replace("{code}", code).replace("{symbol}", symbol)
    if days is not None:
        u = u.replace("{days}", str(int(days)))
    return u


# ---------------------------------------------------------------------------
# Robust numeric parsing
# ---------------------------------------------------------------------------
def _safe_float(val: Any) -> Optional[float]:
    """
    Robust numeric parser:
    - supports Arabic digits
    - strips %, +, commas
    - supports suffix K/M/B
    - supports (1.23) negatives
    """
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
        s = s.replace("SAR", "").replace("ريال", "").replace("USD", "").strip()
        s = s.replace("+", "").strip()

        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()

        s = s.replace("%", "").strip()
        s = s.replace(",", "")

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
    # If input is a fraction (0.0123) convert to %
    if abs(v) <= 1.0:
        return v * 100.0
    return v


def _clean_patch(p: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (p or {}).items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        out[k] = v
    return out


def _pos_52w(cur: Optional[float], lo: Optional[float], hi: Optional[float]) -> Optional[float]:
    if cur is None or lo is None or hi is None:
        return None
    if hi == lo:
        return None
    try:
        return (cur - lo) / (hi - lo) * 100.0
    except Exception:
        return None


def _fill_derived(p: Dict[str, Any]) -> None:
    cur = _safe_float(p.get("current_price"))
    prev = _safe_float(p.get("previous_close"))
    vol = _safe_float(p.get("volume"))

    if p.get("price_change") is None and cur is not None and prev is not None:
        p["price_change"] = cur - prev

    if p.get("percent_change") is None and cur is not None and prev not in (None, 0.0):
        p["percent_change"] = (cur - prev) / prev * 100.0

    if p.get("value_traded") is None and cur is not None and vol is not None:
        p["value_traded"] = cur * vol

    if p.get("position_52w_percent") is None:
        p["position_52w_percent"] = _pos_52w(
            cur,
            _safe_float(p.get("week_52_low")),
            _safe_float(p.get("week_52_high")),
        )

    mc = _safe_float(p.get("market_cap"))
    ff = _safe_float(p.get("free_float"))
    if p.get("free_float_market_cap") is None and mc is not None and ff is not None:
        p["free_float_market_cap"] = mc * (ff / 100.0)


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

    # updated alias
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
# Bounded deep-find (safe for unknown JSON shapes)
# ---------------------------------------------------------------------------
def _coerce_dict(data: Any) -> Dict[str, Any]:
    if isinstance(data, dict):
        return data
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return data[0]
    return {}


def _find_first_value(obj: Any, keys: Sequence[str], *, max_depth: int = 6, max_nodes: int = 2500) -> Any:
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


def _pick_num(obj: Any, *keys: str, max_depth: int = 6) -> Optional[float]:
    return _safe_float(_find_first_value(obj, keys, max_depth=max_depth))


def _pick_pct(obj: Any, *keys: str, max_depth: int = 6) -> Optional[float]:
    return _pct_if_fraction(_find_first_value(obj, keys, max_depth=max_depth))


def _pick_str(obj: Any, *keys: str, max_depth: int = 6) -> Optional[str]:
    return _safe_str(_find_first_value(obj, keys, max_depth=max_depth))


# ---------------------------------------------------------------------------
# TTL cache fallback (import-safe if cachetools missing)
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
# History extraction + analytics (defensive)
# ---------------------------------------------------------------------------
def _safe_dt(x: Any) -> Optional[datetime]:
    """
    Best-effort timestamp parser:
    - epoch seconds/ms
    - ISO strings
    """
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
    obj: Any,
    *,
    required_keys: Sequence[str],
    max_depth: int = 7,
    max_nodes: int = 5000,
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
    """
    Returns (closes_sorted, last_dt_iso_or_none)
    """
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
            out[k] = _return_pct(last, closes[-(n + 1)])

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
    out["forecast_method"] = "tadawul_history_momentum_v1"
    return out


# ---------------------------------------------------------------------------
# Tadawul client
# ---------------------------------------------------------------------------
class TadawulClient:
    """
    Returns PATCH dictionaries for engine merges.

    Success patches include (best-effort):
      current_price, previous_close, open, day_high, day_low, volume, value_traded,
      price_change, percent_change,
      name/sector/industry/sub_sector/listing_date,
      market_cap/shares_outstanding/free_float/...,
      week_52_high/week_52_low, ma20/ma50/ma200, avg_volume_30d,
      plus optional history-derived analytics (returns/MAs/volatility/RSI/forecast).
    """

    def __init__(
        self,
        quote_url: Optional[str] = None,
        fundamentals_url: Optional[str] = None,
        history_url: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout_sec: Optional[float] = None,
        retry_attempts: Optional[int] = None,
    ) -> None:
        self.quote_url = _safe_str(quote_url or _env_str("TADAWUL_QUOTE_URL", ""))
        self.fundamentals_url = _safe_str(
            fundamentals_url
            or _env_str("TADAWUL_FUNDAMENTALS_URL", "")
            or _env_str("TADAWUL_PROFILE_URL", "")
        )
        self.history_url = _safe_str(
            history_url
            or _env_str("TADAWUL_HISTORY_URL", "")
            or _env_str("TADAWUL_CANDLES_URL", "")
        )

        self.timeout_sec = float(timeout_sec) if (timeout_sec and timeout_sec > 0) else _timeout_sec()
        self.retry_attempts = int(retry_attempts) if (retry_attempts and retry_attempts > 0) else _retry_attempts()

        timeout = httpx.Timeout(self.timeout_sec, connect=min(10.0, self.timeout_sec))

        base_headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/json,text/plain,text/html;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.8,ar;q=0.6",
        }

        raw = _safe_str(_env_str("TADAWUL_HEADERS_JSON", ""))
        if raw:
            try:
                obj = json.loads(raw)
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        base_headers[str(k)] = str(v)
            except Exception:
                pass

        if headers:
            for k, v in headers.items():
                base_headers[str(k)] = str(v)

        self._client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers=base_headers,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=40),
        )

        self._quote_cache: TTLCache = TTLCache(maxsize=5000, ttl=_quote_ttl_sec())
        self._fund_cache: TTLCache = TTLCache(maxsize=3000, ttl=_fund_ttl_sec())
        self._hist_cache: TTLCache = TTLCache(maxsize=2000, ttl=_history_ttl_sec())

        logger.info(
            "Tadawul client init v%s | quote_url_set=%s | fund_url_set=%s | hist_url_set=%s | timeout=%.1fs | retries=%s | cachetools=%s",
            PROVIDER_VERSION,
            bool(self.quote_url),
            bool(self.fundamentals_url),
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

    async def _get_json(self, url: str) -> Tuple[Optional[Union[Dict[str, Any], List[Any]]], Optional[str]]:
        retries = max(1, int(self.retry_attempts))
        base_delay = _retry_delay_sec()

        last_err: Optional[str] = None

        for attempt in range(retries):
            try:
                r = await self._client.get(url)
                sc = int(r.status_code)

                if sc == 429 or 500 <= sc < 600:
                    last_err = f"HTTP {sc}"
                    if attempt < (retries - 1):
                        await asyncio.sleep(base_delay * (2**attempt) + random.random() * 0.35)
                        continue
                    return None, last_err

                if sc >= 400:
                    return None, f"HTTP {sc}"

                try:
                    js = r.json()
                    return js, None
                except Exception:
                    txt = (r.text or "").strip()
                    if txt.startswith("{") or txt.startswith("["):
                        try:
                            return json.loads(txt), None
                        except Exception:
                            return None, "invalid JSON"
                    return None, "non-JSON response"

            except Exception as e:
                last_err = f"{e.__class__.__name__}: {e}"
                if attempt < (retries - 1):
                    await asyncio.sleep(base_delay * (2**attempt) + random.random() * 0.35)
                    continue
                return None, last_err

        return None, last_err or "unknown error"

    # -----------------------------------------------------------------------
    # Quote
    # -----------------------------------------------------------------------
    async def fetch_quote_patch(self, symbol: str) -> Dict[str, Any]:
        sym = _normalize_ksa_symbol(symbol)
        if not sym:
            return {}

        if not _enabled():
            return {"error": "warning: tadawul disabled (TADAWUL_MARKET_ENABLED=false)"}

        if not self.quote_url:
            return {"error": "warning: tadawul not configured (TADAWUL_QUOTE_URL)"}

        ck = f"quote::{sym}"
        hit = self._quote_cache.get(ck)
        if isinstance(hit, dict) and hit:
            return dict(hit)

        url = _format_url(self.quote_url, sym)
        t0 = time.perf_counter()
        data, err = await self._get_json(url)
        dt_ms = int((time.perf_counter() - t0) * 1000)

        if not data:
            return {"error": f"warning: tadawul quote empty ({err or 'no data'}) ({dt_ms}ms)"}

        root = _coerce_dict(data)

        price = _pick_num(
            root,
            "price",
            "last",
            "last_price",
            "lastPrice",
            "tradingPrice",
            "close",
            "currentPrice",
            "c",
            "trading_price",
            "lastTradePrice",
        )
        prev = _pick_num(root, "previous_close", "previousClose", "prevClose", "prev_close", "pc", "previousPrice")
        opn = _pick_num(root, "open", "openPrice", "o")
        hi = _pick_num(root, "high", "day_high", "dayHigh", "h", "sessionHigh")
        lo = _pick_num(root, "low", "day_low", "dayLow", "l", "sessionLow")
        vol = _pick_num(root, "volume", "tradedVolume", "qty", "quantity", "volumeTraded", "v", "tradedQty")
        val_traded = _pick_num(root, "valueTraded", "tradingValue", "turnoverValue", "tradedValue", "value")

        chg = _pick_num(root, "change", "price_change", "diff", "delta", "d")
        chg_p = _pick_pct(root, "change_percent", "percent_change", "changePercent", "pctChange", "dp")

        name = _pick_str(root, "name", "company", "companyName", "company_name", "securityName", "issuerName")
        currency = _pick_str(root, "currency") or "SAR"

        if price is None:
            return {"error": f"warning: tadawul quote missing price ({dt_ms}ms)"}

        patch: Dict[str, Any] = {
            "symbol": sym,
            "market": "KSA",
            "origin": _origin_key(),
            "currency": currency,

            "name": name or "",
            "current_price": price,
            "previous_close": prev,
            "open": opn,
            "day_high": hi,
            "day_low": lo,
            "volume": vol,
            "value_traded": val_traded,
            "price_change": chg,
            "percent_change": chg_p,

            "provider": PROVIDER_NAME,
            "data_source": PROVIDER_NAME,
            "provider_version": PROVIDER_VERSION,
            "last_updated_utc": _utc_iso(),
        }

        _fill_derived(patch)
        patch = _clean_patch(_add_forecast_aliases(_with_provenance(patch)))

        self._quote_cache[ck] = dict(patch)
        return patch

    # -----------------------------------------------------------------------
    # Fundamentals
    # -----------------------------------------------------------------------
    async def fetch_fundamentals_patch(self, symbol: str) -> Dict[str, Any]:
        sym = _normalize_ksa_symbol(symbol)
        if not sym:
            return {}

        if not _enabled():
            return {"error": "warning: tadawul disabled (TADAWUL_MARKET_ENABLED=false)"}

        if not self.fundamentals_url:
            return {"error": "warning: tadawul not configured (TADAWUL_FUNDAMENTALS_URL)"}

        ck = f"fund::{sym}"
        hit = self._fund_cache.get(ck)
        if isinstance(hit, dict) and hit:
            return dict(hit)

        url = _format_url(self.fundamentals_url, sym)
        data, err = await self._get_json(url)
        if not data:
            return {"error": f"warning: tadawul fundamentals empty ({err or 'no data'})"}

        root = _coerce_dict(data)

        # Identity
        name = _pick_str(
            root,
            "name",
            "companyName",
            "company_name",
            "securityName",
            "issuerName",
            "shortName",
            "longName",
        )
        sector = _pick_str(root, "sector", "sectorName", "sector_name")
        industry = _pick_str(root, "industry", "industryName", "industry_name")
        sub_sector = _pick_str(root, "sub_sector", "subSector", "subSectorName", "sub_sector_name", "subIndustry")
        listing_date = _pick_str(root, "listing_date", "ipoDate", "IPODate", "listingDate", "ListingDate")

        # Core fundamentals
        market_cap = _pick_num(root, "market_cap", "marketCap", "MarketCap", "mcap", "marketCapitalization")
        shares_out = _pick_num(
            root,
            "shares_outstanding",
            "sharesOutstanding",
            "SharesOutstanding",
            "outstandingShares",
            "issuedShares",
        )
        free_float_pct = _pick_pct(
            root,
            "free_float",
            "freeFloat",
            "freeFloatPercent",
            "freeFloatPct",
            "freeFloatPercentage",
        )

        # Ratios / valuation
        eps_ttm = _pick_num(root, "eps", "eps_ttm", "epsTTM", "earningsPerShare", "EarningsPerShare")
        forward_eps = _pick_num(root, "forward_eps", "epsForward", "forwardEPS")
        pe_ttm = _pick_num(root, "pe", "pe_ttm", "peTTM", "priceEarnings", "PERatio")
        forward_pe = _pick_num(root, "forward_pe", "forwardPE", "peForward")
        pb = _pick_num(root, "pb", "priceToBook", "PBRatio")
        ps = _pick_num(root, "ps", "priceToSales", "PSRatio")
        ev_ebitda = _pick_num(root, "ev_ebitda", "evEbitda", "EVEBITDA")

        dividend_yield = _pick_pct(root, "dividend_yield", "dividendYield", "DividendYield")
        dividend_rate = _pick_num(root, "dividend_rate", "dividendRate", "DividendRate", "dividendPerShare")
        payout_ratio = _pick_pct(root, "payout_ratio", "payoutRatio", "PayoutRatio")

        roe = _pick_pct(root, "roe", "returnOnEquity", "ROE")
        roa = _pick_pct(root, "roa", "returnOnAssets", "ROA")
        net_margin = _pick_pct(root, "net_margin", "netMargin", "NetMargin")
        ebitda_margin = _pick_pct(root, "ebitda_margin", "ebitdaMargin", "EBITDAMargin")
        revenue_growth = _pick_pct(root, "revenue_growth", "revenueGrowth", "RevenueGrowth")
        net_income_growth = _pick_pct(root, "net_income_growth", "netIncomeGrowth", "NetIncomeGrowth")

        beta = _pick_num(root, "beta", "Beta")

        wk_hi = _pick_num(root, "week_52_high", "52WeekHigh", "fiftyTwoWeekHigh", "yearHigh", "high_52w")
        wk_lo = _pick_num(root, "week_52_low", "52WeekLow", "fiftyTwoWeekLow", "yearLow", "low_52w")
        ma20 = _pick_num(root, "ma20", "MA20", "20DayMA", "movingAverage20")
        ma50 = _pick_num(root, "ma50", "MA50", "50DayMA", "movingAverage50")
        avg_vol_30d = _pick_num(root, "avg_volume_30d", "avgVolume30d", "AverageVolume", "averageVolume", "avgVol30")

        patch: Dict[str, Any] = {
            "symbol": sym,
            "market": "KSA",
            "origin": _origin_key(),
            "currency": "SAR",

            "name": name or "",
            "sector": sector or "",
            "industry": industry or "",
            "sub_sector": sub_sector or "",
            "listing_date": listing_date or "",

            "market_cap": market_cap,
            "shares_outstanding": shares_out,
            "free_float": free_float_pct,
            "free_float_market_cap": (market_cap * (free_float_pct / 100.0))
            if (market_cap is not None and free_float_pct is not None)
            else None,

            "eps_ttm": eps_ttm,
            "forward_eps": forward_eps,
            "pe_ttm": pe_ttm,
            "forward_pe": forward_pe,
            "pb": pb,
            "ps": ps,
            "ev_ebitda": ev_ebitda,

            "dividend_yield": dividend_yield,
            "dividend_rate": dividend_rate,
            "payout_ratio": payout_ratio,

            "roe": roe,
            "roa": roa,
            "net_margin": net_margin,
            "ebitda_margin": ebitda_margin,
            "revenue_growth": revenue_growth,
            "net_income_growth": net_income_growth,
            "beta": beta,

            "week_52_high": wk_hi,
            "week_52_low": wk_lo,
            "ma20": ma20,
            "ma50": ma50,
            "avg_volume_30d": avg_vol_30d,

            "provider": PROVIDER_NAME,
            "data_source": PROVIDER_NAME,
            "provider_version": PROVIDER_VERSION,
            "last_updated_utc": _utc_iso(),
        }

        useful = bool((patch.get("name") or "").strip()) or (_safe_float(patch.get("market_cap")) is not None) or (
            _safe_float(patch.get("shares_outstanding")) is not None
        )
        if not useful:
            return {"error": "warning: tadawul fundamentals parsed but no identity/mcap/shares found"}

        _fill_derived(patch)
        patch = _clean_patch(_add_forecast_aliases(_with_provenance(patch)))

        self._fund_cache[ck] = dict(patch)
        return patch

    # -----------------------------------------------------------------------
    # History (optional)
    # -----------------------------------------------------------------------
    async def fetch_history_patch(self, symbol: str, *, days: int = 400) -> Dict[str, Any]:
        sym = _normalize_ksa_symbol(symbol)
        if not sym:
            return {}

        if not _enabled():
            return {"error": "warning: tadawul disabled (TADAWUL_MARKET_ENABLED=false)"}

        if not _history_enabled():
            return {"error": "warning: tadawul history disabled (TADAWUL_ENABLE_HISTORY=false)"}

        if not self.history_url:
            return {"error": "warning: tadawul history not configured (TADAWUL_HISTORY_URL)"}

        days_i = max(30, int(days))
        ck = f"hist::{sym}::{days_i}"
        hit = self._hist_cache.get(ck)
        if isinstance(hit, dict) and hit:
            return dict(hit)

        url = _format_url(self.history_url, sym, days=days_i)
        data, err = await self._get_json(url)
        if not data:
            return {"error": f"warning: tadawul history empty ({err or 'no data'})"}

        closes, last_iso = _extract_close_series(data, max_points=_history_points_max())
        if not closes:
            return {"error": "warning: tadawul history parsed but no close series found"}

        analytics = _compute_history_analytics(closes)

        patch: Dict[str, Any] = {
            "symbol": sym,
            "market": "KSA",
            "origin": _origin_key(),

            "history_points": len(closes),
            "history_source": "tadawul_history",
            "history_last_utc": last_iso or _utc_iso(),
        }
        patch.update(analytics)

        if not _forecast_enabled():
            patch.pop("expected_return_1m", None)
            patch.pop("expected_return_3m", None)
            patch.pop("expected_return_12m", None)
            patch.pop("expected_price_1m", None)
            patch.pop("expected_price_3m", None)
            patch.pop("expected_price_12m", None)
            patch["forecast_method"] = "history_only"

        patch = _clean_patch(_add_forecast_aliases(_with_provenance(patch)))
        self._hist_cache[ck] = dict(patch)
        return patch

    # -----------------------------------------------------------------------
    # Combined (fundamentals + quote + optional history)
    # -----------------------------------------------------------------------
    async def fetch_quote_and_fundamentals_patch(self, symbol: str) -> Dict[str, Any]:
        sym = _normalize_ksa_symbol(symbol)
        if not sym:
            return {}

        f_task = self.fetch_fundamentals_patch(sym)
        q_task = self.fetch_quote_patch(sym)
        f, q = await asyncio.gather(f_task, q_task)

        out: Dict[str, Any] = {}
        warns: List[str] = []

        if isinstance(f, dict) and f:
            if "error" in f and isinstance(f.get("error"), str) and f.get("error"):
                warns.append(str(f.get("error")))
            else:
                out.update({k: v for k, v in f.items() if k != "error"})

        if isinstance(q, dict) and q:
            if "error" in q and isinstance(q.get("error"), str) and q.get("error"):
                warns.append(str(q.get("error")))
            else:
                out.update({k: v for k, v in q.items() if k != "error"})

        if not out:
            joined = " | ".join([w for w in warns if w])
            return {"error": joined or "warning: tadawul unavailable"}

        if warns:
            out["_warn"] = " | ".join([w for w in warns if w])

        _fill_derived(out)
        out = _add_forecast_aliases(_with_provenance(out))
        out["last_updated_utc"] = _utc_iso()

        return _clean_patch(out)

    async def fetch_enriched_quote_patch(self, symbol: str) -> Dict[str, Any]:
        sym = _normalize_ksa_symbol(symbol)
        if not sym:
            return {}

        base = await self.fetch_quote_and_fundamentals_patch(sym)

        # If base is only error, return it
        if base and "error" in base and len(base.keys()) == 1:
            return base

        if _history_enabled() and self.history_url:
            h = await self.fetch_history_patch(sym, days=400)
            if isinstance(h, dict) and h:
                if "error" in h and isinstance(h.get("error"), str) and h.get("error"):
                    base["_warn"] = (base.get("_warn", "") + " | " + str(h.get("error"))).strip(" |")
                else:
                    # merge only missing keys (do not treat 0 as empty)
                    for k, v in h.items():
                        if k == "error":
                            continue
                        if base.get(k) is None and v is not None:
                            base[k] = v

        _fill_derived(base)
        base = _add_forecast_aliases(_with_provenance(base))
        base["last_updated_utc"] = _utc_iso()

        return _clean_patch(base)


# ---------------------------------------------------------------------------
# Lazy singleton
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
# Engine-compatible exported callables (DICT RETURNS)
# ---------------------------------------------------------------------------
async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    c = await get_tadawul_client()
    return await c.fetch_quote_patch(symbol)


async def fetch_fundamentals_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    c = await get_tadawul_client()
    return await c.fetch_fundamentals_patch(symbol)


async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    c = await get_tadawul_client()
    return await c.fetch_enriched_quote_patch(symbol)


async def fetch_quote_and_enrichment_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    c = await get_tadawul_client()
    return await c.fetch_enriched_quote_patch(symbol)


async def fetch_quote_and_fundamentals_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    c = await get_tadawul_client()
    return await c.fetch_quote_and_fundamentals_patch(symbol)


__all__ = [
    "TadawulClient",
    "get_tadawul_client",
    "aclose_tadawul_client",
    "fetch_quote_patch",
    "fetch_fundamentals_patch",
    "fetch_enriched_quote_patch",
    "fetch_quote_and_enrichment_patch",
    "fetch_quote_and_fundamentals_patch",
    "PROVIDER_VERSION",
    "PROVIDER_NAME",
]
