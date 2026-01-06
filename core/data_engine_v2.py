# core/data_engine_v2.py  (FULL REPLACEMENT)
"""
core/data_engine_v2.py
===============================================================
UNIFIED DATA ENGINE (v2.9.0) — KSA-SAFE + PROD SAFE + ROUTER FRIENDLY
+ Forecast-ready + History analytics (returns/vol/MA/RSI)
+ Better scoring + Overall score + Recommendation + Badges
+ Riyadh timestamp + Bounded concurrency for batch refresh

v2.9.0 Improvements
- ✅ Adds stable "last_updated_riyadh" (ISO) alongside last_updated_utc
- ✅ Improves forecasting:
    • historical drift/vol baseline (log-returns)
    • trend adjustment (MA20 vs MA50)
    • mean-reversion adjustment (RSI)
    • adds prediction bands: expected_price_{h}_low/high (+ return bands)
- ✅ Adds overall_score + recommendation (BUY/HOLD/REDUCE/SELL) only if missing
- ✅ Adds badges (rec_badge/momentum_badge/opportunity_badge/risk_badge) only if missing
- ✅ Safer provider defaults when env is empty
- ✅ Bounded concurrency for get_enriched_quotes (ENGINE_MAX_CONCURRENCY)
- ✅ Keeps PROD SAFE behavior (no network at import-time, defensive, stable shapes)
"""

import asyncio
import importlib
import logging
import math
import os
import re
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger("core.data_engine_v2")

ENGINE_VERSION = "2.9.0"

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSEY = {"0", "false", "no", "n", "off", "f"}

# Trading-day approximations (used only for horizon mapping)
_TD_1W = 5
_TD_1M = 21
_TD_3M = 63
_TD_6M = 126
_TD_12M = 252


# ---------------------------------------------------------------------------
# TTLCache (best-effort) with fallback
# ---------------------------------------------------------------------------
try:
    from cachetools import TTLCache  # type: ignore

    _HAS_CACHETOOLS = True
except Exception:  # pragma: no cover
    _HAS_CACHETOOLS = False

    class TTLCache(dict):  # type: ignore
        def __init__(self, maxsize: int = 1024, ttl: int = 60) -> None:
            super().__init__()
            self._maxsize = max(1, int(maxsize))
            self._ttl = max(1, int(ttl))
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
            self._exp[key] = time.time() + float(self._ttl)


# ---------------------------------------------------------------------------
# Pydantic (best-effort) with robust fallback for v1/v2
# ---------------------------------------------------------------------------
try:
    from pydantic import BaseModel, Field  # type: ignore

    try:
        from pydantic import ConfigDict  # type: ignore

        _PYDANTIC_HAS_CONFIGDICT = True
    except Exception:  # pragma: no cover
        ConfigDict = None  # type: ignore
        _PYDANTIC_HAS_CONFIGDICT = False

except Exception:  # pragma: no cover
    _PYDANTIC_HAS_CONFIGDICT = False
    ConfigDict = None  # type: ignore

    class BaseModel:  # type: ignore
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

        def model_dump(self, *a, **k):
            return dict(self.__dict__)

        def dict(self, *a, **k):
            return dict(self.__dict__)

    def Field(default=None, **kwargs):  # type: ignore
        return default


def _model_to_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    try:
        md = getattr(obj, "model_dump", None)
        if callable(md):
            d = md()
            return d if isinstance(d, dict) else dict(d)
    except Exception:
        pass
    try:
        dct = getattr(obj, "dict", None)
        if callable(dct):
            d = dct()
            return d if isinstance(d, dict) else dict(d)
    except Exception:
        pass
    try:
        return dict(getattr(obj, "__dict__", {}) or {})
    except Exception:
        return {}


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _riyadh_iso() -> str:
    # Riyadh is UTC+3 and does not observe DST
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).isoformat()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in _FALSEY:
        return False
    if raw in _TRUTHY:
        return True
    return default


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
        s = s.replace("%", "").replace(",", "").replace("+", "").strip()

        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()

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


def _maybe_percent(v: Any) -> Optional[float]:
    """
    Normalize percent-like fields to percent.
    If provider returns fraction (0.12), convert to 12.0
    If provider already returns percent-like (12.0), keep as-is.
    """
    x = _safe_float(v)
    if x is None:
        return None
    try:
        if abs(x) <= 1.5:
            return x * 100.0
        return x
    except Exception:
        return x


def _parse_list_env(name: str, fallback: str = "") -> List[str]:
    raw = (os.getenv(name) or "").strip()
    if not raw and fallback:
        raw = fallback
    if not raw:
        return []
    parts = []
    for token in raw.replace(";", ",").split(","):
        t = token.strip()
        if t:
            parts.append(t.lower())
    return parts


def _is_ksa(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    return s.endswith(".SR") or s.isdigit() or bool(re.fullmatch(r"\d{3,6}(\.SR)?", s))


def normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", "")
    if s.isdigit() or re.fullmatch(r"\d{3,6}", s):
        return f"{s}.SR"
    return s


def _merge_patch(dst: Dict[str, Any], src: Dict[str, Any]) -> None:
    for k, v in (src or {}).items():
        if v is None:
            continue
        if k not in dst or dst.get(k) is None or dst.get(k) == "":
            dst[k] = v


async def _call_maybe_async(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    out = fn(*args, **kwargs)
    if asyncio.iscoroutine(out):
        return await out
    return out


def _is_tuple2(x: Any) -> bool:
    try:
        return isinstance(x, tuple) and len(x) == 2
    except Exception:
        return False


async def _try_provider_call(
    module_name: str,
    fn_names: List[str],
    *args: Any,
    **kwargs: Any,
) -> Tuple[Dict[str, Any], Optional[str]]:
    try:
        mod = importlib.import_module(module_name)
    except Exception as e:
        return {}, f"{module_name}: import failed ({e})"

    fn = None
    used = None
    for n in fn_names:
        if hasattr(mod, n):
            cand = getattr(mod, n)
            if callable(cand):
                fn = cand
                used = n
                break

    if fn is None:
        return {}, f"{module_name}: no callable in {fn_names}"

    try:
        res = await _call_maybe_async(fn, *args, **kwargs)
        if _is_tuple2(res):
            patch, err = res
            return (patch or {}) if isinstance(patch, dict) else {}, err
        if isinstance(res, dict):
            return res, None
        return {}, f"{module_name}.{used}: unexpected return type"
    except Exception as e:
        return {}, f"{module_name}.{used}: call failed ({e})"


def _apply_derived(out: Dict[str, Any]) -> None:
    cur = _safe_float(out.get("current_price"))
    prev = _safe_float(out.get("previous_close"))
    vol = _safe_float(out.get("volume"))

    if out.get("price_change") is None and cur is not None and prev is not None:
        out["price_change"] = cur - prev

    if out.get("percent_change") is None and cur is not None and prev not in (None, 0.0):
        try:
            out["percent_change"] = (cur - prev) / prev * 100.0
        except Exception:
            pass

    if out.get("value_traded") is None and cur is not None and vol is not None:
        out["value_traded"] = cur * vol

    if out.get("position_52w_percent") is None:
        hi = _safe_float(out.get("week_52_high"))
        lo = _safe_float(out.get("week_52_low"))
        if cur is not None and hi is not None and lo is not None and hi != lo:
            out["position_52w_percent"] = (cur - lo) / (hi - lo) * 100.0

    # Normalize percent-like fundamentals to percent (sheet-ready)
    if out.get("dividend_yield") is not None:
        out["dividend_yield"] = _maybe_percent(out.get("dividend_yield"))
    if out.get("roe") is not None:
        out["roe"] = _maybe_percent(out.get("roe"))
    if out.get("roa") is not None:
        out["roa"] = _maybe_percent(out.get("roa"))
    if out.get("payout_ratio") is not None:
        out["payout_ratio"] = _maybe_percent(out.get("payout_ratio"))

    # Compute market cap if possible
    if out.get("market_cap") is None:
        sh = _safe_float(out.get("shares_outstanding"))
        if cur is not None and sh is not None:
            out["market_cap"] = cur * sh

    # Compute PE if possible
    if out.get("pe_ttm") is None:
        eps = _safe_float(out.get("eps_ttm"))
        if cur is not None and eps not in (None, 0.0):
            try:
                out["pe_ttm"] = cur / eps
            except Exception:
                pass


def _quality_label(out: Dict[str, Any]) -> str:
    must = ["current_price", "previous_close", "day_high", "day_low", "volume"]
    ok = all(_safe_float(out.get(k)) is not None for k in must)
    if _safe_float(out.get("current_price")) is None:
        return "BAD"
    return "FULL" if ok else "PARTIAL"


def _dedup_preserve(items: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for x in items:
        s = (x or "").strip()
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _tadawul_configured() -> bool:
    tq = (os.getenv("TADAWUL_QUOTE_URL") or "").strip()
    tf = (os.getenv("TADAWUL_FUNDAMENTALS_URL") or "").strip()
    return bool(tq or tf)


def _has_any_fundamentals(out: Dict[str, Any]) -> bool:
    for k in (
        "market_cap",
        "pe_ttm",
        "eps_ttm",
        "dividend_yield",
        "dividend_rate",
        "payout_ratio",
        "pb",
        "ps",
        "ev_ebitda",
        "shares_outstanding",
        "free_float",
        "roe",
        "roa",
        "beta",
        "forward_eps",
        "forward_pe",
    ):
        if _safe_float(out.get(k)) is not None:
            return True
    return False


def _missing_key_fundamentals(out: Dict[str, Any]) -> bool:
    """
    Key fundamentals that matter for your sheet:
    market_cap, pe_ttm, dividend_yield, roe, roa
    """
    return any(_safe_float(out.get(k)) is None for k in ("market_cap", "pe_ttm", "dividend_yield", "roe", "roa"))


def _normalize_warning_prefix(msg: str) -> str:
    m = (msg or "").strip()
    if not m:
        return ""
    low = m.lower()
    if low.startswith("warning:"):
        return m[len("warning:") :].strip()
    return m


# ============================================================
# History parsing + analytics (best-effort, provider-agnostic)
# ============================================================

def _to_epoch_seconds(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            v = int(x)
            if v > 10_000_000_000:  # ms
                v = int(v / 1000)
            return v if v > 0 else None
        s = str(x).strip()
        if not s:
            return None
        # ISO date
        if re.match(r"^\d{4}-\d{2}-\d{2}", s):
            try:
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            except Exception:
                dt = datetime.strptime(s[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        return None
    except Exception:
        return None


def _extract_close_series(payload: Any) -> List[Tuple[int, float]]:
    """
    Accepts multiple shapes and tries to normalize to [(ts_sec, close), ...] sorted.
    Supported common shapes:
      - {"history":[{"date":"2025-01-01","close":123}, ...]}
      - {"prices":[{"timestamp":..., "close":...}, ...]}
      - {"candles":{"t":[...], "c":[...]}}  (yahoo-style)
      - {"t":[...], "c":[...]} or {"timestamp":[...], "close":[...]}
      - list of dicts
      - list of tuples (ts, close)
    """
    out: List[Tuple[int, float]] = []
    try:
        if payload is None:
            return out

        if isinstance(payload, dict):
            # {"candles":{"t":[...], "c":[...]}}
            if isinstance(payload.get("candles"), dict):
                c = payload["candles"]
                t_list = c.get("t") or c.get("timestamp") or c.get("time")
                c_list = c.get("c") or c.get("close") or c.get("closes")
                if isinstance(t_list, list) and isinstance(c_list, list) and len(t_list) == len(c_list):
                    for t, cl in zip(t_list, c_list):
                        ts = _to_epoch_seconds(t)
                        fv = _safe_float(cl)
                        if ts is not None and fv is not None:
                            out.append((ts, fv))
                    out.sort(key=lambda x: x[0])
                    return out

            # {"t":[...], "c":[...]}
            t_list = payload.get("t") or payload.get("timestamp") or payload.get("time")
            c_list = payload.get("c") or payload.get("close") or payload.get("closes")
            if isinstance(t_list, list) and isinstance(c_list, list) and len(t_list) == len(c_list):
                for t, cl in zip(t_list, c_list):
                    ts = _to_epoch_seconds(t)
                    fv = _safe_float(cl)
                    if ts is not None and fv is not None:
                        out.append((ts, fv))
                out.sort(key=lambda x: x[0])
                return out

            for key in ("history", "prices", "ohlc", "daily", "series", "data"):
                v = payload.get(key)
                if isinstance(v, list):
                    payload = v
                    break

        if isinstance(payload, list):
            for row in payload:
                if isinstance(row, tuple) and len(row) >= 2:
                    ts = _to_epoch_seconds(row[0])
                    cl = _safe_float(row[1])
                    if ts is not None and cl is not None:
                        out.append((ts, cl))
                    continue

                if isinstance(row, dict):
                    ts = _to_epoch_seconds(
                        row.get("t")
                        or row.get("timestamp")
                        or row.get("time")
                        or row.get("date")
                        or row.get("datetime")
                    )
                    cl = _safe_float(
                        row.get("c")
                        or row.get("close")
                        or row.get("adj_close")
                        or row.get("adjClose")
                        or row.get("close_price")
                    )
                    if ts is not None and cl is not None:
                        out.append((ts, cl))
                    continue

        out.sort(key=lambda x: x[0])
        return out
    except Exception:
        return []


def _sma(values: List[float], n: int) -> Optional[float]:
    if not values or n <= 0 or len(values) < n:
        return None
    try:
        return sum(values[-n:]) / float(n)
    except Exception:
        return None


def _rsi14(closes: List[float], n: int = 14) -> Optional[float]:
    if not closes or len(closes) < n + 1:
        return None
    try:
        gains = 0.0
        losses = 0.0
        for i in range(-n, 0):
            ch = closes[i] - closes[i - 1]
            if ch >= 0:
                gains += ch
            else:
                losses += (-ch)
        if losses == 0:
            return 100.0
        rs = (gains / n) / (losses / n)
        return 100.0 - (100.0 / (1.0 + rs))
    except Exception:
        return None


def _returns_from_history(closes: List[float], td: int) -> Optional[float]:
    if not closes or td <= 0:
        return None
    if len(closes) <= td:
        return None
    try:
        now = closes[-1]
        past = closes[-1 - td]
        if past in (0.0, None) or now is None:
            return None
        return (now / past - 1.0) * 100.0
    except Exception:
        return None


def _vol_ann_from_history(closes: List[float], lookback_td: int = 30) -> Optional[float]:
    """
    Annualized volatility (%) computed from daily log returns.
    Uses numpy if available; safe fallback otherwise.
    """
    if not closes or len(closes) < lookback_td + 2:
        return None
    try:
        import numpy as np  # lazy import

        arr = np.array(closes[-(lookback_td + 1):], dtype=float)
        r = np.diff(np.log(arr))
        if r.size < 5:
            return None
        vol = float(np.std(r, ddof=1)) * math.sqrt(_TD_12M) * 100.0
        if math.isnan(vol) or math.isinf(vol):
            return None
        return vol
    except Exception:
        try:
            subset = closes[-(lookback_td + 1):]
            rets: List[float] = []
            for i in range(1, len(subset)):
                a = subset[i - 1]
                b = subset[i]
                if a <= 0 or b <= 0:
                    continue
                rets.append(math.log(b / a))
            if len(rets) < 5:
                return None
            mean = sum(rets) / len(rets)
            var = sum((x - mean) ** 2 for x in rets) / max(1, (len(rets) - 1))
            vol = math.sqrt(var) * math.sqrt(_TD_12M) * 100.0
            if math.isnan(vol) or math.isinf(vol):
                return None
            return vol
        except Exception:
            return None


def _daily_mu_sigma(closes: List[float], max_points: int = 260) -> Tuple[Optional[float], Optional[float]]:
    """
    Returns (mu_d, sigma_d) for daily log returns, best-effort.
    mu_d and sigma_d are per-day (not annualized).
    """
    if not closes or len(closes) < 35:
        return None, None
    try:
        import numpy as np  # lazy

        arr = np.array(closes[-min(len(closes), max_points):], dtype=float)
        r = np.diff(np.log(arr))
        if r.size < 10:
            return None, None
        mu = float(np.mean(r))
        sig = float(np.std(r, ddof=1))
        if any(math.isnan(x) or math.isinf(x) for x in (mu, sig)):
            return None, None
        return mu, sig
    except Exception:
        try:
            subset = closes[-min(len(closes), max_points):]
            rets: List[float] = []
            for i in range(1, len(subset)):
                a = subset[i - 1]
                b = subset[i]
                if a <= 0 or b <= 0:
                    continue
                rets.append(math.log(b / a))
            if len(rets) < 10:
                return None, None
            mu = sum(rets) / len(rets)
            var = sum((x - mu) ** 2 for x in rets) / max(1, (len(rets) - 1))
            sig = math.sqrt(var)
            if any(math.isnan(x) or math.isinf(x) for x in (mu, sig)):
                return None, None
            return mu, sig
        except Exception:
            return None, None


def _z_for_ci(level: float) -> float:
    """
    Approx z for two-sided confidence interval in Normal space.
    level=0.80 -> z~1.2816, 0.90->1.6449, 0.95->1.96
    """
    if level >= 0.95:
        return 1.96
    if level >= 0.90:
        return 1.6449
    if level >= 0.85:
        return 1.4395
    return 1.2816  # ~80%


def _forecast_expected_and_band(
    cur: float,
    mu_d: float,
    sig_d: float,
    horizon_td: int,
    *,
    ci_level: float = 0.80,
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Forecast price band using lognormal model:
      log(P_T/P_0) ~ Normal(mu*h, sigma*sqrt(h))
    Returns: (expected_price, low_price, high_price)
    expected_price uses exp(mu*h) (median-like drift), stable for sheet targets.
    """
    try:
        if cur <= 0 or horizon_td <= 0:
            return None, None, None
        z = _z_for_ci(ci_level)
        m = mu_d * float(horizon_td)
        s = sig_d * math.sqrt(float(horizon_td))

        exp_p = cur * math.exp(m)
        low_p = cur * math.exp(m - z * s)
        high_p = cur * math.exp(m + z * s)

        if any(math.isnan(x) or math.isinf(x) for x in (exp_p, low_p, high_p)):
            return None, None, None

        return exp_p, low_p, high_p
    except Exception:
        return None, None, None


def _confidence_from(out: Dict[str, Any]) -> int:
    """
    0..100 confidence score using:
    - data_quality
    - history_points
    - volatility availability
    - key fundamentals
    """
    score = 40
    dq = str(out.get("data_quality") or "").upper()
    if dq == "FULL":
        score += 20
    elif dq == "PARTIAL":
        score += 10
    else:
        score -= 10

    hp = out.get("history_points")
    try:
        hp_i = int(hp) if hp is not None else 0
    except Exception:
        hp_i = 0

    if hp_i >= 200:
        score += 20
    elif hp_i >= 120:
        score += 15
    elif hp_i >= 60:
        score += 10
    elif hp_i >= 30:
        score += 5
    else:
        score -= 10

    if _safe_float(out.get("vol_30d_ann")) is not None:
        score += 5

    if _has_any_fundamentals(out):
        score += 5
    else:
        score -= 5

    return int(max(0, min(100, score)))


# ============================================================
# Scoring (enhanced, still stable when fields are missing)
# ============================================================

def _score_quality(p: Dict[str, Any]) -> int:
    roe = _safe_float(p.get("roe"))
    margin = _safe_float(p.get("net_margin"))
    debt = _safe_float(p.get("debt_to_equity"))
    score = 50
    if roe is not None:
        score += 10 if roe >= 10 else 0
        score += 10 if roe >= 15 else 0
    if margin is not None:
        score += 5 if margin >= 10 else 0
        score += 5 if margin >= 20 else 0
    if debt is not None:
        score += 5 if debt <= 1.0 else 0
        score -= 5 if debt >= 2.0 else 0

    # Reward completeness
    if _safe_float(p.get("market_cap")) is not None:
        score += 3
    if _safe_float(p.get("eps_ttm")) is not None:
        score += 3

    return int(max(0, min(100, score)))


def _score_value(p: Dict[str, Any]) -> int:
    pe = _safe_float(p.get("pe_ttm"))
    pb = _safe_float(p.get("pb"))
    score = 50
    if pe is not None:
        score += 10 if pe <= 15 else 0
        score -= 10 if pe >= 30 else 0
    if pb is not None:
        score += 5 if pb <= 2 else 0
        score -= 5 if pb >= 6 else 0
    er3 = _safe_float(p.get("expected_return_3m"))
    if er3 is not None:
        score += 5 if er3 >= 2 else 0
        score -= 5 if er3 <= -2 else 0
    return int(max(0, min(100, score)))


def _score_momentum(p: Dict[str, Any]) -> int:
    r1m = _safe_float(p.get("returns_1m"))
    r3m = _safe_float(p.get("returns_3m"))
    chg = _safe_float(p.get("percent_change"))
    pos52 = _safe_float(p.get("position_52w_percent"))
    rsi = _safe_float(p.get("rsi14"))

    score = 50

    base = r1m if r1m is not None else chg
    if base is not None:
        score += 15 if base >= 2 else 0
        score += 10 if base >= 5 else 0
        score -= 15 if base <= -2 else 0
        score -= 10 if base <= -5 else 0

    if r3m is not None:
        score += 10 if r3m >= 5 else 0
        score -= 10 if r3m <= -5 else 0

    if pos52 is not None:
        score += 5 if pos52 >= 60 else 0
        score -= 5 if pos52 <= 20 else 0

    if rsi is not None:
        score -= 5 if rsi >= 75 else 0
        score += 5 if 40 <= rsi <= 60 else 0

    return int(max(0, min(100, score)))


def _score_risk(p: Dict[str, Any]) -> int:
    beta = _safe_float(p.get("beta"))
    vol = _safe_float(p.get("vol_30d_ann"))
    score = 35

    if beta is not None:
        score += 10 if beta >= 1.2 else 0
        score -= 10 if beta <= 0.8 else 0

    if vol is not None:
        score += 10 if vol >= 35 else 0
        score += 5 if vol >= 25 else 0
        score -= 5 if vol <= 15 else 0

    return int(max(0, min(100, score)))


def _overall_score(qs: float, vs: float, ms: float, opp: float, rs: float, conf: float) -> int:
    """
    Produces a stable 0..100 score.
    risk_score is treated as "riskiness" (higher is worse), so we invert it.
    """
    inv_risk = 100.0 - rs
    s = (0.22 * qs) + (0.22 * vs) + (0.22 * ms) + (0.22 * opp) + (0.12 * inv_risk)
    s += 0.03 * (conf - 50.0)
    return int(max(0, min(100, round(s))))


def _recommend_from_scores(overall: float, risk: float, conf: float) -> str:
    """
    BUY / HOLD / REDUCE / SELL
    """
    try:
        # low confidence -> be conservative
        penalty = 0.0
        if conf < 35:
            penalty = 8.0
        elif conf < 50:
            penalty = 4.0

        adj = overall - penalty

        if adj >= 75 and risk <= 60:
            return "BUY"
        if adj >= 55:
            return "HOLD"
        if adj >= 40:
            return "REDUCE"
        return "SELL"
    except Exception:
        return "HOLD"


def _badge_level(score: Optional[float], *, kind: str) -> str:
    """
    Returns compact badge strings (sheet-friendly).
    """
    s = score if isinstance(score, (int, float)) else None
    if s is None:
        return ""
    try:
        if kind == "risk":
            # riskiness badge (lower is better)
            if s <= 25:
                return "🟢 Low"
            if s <= 55:
                return "🟡 Med"
            return "🔴 High"
        # normal score badge (higher is better)
        if s >= 75:
            return "🟢 Strong"
        if s >= 55:
            return "🟡 OK"
        return "🔴 Weak"
    except Exception:
        return ""


class UnifiedQuote(BaseModel):
    if _PYDANTIC_HAS_CONFIGDICT and ConfigDict is not None:
        model_config = ConfigDict(populate_by_name=True, extra="ignore")  # type: ignore
    else:  # pragma: no cover
        class Config:
            extra = "ignore"
            allow_population_by_field_name = True

    symbol: str
    name: Optional[str] = None
    market: Optional[str] = None
    exchange: Optional[str] = None
    currency: Optional[str] = None

    current_price: Optional[float] = None
    previous_close: Optional[float] = None
    open: Optional[float] = None
    day_high: Optional[float] = None
    day_low: Optional[float] = None

    week_52_high: Optional[float] = None
    week_52_low: Optional[float] = None
    position_52w_percent: Optional[float] = None

    volume: Optional[float] = None
    avg_volume_30d: Optional[float] = None
    value_traded: Optional[float] = None

    price_change: Optional[float] = None
    percent_change: Optional[float] = None

    market_cap: Optional[float] = None
    shares_outstanding: Optional[float] = None
    free_float: Optional[float] = None
    free_float_market_cap: Optional[float] = None

    eps_ttm: Optional[float] = None
    pe_ttm: Optional[float] = None
    forward_eps: Optional[float] = None
    forward_pe: Optional[float] = None

    pb: Optional[float] = None
    ps: Optional[float] = None
    ev_ebitda: Optional[float] = None

    dividend_yield: Optional[float] = None  # percent (sheet-ready)
    dividend_rate: Optional[float] = None
    payout_ratio: Optional[float] = None  # percent (sheet-ready)

    roe: Optional[float] = None  # percent (sheet-ready)
    roa: Optional[float] = None  # percent (sheet-ready)
    beta: Optional[float] = None

    # ===============================
    # History analytics
    # ===============================
    returns_1w: Optional[float] = None
    returns_1m: Optional[float] = None
    returns_3m: Optional[float] = None
    returns_6m: Optional[float] = None
    returns_12m: Optional[float] = None

    ma20: Optional[float] = None
    ma50: Optional[float] = None
    ma200: Optional[float] = None

    rsi14: Optional[float] = None
    vol_30d_ann: Optional[float] = None  # annualized vol (%)

    # ===============================
    # Forward expectations
    # ===============================
    expected_return_1m: Optional[float] = None
    expected_return_3m: Optional[float] = None
    expected_return_12m: Optional[float] = None

    expected_price_1m: Optional[float] = None
    expected_price_3m: Optional[float] = None
    expected_price_12m: Optional[float] = None

    # Prediction bands (non-breaking extra fields)
    expected_price_1m_low: Optional[float] = None
    expected_price_1m_high: Optional[float] = None
    expected_price_3m_low: Optional[float] = None
    expected_price_3m_high: Optional[float] = None
    expected_price_12m_low: Optional[float] = None
    expected_price_12m_high: Optional[float] = None

    expected_return_1m_low: Optional[float] = None
    expected_return_1m_high: Optional[float] = None
    expected_return_3m_low: Optional[float] = None
    expected_return_3m_high: Optional[float] = None
    expected_return_12m_low: Optional[float] = None
    expected_return_12m_high: Optional[float] = None

    confidence_score: Optional[int] = None
    forecast_method: Optional[str] = None

    history_points: Optional[int] = None
    history_source: Optional[str] = None
    history_last_utc: Optional[str] = None

    # Scores
    quality_score: Optional[int] = None
    value_score: Optional[int] = None
    momentum_score: Optional[int] = None
    risk_score: Optional[int] = None
    opportunity_score: Optional[int] = None
    overall_score: Optional[int] = None

    # Badges
    rec_badge: Optional[str] = None
    momentum_badge: Optional[str] = None
    opportunity_badge: Optional[str] = None
    risk_badge: Optional[str] = None

    data_source: Optional[str] = None
    data_quality: Optional[str] = None
    last_updated_utc: Optional[str] = None
    last_updated_riyadh: Optional[str] = None
    error: Optional[str] = None

    symbol_input: Optional[str] = None
    symbol_normalized: Optional[str] = None

    def finalize(self) -> "UnifiedQuote":
        cur = _safe_float(self.current_price)
        prev = _safe_float(self.previous_close)
        vol = _safe_float(self.volume)

        if self.price_change is None and cur is not None and prev is not None:
            self.price_change = cur - prev

        if self.percent_change is None and cur is not None and prev not in (None, 0.0):
            try:
                self.percent_change = (cur - prev) / prev * 100.0
            except Exception:
                pass

        if self.value_traded is None and cur is not None and vol is not None:
            self.value_traded = cur * vol

        if self.position_52w_percent is None:
            hi = _safe_float(self.week_52_high)
            lo = _safe_float(self.week_52_low)
            if cur is not None and hi is not None and lo is not None and hi != lo:
                self.position_52w_percent = (cur - lo) / (hi - lo) * 100.0

        if not self.last_updated_utc:
            self.last_updated_utc = _utc_iso()
        if not self.last_updated_riyadh:
            self.last_updated_riyadh = _riyadh_iso()

        if self.error is None:
            self.error = ""

        return self


class DataEngine:
    def __init__(self) -> None:
        # Safer defaults (if env is empty)
        default_global = "yahoo_chart"
        default_ksa = "argaam,yahoo_chart,tadawul"

        self.providers_global = _parse_list_env("ENABLED_PROVIDERS") or _parse_list_env("PROVIDERS", default_global)
        self.providers_ksa = _parse_list_env("KSA_PROVIDERS", default_ksa)

        self.enable_yahoo_fundamentals_ksa = _env_bool("ENABLE_YAHOO_FUNDAMENTALS_KSA", True)
        self.enable_yahoo_fundamentals_global = _env_bool("ENABLE_YAHOO_FUNDAMENTALS_GLOBAL", False)

        # History analytics toggle
        self.enable_history_analytics = _env_bool("ENABLE_HISTORY_ANALYTICS", True)
        self.history_lookback_days = int((os.getenv("HISTORY_LOOKBACK_DAYS") or "400").strip() or "400")
        if self.history_lookback_days < 60:
            self.history_lookback_days = 60
        if self.history_lookback_days > 1200:
            self.history_lookback_days = 1200

        # Forecast config
        self.forecast_ci_level = float((os.getenv("FORECAST_CI_LEVEL") or "0.80").strip() or "0.80")
        if self.forecast_ci_level < 0.70:
            self.forecast_ci_level = 0.70
        if self.forecast_ci_level > 0.95:
            self.forecast_ci_level = 0.95

        # Batch concurrency
        try:
            self.max_concurrency = int((os.getenv("ENGINE_MAX_CONCURRENCY") or "20").strip() or "20")
        except Exception:
            self.max_concurrency = 20
        if self.max_concurrency < 3:
            self.max_concurrency = 3
        if self.max_concurrency > 80:
            self.max_concurrency = 80

        ttl = 10
        try:
            ttl = int((os.getenv("ENGINE_CACHE_TTL_SEC") or "10").strip())
            if ttl < 3:
                ttl = 3
        except Exception:
            ttl = 10

        self._cache: TTLCache = TTLCache(maxsize=8000, ttl=ttl)

        logger.info(
            "DataEngineV2 init v%s | GLOBAL=%s | KSA=%s | cache_ttl=%ss | yahoo_fund_ksa=%s yahoo_fund_global=%s | history=%s lookback=%sd | ci=%.2f | max_conc=%s | cachetools=%s",
            ENGINE_VERSION,
            ",".join(self.providers_global) if self.providers_global else "(none)",
            ",".join(self.providers_ksa) if self.providers_ksa else "(none)",
            ttl,
            self.enable_yahoo_fundamentals_ksa,
            self.enable_yahoo_fundamentals_global,
            self.enable_history_analytics,
            self.history_lookback_days,
            self.forecast_ci_level,
            self.max_concurrency,
            _HAS_CACHETOOLS,
        )

    async def aclose(self) -> None:
        # Best-effort close of provider clients (never raises)
        closers = [
            ("core.providers.finnhub_provider", "aclose_finnhub_client"),
            ("core.providers.tadawul_provider", "aclose_tadawul_client"),
            ("core.providers.yahoo_chart_provider", "aclose_yahoo_chart_client"),
            ("core.providers.yahoo_fundamentals_provider", "aclose_yahoo_fundamentals_client"),
        ]
        for mod_name, closer in closers:
            try:
                mod = importlib.import_module(mod_name)
                fn = getattr(mod, closer, None)
                if callable(fn):
                    await _call_maybe_async(fn)
            except Exception:
                continue

    def _providers_for(self, sym: str) -> List[str]:
        is_ksa = _is_ksa(sym)
        if is_ksa:
            if self.providers_ksa:
                return list(self.providers_ksa)
            # should not happen due to defaults, but keep safe:
            safe = ["argaam", "yahoo_chart"]
            if _tadawul_configured():
                safe.append("tadawul")
            return safe
        return list(self.providers_global or ["yahoo_chart"])

    async def _fetch_history_best_effort(
        self, sym: str, providers: List[str], *, refresh: bool = False
    ) -> Tuple[List[Tuple[int, float]], Optional[str]]:
        """
        Best-effort history fetch. Returns (series, source_name).
        Never raises.
        """
        if not providers:
            return [], None

        cache_key = f"h::{sym}::{self.history_lookback_days}"
        if refresh:
            try:
                self._cache.pop(cache_key, None)
            except Exception:
                pass

        hit = self._cache.get(cache_key)
        if isinstance(hit, dict) and hit.get("series"):
            series = hit.get("series")
            src = hit.get("source")
            if isinstance(series, list):
                return series, src if isinstance(src, str) else None

        series: List[Tuple[int, float]] = []
        used_src: Optional[str] = None

        pref: List[str] = []
        for p in providers:
            pl = (p or "").strip().lower()
            if pl:
                pref.append(pl)

        fn_hist = [
            "fetch_price_history",
            "fetch_history",
            "fetch_ohlc_history",
            "fetch_history_patch",
            "fetch_prices",
        ]

        provider_modules = {
            "eodhd": "core.providers.eodhd_provider",
            "yahoo_chart": "core.providers.yahoo_chart_provider",
            "finnhub": "core.providers.finnhub_provider",
            "fmp": "core.providers.fmp_provider",
            "tadawul": "core.providers.tadawul_provider",
            "argaam": "core.providers.argaam_provider",
        }

        for p in pref:
            mod_name = provider_modules.get(p)
            if not mod_name:
                continue
            try:
                if p == "finnhub":
                    api_key = os.getenv("FINNHUB_API_KEY")
                    patch, _ = await _try_provider_call(mod_name, fn_hist, sym, api_key)
                else:
                    patch, _ = await _try_provider_call(mod_name, fn_hist, sym)

                s = _extract_close_series(patch)
                if s:
                    series = s
                    used_src = p
                    break
            except Exception:
                continue

        try:
            self._cache[cache_key] = {"series": series, "source": used_src}
        except Exception:
            pass

        return series, used_src

    def _apply_history_analytics(
        self, out: Dict[str, Any], series: List[Tuple[int, float]], history_source: Optional[str]
    ) -> None:
        if not series:
            return

        closes = [c for _, c in series if isinstance(c, (int, float)) and not math.isnan(float(c)) and float(c) > 0]
        if len(closes) < 30:
            return

        if _safe_float(out.get("current_price")) is None:
            out["current_price"] = closes[-1]

        out["history_points"] = len(closes)
        out["history_source"] = history_source
        out["history_last_utc"] = _utc_iso()

        out["returns_1w"] = _returns_from_history(closes, _TD_1W)
        out["returns_1m"] = _returns_from_history(closes, _TD_1M)
        out["returns_3m"] = _returns_from_history(closes, _TD_3M)
        out["returns_6m"] = _returns_from_history(closes, _TD_6M)
        out["returns_12m"] = _returns_from_history(closes, _TD_12M)

        out["ma20"] = _sma(closes, 20)
        out["ma50"] = _sma(closes, 50)
        out["ma200"] = _sma(closes, 200)

        out["rsi14"] = _rsi14(closes, 14)
        out["vol_30d_ann"] = _vol_ann_from_history(closes, 30)

        # Forecast enhancements
        cur = _safe_float(out.get("current_price"))
        mu_d, sig_d = _daily_mu_sigma(closes, 260)

        if cur is not None and cur > 0 and mu_d is not None and sig_d is not None:
            ma20 = _safe_float(out.get("ma20"))
            ma50 = _safe_float(out.get("ma50"))
            rsi = _safe_float(out.get("rsi14"))

            # Trend adjustment: small tilt based on MA20/MA50
            trend_adj = 0.0
            if ma20 is not None and ma50 is not None and ma50 > 0:
                trend = (ma20 / ma50) - 1.0  # ~ -0.05..+0.05 typical
                trend_adj = max(-0.0008, min(0.0008, 0.6 * trend))  # bounded daily tilt

            # Mean reversion adjustment: if RSI very high -> reduce drift; very low -> increase drift
            meanrev_adj = 0.0
            if rsi is not None:
                # center at 50, scale to daily tilt
                z = (50.0 - rsi) / 50.0  # -1..+1
                meanrev_adj = max(-0.0006, min(0.0006, 0.5 * z * 0.0012))

            mu_eff = mu_d + trend_adj + meanrev_adj

            # Compute targets + bands
            for tag, h in (("1m", _TD_1M), ("3m", _TD_3M), ("12m", _TD_12M)):
                exp_p, low_p, high_p = _forecast_expected_and_band(
                    cur, mu_eff, sig_d, h, ci_level=self.forecast_ci_level
                )
                if exp_p is None:
                    continue

                exp_r = (exp_p / cur - 1.0) * 100.0
                low_r = (low_p / cur - 1.0) * 100.0 if low_p is not None else None
                high_r = (high_p / cur - 1.0) * 100.0 if high_p is not None else None

                if tag == "1m":
                    out["expected_price_1m"] = exp_p
                    out["expected_return_1m"] = exp_r
                    out["expected_price_1m_low"] = low_p
                    out["expected_price_1m_high"] = high_p
                    out["expected_return_1m_low"] = low_r
                    out["expected_return_1m_high"] = high_r
                elif tag == "3m":
                    out["expected_price_3m"] = exp_p
                    out["expected_return_3m"] = exp_r
                    out["expected_price_3m_low"] = low_p
                    out["expected_price_3m_high"] = high_p
                    out["expected_return_3m_low"] = low_r
                    out["expected_return_3m_high"] = high_r
                else:
                    out["expected_price_12m"] = exp_p
                    out["expected_return_12m"] = exp_r
                    out["expected_price_12m_low"] = low_p
                    out["expected_price_12m_high"] = high_p
                    out["expected_return_12m_low"] = low_r
                    out["expected_return_12m_high"] = high_r

            out["forecast_method"] = "drift+vol+trend+meanrev"
        else:
            # Fallback: keep prior behavior minimal (if any)
            out["forecast_method"] = out.get("forecast_method") or "history_only"

        out["confidence_score"] = _confidence_from(out)

    async def get_enriched_quote(
        self,
        symbol: str,
        *,
        enrich: bool = True,
        refresh: bool = False,
        fields: Optional[str] = None,  # hint only (safe to ignore)
    ) -> Dict[str, Any]:
        sym = normalize_symbol(symbol)
        if not sym:
            q = UnifiedQuote(symbol=str(symbol or ""), error="empty symbol").finalize()
            out0 = _model_to_dict(q)
            out0["status"] = "error"
            out0["data_quality"] = "BAD"
            return out0

        cache_key = f"q::{sym}::{int(bool(enrich))}"
        if refresh:
            try:
                self._cache.pop(cache_key, None)
            except Exception:
                pass

        hit = self._cache.get(cache_key)
        if (not refresh) and isinstance(hit, dict) and hit:
            return dict(hit)

        is_ksa = _is_ksa(sym)
        providers = self._providers_for(sym)

        q0 = UnifiedQuote(
            symbol=sym,
            market="KSA" if is_ksa else "GLOBAL",
            last_updated_utc=_utc_iso(),
            last_updated_riyadh=_riyadh_iso(),
            error="",
            symbol_input=str(symbol or ""),
            symbol_normalized=sym,
        )
        out: Dict[str, Any] = _model_to_dict(q0)

        if not providers:
            out["status"] = "error"
            out["data_quality"] = "BAD"
            out["error"] = "No providers configured"
            self._cache[cache_key] = dict(out)
            return out

        warnings: List[str] = []
        source_used: List[str] = []

        for p in providers:
            p = (p or "").strip().lower()
            if not p:
                continue

            # Protect KSA from global-only providers unless explicitly listed in KSA_PROVIDERS
            if is_ksa and p in {"eodhd", "fmp", "finnhub"} and (p not in (self.providers_ksa or [])):
                warnings.append(f"{p}: skipped for KSA")
                continue

            if p == "tadawul" and not _tadawul_configured():
                warnings.append("tadawul: skipped (missing TADAWUL_QUOTE_URL/TADAWUL_FUNDAMENTALS_URL)")
                continue

            patch: Dict[str, Any] = {}
            err: Optional[str] = None

            if p == "tadawul":
                patch, err = await _try_provider_call(
                    "core.providers.tadawul_provider",
                    ["fetch_quote_and_fundamentals_patch", "fetch_fundamentals_patch", "fetch_quote_patch"],
                    sym,
                )

            elif p == "argaam":
                patch, err = await _try_provider_call(
                    "core.providers.argaam_provider",
                    ["fetch_quote_patch", "fetch_quote_and_fundamentals_patch", "fetch_enriched_patch", "fetch_enriched_quote_patch"],
                    sym,
                )

            elif p == "yahoo_chart":
                patch, err = await _try_provider_call(
                    "core.providers.yahoo_chart_provider",
                    ["fetch_quote_patch", "fetch_quote_and_enrichment_patch", "fetch_enriched_quote_patch", "fetch_quote"],
                    sym,
                )

            elif p == "finnhub":
                api_key = os.getenv("FINNHUB_API_KEY")
                patch, err = await _try_provider_call(
                    "core.providers.finnhub_provider",
                    ["fetch_quote_and_enrichment_patch", "fetch_enriched_quote_patch", "fetch_quote_patch", "fetch_quote"],
                    sym,
                    api_key,
                )

            elif p == "eodhd":
                fn_list = (
                    [
                        "fetch_enriched_quote_patch",
                        "fetch_quote_and_fundamentals_patch",
                        "fetch_enriched_patch",
                        "fetch_quote_patch",
                        "fetch_quote",
                    ]
                    if enrich
                    else ["fetch_quote_patch", "fetch_quote"]
                )
                patch, err = await _try_provider_call("core.providers.eodhd_provider", fn_list, sym)

            elif p == "fmp":
                patch, err = await _try_provider_call(
                    "core.providers.fmp_provider",
                    ["fetch_quote_and_fundamentals_patch", "fetch_enriched_quote_patch", "fetch_enriched_patch", "fetch_quote_patch", "fetch_quote"],
                    sym,
                )

            else:
                warnings.append(f"{p}: unknown provider")
                continue

            if patch:
                _merge_patch(out, patch)
                source_used.append(p)

            if err:
                low = str(err).strip().lower()
                warnings.append(str(err).strip() if low.startswith(f"{p}:") else f"{p}: {str(err).strip()}")

            _apply_derived(out)

            has_price_now = _safe_float(out.get("current_price")) is not None
            if has_price_now:
                if not enrich:
                    break
                # stop only when key fundamentals are filled
                if not _missing_key_fundamentals(out):
                    break

        # Yahoo Fundamentals supplement (NO yfinance)
        try:
            has_price = _safe_float(out.get("current_price")) is not None
            needs_fund = enrich and has_price and _missing_key_fundamentals(out)
            allow = (is_ksa and self.enable_yahoo_fundamentals_ksa) or ((not is_ksa) and self.enable_yahoo_fundamentals_global)

            if needs_fund and allow:
                patch2, err2 = await _try_provider_call(
                    "core.providers.yahoo_fundamentals_provider",
                    ["fetch_fundamentals_patch", "fetch_quote_patch", "fetch_enriched_quote_patch", "yahoo_fundamentals"],
                    sym,
                )

                if patch2:
                    clean = {
                        k: patch2.get(k)
                        for k in (
                            "currency",
                            "current_price",
                            "market_cap",
                            "shares_outstanding",
                            "eps_ttm",
                            "pe_ttm",
                            "forward_eps",
                            "forward_pe",
                            "pb",
                            "ps",
                            "ev_ebitda",
                            "dividend_yield",
                            "dividend_rate",
                            "payout_ratio",
                            "roe",
                            "roa",
                            "beta",
                        )
                        if patch2.get(k) is not None
                    }
                    if clean:
                        _merge_patch(out, clean)
                        source_used.append("yahoo_fundamentals")

                if err2:
                    warnings.append(f"yahoo_fundamentals: {str(err2).strip()}")

                _apply_derived(out)
        except Exception:
            pass

        # History analytics + expectations (best-effort)
        if enrich and self.enable_history_analytics and _safe_float(out.get("current_price")) is not None:
            try:
                series, hist_src = await self._fetch_history_best_effort(sym, providers, refresh=refresh)
                if series:
                    self._apply_history_analytics(out, series, hist_src)
            except Exception:
                pass

        # Scores
        out["quality_score"] = _score_quality(out)
        out["value_score"] = _score_value(out)
        out["momentum_score"] = _score_momentum(out)
        out["risk_score"] = _score_risk(out)

        qs = float(out.get("quality_score") or 50)
        vs = float(out.get("value_score") or 50)
        ms = float(out.get("momentum_score") or 50)
        rs = float(out.get("risk_score") or 35)

        conf = float(out.get("confidence_score") or 50)

        opp = (0.35 * qs) + (0.35 * vs) + (0.30 * ms) - (0.15 * rs)
        opp += 0.05 * (conf - 50.0)
        out["opportunity_score"] = int(max(0, min(100, round(opp))))

        # overall_score (only if missing)
        if _safe_float(out.get("overall_score")) is None:
            out["overall_score"] = _overall_score(qs, vs, ms, float(out.get("opportunity_score") or 50), rs, conf)

        # recommendation (only if missing)
        rec = str(out.get("recommendation") or "").strip().upper()
        if not rec:
            out["recommendation"] = _recommend_from_scores(
                float(out.get("overall_score") or 50),
                float(out.get("risk_score") or 50),
                float(out.get("confidence_score") or 50),
            )
        else:
            # normalize
            if rec not in {"BUY", "HOLD", "REDUCE", "SELL"}:
                out["recommendation"] = rec

        # badges (only if missing)
        if not str(out.get("rec_badge") or "").strip():
            out["rec_badge"] = {"BUY": "🟢 BUY", "HOLD": "🟡 HOLD", "REDUCE": "🟠 REDUCE", "SELL": "🔴 SELL"}.get(
                str(out.get("recommendation") or "HOLD").strip().upper(),
                "🟡 HOLD",
            )
        if not str(out.get("momentum_badge") or "").strip():
            out["momentum_badge"] = _badge_level(_safe_float(out.get("momentum_score")), kind="score")
        if not str(out.get("opportunity_badge") or "").strip():
            out["opportunity_badge"] = _badge_level(_safe_float(out.get("opportunity_score")), kind="score")
        if not str(out.get("risk_badge") or "").strip():
            out["risk_badge"] = _badge_level(_safe_float(out.get("risk_score")), kind="risk")

        out["data_source"] = ",".join(_dedup_preserve(source_used)) if source_used else (out.get("data_source") or None)
        out["data_quality"] = _quality_label(out)

        # timestamps (ensure)
        if not str(out.get("last_updated_utc") or "").strip():
            out["last_updated_utc"] = _utc_iso()
        if not str(out.get("last_updated_riyadh") or "").strip():
            out["last_updated_riyadh"] = _riyadh_iso()

        has_price = _safe_float(out.get("current_price")) is not None
        base_err = str(out.get("error") or "").strip()
        parts: List[str] = []
        if base_err:
            parts.append(_normalize_warning_prefix(base_err))
        parts += [_normalize_warning_prefix(w) for w in warnings]
        parts = _dedup_preserve([p for p in parts if p])

        if not has_price:
            out["status"] = "error"
            out["error"] = " | ".join(parts) if parts else "No price returned"
        else:
            out["status"] = "success"
            out["error"] = "" if not parts else ("warning: " + " | ".join(parts))

        self._cache[cache_key] = dict(out)
        return out

    async def get_enriched_quotes(
        self,
        symbols: List[str],
        *,
        enrich: bool = True,
        refresh: bool = False,
        fields: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        if not symbols:
            return []

        sem = asyncio.Semaphore(self.max_concurrency)

        async def _one(s: str) -> Dict[str, Any]:
            async with sem:
                return await self.get_enriched_quote(s, enrich=enrich, refresh=refresh, fields=fields)

        tasks = [_one(s) for s in symbols]
        res = await asyncio.gather(*tasks, return_exceptions=True)

        out: List[Dict[str, Any]] = []
        for i, r in enumerate(res):
            if isinstance(r, Exception):
                q = UnifiedQuote(symbol=normalize_symbol(symbols[i]) or str(symbols[i] or ""), error=str(r)).finalize()
                d = _model_to_dict(q)
                d["status"] = "error"
                d["data_quality"] = "BAD"
                out.append(d)
            else:
                out.append(r)
        return out

    async def get_quote(self, symbol: str, *, refresh: bool = False, fields: Optional[str] = None) -> Dict[str, Any]:
        return await self.get_enriched_quote(symbol, enrich=True, refresh=refresh, fields=fields)

    async def get_quotes(self, symbols: List[str], *, refresh: bool = False, fields: Optional[str] = None) -> List[Dict[str, Any]]:
        return await self.get_enriched_quotes(symbols, enrich=True, refresh=refresh, fields=fields)


# Backward-compat alias (many routers/logs refer to "DataEngineV2")
DataEngineV2 = DataEngine

_ENGINE_SINGLETON: Optional[DataEngine] = None
_LOCK = asyncio.Lock()


async def get_engine() -> DataEngine:
    global _ENGINE_SINGLETON
    if _ENGINE_SINGLETON is None:
        async with _LOCK:
            if _ENGINE_SINGLETON is None:
                _ENGINE_SINGLETON = DataEngine()
    return _ENGINE_SINGLETON


async def get_enriched_quote(symbol: str, *, refresh: bool = False, fields: Optional[str] = None) -> Dict[str, Any]:
    e = await get_engine()
    return await e.get_enriched_quote(symbol, enrich=True, refresh=refresh, fields=fields)


async def get_enriched_quotes(symbols: List[str], *, refresh: bool = False, fields: Optional[str] = None) -> List[Dict[str, Any]]:
    e = await get_engine()
    return await e.get_enriched_quotes(symbols, enrich=True, refresh=refresh, fields=fields)


async def get_quote(symbol: str, *, refresh: bool = False, fields: Optional[str] = None) -> Dict[str, Any]:
    return await get_enriched_quote(symbol, refresh=refresh, fields=fields)


async def get_quotes(symbols: List[str], *, refresh: bool = False, fields: Optional[str] = None) -> List[Dict[str, Any]]:
    return await get_enriched_quotes(symbols, refresh=refresh, fields=fields)


__all__ = [
    "ENGINE_VERSION",
    "UnifiedQuote",
    "normalize_symbol",
    "DataEngine",
    "DataEngineV2",
    "get_engine",
    "get_quote",
    "get_quotes",
    "get_enriched_quote",
    "get_enriched_quotes",
]
