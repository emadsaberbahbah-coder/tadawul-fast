# core/data_engine_v2.py
"""
core/data_engine_v2.py
===============================================================
UNIFIED DATA ENGINE (v2.8.0) — KSA-SAFE + PROD SAFE + ROUTER FRIENDLY
+ Forecast-ready + History analytics (returns/vol/MA/RSI) + Better scoring

What’s new in v2.8.0 (your “future expectation + analysis” upgrade):
- Adds lightweight historical analytics (no new deps required; uses numpy if available).
- Computes returns (1W/1M/3M/6M/12M), MA20/50/200, RSI14, Volatility (30D, annualized).
- Adds forward expectations (ExpectedReturn/ExpectedPrice for 1M/3M/12M) using drift+vol model.
- Adds confidence_score + forecast_method + history metadata.
- Adds refresh=1 support (router can bypass cache safely).
- Keeps PROD-SAFE behavior:
  - Never hard-requires provider history functions (best-effort only).
  - Never breaks startup if numpy not available (safe fallbacks).
  - Always returns a dict with status/data_quality/error.

Provider integration:
- Tries optional provider history functions if they exist:
  - eodhd_provider: fetch_price_history / fetch_history / fetch_ohlc_history / fetch_history_patch
  - yahoo_chart_provider: fetch_price_history / fetch_history / fetch_ohlc_history / fetch_history_patch
  - finnhub/fmp/tadawul/argaam: best-effort if they expose similar history functions
If none exist, analytics fields remain None and API stays stable.
"""

import asyncio
import importlib
import logging
import math
import os
import re
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from cachetools import TTLCache

logger = logging.getLogger("core.data_engine_v2")

ENGINE_VERSION = "2.8.0"

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSEY = {"0", "false", "no", "n", "off", "f"}

# Trading-day approximations (used only for horizon mapping)
_TD_1W = 5
_TD_1M = 21
_TD_3M = 63
_TD_6M = 126
_TD_12M = 252


# -----------------------------------------------------------------------------
# Pydantic (best-effort) with robust fallback for v1/v2
# -----------------------------------------------------------------------------
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
    pb: Optional[float] = None
    ps: Optional[float] = None
    dividend_yield: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None
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
    # Forward expectations (simple drift+vol)
    # ===============================
    expected_return_1m: Optional[float] = None
    expected_return_3m: Optional[float] = None
    expected_return_12m: Optional[float] = None

    expected_price_1m: Optional[float] = None
    expected_price_3m: Optional[float] = None
    expected_price_12m: Optional[float] = None

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

    data_source: Optional[str] = None
    data_quality: Optional[str] = None
    last_updated_utc: Optional[str] = None
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

        if self.error is None:
            self.error = ""

        return self


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def _parse_list_env(name: str, fallback: str = "") -> List[str]:
    raw = (os.getenv(name) or "").strip()
    if not raw and fallback:
        raw = fallback
    if not raw:
        return []
    return [p.strip().lower() for p in raw.split(",") if p.strip()]


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
        "pb",
        "ps",
        "shares_outstanding",
        "free_float",
        "roe",
        "roa",
        "beta",
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


def _drift_expected_return(closes: List[float], horizon_td: int) -> Optional[float]:
    """
    Expected return (%) using average daily log return * horizon.
    """
    if not closes or len(closes) < 30 or horizon_td <= 0:
        return None
    try:
        import numpy as np  # lazy

        arr = np.array(closes[-(min(len(closes), 260)):], dtype=float)
        r = np.diff(np.log(arr))
        if r.size < 10:
            return None
        mu_d = float(np.mean(r))
        exp_ret = (math.exp(mu_d * horizon_td) - 1.0) * 100.0
        if math.isnan(exp_ret) or math.isinf(exp_ret):
            return None
        return exp_ret
    except Exception:
        try:
            subset = closes[-(min(len(closes), 260)):]

            rets: List[float] = []
            for i in range(1, len(subset)):
                a = subset[i - 1]
                b = subset[i]
                if a <= 0 or b <= 0:
                    continue
                rets.append(math.log(b / a))
            if len(rets) < 10:
                return None

            mu_d = sum(rets) / len(rets)
            exp_ret = (math.exp(mu_d * horizon_td) - 1.0) * 100.0
            if math.isnan(exp_ret) or math.isinf(exp_ret):
                return None
            return exp_ret
        except Exception:
            return None


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
    # If expected returns are positive, slight bump (future expectation tie-in)
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


class DataEngine:
    def __init__(self) -> None:
        self.providers_global = _parse_list_env("ENABLED_PROVIDERS") or _parse_list_env("PROVIDERS")
        self.providers_ksa = _parse_list_env("KSA_PROVIDERS")

        self.enable_yahoo_fundamentals_ksa = _env_bool("ENABLE_YAHOO_FUNDAMENTALS_KSA", True)
        self.enable_yahoo_fundamentals_global = _env_bool("ENABLE_YAHOO_FUNDAMENTALS_GLOBAL", False)

        # History analytics toggle
        self.enable_history_analytics = _env_bool("ENABLE_HISTORY_ANALYTICS", True)
        self.history_lookback_days = int((os.getenv("HISTORY_LOOKBACK_DAYS") or "400").strip() or "400")
        if self.history_lookback_days < 60:
            self.history_lookback_days = 60
        if self.history_lookback_days > 1200:
            self.history_lookback_days = 1200

        ttl = 10
        try:
            ttl = int((os.getenv("ENGINE_CACHE_TTL_SEC") or "10").strip())
            if ttl < 3:
                ttl = 3
        except Exception:
            ttl = 10

        self._cache: TTLCache = TTLCache(maxsize=8000, ttl=ttl)

        logger.info(
            "DataEngineV2 init v%s | GLOBAL=%s | KSA=%s | cache_ttl=%ss | yahoo_fund_ksa=%s yahoo_fund_global=%s | history=%s lookback=%sd",
            ENGINE_VERSION,
            ",".join(self.providers_global) if self.providers_global else "(none)",
            ",".join(self.providers_ksa) if self.providers_ksa else "(none)",
            ttl,
            self.enable_yahoo_fundamentals_ksa,
            self.enable_yahoo_fundamentals_global,
            self.enable_history_analytics,
            self.history_lookback_days,
        )

    async def aclose(self) -> None:
        for mod_name, closer in [
            ("core.providers.finnhub_provider", "aclose_finnhub_client"),
            ("core.providers.tadawul_provider", "aclose_tadawul_client"),
        ]:
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
            safe = [p for p in (self.providers_global or []) if p in {"tadawul", "argaam", "yahoo_chart"}]
            return safe
        return list(self.providers_global or [])

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

        cur = _safe_float(out.get("current_price"))
        if cur is not None and cur > 0:
            er_1m = _drift_expected_return(closes, _TD_1M)
            er_3m = _drift_expected_return(closes, _TD_3M)
            er_12m = _drift_expected_return(closes, _TD_12M)

            out["expected_return_1m"] = er_1m
            out["expected_return_3m"] = er_3m
            out["expected_return_12m"] = er_12m

            out["expected_price_1m"] = (cur * (1.0 + (er_1m / 100.0))) if er_1m is not None else None
            out["expected_price_3m"] = (cur * (1.0 + (er_3m / 100.0))) if er_3m is not None else None
            out["expected_price_12m"] = (cur * (1.0 + (er_12m / 100.0))) if er_12m is not None else None

            out["forecast_method"] = "historical_drift_vol"

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
                if _has_any_fundamentals(out):
                    break

        # Yahoo Fundamentals supplement
        try:
            has_price = _safe_float(out.get("current_price")) is not None
            needs_fund = enrich and has_price and _missing_key_fundamentals(out)
            allow = (is_ksa and self.enable_yahoo_fundamentals_ksa) or ((not is_ksa) and self.enable_yahoo_fundamentals_global)

            if needs_fund and allow:
                patch2, err2 = await _try_provider_call(
                    "core.providers.yahoo_fundamentals_provider",
                    ["fetch_fundamentals_patch", "fetch_patch", "yahoo_fundamentals", "fetch_fundamentals"],
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
                            "pb",
                            "dividend_yield",
                            "dividend_rate",
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

        out["data_source"] = ",".join(_dedup_preserve(source_used)) if source_used else (out.get("data_source") or None)
        out["data_quality"] = _quality_label(out)

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
        tasks = [self.get_enriched_quote(s, enrich=enrich, refresh=refresh, fields=fields) for s in symbols]
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
