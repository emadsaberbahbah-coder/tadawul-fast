# core/data_engine_v2.py  (FULL REPLACEMENT)
"""
core/data_engine_v2.py
===============================================================
UNIFIED DATA ENGINE (v2.9.0) — KSA-SAFE + PROD SAFE + ROUTER FRIENDLY
Emad Bahbah — Financial Leader Edition

Adds (non-breaking)
- ✅ “Emad 59-column” sheet readiness:
    • Adds optional fields: turnover_percent, free_float_market_cap, liquidity_score,
      fair_value, upside_percent, valuation_label, overall_score, recommendation
- ✅ Forecasting upgrade:
    • GBM-based forecast (drift + volatility) using daily log returns
    • expected_return_{1m,3m,12m}, expected_price_{1m,3m,12m}
    • Adds confidence bands (P5/P95) for prices & returns (extra fields; safe)
- ✅ Fair Value computation:
    • Uses best available blend: expected_price_12m, forward valuation, MA200/MA50
    • Upside % + valuation label computed consistently
- ✅ Recommendation standardized enum: BUY / HOLD / REDUCE / SELL
- ✅ Batch concurrency limit for get_enriched_quotes (ENGINE_BATCH_CONCURRENCY)

Still
- Import safe (no network at import-time)
- KSA safe provider routing
- TTL cache
"""

import asyncio
import importlib
import logging
import math
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger("core.data_engine_v2")

ENGINE_VERSION = "2.9.0"

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSEY = {"0", "false", "no", "n", "off", "f"}

_TD_1W = 5
_TD_1M = 21
_TD_3M = 63
_TD_6M = 126
_TD_12M = 252

_RECO_ENUM = ("BUY", "HOLD", "REDUCE", "SELL")


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


def _to_riyadh_iso(utc_any: Any) -> Optional[str]:
    if utc_any is None or utc_any == "":
        return None
    try:
        s = str(utc_any).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        from zoneinfo import ZoneInfo  # py3.9+

        return dt.astimezone(ZoneInfo("Asia/Riyadh")).isoformat()
    except Exception:
        return None


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
    x = _safe_float(v)
    if x is None:
        return None
    try:
        if abs(x) <= 1.5:
            return x * 100.0
        return x
    except Exception:
        return x


def _ff_to_fraction(x: Any) -> Optional[float]:
    f = _safe_float(x)
    if f is None:
        return None
    if f > 1.5:
        return max(0.0, min(1.0, f / 100.0))
    return max(0.0, min(1.0, f))


def _normalize_recommendation(x: Any) -> str:
    if x is None:
        return "HOLD"
    try:
        s = str(x).strip().upper()
    except Exception:
        return "HOLD"
    if not s:
        return "HOLD"
    if s in _RECO_ENUM:
        return s
    s2 = re.sub(r"[\s\-_/]+", " ", s).strip()

    buy_like = {"STRONG BUY", "BUY", "ACCUMULATE", "ADD", "OUTPERFORM", "OVERWEIGHT", "LONG"}
    hold_like = {"HOLD", "NEUTRAL", "MAINTAIN", "MARKET PERFORM", "EQUAL WEIGHT", "WAIT"}
    reduce_like = {"REDUCE", "TRIM", "LIGHTEN", "UNDERWEIGHT", "PARTIAL SELL", "TAKE PROFIT", "TAKE PROFITS"}
    sell_like = {"SELL", "STRONG SELL", "EXIT", "AVOID", "UNDERPERFORM", "SHORT"}

    if s2 in buy_like:
        return "BUY"
    if s2 in hold_like:
        return "HOLD"
    if s2 in reduce_like:
        return "REDUCE"
    if s2 in sell_like:
        return "SELL"

    if "SELL" in s2:
        return "SELL"
    if "REDUCE" in s2 or "TRIM" in s2 or "UNDERWEIGHT" in s2:
        return "REDUCE"
    if "HOLD" in s2 or "NEUTRAL" in s2 or "MAINTAIN" in s2:
        return "HOLD"
    if "BUY" in s2 or "ACCUMULATE" in s2 or "OVERWEIGHT" in s2:
        return "BUY"
    return "HOLD"


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

    turnover_percent: Optional[float] = None
    liquidity_score: Optional[float] = None

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

    # Optional profitability/growth fields (if providers supply)
    net_margin: Optional[float] = None
    ebitda_margin: Optional[float] = None
    revenue_growth: Optional[float] = None
    net_income_growth: Optional[float] = None

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
    # Forecast (GBM drift+vol)
    # ===============================
    expected_return_1m: Optional[float] = None
    expected_return_3m: Optional[float] = None
    expected_return_12m: Optional[float] = None

    expected_price_1m: Optional[float] = None
    expected_price_3m: Optional[float] = None
    expected_price_12m: Optional[float] = None

    # Confidence bands (extra; safe)
    expected_return_1m_p5: Optional[float] = None
    expected_return_1m_p95: Optional[float] = None
    expected_return_3m_p5: Optional[float] = None
    expected_return_3m_p95: Optional[float] = None
    expected_return_12m_p5: Optional[float] = None
    expected_return_12m_p95: Optional[float] = None

    expected_price_1m_p5: Optional[float] = None
    expected_price_1m_p95: Optional[float] = None
    expected_price_3m_p5: Optional[float] = None
    expected_price_3m_p95: Optional[float] = None
    expected_price_12m_p5: Optional[float] = None
    expected_price_12m_p95: Optional[float] = None

    confidence_score: Optional[int] = None
    forecast_method: Optional[str] = None

    # Valuation helpers
    fair_value: Optional[float] = None
    upside_percent: Optional[float] = None
    valuation_label: Optional[str] = None

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
    recommendation: Optional[str] = None

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
            self.last_updated_riyadh = _to_riyadh_iso(self.last_updated_utc) or ""

        if self.error is None:
            self.error = ""

        if self.recommendation:
            self.recommendation = _normalize_recommendation(self.recommendation)

        return self


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


def _compute_turnover_percent(volume: Any, shares_outstanding: Any) -> Optional[float]:
    v = _safe_float(volume)
    sh = _safe_float(shares_outstanding)
    if v is None or sh in (None, 0.0):
        return None
    try:
        return round((v / sh) * 100.0, 4)
    except Exception:
        return None


def _compute_free_float_mkt_cap(market_cap: Any, free_float: Any) -> Optional[float]:
    mc = _safe_float(market_cap)
    ff = _ff_to_fraction(free_float)
    if mc is None or ff is None:
        return None
    try:
        return mc * ff
    except Exception:
        return None


def _compute_liquidity_score(value_traded: Any) -> Optional[float]:
    vt = _safe_float(value_traded)
    if vt is None or vt <= 0:
        return None
    try:
        x = math.log10(vt)
        score = (x - 6.0) / (9.0 - 6.0) * 100.0
        return float(max(0.0, min(100.0, round(score, 2))))
    except Exception:
        return None


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
    for k in ("dividend_yield", "roe", "roa", "payout_ratio", "net_margin", "ebitda_margin", "revenue_growth", "net_income_growth"):
        if out.get(k) is not None:
            out[k] = _maybe_percent(out.get(k))

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

    # Turnover %
    if out.get("turnover_percent") is None:
        out["turnover_percent"] = _compute_turnover_percent(out.get("volume"), out.get("shares_outstanding"))

    # Free float market cap
    if out.get("free_float_market_cap") is None:
        out["free_float_market_cap"] = _compute_free_float_mkt_cap(out.get("market_cap"), out.get("free_float"))

    # Liquidity score
    if out.get("liquidity_score") is None:
        out["liquidity_score"] = _compute_liquidity_score(out.get("value_traded"))

    # Last updated riyadh
    if not out.get("last_updated_utc"):
        out["last_updated_utc"] = _utc_iso()
    if not out.get("last_updated_riyadh"):
        out["last_updated_riyadh"] = _to_riyadh_iso(out.get("last_updated_utc")) or ""


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
    out: List[Tuple[int, float]] = []
    try:
        if payload is None:
            return out

        if isinstance(payload, dict):
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
    if not closes or td <= 0 or len(closes) <= td:
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
    if not closes or len(closes) < lookback_td + 2:
        return None
    try:
        import numpy as np  # lazy

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


def _gbm_forecast(closes: List[float], horizon_td: int) -> Optional[Dict[str, float]]:
    """
    GBM forecast using daily log returns.
    Returns percent returns + price percentiles relative to current price.
    """
    if not closes or len(closes) < 60 or horizon_td <= 0:
        return None
    try:
        # use up to ~1y history for stability
        subset = closes[-min(len(closes), 260):]
        logrets: List[float] = []
        for i in range(1, len(subset)):
            a = subset[i - 1]
            b = subset[i]
            if a <= 0 or b <= 0:
                continue
            logrets.append(math.log(b / a))
        if len(logrets) < 20:
            return None

        mu = sum(logrets) / len(logrets)
        var = sum((x - mu) ** 2 for x in logrets) / max(1, (len(logrets) - 1))
        sigma = math.sqrt(var)

        S0 = subset[-1]

        # mean of GBM: E[S_T] = S0 * exp(mu*h)
        mean_price = S0 * math.exp(mu * horizon_td)

        # quantiles of lognormal: ln(S_T/S0) ~ N(mu*h, sigma*sqrt(h))
        z5 = -1.6448536269514722
        z95 = 1.6448536269514722
        s = sigma * math.sqrt(horizon_td)
        p5 = S0 * math.exp(mu * horizon_td + z5 * s)
        p95 = S0 * math.exp(mu * horizon_td + z95 * s)

        def pct(S: float) -> float:
            return (S / S0 - 1.0) * 100.0

        return {
            "ret_mean": pct(mean_price),
            "ret_p5": pct(p5),
            "ret_p95": pct(p95),
            "px_mean": mean_price,
            "px_p5": p5,
            "px_p95": p95,
        }
    except Exception:
        return None


def _confidence_from(out: Dict[str, Any]) -> int:
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
# Scoring (kept compatible)
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


def _compute_fair_value(out: Dict[str, Any]) -> Optional[float]:
    """
    Fair value blend (best-effort, stable):
    1) expected_price_12m
    2) forward_eps * forward_pe
    3) forward_eps * pe_ttm
    4) ma200 / ma50
    """
    candidates: List[float] = []

    for k in ("expected_price_12m", "expected_price_3m"):
        v = _safe_float(out.get(k))
        if v is not None and v > 0:
            candidates.append(v)

    fe = _safe_float(out.get("forward_eps"))
    fpe = _safe_float(out.get("forward_pe"))
    if fe is not None and fpe is not None and fe > 0 and fpe > 0:
        candidates.append(fe * fpe)

    pe = _safe_float(out.get("pe_ttm"))
    if fe is not None and pe is not None and fe > 0 and pe > 0:
        candidates.append(fe * pe)

    for k in ("ma200", "ma50"):
        v = _safe_float(out.get(k))
        if v is not None and v > 0:
            candidates.append(v)

    if not candidates:
        return None

    # robust center (median-ish)
    candidates.sort()
    mid = candidates[len(candidates) // 2]
    return float(mid)


def _compute_upside_and_label(price: Any, fair: Any) -> Tuple[Optional[float], Optional[str]]:
    p = _safe_float(price)
    f = _safe_float(fair)
    if p is None or f is None or p <= 0:
        return None, None
    up = (f / p - 1.0) * 100.0
    label = "Fairly Valued"
    if up >= 15:
        label = "Undervalued"
    elif up <= -15:
        label = "Overvalued"
    return round(up, 2), label


def _recommend_from_scores(out: Dict[str, Any]) -> str:
    """
    Stable rule-based recommendation (sheet-friendly, not “advice”):
    - Uses opportunity_score + upside_percent + risk_score
    """
    opp = float(out.get("opportunity_score") or 50)
    up = _safe_float(out.get("upside_percent"))
    risk = float(out.get("risk_score") or 35)

    if up is None:
        up = 0.0

    if opp >= 70 and up >= 10 and risk <= 60:
        return "BUY"
    if opp >= 55:
        return "HOLD"
    if opp >= 40:
        return "REDUCE"
    return "SELL"


class DataEngine:
    def __init__(self) -> None:
        self.providers_global = _parse_list_env("ENABLED_PROVIDERS") or _parse_list_env("PROVIDERS")
        self.providers_ksa = _parse_list_env("KSA_PROVIDERS")

        self.enable_yahoo_fundamentals_ksa = _env_bool("ENABLE_YAHOO_FUNDAMENTALS_KSA", True)
        self.enable_yahoo_fundamentals_global = _env_bool("ENABLE_YAHOO_FUNDAMENTALS_GLOBAL", False)

        self.enable_history_analytics = _env_bool("ENABLE_HISTORY_ANALYTICS", True)
        self.history_lookback_days = int((os.getenv("HISTORY_LOOKBACK_DAYS") or "400").strip() or "400")
        self.history_lookback_days = min(1200, max(60, self.history_lookback_days))

        ttl = 10
        try:
            ttl = int((os.getenv("ENGINE_CACHE_TTL_SEC") or "10").strip())
            ttl = max(3, ttl)
        except Exception:
            ttl = 10

        self._cache: TTLCache = TTLCache(maxsize=8000, ttl=ttl)

        self.batch_concurrency = 15
        try:
            self.batch_concurrency = int((os.getenv("ENGINE_BATCH_CONCURRENCY") or "15").strip())
            self.batch_concurrency = max(1, min(50, self.batch_concurrency))
        except Exception:
            self.batch_concurrency = 15

        logger.info(
            "DataEngineV2 init v%s | GLOBAL=%s | KSA=%s | cache_ttl=%ss | yahoo_fund_ksa=%s yahoo_fund_global=%s | history=%s lookback=%sd | batch_conc=%s | cachetools=%s",
            ENGINE_VERSION,
            ",".join(self.providers_global) if self.providers_global else "(none)",
            ",".join(self.providers_ksa) if self.providers_ksa else "(none)",
            ttl,
            self.enable_yahoo_fundamentals_ksa,
            self.enable_yahoo_fundamentals_global,
            self.enable_history_analytics,
            self.history_lookback_days,
            self.batch_concurrency,
            _HAS_CACHETOOLS,
        )

    async def aclose(self) -> None:
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
            safe = [p for p in (self.providers_global or []) if p in {"tadawul", "argaam", "yahoo_chart"}]
            return safe
        return list(self.providers_global or [])

    async def _fetch_history_best_effort(
        self, sym: str, providers: List[str], *, refresh: bool = False
    ) -> Tuple[List[Tuple[int, float]], Optional[str]]:
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

        pref = [(p or "").strip().lower() for p in providers if (p or "").strip()]
        fn_hist = ["fetch_price_history", "fetch_history", "fetch_ohlc_history", "fetch_history_patch", "fetch_prices"]

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

    def _apply_history_analytics(self, out: Dict[str, Any], series: List[Tuple[int, float]], history_source: Optional[str]) -> None:
        if not series:
            return

        closes = [c for _, c in series if isinstance(c, (int, float)) and float(c) > 0 and not math.isnan(float(c))]
        if len(closes) < 60:
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
            f1 = _gbm_forecast(closes, _TD_1M)
            f3 = _gbm_forecast(closes, _TD_3M)
            f12 = _gbm_forecast(closes, _TD_12M)

            if f1:
                out["expected_return_1m"] = f1["ret_mean"]
                out["expected_return_1m_p5"] = f1["ret_p5"]
                out["expected_return_1m_p95"] = f1["ret_p95"]
                out["expected_price_1m"] = f1["px_mean"]
                out["expected_price_1m_p5"] = f1["px_p5"]
                out["expected_price_1m_p95"] = f1["px_p95"]

            if f3:
                out["expected_return_3m"] = f3["ret_mean"]
                out["expected_return_3m_p5"] = f3["ret_p5"]
                out["expected_return_3m_p95"] = f3["ret_p95"]
                out["expected_price_3m"] = f3["px_mean"]
                out["expected_price_3m_p5"] = f3["px_p5"]
                out["expected_price_3m_p95"] = f3["px_p95"]

            if f12:
                out["expected_return_12m"] = f12["ret_mean"]
                out["expected_return_12m_p5"] = f12["ret_p5"]
                out["expected_return_12m_p95"] = f12["ret_p95"]
                out["expected_price_12m"] = f12["px_mean"]
                out["expected_price_12m_p5"] = f12["px_p5"]
                out["expected_price_12m_p95"] = f12["px_p95"]

            if f1 or f3 or f12:
                out["forecast_method"] = "gbm_drift_vol"

        out["confidence_score"] = _confidence_from(out)

    async def get_enriched_quote(
        self,
        symbol: str,
        *,
        enrich: bool = True,
        refresh: bool = False,
        fields: Optional[str] = None,
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
            last_updated_riyadh=_to_riyadh_iso(_utc_iso()) or "",
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
                    ["fetch_enriched_quote_patch", "fetch_quote_and_fundamentals_patch", "fetch_enriched_patch", "fetch_quote_patch", "fetch_quote"]
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

        # History analytics + forecasting (GBM)
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

        # Overall score (sheet helper): lean on opportunity
        out["overall_score"] = int(max(0, min(100, round(float(out.get("opportunity_score") or 50)))))

        # Fair value + upside + label
        try:
            if _safe_float(out.get("fair_value")) is None:
                out["fair_value"] = _compute_fair_value(out)
            up, label = _compute_upside_and_label(out.get("current_price"), out.get("fair_value"))
            if out.get("upside_percent") is None:
                out["upside_percent"] = up
            if not out.get("valuation_label"):
                out["valuation_label"] = label
        except Exception:
            pass

        # Recommendation (enum)
        if not out.get("recommendation"):
            out["recommendation"] = _recommend_from_scores(out)
        else:
            out["recommendation"] = _normalize_recommendation(out.get("recommendation"))

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

        sem = asyncio.Semaphore(self.batch_concurrency)

        async def _one(s: str) -> Dict[str, Any]:
            async with sem:
                try:
                    return await self.get_enriched_quote(s, enrich=enrich, refresh=refresh, fields=fields)
                except Exception as e:
                    q = UnifiedQuote(symbol=normalize_symbol(s) or str(s or ""), error=str(e)).finalize()
                    d = _model_to_dict(q)
                    d["status"] = "error"
                    d["data_quality"] = "BAD"
                    return d

        res = await asyncio.gather(*[_one(s) for s in symbols], return_exceptions=False)
        return list(res)

    async def get_quote(self, symbol: str, *, refresh: bool = False, fields: Optional[str] = None) -> Dict[str, Any]:
        return await self.get_enriched_quote(symbol, enrich=True, refresh=refresh, fields=fields)

    async def get_quotes(self, symbols: List[str], *, refresh: bool = False, fields: Optional[str] = None) -> List[Dict[str, Any]]:
        return await self.get_enriched_quotes(symbols, enrich=True, refresh=refresh, fields=fields)


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
