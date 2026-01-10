"""
core/data_engine_v2.py
===============================================================
UNIFIED DATA ENGINE (v2.9.1) — KSA-SAFE + PROD SAFE + ROUTER FRIENDLY
+ Forecast-ready + History analytics (returns/vol/MA/RSI)
+ Better scoring + Overall score + Recommendation + Badges
+ Riyadh timestamp + Bounded concurrency for batch refresh

v2.9.1 Adjustments
- ✅ Safer provider defaults if env is empty:
    • GLOBAL defaults to eodhd,finnhub
    • KSA defaults to yahoo_chart,argaam (tadawul only if configured)
- ✅ Adds forecast aliases for Sheets mapping:
    • forecast_price_1m/3m/12m
    • expected_roi_1m/3m/12m
    • forecast_confidence
    • forecast_updated_utc / forecast_updated_riyadh
- ✅ Keeps PROD SAFE behavior (no network at import-time, defensive, stable shapes)
"""

from __future__ import annotations

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

ENGINE_VERSION = "2.9.1"

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
    parts: List[str] = []
    for token in raw.replace(";", ",").split(","):
        t = token.strip().lower()
        if t:
            parts.append(t)
    # dedupe preserve order
    out: List[str] = []
    seen = set()
    for x in parts:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


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
    # Key fundamentals used widely in your sheets
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
    """
    Normalize to [(ts_sec, close), ...] sorted.
    Supports common shapes:
      - {"history":[{"date":"YYYY-MM-DD","close":...}, ...]}
      - {"prices":[{"timestamp":..., "close":...}, ...]}
      - {"candles":{"t":[...], "c":[...]}} (yahoo-style)
      - {"t":[...], "c":[...]} or {"timestamp":[...], "close":[...]}
      - list of dicts
      - list of tuples (ts, close)
    """
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

            t_list = payload.get
