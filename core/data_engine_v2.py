"""
core/data_engine_v2.py
===============================================================
UNIFIED DATA ENGINE (v2.17.0) — PROD SAFE + ADVANCED ANALYTICS

What’s improved in v2.17.0
- ✅ Non-blocking provider calls: sync providers run in a thread via asyncio.to_thread (prevents event-loop blocking).
- ✅ Provider timeouts: optional per-provider timeout via PROVIDER_TIMEOUT_SEC (default 12s).
- ✅ Clearer provider diagnostics: captures module/function used + exception class.
- ✅ Trend intelligence hardened: slope uses last N bars, adds neutral threshold, keeps backward-compatible keys.
- ✅ Safer math: guards for None/NaN/Inf everywhere (best-effort, never raises outward).
- ✅ Cache stability preserved: cache key remains (symbol_norm + fields); sheet snapshot keys remain deterministic.

Design goals
- ✅ PROD SAFE: Pure Python math (no pandas/numpy hard deps) for fast boot.
- ✅ Router-friendly: Returns dicts (Sheets-safe) and never raises outward.
- ✅ Cache key includes (symbol_norm + fields) and remains stable.
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import logging
import math
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger("core.data_engine_v2")

ENGINE_VERSION = "2.17.0"

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled"}
_FALSEY = {"0", "false", "no", "n", "off", "f", "disable", "disabled"}

# Trading-day approximations (used only for horizon mapping)
_TD_1W = 5
_TD_1M = 21
_TD_3M = 63
_TD_6M = 126
_TD_12M = 252

# Provider runtime controls
_PROVIDER_TIMEOUT_SEC_DEFAULT = 12  # seconds
_PROVIDER_TIMEOUT_SEC = None  # lazy env read


def _provider_timeout_sec() -> float:
    global _PROVIDER_TIMEOUT_SEC
    if _PROVIDER_TIMEOUT_SEC is not None:
        return _PROVIDER_TIMEOUT_SEC
    raw = (os.getenv("PROVIDER_TIMEOUT_SEC") or "").strip()
    if not raw:
        _PROVIDER_TIMEOUT_SEC = float(_PROVIDER_TIMEOUT_SEC_DEFAULT)
        return _PROVIDER_TIMEOUT_SEC
    try:
        v = float(raw)
        if v <= 0:
            v = float(_PROVIDER_TIMEOUT_SEC_DEFAULT)
        _PROVIDER_TIMEOUT_SEC = float(min(120.0, max(2.0, v)))
        return _PROVIDER_TIMEOUT_SEC
    except Exception:
        _PROVIDER_TIMEOUT_SEC = float(_PROVIDER_TIMEOUT_SEC_DEFAULT)
        return _PROVIDER_TIMEOUT_SEC


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
        def __init__(self, **kwargs: Any):
            self.__dict__.update(kwargs)

        def model_dump(self, *a: Any, **k: Any) -> Dict[str, Any]:
            return dict(self.__dict__)

        def dict(self, *a: Any, **k: Any) -> Dict[str, Any]:
            return dict(self.__dict__)

    def Field(default: Any = None, **kwargs: Any) -> Any:  # type: ignore
        return default


# ---------------------------------------------------------------------------
# Scoring Engine Import (Lazy)
# ---------------------------------------------------------------------------
def _enrich_with_scores(q: Any) -> Any:
    """Delegates to core.scoring_engine to add scores, badges, and reco."""
    try:
        from core.scoring_engine import enrich_with_scores

        return enrich_with_scores(q)
    except Exception:
        return q


# ---------------------------------------------------------------------------
# Recommendation normalizer (optional)
# ---------------------------------------------------------------------------
try:
    from core.reco_normalize import normalize_recommendation as _normalize_recommendation  # type: ignore
except Exception:  # pragma: no cover

    def _normalize_recommendation(x: Any) -> str:  # type: ignore
        s = str(x or "").strip().upper()
        return s if s in {"BUY", "HOLD", "REDUCE", "SELL"} else "HOLD"


def _norm_reco(x: Any, default: str = "HOLD") -> str:
    d = str(default or "HOLD").strip().upper()
    if d not in {"BUY", "HOLD", "REDUCE", "SELL"}:
        d = "HOLD"
    try:
        if x is None or str(x).strip() == "":
            return d
        return _normalize_recommendation(x)
    except Exception:
        return d


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _riyadh_iso() -> str:
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


def _safe_int(v: Any, default: int) -> int:
    try:
        n = int(str(v).strip())
        return n
    except Exception:
        return default


def _safe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        if isinstance(val, (int, float)) and not isinstance(val, bool):
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
    out: List[str] = []
    seen = set()
    for x in parts:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _dedup_preserve(items: Sequence[str]) -> List[str]:
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


def _is_ksa(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    return s.endswith(".SR") or s.isdigit() or bool(re.fullmatch(r"\d{3,6}(\.SR)?", s))


def _fallback_normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    s = s.translate(_ARABIC_DIGITS).strip()

    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", "")

    if any(ch in s for ch in ("^", "=", "/")) or s.endswith("=X") or s.endswith("=F"):
        return s

    if s.isdigit() or re.fullmatch(r"\d{3,6}", s):
        return f"{s}.SR"
    if s.endswith(".SR"):
        return s

    return s


def normalize_symbol(symbol: str) -> str:
    try:
        from core.symbols.normalize import normalize_symbol as _ext  # type: ignore

        out = _ext(symbol)
        return (out or "").strip().upper() if out else _fallback_normalize_symbol(symbol)
    except Exception:
        return _fallback_normalize_symbol(symbol)


def _provider_symbol(sym_norm: str, provider_key: str) -> str:
    """
    Provider-specific symbol adjustments (best-effort).
    """
    s = (sym_norm or "").strip().upper()
    k = (provider_key or "").strip().lower()

    # Most providers accept the canonical symbol as-is.
    # Keep rule minimal; do not break KSA (.SR) handling.
    if k in {"finnhub", "yahoo_chart", "yahoo", "yfinance"}:
        # If some upstream code ever produces ".US", strip it for Yahoo-style consumers.
        return s[:-3] if s.endswith(".US") else s

    return s


def _merge_patch(dst: Dict[str, Any], src: Dict[str, Any]) -> None:
    for k, v in (src or {}).items():
        if v is None:
            continue
        if k not in dst or dst.get(k) is None or dst.get(k) == "":
            dst[k] = v


def _call_provider_best_effort(fn: Callable[..., Any], symbol: str, refresh: bool, fields: Optional[str]) -> Any:
    """
    Calls a provider in a compatibility manner, trying multiple common signatures.
    This function is synchronous; if fn is async it will return an awaitable.
    """
    try:
        return fn(symbol, refresh=refresh, fields=fields)
    except TypeError:
        pass
    try:
        return fn(symbol, refresh=refresh)
    except TypeError:
        pass
    try:
        return fn(symbol, fields=fields)
    except TypeError:
        pass
    try:
        return fn(symbol, refresh)
    except TypeError:
        pass
    try:
        return fn(symbol)
    except TypeError:
        pass

    for key in ("symbol", "ticker", "sym"):
        try:
            return fn(**{key: symbol, "refresh": refresh, "fields": fields})
        except TypeError:
            pass
        try:
            return fn(**{key: symbol, "refresh": refresh})
        except TypeError:
            pass
        try:
            return fn(**{key: symbol, "fields": fields})
        except TypeError:
            pass
        try:
            return fn(**{key: symbol})
        except TypeError:
            pass

    return fn(symbol)


async def _call_provider_nonblocking(fn: Callable[..., Any], symbol: str, refresh: bool, fields: Optional[str]) -> Any:
    """
    Non-blocking wrapper:
    - If provider is async/coro -> await it.
    - If provider is sync -> run in thread via asyncio.to_thread.
    - Always applies a timeout (best-effort) with asyncio.wait_for.
    """
    timeout = _provider_timeout_sec()

    async def _runner() -> Any:
        if inspect.iscoroutinefunction(fn):
            raw = _call_provider_best_effort(fn, symbol, refresh, fields)
            if inspect.isawaitable(raw):
                return await raw
            return raw
        # sync provider: execute in thread to avoid blocking loop
        raw = await asyncio.to_thread(_call_provider_best_effort, fn, symbol, refresh, fields)
        if inspect.isawaitable(raw):
            return await raw
        return raw

    try:
        return await asyncio.wait_for(_runner(), timeout=timeout)
    except asyncio.TimeoutError:
        raise
    except Exception:
        raise


def _discover_callable(mod: Any, fn_names: List[str]) -> Tuple[Optional[Callable[..., Any]], Optional[str]]:
    for n in fn_names:
        try:
            cand = getattr(mod, n, None)
            if callable(cand):
                return cand, n
        except Exception:
            continue

    for export_name in ("exports", "EXPORTS"):
        try:
            ex = getattr(mod, export_name, None)
            if isinstance(ex, dict):
                for n in fn_names:
                    cand = ex.get(n)
                    if callable(cand):
                        return cand, f"{export_name}.{n}"
        except Exception:
            continue

    for obj_name in ("provider", "PROVIDER"):
        try:
            obj = getattr(mod, obj_name, None)
            if obj is None:
                continue
            for n in fn_names:
                cand = getattr(obj, n, None)
                if callable(cand):
                    return cand, f"{obj_name}.{n}"
        except Exception:
            continue

    return None, None


async def _try_provider_call(
    module_name: str,
    fn_names: List[str],
    symbol: str,
    refresh: bool,
    fields: Optional[str],
) -> Tuple[Dict[str, Any], Optional[str], Optional[str]]:
    try:
        mod = importlib.import_module(module_name)
    except Exception as e:
        return {}, f"{module_name}: import failed ({e.__class__.__name__}: {e})", None

    fn, used = _discover_callable(mod, fn_names)
    if fn is None:
        return {}, f"{module_name}: no callable in {fn_names}", None

    try:
        raw = await _call_provider_nonblocking(fn, symbol, refresh, fields)

        patch: Any = raw
        err: Optional[str] = None

        try:
            if isinstance(raw, tuple) and len(raw) >= 2:
                patch = raw[0]
                maybe_err = raw[1]
                if isinstance(maybe_err, str) and maybe_err.strip():
                    err = maybe_err.strip()
                elif maybe_err is not None:
                    err = str(maybe_err)
        except Exception:
            err = None

        if isinstance(patch, dict):
            return patch, err, used
        return {}, f"{module_name}.{used}: unexpected return type", used

    except asyncio.TimeoutError:
        return {}, f"{module_name}.{used}: timeout after {_provider_timeout_sec():g}s", used
    except Exception as e:
        return {}, f"{module_name}.{used}: call failed ({e.__class__.__name__}: {e})", used


async def _try_provider_candidates(
    module_candidates: List[str],
    fn_candidates: List[str],
    symbol: str,
    refresh: bool,
    fields: Optional[str],
) -> Tuple[Dict[str, Any], Optional[str], Optional[str]]:
    last_err: Optional[str] = None
    last_used: Optional[str] = None
    for mod in module_candidates:
        patch, err, used = await _try_provider_call(mod, fn_candidates, symbol=symbol, refresh=refresh, fields=fields)
        if patch:
            return patch, err, mod
        last_err = err
        last_used = used
    return {}, last_err, last_used


def _tadawul_configured() -> bool:
    tq = (os.getenv("TADAWUL_QUOTE_URL") or "").strip()
    tf = (os.getenv("TADAWUL_FUNDAMENTALS_URL") or "").strip()
    return bool(tq or tf)


# ============================================================
# History parsing + analytics (best-effort, provider-agnostic)
# ============================================================
def _to_epoch_seconds(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)) and not isinstance(x, bool):
            v = int(x)
            if v > 10_000_000_000:
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
                c = payload.get("candles") or {}
                t_list = c.get("t") or c.get("timestamp") or c.get("time") or []
                c_list = c.get("c") or c.get("close") or c.get("closes") or []
                if isinstance(t_list, list) and isinstance(c_list, list) and len(t_list) == len(c_list):
                    for t, cl in zip(t_list, c_list):
                        ts = _to_epoch_seconds(t)
                        fv = _safe_float(cl)
                        if ts is not None and fv is not None:
                            out.append((ts, fv))
                    out.sort(key=lambda x: x[0])
                    return out

            t_list = payload.get("t") or payload.get("timestamp") or payload.get("time") or []
            c_list = payload.get("c") or payload.get("close") or payload.get("closes") or []
            if isinstance(t_list, list) and isinstance(c_list, list) and len(t_list) == len(c_list) and len(t_list) > 1:
                for t, cl in zip(t_list, c_list):
                    ts = _to_epoch_seconds(t)
                    fv = _safe_float(cl)
                    if ts is not None and fv is not None:
                        out.append((ts, fv))
                out.sort(key=lambda x: x[0])
                return out

            for key in ("history", "prices", "price_history", "ohlc", "candles_list"):
                arr = payload.get(key)
                if isinstance(arr, list) and arr:
                    payload = arr
                    break
            else:
                arr = payload.get("data")
                if isinstance(arr, list) and arr:
                    payload = arr

        if isinstance(payload, list):
            for item in payload:
                if item is None:
                    continue
                if isinstance(item, (tuple, list)) and len(item) >= 2:
                    ts = _to_epoch_seconds(item[0])
                    fv = _safe_float(item[1])
                    if ts is not None and fv is not None:
                        out.append((ts, fv))
                    continue
                if isinstance(item, dict):
                    ts = _to_epoch_seconds(
                        item.get("t")
                        or item.get("timestamp")
                        or item.get("time")
                        or item.get("date")
                        or item.get("datetime")
                    )
                    fv = _safe_float(item.get("c") or item.get("close") or item.get("adjClose") or item.get("adj_close"))
                    if ts is not None and fv is not None:
                        out.append((ts, fv))
                    continue

        out.sort(key=lambda x: x[0])
        return out
    except Exception:
        return []


# ============================================================
# Technical Analysis Mathematics (Pure Python)
# ============================================================
def _ema(values: List[float], period: int) -> List[Optional[float]]:
    if period <= 1:
        return [float(v) for v in values]  # type: ignore[list-item]
    if len(values) < period:
        return [None] * len(values)

    alpha = 2.0 / (period + 1.0)

    # Simple MA for first point
    sma_first = sum(values[:period]) / float(period)
    ema: List[Optional[float]] = [None] * (period - 1) + [sma_first]

    for v in values[period:]:
        prev = ema[-1]
        if prev is None:
            prev = v
        curr = (v * alpha) + (float(prev) * (1.0 - alpha))
        ema.append(curr)

    return ema


def _macd(closes: List[float]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    # MACD(12, 26, 9) - last values
    if len(closes) < 35:
        return None, None, None

    ema12 = _ema(closes, 12)
    ema26 = _ema(closes, 26)

    macd_line: List[Optional[float]] = []
    for i in range(len(closes)):
        a = ema12[i]
        b = ema26[i]
        macd_line.append((a - b) if (a is not None and b is not None) else None)

    valid_macd = [m for m in macd_line if m is not None]
    if len(valid_macd) < 9:
        return None, None, None

    signal_line = _ema([float(x) for x in valid_macd], 9)
    last_macd = valid_macd[-1]
    last_signal = signal_line[-1]

    if last_macd is None or last_signal is None:
        return None, None, None

    hist = float(last_macd) - float(last_signal)
    return float(last_macd), float(last_signal), float(hist)


def _linear_slope(values: List[float]) -> Optional[float]:
    """Simple linear regression slope."""
    n = len(values)
    if n < 2:
        return None
    x = list(range(n))
    y = values

    sum_x = sum(x)
    sum_y = sum(y)
    sum_xy = sum(i * j for i, j in zip(x, y))
    sum_xx = sum(i * i for i in x)

    denominator = (n * sum_xx - sum_x * sum_x)
    if denominator == 0:
        return 0.0

    slope = (n * sum_xy - sum_x * sum_y) / denominator
    return float(slope)


# ============================================================
# Basic Math Helpers
# ============================================================
def _pct(a: float, b: float) -> Optional[float]:
    try:
        if b == 0:
            return None
        return (a - b) / b * 100.0
    except Exception:
        return None


def _compute_returns(closes: List[float]) -> Dict[str, Optional[float]]:
    res: Dict[str, Optional[float]] = {
        "returns_1w": None,
        "returns_1m": None,
        "returns_3m": None,
        "returns_6m": None,
        "returns_12m": None,
    }
    n = len(closes)
    if n < 3:
        return res

    last = closes[-1]

    def ret(days: int) -> Optional[float]:
        if n <= days:
            return None
        base = closes[-(days + 1)]
        return _pct(last, base)

    res["returns_1w"] = ret(_TD_1W)
    res["returns_1m"] = ret(_TD_1M)
    res["returns_3m"] = ret(_TD_3M)
    res["returns_6m"] = ret(_TD_6M)
    res["returns_12m"] = ret(_TD_12M)
    return res


def _sma(vals: List[float], window: int) -> Optional[float]:
    try:
        if window <= 0 or len(vals) < window:
            return None
        chunk = vals[-window:]
        return sum(chunk) / float(window)
    except Exception:
        return None


def _volatility_30d(closes: List[float]) -> Optional[float]:
    try:
        if len(closes) < 35:
            return None
        window = closes[-31:]
        rets: List[float] = []
        for i in range(1, len(window)):
            prev = window[i - 1]
            cur = window[i]
            if prev <= 0:
                continue
            rets.append((cur - prev) / prev)
        if len(rets) < 10:
            return None
        mean = sum(rets) / len(rets)
        var = sum((r - mean) ** 2 for r in rets) / max(1, (len(rets) - 1))
        sd = math.sqrt(var)
        return (sd * math.sqrt(252.0)) * 100.0
    except Exception:
        return None


def _rsi_14(closes: List[float]) -> Optional[float]:
    try:
        if len(closes) < 20:
            return None
        period = 14
        gains = 0.0
        losses = 0.0
        for i in range(-period, 0):
            ch = closes[i] - closes[i - 1]
            if ch >= 0:
                gains += ch
            else:
                losses += abs(ch)
        avg_gain = gains / period
        avg_loss = losses / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))
    except Exception:
        return None


def _apply_history_analytics(out: Dict[str, Any]) -> None:
    try:
        payload = out.get("history_payload") or out.get("history")
        series = _extract_close_series(payload)
        if len(series) < 10:
            return
        closes = [c for _, c in series if c is not None]
        if len(closes) < 10:
            return

        _merge_patch(out, _compute_returns(closes))
        out.setdefault("ma20", _sma(closes, 20))
        out.setdefault("ma50", _sma(closes, 50))
        out.setdefault("ma200", _sma(closes, 200))
        out.setdefault("volatility_30d", _volatility_30d(closes))
        out.setdefault("rsi_14", _rsi_14(closes))

        macd, sig, hist = _macd(closes)
        out.setdefault("macd_line", macd)
        out.setdefault("macd_signal", sig)
        out.setdefault("macd_hist", hist)

        # Trend Analysis (last 30 bars by default)
        window = min(30, len(closes))
        slope = _linear_slope(closes[-window:])
        out.setdefault("trend_30d", slope)

        # Trend signal with a small neutral threshold (avoid noisy flips)
        trend_sig = "NEUTRAL"
        if slope is not None:
            thr = float(os.getenv("TREND_SLOPE_THRESHOLD", "0") or "0")
            # If not provided, derive a tiny threshold from recent price scale
            if thr == 0.0:
                last = closes[-1]
                thr = max(0.0, abs(last) * 0.00002)  # ~0.002% of price per bar
            if slope > thr:
                trend_sig = "UPTREND"
            elif slope < -thr:
                trend_sig = "DOWNTREND"
            else:
                trend_sig = "NEUTRAL"
        out.setdefault("trend_signal", trend_sig)

        if not _env_bool("KEEP_RAW_HISTORY", False):
            out.pop("history_payload", None)
    except Exception:
        return


# ============================================================
# Canonical + alias mapping
# ============================================================
def _map_common_aliases(out: Dict[str, Any]) -> None:
    if out.get("current_price") is None:
        for k in ("price", "last", "last_price", "regularMarketPrice", "close", "c"):
            v = _safe_float(out.get(k))
            if v is not None:
                out["current_price"] = v
                break

    if out.get("previous_close") is None:
        for k in ("prev_close", "previousClose", "regularMarketPreviousClose", "pc"):
            v = _safe_float(out.get(k))
            if v is not None:
                out["previous_close"] = v
                break

    if out.get("day_high") is None:
        for k in ("high", "dayHigh", "regularMarketDayHigh", "h"):
            v = _safe_float(out.get(k))
            if v is not None:
                out["day_high"] = v
                break

    if out.get("day_low") is None:
        for k in ("low", "dayLow", "regularMarketDayLow", "l"):
            v = _safe_float(out.get(k))
            if v is not None:
                out["day_low"] = v
                break

    if out.get("week_52_high") is None:
        for k in ("52_week_high", "fiftyTwoWeekHigh", "week52High", "w52High"):
            v = _safe_float(out.get(k))
            if v is not None:
                out["week_52_high"] = v
                break

    if out.get("week_52_low") is None:
        for k in ("52_week_low", "fiftyTwoWeekLow", "week52Low", "w52Low"):
            v = _safe_float(out.get(k))
            if v is not None:
                out["week_52_low"] = v
                break

    if out.get("volume") is None:
        for k in ("vol", "regularMarketVolume", "v"):
            v = _safe_float(out.get(k))
            if v is not None:
                out["volume"] = v
                break

    if out.get("name") is None:
        for k in ("shortName", "longName", "companyName", "name_en", "name_ar"):
            vv = out.get(k)
            if vv is not None and str(vv).strip():
                out["name"] = str(vv).strip()
                break

    if out.get("currency") is None:
        for k in ("cur", "currency_code", "quoteCurrency", "financialCurrency"):
            vv = out.get(k)
            if vv is not None and str(vv).strip():
                out["currency"] = str(vv).strip()
                break

    if out.get("market_cap") is None:
        for k in ("mktCap", "marketCap", "market_capitalization", "marketCapitalization"):
            v = _safe_float(out.get(k))
            if v is not None:
                out["market_cap"] = v
                break

    if out.get("shares_outstanding") is None:
        for k in ("sharesOutstanding", "shares", "share_count"):
            v = _safe_float(out.get(k))
            if v is not None:
                out["shares_outstanding"] = v
                break

    if out.get("dividend_yield") is None:
        for k in ("divYield", "dividendYield", "trailingAnnualDividendYield"):
            v = _safe_float(out.get(k))
            if v is not None:
                out["dividend_yield"] = _maybe_percent(v)
                break

    if out.get("pe_ttm") is None:
        for k in ("pe", "trailingPE", "peRatio", "PERatio"):
            v = _safe_float(out.get(k))
            if v is not None:
                out["pe_ttm"] = v
                break

    if out.get("pb") is None:
        for k in ("pbRatio", "priceToBook", "PBRatio"):
            v = _safe_float(out.get(k))
            if v is not None:
                out["pb"] = v
                break


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

    for k in ("dividend_yield", "roe", "roa", "payout_ratio", "net_margin", "ebitda_margin"):
        if out.get(k) is not None:
            out[k] = _maybe_percent(out.get(k))

    if out.get("market_cap") is None:
        sh = _safe_float(out.get("shares_outstanding"))
        if cur is not None and sh is not None:
            out["market_cap"] = cur * sh

    if out.get("pe_ttm") is None:
        eps = _safe_float(out.get("eps_ttm"))
        if cur is not None and eps not in (None, 0.0):
            try:
                out["pe_ttm"] = cur / eps
            except Exception:
                pass

    if out.get("free_float_market_cap") is None:
        ff = _safe_float(out.get("free_float"))
        mc = _safe_float(out.get("market_cap"))
        if ff is not None and mc is not None:
            if ff > 1.5:
                out["free_float_market_cap"] = mc * (ff / 100.0)
            else:
                out["free_float_market_cap"] = mc * ff

    cur2 = _safe_float(out.get("current_price"))
    fv = _safe_float(out.get("fair_value"))
    if fv is None:
        for k in ("target_price", "analyst_target_price", "intrinsic_value", "dcf_value", "tp"):
            fv = _safe_float(out.get(k))
            if fv is not None:
                out.setdefault("fair_value", fv)
                break
    if out.get("upside_percent") is None and cur2 not in (None, 0.0) and fv is not None:
        try:
            out["upside_percent"] = (fv - cur2) / cur2 * 100.0
        except Exception:
            pass
    if out.get("valuation_label") is None and _safe_float(out.get("upside_percent")) is not None:
        up = float(out["upside_percent"])
        if up >= 15:
            out["valuation_label"] = "UNDERVALUED"
        elif up <= -15:
            out["valuation_label"] = "OVERVALUED"
        else:
            out["valuation_label"] = "FAIR"


def _quality_label(out: Dict[str, Any]) -> str:
    must = ["current_price", "previous_close", "day_high", "day_low", "volume"]
    ok = all(_safe_float(out.get(k)) is not None for k in must)
    if _safe_float(out.get("current_price")) is None:
        return "BAD"
    return "FULL" if ok else "PARTIAL"


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


def _coerce_confidence(v: Any) -> Optional[float]:
    x = _safe_float(v)
    if x is None:
        return None
    if x > 1.0 and x <= 100.0:
        return max(0.0, min(1.0, x / 100.0))
    return max(0.0, min(1.0, x))


def _map_forecast_aliases(out: Dict[str, Any]) -> None:
    for horizon in ("1m", "3m", "12m"):
        er_k = f"expected_return_{horizon}"
        ep_k = f"expected_price_{horizon}"
        roi_k = f"expected_roi_{horizon}"
        fp_k = f"forecast_price_{horizon}"

        if out.get(roi_k) is None and out.get(er_k) is not None:
            out[roi_k] = _safe_float(out.get(er_k))
        if out.get(fp_k) is None and out.get(ep_k) is not None:
            out[fp_k] = _safe_float(out.get(ep_k))

    if out.get("forecast_confidence") is None and out.get("confidence_score") is not None:
        out["forecast_confidence"] = _coerce_confidence(out.get("confidence_score"))
    elif out.get("forecast_confidence") is not None:
        out["forecast_confidence"] = _coerce_confidence(out.get("forecast_confidence"))

    out.setdefault("forecast_updated_utc", _utc_iso())
    out.setdefault("forecast_updated_riyadh", _riyadh_iso())


def _mirror_forecast_aliases(out: Dict[str, Any]) -> None:
    for horizon in ("1m", "3m", "12m"):
        roi_k = f"expected_roi_{horizon}"
        er_k = f"expected_return_{horizon}"
        fp_k = f"forecast_price_{horizon}"
        ep_k = f"expected_price_{horizon}"

        if out.get(er_k) is None and out.get(roi_k) is not None:
            out[er_k] = out.get(roi_k)
        if out.get(ep_k) is None and out.get(fp_k) is not None:
            out[ep_k] = out.get(fp_k)

    fc = _coerce_confidence(out.get("forecast_confidence"))
    if out.get("confidence_score") is None and fc is not None:
        out["confidence_score"] = round(fc * 100.0, 2)

    if out.get("forecast_updated") is None and out.get("forecast_updated_utc"):
        out["forecast_updated"] = out.get("forecast_updated_utc")


def _forecast_from_momentum(out: Dict[str, Any]) -> None:
    cur = _safe_float(out.get("current_price"))
    if cur is None or cur <= 0:
        return

    r1m = _safe_float(out.get("returns_1m"))
    r3m = _safe_float(out.get("returns_3m"))
    r12m = _safe_float(out.get("returns_12m"))

    if out.get("expected_roi_1m") is None and r1m is not None:
        out["expected_roi_1m"] = r1m
    if out.get("expected_roi_3m") is None and r3m is not None:
        out["expected_roi_3m"] = r3m
    if out.get("expected_roi_12m") is None and r12m is not None:
        out["expected_roi_12m"] = r12m

    trend_sig = out.get("trend_signal")
    if trend_sig == "DOWNTREND":
        if out.get("expected_roi_1m") is not None and float(out["expected_roi_1m"]) > 0:
            out["expected_roi_1m"] = float(out["expected_roi_1m"]) * 0.5
        if out.get("expected_roi_3m") is not None and float(out["expected_roi_3m"]) > 0:
            out["expected_roi_3m"] = float(out["expected_roi_3m"]) * 0.7

    er1 = _safe_float(out.get("expected_roi_1m"))
    er3 = _safe_float(out.get("expected_roi_3m"))
    er12 = _safe_float(out.get("expected_roi_12m"))

    if out.get("forecast_price_1m") is None and er1 is not None:
        out["forecast_price_1m"] = cur * (1.0 + er1 / 100.0)
    if out.get("forecast_price_3m") is None and er3 is not None:
        out["forecast_price_3m"] = cur * (1.0 + er3 / 100.0)
    if out.get("forecast_price_12m") is None and er12 is not None:
        out["forecast_price_12m"] = cur * (1.0 + er12 / 100.0)

    if out.get("forecast_confidence") is None:
        vol = _safe_float(out.get("volatility_30d"))
        has_f = _has_any_fundamentals(out)
        base = 0.60 if has_f else 0.48
        if vol is None:
            conf = base
        else:
            conf = base - min(0.25, max(0.0, (vol - 20.0) / 200.0))

        if trend_sig == "UPTREND" and (er3 or 0) > 0:
            conf += 0.10
        elif trend_sig == "DOWNTREND" and (er3 or 0) < 0:
            conf += 0.10

        conf = max(0.20, min(0.90, conf))
        out["forecast_confidence"] = conf

    out.setdefault("forecast_method", "momentum_trend_v2")
    out.setdefault("forecast_updated_utc", _utc_iso())
    out.setdefault("forecast_updated_riyadh", _riyadh_iso())


# ============================================================
# UnifiedQuote model (optional / for typed consumers)
# ============================================================
class UnifiedQuote(BaseModel):
    symbol: str = Field(default="")
    symbol_normalized: str = Field(default="")
    symbol_input: str = Field(default="")

    name: Optional[str] = None
    market: Optional[str] = None
    currency: Optional[str] = None

    current_price: Optional[float] = None
    previous_close: Optional[float] = None
    price_change: Optional[float] = None
    percent_change: Optional[float] = None
    day_high: Optional[float] = None
    day_low: Optional[float] = None
    week_52_high: Optional[float] = None
    week_52_low: Optional[float] = None
    position_52w_percent: Optional[float] = None

    volume: Optional[float] = None
    avg_volume_30d: Optional[float] = None
    value_traded: Optional[float] = None

    shares_outstanding: Optional[float] = None
    free_float: Optional[float] = None
    market_cap: Optional[float] = None
    free_float_market_cap: Optional[float] = None

    eps_ttm: Optional[float] = None
    forward_eps: Optional[float] = None
    pe_ttm: Optional[float] = None
    forward_pe: Optional[float] = None
    pb: Optional[float] = None
    ps: Optional[float] = None
    ev_ebitda: Optional[float] = None

    dividend_yield: Optional[float] = None
    dividend_rate: Optional[float] = None
    payout_ratio: Optional[float] = None

    roe: Optional[float] = None
    roa: Optional[float] = None
    net_margin: Optional[float] = None
    ebitda_margin: Optional[float] = None
    revenue_growth: Optional[float] = None
    net_income_growth: Optional[float] = None

    beta: Optional[float] = None
    volatility_30d: Optional[float] = None
    rsi_14: Optional[float] = None

    macd_line: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_hist: Optional[float] = None
    trend_30d: Optional[float] = None
    trend_signal: Optional[str] = None

    fair_value: Optional[float] = None
    upside_percent: Optional[float] = None
    valuation_label: Optional[str] = None

    returns_1w: Optional[float] = None
    returns_1m: Optional[float] = None
    returns_3m: Optional[float] = None
    returns_6m: Optional[float] = None
    returns_12m: Optional[float] = None
    ma20: Optional[float] = None
    ma50: Optional[float] = None
    ma200: Optional[float] = None

    expected_roi_1m: Optional[float] = None
    expected_roi_3m: Optional[float] = None
    expected_roi_12m: Optional[float] = None
    forecast_price_1m: Optional[float] = None
    forecast_price_3m: Optional[float] = None
    forecast_price_12m: Optional[float] = None
    forecast_confidence: Optional[float] = None
    forecast_method: Optional[str] = None
    forecast_updated_utc: Optional[str] = None
    forecast_updated_riyadh: Optional[str] = None

    expected_return_1m: Optional[float] = None
    expected_return_3m: Optional[float] = None
    expected_return_12m: Optional[float] = None
    expected_price_1m: Optional[float] = None
    expected_price_3m: Optional[float] = None
    expected_price_12m: Optional[float] = None
    confidence_score: Optional[float] = None
    forecast_updated: Optional[str] = None

    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    risk_score: Optional[float] = None
    overall_score: Optional[float] = None
    badges: Optional[List[str]] = None
    recommendation: Optional[str] = None

    data_source: Optional[str] = None
    data_quality: Optional[str] = None
    error: Optional[str] = None

    last_updated_utc: Optional[str] = None
    last_updated_riyadh: Optional[str] = None

    if _PYDANTIC_HAS_CONFIGDICT:
        model_config = ConfigDict(extra="allow")  # type: ignore


# ============================================================
# Provider registry (candidate modules + candidate functions)
# ============================================================
_PROVIDER_REGISTRY: Dict[str, Dict[str, Any]] = {
    "eodhd": {
        "modules": ["core.providers.eodhd_provider", "providers.eodhd_provider", "eodhd_provider"],
        "functions": [
            "fetch_enriched_quote_patch",
            "fetch_enriched_patch",
            "fetch_quote_and_enrichment_patch",
            "fetch_quote_and_fundamentals_patch",
            "fetch_quote_patch",
            "get_enriched_quote",
            "get_quote",
            "fetch_quote",
            "quote",
            "fetch",
        ],
    },
    "finnhub": {
        "modules": ["core.providers.finnhub_provider", "providers.finnhub_provider", "finnhub_provider"],
        "functions": [
            "fetch_enriched_quote_patch",
            "fetch_quote_patch",
            "get_enriched_quote",
            "get_quote",
            "fetch_quote",
            "quote",
            "fetch",
        ],
    },
    "yahoo_chart": {
        "modules": [
            "core.providers.yahoo_chart_provider",
            "core.providers.yahoo_provider",
            "providers.yahoo_chart_provider",
            "yahoo_chart_provider",
        ],
        "functions": [
            "fetch_enriched_quote_patch",
            "fetch_quote_patch",
            "get_enriched_quote",
            "get_quote",
            "fetch_quote",
            "quote",
            "fetch",
        ],
    },
    "yfinance": {
        "modules": ["core.providers.yfinance_provider", "providers.yfinance_provider", "yfinance_provider"],
        "functions": ["get_enriched_quote", "get_quote", "fetch_quote", "quote", "fetch"],
    },
    "argaam": {
        "modules": ["core.providers.argaam_provider", "providers.argaam_provider", "argaam_provider"],
        "functions": [
            "fetch_enriched_quote_patch",
            "fetch_quote_patch",
            "get_enriched_quote",
            "get_quote",
            "fetch_quote",
            "quote",
            "fetch",
        ],
    },
    "tadawul": {
        "modules": ["core.providers.tadawul_provider", "providers.tadawul_provider", "tadawul_provider"],
        "functions": ["get_enriched_quote", "get_quote", "fetch_quote", "quote", "fetch"],
    },
}

# ============================================================
# Sheet-name canonicalization (KEY-STABLE cache)
# ============================================================
_SHEET_ALIASES: Dict[str, str] = {
    "market_leaders": "market_leaders",
    "global_markets": "global_markets",
    "mutual_funds": "mutual_funds",
    "commodities_fx": "commodities_fx",
    "market leaders": "market_leaders",
    "global markets": "global_markets",
    "mutual funds": "mutual_funds",
    "commodities fx": "commodities_fx",
    "commodities&fx": "commodities_fx",
    "commodities/fx": "commodities_fx",
}

_SHEETKEY_BY_CANON: Dict[str, str] = {
    "market_leaders": "MARKET_LEADERS",
    "global_markets": "GLOBAL_MARKETS",
    "mutual_funds": "MUTUAL_FUNDS",
    "commodities_fx": "COMMODITIES_FX",
}

_CANON_BY_SHEETKEY: Dict[str, str] = {v: k for k, v in _SHEETKEY_BY_CANON.items()}


def _canon_sheet_name(name: str) -> str:
    s = (name or "").strip()
    if not s:
        return ""
    s2 = re.sub(r"\s+", " ", s).strip()
    low = s2.casefold()
    low = low.replace("-", " ").replace(".", " ").replace("__", " ").strip()
    low = re.sub(r"\s+", " ", low)

    if low in _SHEET_ALIASES:
        return _SHEET_ALIASES[low]

    unders = re.sub(r"[^a-z0-9]+", "_", low).strip("_")
    if unders in _SHEET_ALIASES:
        return _SHEET_ALIASES[unders]

    up = s2.strip().upper()
    if up in _CANON_BY_SHEETKEY:
        return _CANON_BY_SHEETKEY[up]

    return unders or low


def _sheet_key_variants(sheet_name: str) -> List[str]:
    raw = (sheet_name or "").strip()
    if not raw:
        return []

    canon = _canon_sheet_name(raw)
    sheetkey = _SHEETKEY_BY_CANON.get(canon, "")

    raw_norm = raw.casefold().strip()
    raw_upper = raw.upper().strip()
    raw_lower = raw.lower().strip()
    raw_unders = re.sub(r"[^A-Za-z0-9]+", "_", raw).strip("_")

    variants: List[str] = []

    def add(x: str) -> None:
        if not x:
            return
        k = f"sheet::{x}"
        if k not in variants:
            variants.append(k)

    add(raw)
    add(raw_norm)
    add(raw_upper)
    add(raw_lower)
    add(raw_unders)

    add(canon)
    add(canon.upper())
    add(canon.lower())

    if sheetkey:
        add(sheetkey)
        add(sheetkey.lower())

    return variants


# ============================================================
# Engine
# ============================================================
class DataEngine:
    """
    Router-friendly engine that returns dicts (Sheets-safe).
    Adds a second cache for "Sheet Snapshots" so Investment Advisor can reuse page data.
    """

    def __init__(self, settings: Any = None, *args: Any, **kwargs: Any) -> None:  # noqa: ARG002
        self.settings = settings

        ttl = _safe_int(os.getenv("ENGINE_CACHE_TTL_SEC", "45"), 45)
        ttl = max(5, min(600, ttl))
        self.cache_ttl_sec = ttl

        maxsize = _safe_int(os.getenv("ENGINE_CACHE_MAXSIZE", "4096"), 4096)
        maxsize = max(256, min(50_000, maxsize))

        conc = _safe_int(os.getenv("ENGINE_CONCURRENCY", "12"), 12)
        conc = max(2, min(64, conc))
        self.max_concurrency = conc
        self._sem = asyncio.Semaphore(self.max_concurrency)

        self._cache: TTLCache = TTLCache(maxsize=maxsize, ttl=self.cache_ttl_sec)  # type: ignore

        sheet_ttl = _safe_int(os.getenv("SHEET_CACHE_TTL_SEC", "180"), 180)
        sheet_ttl = max(30, min(3600, sheet_ttl))
        sheet_max = _safe_int(os.getenv("SHEET_CACHE_MAXSIZE", "64"), 64)
        sheet_max = max(8, min(512, sheet_max))

        self.sheet_cache_ttl_sec = sheet_ttl
        self._sheet_cache: TTLCache = TTLCache(maxsize=sheet_max, ttl=self.sheet_cache_ttl_sec)  # type: ignore

        default_global = (os.getenv("PROVIDERS") or os.getenv("ENABLED_PROVIDERS") or "eodhd,finnhub").strip() or "eodhd,finnhub"
        default_ksa = (os.getenv("KSA_PROVIDERS") or "yahoo_chart,argaam").strip() or "yahoo_chart,argaam"

        s_global: Optional[List[str]] = None
        s_ksa: Optional[List[str]] = None
        try:
            if settings is not None:
                p = getattr(settings, "enabled_providers", None) or getattr(settings, "providers", None)
                k = getattr(settings, "ksa_providers", None) or getattr(settings, "providers_ksa", None)
                if isinstance(p, list):
                    s_global = [str(x).strip().lower() for x in p if str(x).strip()]
                if isinstance(k, list):
                    s_ksa = [str(x).strip().lower() for x in k if str(x).strip()]
        except Exception:
            s_global = None
            s_ksa = None

        self.global_providers = _dedup_preserve(s_global or _parse_list_env("ENABLED_PROVIDERS", fallback=default_global))
        self.ksa_providers = _dedup_preserve(s_ksa or _parse_list_env("KSA_PROVIDERS", fallback=default_ksa))

        if _tadawul_configured() and "tadawul" not in self.ksa_providers:
            self.ksa_providers.append("tadawul")

        self.enable_history = _env_bool("ENABLE_HISTORY_ANALYTICS", True)
        self.include_warnings = _env_bool("ENGINE_INCLUDE_WARNINGS", False)

        logger.info(
            "DataEngine v%s | ttl=%ss | maxsize=%s | conc=%s | global=%s | ksa=%s | history=%s | cachetools=%s | sheet_ttl=%ss | sheet_max=%s | provider_timeout=%ss",
            ENGINE_VERSION,
            self.cache_ttl_sec,
            maxsize,
            self.max_concurrency,
            self.global_providers,
            self.ksa_providers,
            self.enable_history,
            _HAS_CACHETOOLS,
            self.sheet_cache_ttl_sec,
            sheet_max,
            _provider_timeout_sec(),
        )

    async def aclose(self) -> None:
        return None

    # ============================================================
    # Sheet Snapshot Cache API (KEY-STABLE)
    # ============================================================
    def set_cached_sheet_snapshot(
        self,
        sheet_name: str,
        headers: List[str],
        rows: List[List[Any]],
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        try:
            name = (sheet_name or "").strip()
            if not name:
                return

            payload = {
                "sheet": name,
                "sheet_canon": _canon_sheet_name(name),
                "sheet_key": _SHEETKEY_BY_CANON.get(_canon_sheet_name(name), None),
                "headers": list(headers or []),
                "rows": list(rows or []),
                "meta": dict(meta or {}),
                "headers_count": len(headers or []),
                "rows_count": len(rows or []),
                "cached_at_utc": _utc_iso(),
                "cached_at_riyadh": _riyadh_iso(),
                "engine_version": ENGINE_VERSION,
            }

            for k in _sheet_key_variants(name):
                try:
                    self._sheet_cache[k] = payload  # type: ignore[index]
                except Exception:
                    pass
        except Exception:
            return

    def get_cached_sheet_snapshot(self, sheet_name: str) -> Optional[Dict[str, Any]]:
        try:
            name = (sheet_name or "").strip()
            if not name:
                return None

            for k in _sheet_key_variants(name):
                v = self._sheet_cache.get(k)
                if isinstance(v, dict):
                    return dict(v)

            legacy = f"sheet::{name}"
            v2 = self._sheet_cache.get(legacy)
            return dict(v2) if isinstance(v2, dict) else None
        except Exception:
            return None

    def get_cached_multi_sheet_snapshots(self, sheet_names: List[str]) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        try:
            for s in (sheet_names or []):
                snap = self.get_cached_sheet_snapshot(s)
                if snap:
                    out[str(s)] = snap
        except Exception:
            pass
        return out

    def clear_sheet_cache(self, sheet_name: Optional[str] = None) -> None:
        try:
            if not sheet_name:
                try:
                    self._sheet_cache.clear()
                except Exception:
                    for k in list(getattr(self._sheet_cache, "keys", lambda: [])()):
                        try:
                            self._sheet_cache.pop(k, None)
                        except Exception:
                            pass
                return

            for k in _sheet_key_variants(sheet_name):
                try:
                    self._sheet_cache.pop(k, None)
                except Exception:
                    pass
        except Exception:
            return

    # --------------------
    # Public API (single)
    # --------------------
    async def get_enriched_quote(self, symbol: str, refresh: bool = False, fields: Optional[str] = None) -> Dict[str, Any]:
        sym_in = symbol or ""
        sym_norm = normalize_symbol(sym_in)
        if not sym_norm:
            return self._placeholder(sym_in, err="Missing symbol")

        cache_key = f"q::{sym_norm}::{(fields or '').strip()}"
        if not refresh:
            cached = self._cache.get(cache_key)
            if isinstance(cached, dict) and cached.get("symbol_normalized") == sym_norm:
                return dict(cached)

        try:
            async with self._sem:
                out = await self._fetch_and_build(sym_in, sym_norm, refresh=refresh, fields=fields)
        except Exception as e:
            out = self._placeholder(sym_in, err=f"Engine error: {e.__class__.__name__}: {e}")

        try:
            self._cache[cache_key] = dict(out)  # type: ignore[index]
        except Exception:
            pass
        return out

    async def get_quote(self, symbol: str, refresh: bool = False, fields: Optional[str] = None) -> Dict[str, Any]:
        return await self.get_enriched_quote(symbol, refresh=refresh, fields=fields)

    # --------------------
    # Public API (batch)
    # --------------------
    async def get_enriched_quotes(self, symbols: List[str], refresh: bool = False, fields: Optional[str] = None) -> List[Dict[str, Any]]:
        symbols = symbols or []
        if not symbols:
            return []

        chunk = max(50, min(400, self.max_concurrency * 50))
        out: List[Dict[str, Any]] = []
        for i in range(0, len(symbols), chunk):
            batch = symbols[i : i + chunk]
            tasks = [self.get_enriched_quote(s, refresh=refresh, fields=fields) for s in batch]
            res = await asyncio.gather(*tasks, return_exceptions=True)
            for j, r in enumerate(res):
                if isinstance(r, Exception):
                    out.append(self._placeholder(batch[j], err=f"Engine error: {r.__class__.__name__}: {r}"))
                else:
                    out.append(r if isinstance(r, dict) else self._placeholder(batch[j], err="Unexpected quote type"))
        return out

    async def get_quotes(self, symbols: List[str], refresh: bool = False, fields: Optional[str] = None) -> List[Dict[str, Any]]:
        return await self.get_enriched_quotes(symbols, refresh=refresh, fields=fields)

    # --------------------
    # Core build
    # --------------------
    async def _fetch_and_build(self, symbol_input: str, sym_norm: str, refresh: bool, fields: Optional[str]) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "symbol": sym_norm,
            "symbol_normalized": sym_norm,
            "symbol_input": (symbol_input or "").strip(),
            "market": "KSA" if _is_ksa(sym_norm) else "GLOBAL",
            "last_updated_utc": _utc_iso(),
            "last_updated_riyadh": _riyadh_iso(),
        }

        providers = self.ksa_providers if _is_ksa(sym_norm) else self.global_providers
        providers = providers or (["yahoo_chart", "argaam"] if _is_ksa(sym_norm) else ["eodhd", "finnhub"])

        used_sources: List[str] = []
        warnings: List[str] = []

        for key in providers:
            reg = _PROVIDER_REGISTRY.get(key)
            if not reg:
                warnings.append(f"{key}: unknown provider key")
                continue

            sym_for_provider = _provider_symbol(sym_norm, key)

            patch, err, mod_used = await _try_provider_candidates(
                module_candidates=list(reg.get("modules") or []),
                fn_candidates=list(reg.get("functions") or []),
                symbol=sym_for_provider,
                refresh=refresh,
                fields=fields,
            )

            if patch:
                p_err = patch.get("error")
                if isinstance(p_err, str) and p_err.strip():
                    warnings.append(f"{key}: {p_err.strip()}")
                    try:
                        patch = dict(patch)
                        patch.pop("error", None)
                    except Exception:
                        pass

                _merge_patch(out, patch)

                tag = key
                if mod_used:
                    tag = f"{key}:{mod_used.split('.')[-1]}"
                used_sources.append(tag)

            if err:
                warnings.append(f"{key}: {err}")

        _map_common_aliases(out)
        _apply_derived(out)
        out["data_quality"] = _quality_label(out)

        _map_forecast_aliases(out)

        if self.enable_history:
            _apply_history_analytics(out)

        _forecast_from_momentum(out)
        _mirror_forecast_aliases(out)

        # Scoring integration
        out = _enrich_with_scores(out)
        out["recommendation"] = _norm_reco(out.get("recommendation"), default="HOLD")

        out["data_source"] = ",".join(_dedup_preserve(used_sources)) if used_sources else "none"

        if _safe_float(out.get("current_price")) is None:
            out["error"] = "MISSING: current_price"
            out["data_quality"] = "BAD"
        else:
            if warnings and self.include_warnings:
                out["error"] = " | ".join(warnings[:6])

        return out

    def _placeholder(self, symbol_input: str, err: str) -> Dict[str, Any]:
        sym_norm = normalize_symbol(symbol_input or "")
        now_utc = _utc_iso()
        now_riy = _riyadh_iso()
        return {
            "symbol": sym_norm or (symbol_input or ""),
            "symbol_normalized": sym_norm or (symbol_input or ""),
            "symbol_input": (symbol_input or "").strip(),
            "market": "KSA" if _is_ksa(sym_norm) else "GLOBAL",
            "current_price": None,
            "data_source": "none",
            "data_quality": "BAD",
            "error": err,
            "recommendation": "HOLD",
            "badges": [],
            "last_updated_utc": now_utc,
            "last_updated_riyadh": now_riy,
            "forecast_updated_utc": now_utc,
            "forecast_updated_riyadh": now_riy,
            "forecast_updated": now_utc,
            "forecast_confidence": 0.20,
            "confidence_score": 20.0,
        }


DataEngineV2 = DataEngine

_ENGINE_SINGLETON: Optional[DataEngine] = None


def get_engine(settings: Any = None) -> DataEngine:
    global _ENGINE_SINGLETON
    if _ENGINE_SINGLETON is None:
        _ENGINE_SINGLETON = DataEngine(settings=settings)
    return _ENGINE_SINGLETON


__all__ = [
    "ENGINE_VERSION",
    "UnifiedQuote",
    "DataEngine",
    "DataEngineV2",
    "get_engine",
    "normalize_symbol",
]
