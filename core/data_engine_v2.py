# core/data_engine_v2.py  (FULL REPLACEMENT)
"""
core/data_engine_v2.py
===============================================================
UNIFIED DATA ENGINE (v2.11.2) — PROD SAFE + ROUTER FRIENDLY

Fixes (Global returning data_source=none / BAD):
- ✅ Provider function discovery supports patch-style exports + provider objects + exports dicts
- ✅ Uses shared symbol normalization (core/symbols/normalize.py) when present
- ✅ Provider call signature mismatch hardened (symbol/ticker kw + positional + kwargs)
- ✅ Never raises: always returns a dict placeholder on failure
- ✅ Accepts optional `settings` (kw or positional) for legacy adapter compatibility

v2.11.2 upgrades (alignment + completeness)
- ✅ Stronger recommendation normalization integration (core.reco_normalize)
- ✅ Adds common field alias mapping (price/last/close, prevClose, 52w fields, mktCap, etc.)
- ✅ Cache key includes `fields` to avoid cross-request pollution
- ✅ Provider tuple errors preserved as warnings (patch, err, meta...) best-effort
- ✅ Forecast aliases mapped both directions (canonical + back-compat)
- ✅ Keeps canonical engine fields for Sheets:
    forecast_price_{1m,3m,12m}, expected_roi_{1m,3m,12m},
    forecast_confidence (0..1), forecast_updated_utc/riyadh

Key guarantees
- ✅ PROD SAFE: no network at import-time
- ✅ Global routing: EODHD primary (best-effort), then Finnhub
- ✅ KSA routing: Yahoo primary, then Argaam (+ Tadawul if configured)
- ✅ Forecast-ready: fills Forecast Price/Expected ROI/Confidence/Updated
- ✅ History analytics: returns/vol/MA/RSI (best-effort when raw history exists)
- ✅ Scores + Recommendation + Badges (BUY/HOLD/REDUCE/SELL)
- ✅ Riyadh timestamps

Environment knobs (optional)
- ENABLED_PROVIDERS / PROVIDERS          (global providers list)
- KSA_PROVIDERS                          (ksa providers list)
- ENABLE_HISTORY_ANALYTICS=true|false    (default true)
- ENGINE_CACHE_TTL_SEC                   (default 45)
- ENGINE_CACHE_MAXSIZE                   (default 4096)
- ENGINE_CONCURRENCY                     (default 12)
- ENGINE_INCLUDE_WARNINGS=true|false     (default false)
- KEEP_RAW_HISTORY=true|false            (default false)
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
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger("core.data_engine_v2")

ENGINE_VERSION = "2.11.2"

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled"}
_FALSEY = {"0", "false", "no", "n", "off", "f", "disable", "disabled"}

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
    """
    Safe wrapper around normalize_recommendation.
    - Always returns BUY/HOLD/REDUCE/SELL
    - Uses `default` if input is missing/unrecognized.
    """
    d = str(default or "HOLD").strip().upper()
    if d not in {"BUY", "HOLD", "REDUCE", "SELL"}:
        d = "HOLD"
    try:
        if x is None or str(x).strip() == "":
            return d
    except Exception:
        return d
    try:
        out = _normalize_recommendation(x)
        out = str(out or "").strip().upper()
        return out if out in {"BUY", "HOLD", "REDUCE", "SELL"} else d
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


def _is_ksa(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    return s.endswith(".SR") or s.isdigit() or bool(re.fullmatch(r"\d{3,6}(\.SR)?", s))


def _fallback_normalize_symbol(symbol: str) -> str:
    """
    Fallback normalizer (only used if core.symbols.normalize is missing).
    IMPORTANT: do NOT force ".US" here.
    """
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    s = s.translate(_ARABIC_DIGITS).strip()

    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", "")

    # indices / fx / futures etc: passthrough
    if any(ch in s for ch in ("^", "=", "/")) or s.endswith("=X") or s.endswith("=F"):
        return s

    # KSA
    if s.isdigit() or re.fullmatch(r"\d{3,6}", s):
        return f"{s}.SR"
    if s.endswith(".SR"):
        return s

    # Keep explicit exchange suffixes (AAPL.US, VOD.L, etc.)
    if "." in s:
        return s

    # global equity: keep plain ticker
    return s


def normalize_symbol(symbol: str) -> str:
    """
    Preferred symbol normalizer:
    - Uses core.symbols.normalize.normalize_symbol if present.
    - Else uses fallback.
    """
    try:
        from core.symbols.normalize import normalize_symbol as _ext  # type: ignore

        out = _ext(symbol)
        return (out or "").strip().upper() if out else _fallback_normalize_symbol(symbol)
    except Exception:
        return _fallback_normalize_symbol(symbol)


def _provider_symbol(sym_norm: str, provider_key: str) -> str:
    """
    Provider-specific symbol mapping (conservative).
    """
    s = (sym_norm or "").strip().upper()
    k = (provider_key or "").strip().lower()

    if k in {"finnhub"}:
        return s[:-3] if s.endswith(".US") else s

    if k.startswith("yahoo") or k in {"yfinance"}:
        return s[:-3] if s.endswith(".US") else s

    return s


def _merge_patch(dst: Dict[str, Any], src: Dict[str, Any]) -> None:
    """Patch-style merge: does NOT override existing non-empty values in dst."""
    for k, v in (src or {}).items():
        if v is None:
            continue
        if k not in dst or dst.get(k) is None or dst.get(k) == "":
            dst[k] = v


async def _call_maybe_async(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    out = fn(*args, **kwargs)
    if inspect.isawaitable(out):
        return await out
    return out


def _call_provider_best_effort(fn: Callable[..., Any], symbol: str, refresh: bool, fields: Optional[str]) -> Any:
    """
    Try common signatures without assuming provider API.
    """
    # positional + kwargs
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

    # keyword symbol variants
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


def _discover_callable(mod: Any, fn_names: List[str]) -> Tuple[Optional[Callable[..., Any]], Optional[str]]:
    """
    Discovers a callable in multiple patterns:
      1) module.<name>
      2) module.exports / module.EXPORTS dict
      3) module.provider / module.PROVIDER object with method
    """
    # 1) direct functions
    for n in fn_names:
        try:
            cand = getattr(mod, n, None)
            if callable(cand):
                return cand, n
        except Exception:
            continue

    # 2) dict exports
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

    # 3) provider objects
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
    """
    Provider callable may return:
      - dict
      - (dict, error_str)
      - (dict, error_str, meta...)
    Returns (patch, err, fn_used)
    """
    try:
        mod = importlib.import_module(module_name)
    except Exception as e:
        return {}, f"{module_name}: import failed ({e})", None

    fn, used = _discover_callable(mod, fn_names)
    if fn is None:
        return {}, f"{module_name}: no callable in {fn_names}", None

    try:
        raw = await _call_maybe_async(_call_provider_best_effort, fn, symbol, refresh, fields)

        patch: Any = raw
        err: Optional[str] = None

        # Keep tuple error if present (patch, err, ...)
        try:
            if isinstance(raw, tuple) and len(raw) >= 2:
                patch = raw[0]
                maybe_err = raw[1]
                if isinstance(maybe_err, str) and maybe_err.strip():
                    err = maybe_err.strip()
                elif maybe_err is not None:
                    # Some providers return Exception/object here
                    err = str(maybe_err)
        except Exception:
            err = None

        if isinstance(patch, dict):
            return patch, err, used
        return {}, f"{module_name}.{used}: unexpected return type", used
    except Exception as e:
        return {}, f"{module_name}.{used}: call failed ({e})", used


async def _try_provider_candidates(
    module_candidates: List[str],
    fn_candidates: List[str],
    symbol: str,
    refresh: bool,
    fields: Optional[str],
) -> Tuple[Dict[str, Any], Optional[str], Optional[str]]:
    """
    Try multiple modules for the same provider key.
    Returns (patch, err, module_used) where err is non-fatal warning.
    """
    last_err: Optional[str] = None
    for mod in module_candidates:
        patch, err, _used = await _try_provider_call(mod, fn_candidates, symbol=symbol, refresh=refresh, fields=fields)
        if patch:
            return patch, err, mod
        last_err = err
    return {}, last_err, None


def _map_common_aliases(out: Dict[str, Any]) -> None:
    """
    Map common provider field names into canonical engine fields (only if missing).
    This increases compatibility across providers without breaking schema.
    """
    # Prices
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

    # 52w
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

    # Volume
    if out.get("volume") is None:
        for k in ("vol", "regularMarketVolume", "v"):
            v = _safe_float(out.get(k))
            if v is not None:
                out["volume"] = v
                break

    # Identity
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

    # Fundamentals
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

    # Fair value / upside (best-effort)
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


def _tadawul_configured() -> bool:
    tq = (os.getenv("TADAWUL_QUOTE_URL") or "").strip()
    tf = (os.getenv("TADAWUL_FUNDAMENTALS_URL") or "").strip()
    return bool(tq or tf)


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


# ============================================================
# History parsing + analytics (best-effort, provider-agnostic)
# ============================================================
def _to_epoch_seconds(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)) and not isinstance(x, bool):
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

        if not _env_bool("KEEP_RAW_HISTORY", False):
            out.pop("history_payload", None)
    except Exception:
        return


def _coerce_confidence(v: Any) -> Optional[float]:
    """
    Normalize forecast confidence into 0..1
    Accepts:
      - 0..1 fraction
      - 0..100 percent (confidence_score style)
    """
    x = _safe_float(v)
    if x is None:
        return None
    if x > 1.0 and x <= 100.0:
        return max(0.0, min(1.0, x / 100.0))
    return max(0.0, min(1.0, x))


def _map_forecast_aliases(out: Dict[str, Any]) -> None:
    """
    Map provider variants into canonical fields:
      expected_return_*  -> expected_roi_*
      expected_price_*   -> forecast_price_*
      confidence_score   -> forecast_confidence (0..1)
    """
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
    """
    Add backward-compatible aliases (without breaking canonical fields):
      forecast_price_*  -> expected_price_*
      expected_roi_*    -> expected_return_*
      forecast_confidence -> confidence_score (percent)
      forecast_updated_utc -> forecast_updated
    """
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
        conf = max(0.20, min(0.90, conf))
        out["forecast_confidence"] = conf

    out.setdefault("forecast_method", "momentum_returns")
    out.setdefault("forecast_updated_utc", _utc_iso())
    out.setdefault("forecast_updated_riyadh", _riyadh_iso())


# ============================================================
# Scoring + recommendation (simple, stable, Sheets-friendly)
# ============================================================
def _score_from_percent(x: Optional[float], lo: float, hi: float) -> float:
    if x is None:
        return 50.0
    try:
        v = float(x)
        if v <= lo:
            return 0.0
        if v >= hi:
            return 100.0
        return (v - lo) / (hi - lo) * 100.0
    except Exception:
        return 50.0


def _score_inverse_percent(x: Optional[float], lo: float, hi: float) -> float:
    if x is None:
        return 50.0
    try:
        v = float(x)
        if v <= lo:
            return 100.0
        if v >= hi:
            return 0.0
        return (hi - v) / (hi - lo) * 100.0
    except Exception:
        return 50.0


def _compute_scores(out: Dict[str, Any]) -> None:
    upside = _safe_float(out.get("upside_percent"))
    roe = _safe_float(out.get("roe"))
    roa = _safe_float(out.get("roa"))
    nm = _safe_float(out.get("net_margin"))
    rg = _safe_float(out.get("revenue_growth"))
    ng = _safe_float(out.get("net_income_growth"))

    r1m = _safe_float(out.get("returns_1m"))
    r3m = _safe_float(out.get("returns_3m"))
    r12m = _safe_float(out.get("returns_12m"))
    rsi = _safe_float(out.get("rsi_14"))

    vol = _safe_float(out.get("volatility_30d"))
    beta = _safe_float(out.get("beta"))

    pb = _safe_float(out.get("pb"))
    pe = _safe_float(out.get("pe_ttm"))

    value = 0.6 * _score_from_percent(upside, lo=-20.0, hi=60.0)
    value += 0.2 * (_score_inverse_percent(pb, lo=0.8, hi=6.0) if pb is not None else 50.0)
    value += 0.2 * (_score_inverse_percent(pe, lo=8.0, hi=45.0) if pe is not None else 50.0)
    value_score = max(0.0, min(100.0, value))

    quality = (
        0.35 * _score_from_percent(roe, lo=0.0, hi=25.0)
        + 0.20 * _score_from_percent(roa, lo=0.0, hi=12.0)
        + 0.20 * _score_from_percent(nm, lo=0.0, hi=20.0)
        + 0.15 * _score_from_percent(rg, lo=-10.0, hi=25.0)
        + 0.10 * _score_from_percent(ng, lo=-15.0, hi=35.0)
    )
    quality_score = max(0.0, min(100.0, quality))

    mom = (
        0.45 * _score_from_percent(r1m, lo=-15.0, hi=20.0)
        + 0.35 * _score_from_percent(r3m, lo=-25.0, hi=40.0)
        + 0.10 * _score_from_percent(r12m, lo=-40.0, hi=80.0)
    )
    if rsi is not None:
        rsi_score = 100.0 - min(100.0, abs(rsi - 55.0) * 3.0)
        mom += 0.10 * max(0.0, min(100.0, rsi_score))
    else:
        mom += 0.10 * 50.0
    momentum_score = max(0.0, min(100.0, mom))

    risk_good = 0.65 * _score_inverse_percent(vol, lo=12.0, hi=70.0) + 0.35 * _score_inverse_percent(beta, lo=0.7, hi=2.2)
    risk_score = max(0.0, min(100.0, risk_good))

    opportunity_score = max(0.0, min(100.0, (0.40 * value_score + 0.30 * momentum_score + 0.30 * quality_score)))
    overall_score = max(0.0, min(100.0, (0.30 * value_score + 0.30 * quality_score + 0.25 * momentum_score + 0.15 * risk_score)))

    out.setdefault("value_score", round(value_score, 2))
    out.setdefault("quality_score", round(quality_score, 2))
    out.setdefault("momentum_score", round(momentum_score, 2))
    out.setdefault("risk_score", round(risk_score, 2))
    out.setdefault("opportunity_score", round(opportunity_score, 2))
    out.setdefault("overall_score", round(overall_score, 2))


def _badges(out: Dict[str, Any]) -> List[str]:
    b: List[str] = []
    if _safe_float(out.get("overall_score")) is None:
        return b
    if (_safe_float(out.get("value_score")) or 0) >= 75:
        b.append("Value")
    if (_safe_float(out.get("quality_score")) or 0) >= 75:
        b.append("Quality")
    if (_safe_float(out.get("momentum_score")) or 0) >= 75:
        b.append("Momentum")
    if (_safe_float(out.get("risk_score")) or 0) >= 75:
        b.append("Low Risk")
    if (_safe_float(out.get("opportunity_score")) or 0) >= 80:
        b.append("Top Opportunity")
    return b


def _recommendation(out: Dict[str, Any]) -> str:
    overall = _safe_float(out.get("overall_score")) or 0.0
    risk = _safe_float(out.get("risk_score")) or 50.0
    mom = _safe_float(out.get("momentum_score")) or 50.0

    if overall >= 78 and risk >= 55:
        return "BUY"
    if overall >= 70 and mom >= 60:
        return "BUY"
    if overall < 40 and mom < 40:
        return "SELL"
    if overall < 50 and risk < 35:
        return "REDUCE"
    if overall < 55:
        return "HOLD"
    return "HOLD"


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

    # legacy aliases (often used by older routers)
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
# Engine
# ============================================================
class DataEngine:
    """
    Router-friendly engine that returns dicts (Sheets-safe).

    Accepts optional `settings` (kw or positional) for compatibility with legacy adapter.
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

        # Provider defaults (NO env mutation)
        default_global = (os.getenv("PROVIDERS") or os.getenv("ENABLED_PROVIDERS") or "eodhd,finnhub").strip() or "eodhd,finnhub"
        default_ksa = (os.getenv("KSA_PROVIDERS") or "yahoo_chart,argaam").strip() or "yahoo_chart,argaam"

        # If settings provided, prefer them
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

        # Tadawul provider only if configured
        if _tadawul_configured() and "tadawul" not in self.ksa_providers:
            self.ksa_providers.append("tadawul")

        self.enable_history = _env_bool("ENABLE_HISTORY_ANALYTICS", True)
        self.include_warnings = _env_bool("ENGINE_INCLUDE_WARNINGS", False)

        logger.info(
            "DataEngine v%s | ttl=%ss | maxsize=%s | conc=%s | global=%s | ksa=%s | history=%s | cachetools=%s",
            ENGINE_VERSION,
            self.cache_ttl_sec,
            maxsize,
            self.max_concurrency,
            self.global_providers,
            self.ksa_providers,
            self.enable_history,
            _HAS_CACHETOOLS,
        )

    async def aclose(self) -> None:
        """Best-effort close hook (kept for legacy adapter compatibility)."""
        return None

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
            out = self._placeholder(sym_in, err=f"Engine error: {e}")

        try:
            self._cache[cache_key] = dict(out)
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
                    out.append(self._placeholder(batch[j], err=f"Engine error: {r}"))
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

        # Provider loop (patch-style)
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

        # Alias mapping + derived computations
        _map_common_aliases(out)
        _apply_derived(out)
        out["data_quality"] = _quality_label(out)

        # Forecast aliases -> canonical
        _map_forecast_aliases(out)

        # History analytics (best-effort)
        if self.enable_history:
            _apply_history_analytics(out)

        # Forecast fallback (momentum) if still missing
        _forecast_from_momentum(out)

        # Backward compatible aliases after canonical is ready
        _mirror_forecast_aliases(out)

        # Score + reco + badges
        _compute_scores(out)
        out["badges"] = _badges(out)

        # Always compute recommendation from engine scores (canonical enum)
        out["recommendation"] = _norm_reco(_recommendation(out), default="HOLD")

        # Source
        out["data_source"] = ",".join(_dedup_preserve(used_sources)) if used_sources else "none"

        # If no price => hard error
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


# Back-compat alias names
DataEngineV2 = DataEngine

# ============================================================
# Singleton accessor (preferred by routers)
# ============================================================
_ENGINE_SINGLETON: Optional[DataEngine] = None


def get_engine(settings: Any = None) -> DataEngine:
    """
    Singleton accessor (sync). If you pass settings on first call, it will be used.
    """
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
