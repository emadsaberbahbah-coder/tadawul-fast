```python
# core/data_engine_v2.py  (FULL REPLACEMENT)
"""
core/data_engine_v2.py
===============================================================
UNIFIED DATA ENGINE (v2.7.4) — KSA-SAFE + PROD SAFE (HARDENED)

Goals (what this file guarantees)
- ✅ Never crashes app startup due to optional dependencies (bs4, yfinance, cachetools).
- ✅ KSA-SAFE routing: no accidental EODHD usage for .SR; yfinance for KSA is OFF by default.
- ✅ Strong Yahoo Chart fallback (works when yfinance breaks / Yahoo changes).
- ✅ Adds more “missing data” without extra fragility:
    - Technicals from Yahoo Chart series when available: MA20/MA50, RSI14, MACD, Volatility30D
    - AvgVolume(30D) from series
    - Shares estimate when MarketCap & Price exist (best-effort)
    - Simple valuation/target/upside label (heuristic; can be disabled)
- ✅ Bounded errors + safe merges (never override existing non-null fields).
- ✅ Keeps module-level APIs for backward compatibility:
    get_enriched_quote / get_enriched_quotes / get_quote / get_quotes

KSA routing order (KSA_PROVIDERS):
  - tadawul  (optional, via TADAWUL_QUOTE_URL + optional TADAWUL_HEADERS_JSON)
  - argaam   (best-effort HTML snapshot)
  - yfinance (OPTIONAL last resort; OFF by default for KSA)
  - yahoo_chart (robust fallback; ON by default for KSA)

GLOBAL routing order (PROVIDERS / ENABLED_PROVIDERS):
  - eodhd -> finnhub -> fmp -> yfinance (optional) -> yahoo_chart (optional last-ditch)

Key env toggles (optional)
- ENABLE_YFINANCE=true/false
- ENABLE_YFINANCE_KSA=true/false     (default false)
- ENABLE_YAHOO_CHART_GLOBAL=true/false
- ENABLE_YAHOO_CHART_KSA=true/false
- ENABLE_YAHOO_CHART_SUPPLEMENT=true/false
- ENABLE_TECHNICALS=true/false       (default true)
- ENABLE_SIMPLE_VALUATION=true/false (default true)

Yahoo chart request tuning
- YAHOO_CHART_RANGE (default "3mo")
- YAHOO_CHART_INTERVAL (default "1d")
- YAHOO_CHART_TTL_SEC (default 20)

Notes
- Self-contained (no external yahoo provider module required).
- Designed to be safe under Render + FastAPI async lifecycle.
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
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import httpx
from pydantic import BaseModel, ConfigDict, Field

# -----------------------------------------------------------------------------
# Optional deps (graceful fallback)
# -----------------------------------------------------------------------------
try:
    from cachetools import TTLCache as _TTLCache  # type: ignore
except Exception:  # pragma: no cover
    _TTLCache = None  # type: ignore

try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:  # pragma: no cover
    BeautifulSoup = None  # type: ignore

try:
    import yfinance as yf  # type: ignore
except Exception:  # pragma: no cover
    yf = None  # type: ignore

# Settings shim (core.config is designed to be PROD SAFE)
try:
    from core.config import get_settings  # type: ignore

    settings = get_settings()
except Exception:  # pragma: no cover
    class _FallbackSettings:
        pass

    settings = _FallbackSettings()  # type: ignore

ENGINE_VERSION = "2.7.4"
logger = logging.getLogger("core.data_engine_v2")

# =============================================================================
# Provider URLs (GLOBAL)
# =============================================================================
EODHD_RT_URL = "https://eodhd.com/api/real-time/{symbol}"
EODHD_FUND_URL = "https://eodhd.com/api/fundamentals/{symbol}"
FMP_QUOTE_URL = "https://financialmodelingprep.com/api/v3/quote/{symbol}"
FMP_PROFILE_URL = "https://financialmodelingprep.com/api/v3/profile/{symbol}"
FMP_RATIOS_TTM_URL = "https://financialmodelingprep.com/api/v3/ratios-ttm/{symbol}"
FMP_KEYMETRICS_TTM_URL = "https://financialmodelingprep.com/api/v3/key-metrics-ttm/{symbol}"

FINNHUB_QUOTE_URL = "https://finnhub.io/api/v1/quote"
FINNHUB_PROFILE_URL = "https://finnhub.io/api/v1/stock/profile2"
FINNHUB_METRIC_URL = "https://finnhub.io/api/v1/stock/metric"

# =============================================================================
# KSA (ARGAAM best-effort HTML; TADAWUL optional JSON via env)
# =============================================================================
ARGAAM_PRICES_URLS = [
    "https://www.argaam.com/ar/company/companies-prices/3",
    "https://www.argaam.com/en/company/companies-prices/3",
]
ARGAAM_VOLUME_URLS = [
    "https://www.argaam.com/ar/company/companies-volume/14",
    "https://www.argaam.com/en/company/companies-volume/14",
]

# Yahoo Chart (robust fallback vs yfinance)
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Riyadh is UTC+3 (no DST)
RIYADH_TZ = timezone(timedelta(hours=3))

# =============================================================================
# Minimal TTL cache fallback (if cachetools is missing)
# =============================================================================
class _MiniTTLCache:
    def __init__(self, maxsize: int, ttl: float) -> None:
        self.maxsize = max(50, int(maxsize or 0))
        self.ttl = max(1.0, float(ttl or 0))
        self._d: Dict[Any, Tuple[float, Any]] = {}

    def _purge(self) -> None:
        now = time.time()
        # remove expired
        expired = [k for k, (exp, _) in self._d.items() if exp <= now]
        for k in expired:
            self._d.pop(k, None)
        # enforce maxsize
        if len(self._d) <= self.maxsize:
            return
        # drop oldest expiry first
        items = sorted(self._d.items(), key=lambda kv: kv[1][0])
        for k, _ in items[: max(0, len(items) - self.maxsize)]:
            self._d.pop(k, None)

    def get(self, key: Any, default: Any = None) -> Any:
        self._purge()
        it = self._d.get(key)
        if not it:
            return default
        exp, val = it
        if exp <= time.time():
            self._d.pop(key, None)
            return default
        return val

    def __setitem__(self, key: Any, value: Any) -> None:
        self._purge()
        if len(self._d) >= self.maxsize:
            # drop one item (oldest expiry) before insert
            k_old = min(self._d.items(), key=lambda kv: kv[1][0])[0]
            self._d.pop(k_old, None)
        self._d[key] = (time.time() + self.ttl, value)

# choose cache implementation
def _make_ttl_cache(maxsize: int, ttl: float):
    if _TTLCache is not None:
        try:
            return _TTLCache(maxsize=maxsize, ttl=ttl)
        except Exception:
            return _MiniTTLCache(maxsize=maxsize, ttl=ttl)
    return _MiniTTLCache(maxsize=maxsize, ttl=ttl)

# =============================================================================
# Helpers (env/settings adapters)
# =============================================================================
_TRUTHY = {"1", "true", "yes", "on", "y", "t"}
_FALSY = {"0", "false", "no", "off", "n", "f"}

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_riyadh_iso() -> str:
    return datetime.now(RIYADH_TZ).isoformat()


def _settings_get(name: str) -> Any:
    try:
        if isinstance(settings, dict):
            return settings.get(name)
        return getattr(settings, name, None)
    except Exception:
        return None


def _get_attr_or_env(names: Sequence[str], default: Any = None) -> Any:
    """
    Tries: settings.<name> then env <name> (case-insensitive).
    Returns first non-None, else default.
    """
    for n in names:
        v = _settings_get(n)
        if v is not None:
            return v

    for n in names:
        v = os.getenv(n) or os.getenv(n.upper()) or os.getenv(n.lower())
        if v is not None:
            return v

    return default


def _get_bool(names: Sequence[str], default: bool) -> bool:
    raw = _get_attr_or_env(names, None)
    if isinstance(raw, bool):
        return raw
    if raw is None:
        return default
    s = str(raw).strip().lower()
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default


def _get_int(names: Sequence[str], default: int) -> int:
    raw = _get_attr_or_env(names, None)
    if raw is None:
        return default
    try:
        v = int(str(raw).strip())
        return v if v > 0 else default
    except Exception:
        return default


def _get_float(names: Sequence[str], default: float) -> float:
    raw = _get_attr_or_env(names, None)
    if raw is None:
        return default
    try:
        v = float(str(raw).strip())
        return v if v > 0 else default
    except Exception:
        return default


def _safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def _safe_float(val: Any) -> Optional[float]:
    """
    Robust numeric parser:
    - supports Arabic digits
    - strips %, +, currency tokens, commas
    - supports suffix K/M/B (e.g. 1.2B)
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
        s = s.replace("SAR", "").replace("USD", "").replace("ريال", "").strip()
        s = s.replace("+", "").strip()

        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()

        s = s.replace("%", "").strip()
        s = s.replace(",", "")

        mult = 1.0
        m = re.match(r"^(-?\d+(\.\d+)?)([KMB])$", s, re.IGNORECASE)
        if m:
            num = m.group(1)
            suf = m.group(3).upper()
            if suf == "K":
                mult = 1_000.0
            elif suf == "M":
                mult = 1_000_000.0
            elif suf == "B":
                mult = 1_000_000_000.0
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


def normalize_symbol(raw: str) -> str:
    """
    - KSA numeric => 1120.SR
    - already has suffix => keep (upper)
    - Yahoo-style (^GSPC, GC=F, EURUSD=X) => keep
    - otherwise => .US (AAPL => AAPL.US)
    """
    s = (raw or "").strip().upper()
    if not s:
        return ""

    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", "")

    if any(ch in s for ch in ("=", "^")):
        return s

    if s.isdigit():
        return f"{s}.SR"

    if "." in s:
        return s

    return f"{s}.US"


def is_ksa_symbol(symbol: str) -> bool:
    s = (symbol or "").upper().strip()
    return s.endswith(".SR") or s.isdigit()


def _ksa_code(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    return s.split(".", 1)[0] if "." in s else s


def _fmp_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    return s[:-3] if s.endswith(".US") else s


def _finnhub_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        return s
    if any(ch in s for ch in ("=", "^")):
        return s
    if s.endswith(".US"):
        return s[:-3]
    return s


def _yahoo_symbol(symbol: str) -> str:
    """
    Yahoo endpoints are generally happier with plain tickers (AAPL not AAPL.US).
    Keep special symbols unchanged. Keep .SR unchanged.
    """
    s = (symbol or "").strip().upper()
    if not s:
        return s
    if any(ch in s for ch in ("=", "^")):
        return s
    if s.endswith(".US"):
        return s[:-3]
    return s


def _parse_csv_list(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x).strip().lower() for x in v if str(x).strip()]
    s = str(v).strip()
    if not s:
        return []
    return [x.strip().lower() for x in s.split(",") if x.strip()]


def _dedupe_keep_order(items: Sequence[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in items:
        t = (x or "").strip().lower()
        if not t or t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def _get_enabled_providers() -> List[str]:
    """
    GLOBAL provider order (first match wins):
      - settings.enabled_providers (list)
      - settings.providers_list (list)
      - settings.providers (csv)
      - env ENABLED_PROVIDERS / PROVIDERS (csv)

    Default: eodhd, finnhub, fmp
    """
    for attr in ("enabled_providers", "providers_list"):
        out = _dedupe_keep_order(_parse_csv_list(_settings_get(attr)))
        if out:
            return out

    out = _dedupe_keep_order(_parse_csv_list(_settings_get("providers")))
    if out:
        return out

    env_csv = os.getenv("ENABLED_PROVIDERS") or os.getenv("PROVIDERS") or ""
    out = _dedupe_keep_order(_parse_csv_list(env_csv))
    if out:
        return out

    return ["eodhd", "finnhub", "fmp"]


def _get_enabled_ksa_providers() -> List[str]:
    """
    KSA provider order (first match wins):
      - settings.enabled_ksa_providers (list)
      - settings.ksa_providers (csv or list)
      - env KSA_PROVIDERS (csv)

    Default: tadawul, argaam, yahoo_chart
    """
    for attr in ("enabled_ksa_providers", "ksa_providers"):
        out = _dedupe_keep_order(_parse_csv_list(_settings_get(attr)))
        if out:
            return out

    env_csv = os.getenv("KSA_PROVIDERS") or ""
    out = _dedupe_keep_order(_parse_csv_list(env_csv))
    if out:
        return out

    return ["tadawul", "argaam", "yahoo_chart"]


def _get_key(*names: str) -> Optional[str]:
    for n in names:
        v = _settings_get(n)
        if isinstance(v, str) and v.strip():
            return v.strip()
    for n in names:
        v = os.getenv(n.upper()) or os.getenv(n)
        if v and v.strip():
            return v.strip()
    return None


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _last_non_none(seq: Any) -> Optional[float]:
    """Return last non-None numeric item from a list-like."""
    try:
        if not isinstance(seq, list):
            return None
        for i in range(len(seq) - 1, -1, -1):
            v = _safe_float(seq[i])
            if v is not None:
                return v
    except Exception:
        return None
    return None


def _prev_from_close_series(close_series: Any) -> Optional[float]:
    """Return previous close from a close-series when meta.previousClose is missing."""
    try:
        if not isinstance(close_series, list) or len(close_series) < 2:
            return None
        last = None
        prev = None
        for i in range(len(close_series) - 1, -1, -1):
            v = _safe_float(close_series[i])
            if v is None:
                continue
            if last is None:
                last = v
                continue
            prev = v
            break
        return prev
    except Exception:
        return None


def _is_special_yahoo_symbol(s: str) -> bool:
    ss = (s or "").strip().upper()
    return any(ch in ss for ch in ("^", "="))


def _join_errors(errors: Sequence[str], max_len: int = 900) -> str:
    """Join error strings but keep bounded to avoid oversized payloads/logs."""
    if not errors:
        return ""
    out = " | ".join([e for e in errors if e])
    if len(out) <= max_len:
        return out
    return out[: max_len - 12] + " ...TRUNC..."


# =============================================================================
# Technical indicators (pure python, safe)
# =============================================================================
def _ema(series: List[float], span: int) -> Optional[float]:
    if not series or span <= 1:
        return None
    alpha = 2.0 / (span + 1.0)
    e = series[0]
    for v in series[1:]:
        e = alpha * v + (1 - alpha) * e
    return e


def _rsi14(closes: List[float]) -> Optional[float]:
    if len(closes) < 15:
        return None
    gains = 0.0
    losses = 0.0
    # last 14 changes
    for i in range(-14, 0):
        ch = closes[i] - closes[i - 1]
        if ch >= 0:
            gains += ch
        else:
            losses += -ch
    avg_gain = gains / 14.0
    avg_loss = losses / 14.0
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return _clamp(rsi, 0.0, 100.0)


def _stdev(xs: List[float]) -> Optional[float]:
    n = len(xs)
    if n < 2:
        return None
    mean = sum(xs) / n
    var = sum((x - mean) ** 2 for x in xs) / (n - 1)
    return math.sqrt(var)


def _volatility_30d(closes: List[float]) -> Optional[float]:
    if len(closes) < 31:
        return None
    rets: List[float] = []
    tail = closes[-31:]
    for i in range(1, len(tail)):
        if tail[i - 1] == 0:
            continue
        rets.append((tail[i] / tail[i - 1]) - 1.0)
    sd = _stdev(rets)
    if sd is None:
        return None
    # annualized, in %
    return sd * math.sqrt(252.0) * 100.0


def _macd(closes: List[float]) -> Optional[float]:
    if len(closes) < 35:
        return None
    ema12 = _ema(closes, 12)
    ema26 = _ema(closes, 26)
    if ema12 is None or ema26 is None:
        return None
    macd_line = ema12 - ema26
    # We don’t compute the full series signal line (needs series of MACD values),
    # but we return macd_line as a stable single-number technical feature.
    return macd_line


def _ma(closes: List[float], n: int) -> Optional[float]:
    if len(closes) < n:
        return None
    tail = closes[-n:]
    return sum(tail) / float(n)


# =============================================================================
# Data model (Pydantic v2)
# =============================================================================
class UnifiedQuote(BaseModel):
    model_config = ConfigDict(populate_by_name=True, from_attributes=True, validate_assignment=True)

    # Identity
    symbol: str
    name: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    sub_sector: Optional[str] = None
    market: str = "UNKNOWN"  # KSA / GLOBAL / UNKNOWN
    currency: Optional[str] = None
    listing_date: Optional[str] = None

    # Shares / Float / Cap
    shares_outstanding: Optional[float] = None
    free_float: Optional[float] = None
    market_cap: Optional[float] = None
    free_float_market_cap: Optional[float] = None

    # Prices
    current_price: Optional[float] = None
    previous_close: Optional[float] = None
    open: Optional[float] = None
    day_high: Optional[float] = None
    day_low: Optional[float] = None
    high_52w: Optional[float] = None
    low_52w: Optional[float] = None
    position_52w_percent: Optional[float] = None
    price_change: Optional[float] = None
    percent_change: Optional[float] = None

    # Volume / Liquidity
    volume: Optional[float] = None
    avg_volume_30d: Optional[float] = None
    value_traded: Optional[float] = None
    turnover_percent: Optional[float] = None
    liquidity_score: Optional[float] = None

    # Fundamentals
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
    debt_to_equity: Optional[float] = None
    current_ratio: Optional[float] = None
    quick_ratio: Optional[float] = None

    # Technicals
    rsi_14: Optional[float] = None
    volatility_30d: Optional[float] = None
    macd: Optional[float] = None
    ma20: Optional[float] = None
    ma50: Optional[float] = None

    # Valuation / Targets
    fair_value: Optional[float] = None
    target_price: Optional[float] = None
    upside_percent: Optional[float] = None
    valuation_label: Optional[str] = None
    analyst_rating: Optional[str] = None

    # Scores / Recommendation
    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    overall_score: Optional[float] = None
    recommendation: Optional[str] = None
    confidence: Optional[float] = None
    risk_score: Optional[float] = None

    # Meta
    data_source: str = "none"
    data_quality: str = "MISSING"  # FULL / PARTIAL / MISSING
    last_updated_utc: str = Field(default_factory=_now_utc_iso)
    last_updated_riyadh: str = Field(default_factory=_now_riyadh_iso)
    error: Optional[str] = None

    # Request tracing
    symbol_input: Optional[str] = None
    symbol_normalized: Optional[str] = None
    status: str = "success"

    def finalize(self) -> "UnifiedQuote":
        if not self.last_updated_utc:
            self.last_updated_utc = _now_utc_iso()
        if not self.last_updated_riyadh:
            self.last_updated_riyadh = _now_riyadh_iso()

        if not self.market or self.market == "UNKNOWN":
            self.market = "KSA" if is_ksa_symbol(self.symbol) else "GLOBAL"

        if not self.currency:
            self.currency = "SAR" if self.market == "KSA" else "USD"

        # status from error
        if self.error and str(self.error).strip():
            self.status = "error"
        else:
            self.status = "success"

        # derived: price change / percent change
        if self.current_price is not None and self.previous_close is not None:
            if self.price_change is None:
                self.price_change = _safe_float(self.current_price - self.previous_close)
            if self.percent_change is None and self.previous_close not in (0, None):
                self.percent_change = _safe_float(
                    (self.current_price - self.previous_close) / self.previous_close * 100.0
                )

        # derived: 52w position
        if (
            self.position_52w_percent is None
            and self.current_price is not None
            and self.high_52w is not None
            and self.low_52w is not None
        ):
            rng = self.high_52w - self.low_52w
            if rng and rng > 0:
                self.position_52w_percent = _safe_float((self.current_price - self.low_52w) / rng * 100.0)

        # value traded (best-effort)
        if self.value_traded is None and self.current_price is not None and self.volume is not None:
            self.value_traded = _safe_float(self.current_price * self.volume)

        # turnover
        if self.turnover_percent is None and self.volume is not None and self.shares_outstanding not in (None, 0):
            self.turnover_percent = _safe_float((self.volume / self.shares_outstanding) * 100.0)

        # liquidity score (0..100) based on value_traded log scale
        if self.liquidity_score is None and self.value_traded is not None and self.value_traded >= 0:
            # 1e4 -> 0, 1e8 -> ~100
            ls = (math.log10(self.value_traded + 1.0) - 4.0) * 25.0
            self.liquidity_score = _safe_float(_clamp(ls, 0.0, 100.0))

        # shares estimate if possible
        if self.shares_outstanding is None and self.market_cap is not None and self.current_price not in (None, 0):
            est = self.market_cap / self.current_price
            if est > 0:
                self.shares_outstanding = _safe_float(est)

        # market cap estimate if possible
        if self.market_cap is None and self.shares_outstanding is not None and self.current_price is not None:
            est = self.shares_outstanding * self.current_price
            if est > 0:
                self.market_cap = _safe_float(est)

        # free float market cap
        if (
            self.free_float_market_cap is None
            and self.market_cap is not None
            and self.free_float is not None
        ):
            # free_float expected as % (0..100) or fraction
            ff = self.free_float
            if ff <= 1.0:
                ff = ff * 100.0
            self.free_float_market_cap = _safe_float(self.market_cap * (ff / 100.0))

        # data_quality heuristic
        if self.current_price is None:
            self.data_quality = "MISSING"
        else:
            have_core = all(
                x is not None for x in (self.current_price, self.previous_close, self.day_high, self.day_low)
            )
            self.data_quality = "FULL" if have_core else "PARTIAL"

        # simple scores if not provided externally
        if self.overall_score is None:
            self._calculate_simple_scores()

        return self

    def _calculate_simple_scores(self) -> None:
        score = 50.0

        # momentum proxy
        if self.percent_change is not None:
            score += 0.8 * max(-10.0, min(10.0, self.percent_change))

        # valuation proxy
        if self.pe_ttm is not None:
            if 0 < self.pe_ttm < 15:
                score += 8
            elif self.pe_ttm > 35:
                score -= 8

        # dividend proxy
        if self.dividend_yield is not None and self.dividend_yield > 0:
            score += min(6.0, self.dividend_yield * 2.0)

        # risk proxy (beta / volatility)
        if self.beta is not None:
            score -= min(10.0, max(0.0, (self.beta - 1.0) * 6.0))
        if self.volatility_30d is not None:
            score -= min(10.0, max(0.0, (self.volatility_30d - 20.0) * 0.3))

        score = max(0.0, min(100.0, score))
        self.overall_score = score

        if self.recommendation:
            return
        if score >= 80:
            self.recommendation = "STRONG BUY"
        elif score >= 65:
            self.recommendation = "BUY"
        elif score <= 35:
            self.recommendation = "SELL"
        else:
            self.recommendation = "HOLD"


# =============================================================================
# Engine
# =============================================================================
class DataEngine:
    """
    Async multi-provider engine with KSA-safe routing.
    """

    def __init__(self) -> None:
        self.enabled_providers: List[str] = _get_enabled_providers()
        self.enabled_ksa_providers: List[str] = _get_enabled_ksa_providers()

        self._timeout_sec: float = _get_float(["HTTP_TIMEOUT_SEC", "http_timeout_sec", "HTTP_TIMEOUT"], 25.0)
        self._retry_attempts: int = _get_int(["HTTP_RETRY_ATTEMPTS"], 3)

        max_conn = _get_int(["HTTP_MAX_CONNECTIONS", "MAX_CONNECTIONS"], 40)
        max_keepalive = _get_int(["HTTP_MAX_KEEPALIVE", "MAX_KEEPALIVE_CONNECTIONS"], 20)

        timeout = httpx.Timeout(self._timeout_sec, connect=min(10.0, self._timeout_sec))

        self.client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.8,ar;q=0.6",
            },
            limits=httpx.Limits(max_keepalive_connections=max_keepalive, max_connections=max_conn),
        )

        # Tadawul provider (resolved at init, not import time)
        self.tadawul_quote_url: Optional[str] = _safe_str(
            _get_attr_or_env(["TADAWUL_QUOTE_URL", "TADAWUL_API_URL"], None)
        )
        self._tadawul_headers: Dict[str, str] = {}
        tad_headers_json = _safe_str(_get_attr_or_env(["TADAWUL_HEADERS_JSON"], None))
        if tad_headers_json:
            try:
                obj = json.loads(tad_headers_json)
                if isinstance(obj, dict):
                    self._tadawul_headers = {str(k): str(v) for k, v in obj.items()}
            except Exception:
                self._tadawul_headers = {}

        # Global yfinance switch
        self.enable_yfinance = _get_bool(["ENABLE_YFINANCE", "enable_yfinance"], True)

        # KSA yfinance policy (HARDENED): OFF by default
        self.enable_yfinance_ksa = (
            self.enable_yfinance
            and _get_bool(
                ["ENABLE_YFINANCE_KSA", "ENABLE_YFINANCE_FOR_KSA", "KSA_ENABLE_YFINANCE", "KSA_ALLOW_YFINANCE"],
                False,
            )
        )

        # Yahoo Chart for KSA + GLOBAL
        self.enable_yahoo_chart_ksa = _get_bool(
            ["ENABLE_YAHOO_CHART_KSA", "KSA_ENABLE_YAHOO_CHART", "KSA_ALLOW_YAHOO_CHART"],
            True,
        )
        self.enable_yahoo_chart_global = _get_bool(["ENABLE_YAHOO_CHART_GLOBAL"], True)

        # Supplement mode: fill missing core fields from Yahoo Chart
        self.enable_yahoo_chart_supplement = _get_bool(["ENABLE_YAHOO_CHART_SUPPLEMENT"], True)

        # Special symbol Yahoo Chart support
        self.enable_yahoo_chart_special = _get_bool(["ENABLE_YAHOO_CHART_SPECIAL"], True)

        # More data toggles
        self.enable_technicals = _get_bool(["ENABLE_TECHNICALS", "ENABLE_YAHOO_TECHNICALS"], True)
        self.enable_simple_valuation = _get_bool(["ENABLE_SIMPLE_VALUATION"], True)

        quote_ttl = _get_float(["QUOTE_TTL_SEC", "quote_ttl_sec"], 30.0)
        cache_ttl = _get_float(["CACHE_TTL_SEC", "cache_ttl_sec"], 20.0)
        fund_ttl = _get_float(["FUNDAMENTALS_TTL_SEC", "fundamentals_ttl_sec"], 21600.0)
        snap_ttl = _get_float(["ARGAAM_SNAPSHOT_TTL_SEC", "argaam_snapshot_ttl_sec"], 30.0)
        yahoo_ttl = _get_float(["YAHOO_CHART_TTL_SEC", "yahoo_chart_ttl_sec"], 20.0)

        self.quote_cache = _make_ttl_cache(maxsize=3000, ttl=max(5.0, float(quote_ttl)))
        self.stale_cache = _make_ttl_cache(maxsize=5000, ttl=max(120.0, float(cache_ttl) * 10.0))
        self.fund_cache = _make_ttl_cache(maxsize=2500, ttl=max(300.0, float(fund_ttl)))
        self.snapshot_cache = _make_ttl_cache(maxsize=50, ttl=max(10.0, float(snap_ttl)))
        self.yahoo_chart_cache = _make_ttl_cache(maxsize=5000, ttl=max(5.0, float(yahoo_ttl)))

        self._sem = asyncio.Semaphore(_get_int(["ENGINE_CONCURRENCY", "ENRICHED_BATCH_CONCURRENCY"], 10))

        logger.info(
            "DataEngine v%s init | providers=%s | ksa=%s | yfinance_global=%s | yfinance_ksa=%s | "
            "yahoo_chart_ksa=%s | yahoo_chart_global=%s | supplement=%s | technicals=%s | valuation=%s | timeout=%.1fs | retries=%s",
            ENGINE_VERSION,
            ",".join(self.enabled_providers) if self.enabled_providers else "(none)",
            ",".join(self.enabled_ksa_providers) if self.enabled_ksa_providers else "(none)",
            self.enable_yfinance,
            self.enable_yfinance_ksa,
            self.enable_yahoo_chart_ksa,
            self.enable_yahoo_chart_global,
            self.enable_yahoo_chart_supplement,
            self.enable_technicals,
            self.enable_simple_valuation,
            self._timeout_sec,
            self._retry_attempts,
        )

    async def aclose(self) -> None:
        await self.client.aclose()

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------
    def _attach_io(self, q: UnifiedQuote, symbol_input: str, symbol_normalized: str) -> UnifiedQuote:
        try:
            return q.model_copy(update={"symbol_input": symbol_input, "symbol_normalized": symbol_normalized}).finalize()
        except Exception:
            q.symbol_input = symbol_input
            q.symbol_normalized = symbol_normalized
            return q.finalize()

    async def get_quote(self, symbol: str) -> UnifiedQuote:
        raw_in = str(symbol or "").strip()
        s = normalize_symbol(raw_in)

        if not raw_in:
            return UnifiedQuote(
                symbol="",
                symbol_input="",
                symbol_normalized="",
                data_quality="MISSING",
                error="Empty symbol",
                status="error",
            ).finalize()

        if not s:
            return UnifiedQuote(
                symbol=raw_in,
                symbol_input=raw_in,
                symbol_normalized=raw_in,
                data_quality="MISSING",
                error="Invalid symbol",
                status="error",
            ).finalize()

        hit = self.quote_cache.get(s)
        if isinstance(hit, UnifiedQuote):
            return self._attach_io(hit, raw_in, s)

        stale = self.stale_cache.get(s)
        stale_q = stale if isinstance(stale, UnifiedQuote) else None

        try:
            async with self._sem:
                q = await (self._fetch_ksa(s) if is_ksa_symbol(s) else self._fetch_global(s))

            q = q.finalize()

            # Optional scoring module (no hard dependency)
            try:
                from core.scoring_engine import enrich_with_scores  # type: ignore

                q = enrich_with_scores(q)
            except Exception:
                pass

            self.quote_cache[s] = q
            self.stale_cache[s] = q
            return self._attach_io(q, raw_in, s)

        except Exception as exc:
            logger.exception("get_quote failed for %s", s, exc_info=exc)
            msg = str(exc).strip() or exc.__class__.__name__
            if stale_q is not None:
                try:
                    stale2 = stale_q.model_copy(update={"error": f"Live fetch failed; returned stale. {msg}"})
                    return self._attach_io(stale2, raw_in, s)
                except Exception:
                    return self._attach_io(stale_q, raw_in, s)
            return UnifiedQuote(
                symbol=s,
                symbol_input=raw_in,
                symbol_normalized=s,
                data_quality="MISSING",
                error=msg,
                status="error",
            ).finalize()

    async def get_quotes(self, symbols: Sequence[str]) -> List[UnifiedQuote]:
        items = [s for s in (symbols or []) if s and str(s).strip()]
        if not items:
            return []
        results = await asyncio.gather(*(self.get_quote(s) for s in items), return_exceptions=True)

        out: List[UnifiedQuote] = []
        for raw_sym, res in zip(items, results):
            if isinstance(res, Exception):
                sym_in = str(raw_sym or "").strip()
                sym_norm = normalize_symbol(sym_in) or sym_in
                out.append(
                    UnifiedQuote(
                        symbol=sym_norm or sym_in,
                        symbol_input=sym_in,
                        symbol_normalized=sym_norm,
                        data_quality="MISSING",
                        error=str(res).strip() or res.__class__.__name__,
                        status="error",
                    ).finalize()
                )
            else:
                out.append(res)
        return out

    async def get_enriched_quote(self, symbol: str) -> UnifiedQuote:
        return await self.get_quote(symbol)

    async def get_enriched_quotes(self, symbols: Sequence[str]) -> List[UnifiedQuote]:
        return await self.get_quotes(symbols)

    # -------------------------------------------------------------------------
    # Safe merge helpers (never override existing non-null)
    # -------------------------------------------------------------------------
    def _merge_missing(self, base: UnifiedQuote, src: UnifiedQuote, fields: Sequence[str]) -> UnifiedQuote:
        upd: Dict[str, Any] = {}
        for k in fields:
            if getattr(base, k) is None and getattr(src, k) is not None:
                upd[k] = getattr(src, k)
        if not upd:
            return base
        try:
            return base.model_copy(update=upd)
        except Exception:
            for k, v in upd.items():
                setattr(base, k, v)
            return base

    # -------------------------------------------------------------------------
    # Supplement helper: fill missing core fields from Yahoo Chart
    # -------------------------------------------------------------------------
    async def _supplement_from_yahoo_chart(self, q: UnifiedQuote, symbol: str, market: str) -> UnifiedQuote:
        if not self.enable_yahoo_chart_supplement:
            return q
        if q.current_price is None:
            return q
        if (q.data_source or "").lower() == "yahoo_chart":
            return q

        if market == "KSA" and not self.enable_yahoo_chart_ksa:
            return q
        if market == "GLOBAL" and not self.enable_yahoo_chart_global:
            return q

        need = any(
            getattr(q, k) is None
            for k in (
                "previous_close",
                "open",
                "day_high",
                "day_low",
                "volume",
                "high_52w",
                "low_52w",
                "market_cap",
                "avg_volume_30d",
                "ma20",
                "ma50",
                "rsi_14",
                "volatility_30d",
                "macd",
            )
        )
        if not need:
            return q

        yc = await self._fetch_yahoo_chart(symbol, market=market)
        if yc.current_price is None:
            return q

        q = self._merge_missing(
            q,
            yc,
            (
                "previous_close",
                "open",
                "day_high",
                "day_low",
                "volume",
                "high_52w",
                "low_52w",
                "market_cap",
                "currency",
                "avg_volume_30d",
                "ma20",
                "ma50",
                "rsi_14",
                "volatility_30d",
                "macd",
            ),
        )

        # heuristic valuation fill (optional)
        if self.enable_simple_valuation:
            q = self._apply_simple_valuation(q)

        return q

    def _apply_simple_valuation(self, q: UnifiedQuote) -> UnifiedQuote:
        if q.current_price is None:
            return q
        # If analyst/valuation already set, do nothing.
        if q.target_price is not None or q.fair_value is not None or q.upside_percent is not None:
            return q

        # Very simple heuristic fair value:
        # - if EPS exists: fair_value = EPS * multiple (default 15)
        # - else: skip
        if q.eps_ttm is None or q.eps_ttm <= 0:
            return q

        multiple = _get_float(["SIMPLE_FAIR_PE"], 15.0)
        fv = q.eps_ttm * multiple
        if fv <= 0:
            return q

        upside = ((fv - q.current_price) / q.current_price) * 100.0 if q.current_price else None
        label = None
        if upside is not None:
            if upside >= 15:
                label = "UNDERVALUED"
            elif upside <= -15:
                label = "OVERVALUED"
            else:
                label = "FAIR"

        upd = {
            "fair_value": _safe_float(fv),
            "target_price": _safe_float(fv),
            "upside_percent": _safe_float(upside) if upside is not None else None,
            "valuation_label": label,
        }
        try:
            return q.model_copy(update={k: v for k, v in upd.items() if v is not None or k == "valuation_label"})
        except Exception:
            for k, v in upd.items():
                if v is not None or k == "valuation_label":
                    setattr(q, k, v)
            return q

    # -------------------------------------------------------------------------
    # KSA
    # -------------------------------------------------------------------------
    async def _fetch_ksa(self, symbol: str) -> UnifiedQuote:
        """
        KSA routing is controlled by self.enabled_ksa_providers.

        HARD POLICY:
          - yfinance will NOT be used for KSA unless ENABLE_YFINANCE_KSA=true
        """
        code = _ksa_code(symbol)
        errors: List[str] = []
        attempted_yahoo_chart = False

        for prov in (self.enabled_ksa_providers or []):
            p = (prov or "").strip().lower()
            if not p:
                continue

            if p == "tadawul":
                if not self.tadawul_quote_url:
                    errors.append("tadawul:no_url")
                    continue
                q = await self._fetch_tadawul_quote(symbol)
                if q.current_price is not None:
                    q.data_source = "tadawul"
                    q.market = "KSA"
                    q.currency = q.currency or "SAR"
                    q = await self._supplement_from_yahoo_chart(q, symbol, "KSA")
                    return q.finalize()
                errors.append(f"tadawul:{q.error or 'no_price'}")

            elif p == "argaam":
                snap = await self._argaam_snapshot()
                row = snap.get(code)
                if row and row.get("price") is not None:
                    q = UnifiedQuote(
                        symbol=symbol,
                        name=row.get("name"),
                        market="KSA",
                        currency="SAR",
                        current_price=row.get("price"),
                        price_change=row.get("change"),
                        percent_change=row.get("change_percent"),
                        volume=row.get("volume"),
                        value_traded=row.get("value_traded"),
                        data_source="argaam",
                        data_quality="PARTIAL",
                        error=None,
                    )
                    q = await self._supplement_from_yahoo_chart(q, symbol, "KSA")
                    return q.finalize()
                errors.append("argaam:no_row")

            elif p in {"yfinance", "yahoo"}:
                if self.enable_yfinance_ksa and yf:
                    q = await self._fetch_yfinance(symbol, market="KSA")
                    if q.current_price is not None:
                        q.data_source = "yfinance"
                        q = await self._supplement_from_yahoo_chart(q, symbol, "KSA")
                        return q.finalize()
                    errors.append(f"yfinance:{q.error or 'no_price'}")
                else:
                    errors.append("yfinance:ksa_disabled_or_missing")

            elif p in {"yahoo_chart", "yahoo-chart", "chart"}:
                attempted_yahoo_chart = True
                if self.enable_yahoo_chart_ksa:
                    q = await self._fetch_yahoo_chart(symbol, market="KSA")
                    if q.current_price is not None:
                        q = self._apply_simple_valuation(q) if self.enable_simple_valuation else q
                        return q.finalize()
                    errors.append(f"yahoo_chart:{q.error or 'no_price'}")
                else:
                    errors.append("yahoo_chart:ksa_disabled")

        # Final KSA fallback only if not already attempted
        if self.enable_yahoo_chart_ksa and not attempted_yahoo_chart:
            q = await self._fetch_yahoo_chart(symbol, market="KSA")
            if q.current_price is not None:
                q = self._apply_simple_valuation(q) if self.enable_simple_valuation else q
                return q.finalize()
            errors.append(f"yahoo_chart:{q.error or 'no_price'}")

        return UnifiedQuote(
            symbol=symbol,
            market="KSA",
            currency="SAR",
            data_quality="MISSING",
            error="No KSA provider data. " + (_join_errors(errors) if errors else "no_ksa_providers_enabled"),
            status="error",
        ).finalize()

    async def _fetch_tadawul_quote(self, symbol: str) -> UnifiedQuote:
        """Optional Tadawul JSON provider. self.tadawul_quote_url should include {code} or {symbol}."""
        code = _ksa_code(symbol)
        url = str(self.tadawul_quote_url or "").strip()
        if not url:
            return UnifiedQuote(
                symbol=symbol,
                market="KSA",
                currency="SAR",
                data_quality="MISSING",
                error="Tadawul URL missing",
                status="error",
            )

        url = url.replace("{code}", code).replace("{symbol}", symbol)

        data = await self._http_get_json(url, params=None, headers=self._tadawul_headers or None)
        if not data:
            return UnifiedQuote(
                symbol=symbol,
                market="KSA",
                currency="SAR",
                data_quality="MISSING",
                error="Tadawul empty response",
                status="error",
            )

        obj: Dict[str, Any] = {}
        if isinstance(data, dict):
            obj = data
        elif isinstance(data, list) and data and isinstance(data[0], dict):
            obj = data[0]
        else:
            return UnifiedQuote(
                symbol=symbol,
                market="KSA",
                currency="SAR",
                data_quality="MISSING",
                error="Tadawul non-dict response",
                status="error",
            )

        price = (
            _safe_float(obj.get("price"))
            or _safe_float(obj.get("last"))
            or _safe_float(obj.get("last_price"))
            or _safe_float(obj.get("lastPrice"))
            or _safe_float(obj.get("tradingPrice"))
            or _safe_float(obj.get("close"))
        )
        prev = _safe_float(obj.get("previous_close")) or _safe_float(obj.get("previousClose")) or _safe_float(
            obj.get("prevClose")
        )
        high = _safe_float(obj.get("high")) or _safe_float(obj.get("day_high")) or _safe_float(obj.get("dayHigh"))
        low = _safe_float(obj.get("low")) or _safe_float(obj.get("day_low")) or _safe_float(obj.get("dayLow"))
        vol = _safe_float(obj.get("volume")) or _safe_float(obj.get("tradedVolume")) or _safe_float(obj.get("qty"))

        chg = _safe_float(obj.get("change")) or _safe_float(obj.get("price_change"))
        chg_p = _pct_if_fraction(obj.get("change_percent") or obj.get("percent_change") or obj.get("changeP"))

        name = _safe_str(obj.get("name")) or _safe_str(obj.get("company")) or _safe_str(obj.get("companyName"))
        currency = _safe_str(obj.get("currency")) or "SAR"

        return UnifiedQuote(
            symbol=symbol,
            name=name,
            market="KSA",
            currency=currency,
            current_price=price,
            previous_close=prev,
            day_high=high,
            day_low=low,
            volume=vol,
            price_change=chg,
            percent_change=chg_p,
            data_source="tadawul",
            data_quality="PARTIAL" if price is not None else "MISSING",
            error=None if price is not None else "Tadawul returned no price",
            status="success" if price is not None else "error",
        )

    async def _argaam_snapshot(self) -> Dict[str, Dict[str, Any]]:
        key = "argaam_snapshot_v3"
        cached = self.snapshot_cache.get(key)
        if isinstance(cached, dict):
            return cached

        merged: Dict[str, Dict[str, Any]] = {}

        prices_html = await self._http_get_first_text(ARGAAM_PRICES_URLS)
        if prices_html:
            part = self._parse_argaam_table(prices_html)
            for k, v in part.items():
                merged.setdefault(k, {}).update(v)

        vol_html = await self._http_get_first_text(ARGAAM_VOLUME_URLS)
        if vol_html:
            part = self._parse_argaam_table(vol_html)
            for k, v in part.items():
                merged.setdefault(k, {}).update(v)

        self.snapshot_cache[key] = merged
        return merged

    async def _http_get_first_text(self, urls: Sequence[str]) -> Optional[str]:
        tasks = [self._http_get_text(u) for u in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        def _looks_ok(txt: str) -> bool:
            t = txt.lower()
            if len(txt) < 500:
                return False
            if "access denied" in t or "cloudflare" in t or "captcha" in t:
                return False
            return True

        for r in results:
            if isinstance(r, str) and _looks_ok(r):
                return r
        for r in results:
            if isinstance(r, str) and r.strip():
                return r
        return None

    def _parse_argaam_table(self, html: str) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        if not BeautifulSoup or not html:
            return out

        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception:
            soup = BeautifulSoup(html, "html.parser")

        for row in soup.find_all("tr"):
            cols = row.find_all("td")
            if len(cols) < 4:
                continue

            txt = [c.get_text(" ", strip=True) for c in cols]

            code = None
            code_idx = None
            for i in range(min(3, len(txt))):
                cand = (txt[i] or "").strip().translate(_ARABIC_DIGITS)
                if re.match(r"^\d{3,6}$", cand):
                    code = cand
                    code_idx = i
                    break
            if not code or code_idx is None:
                continue

            name = _safe_str(txt[code_idx + 1]) if (code_idx + 1) < len(txt) else None

            nums: List[Optional[float]] = []
            for j in range(code_idx + 2, len(txt)):
                nums.append(_safe_float(txt[j]))

            price = nums[0] if len(nums) >= 1 else None
            change = nums[1] if len(nums) >= 2 else None
            chg_p = nums[2] if len(nums) >= 3 else None

            volume = None
            value_traded = None
            tail = txt[-10:] if len(txt) >= 10 else txt
            tail_nums = [_safe_float(x) for x in tail]

            for v in tail_nums:
                if v is not None and v > 0:
                    volume = volume or v

            if price and volume:
                vt_guess = price * volume
                for v in tail_nums:
                    if v is not None and vt_guess > 0 and (0.2 * vt_guess) <= v <= (5.0 * vt_guess):
                        value_traded = v
                        break

            out[code] = {
                "name": name,
                "price": price,
                "change": change,
                "change_percent": chg_p,
                "volume": volume,
                "value_traded": value_traded,
            }

        return out

    # -------------------------------------------------------------------------
    # GLOBAL: provider loop
    # -------------------------------------------------------------------------
    async def _fetch_global(self, symbol: str) -> UnifiedQuote:
        # Special symbols: try Yahoo Chart first (optional), else yfinance
        if _is_special_yahoo_symbol(symbol):
            if self.enable_yahoo_chart_special and self.enable_yahoo_chart_global:
                q = await self._fetch_yahoo_chart(symbol, market="GLOBAL")
                if q.current_price is not None:
                    q.data_source = "yahoo_chart"
                    q = self._apply_simple_valuation(q) if self.enable_simple_valuation else q
                    return q.finalize()

            if self.enable_yfinance and yf:
                q = await self._fetch_yfinance(symbol, market="GLOBAL")
                if q.current_price is not None:
                    q.data_source = "yfinance"
                    q = await self._supplement_from_yahoo_chart(q, symbol, "GLOBAL")
                    return q.finalize()

            return UnifiedQuote(
                symbol=symbol,
                market="GLOBAL",
                currency="USD",
                data_quality="MISSING",
                error="Special symbol requires Yahoo Chart or yfinance (both disabled/unavailable)",
                status="error",
            ).finalize()

        errors: List[str] = []
        attempted_yfinance = False

        for prov in self.enabled_providers:
            p = (prov or "").strip().lower()
            if not p:
                continue

            if p == "eodhd":
                api_key = _get_key("eodhd_api_key", "EODHD_API_KEY")
                if not api_key:
                    errors.append("eodhd:no_api_key")
                    continue
                q = await self._fetch_eodhd_realtime(symbol, api_key)
                if q.current_price is not None:
                    # fundamentals auto-fill (only if missing)
                    if _get_bool(["EODHD_FETCH_FUNDAMENTALS", "FUNDAMENTALS_AUTO"], True):
                        fund = await self._fetch_eodhd_fundamentals(symbol, api_key)
                        if fund:
                            try:
                                q = q.model_copy(update={k: v for k, v in fund.items() if getattr(q, k, None) is None and v is not None})
                            except Exception:
                                for k, v in fund.items():
                                    if getattr(q, k, None) is None and v is not None:
                                        setattr(q, k, v)

                    q.data_source = "eodhd"
                    q.market = "GLOBAL"
                    q.currency = q.currency or "USD"
                    q = await self._supplement_from_yahoo_chart(q, symbol, "GLOBAL")
                    return q.finalize()
                errors.append(f"eodhd:{q.error or 'no_price'}")

            elif p == "finnhub":
                api_key = _get_key("finnhub_api_key", "FINNHUB_API_KEY")
                if not api_key:
                    errors.append("finnhub:no_api_key")
                    continue
                q = await self._fetch_finnhub_quote(symbol, api_key)
                if q.current_price is not None:
                    try:
                        prof, met = await self._fetch_finnhub_profile_and_metrics(symbol, api_key)
                        upd: Dict[str, Any] = {}
                        upd.update({k: v for k, v in prof.items() if v is not None and getattr(q, k, None) is None})
                        upd.update({k: v for k, v in met.items() if v is not None and getattr(q, k, None) is None})
                        if upd:
                            q = q.model_copy(update=upd)
                    except Exception:
                        pass
                    q.data_source = "finnhub"
                    q.market = "GLOBAL"
                    q.currency = q.currency or "USD"
                    q = await self._supplement_from_yahoo_chart(q, symbol, "GLOBAL")
                    return q.finalize()
                errors.append(f"finnhub:{q.error or 'no_price'}")

            elif p == "fmp":
                api_key = _get_key("fmp_api_key", "FMP_API_KEY")
                if not api_key:
                    errors.append("fmp:no_api_key")
                    continue
                q = await self._fetch_fmp_quote(symbol, api_key)
                if q.current_price is not None:
                    # extra FMP fundamentals (safe, cached)
                    if _get_bool(["FMP_FETCH_PROFILE_METRICS", "FUNDAMENTALS_AUTO"], True):
                        try:
                            fund = await self._fetch_fmp_profile_and_ttm(symbol, api_key)
                            if fund:
                                q = q.model_copy(update={k: v for k, v in fund.items() if v is not None and getattr(q, k, None) is None})
                        except Exception:
                            pass

                    q.data_source = "fmp"
                    q.market = "GLOBAL"
                    q.currency = q.currency or "USD"
                    q = await self._supplement_from_yahoo_chart(q, symbol, "GLOBAL")
                    return q.finalize()
                errors.append(f"fmp:{q.error or 'no_price'}")

            elif p in {"yfinance", "yahoo"}:
                attempted_yfinance = True
                if self.enable_yfinance and yf:
                    q = await self._fetch_yfinance(symbol, market="GLOBAL")
                    if q.current_price is not None:
                        q.data_source = "yfinance"
                        q = await self._supplement_from_yahoo_chart(q, symbol, "GLOBAL")
                        return q.finalize()
                    errors.append(f"yfinance:{q.error or 'no_price'}")
                else:
                    errors.append("yfinance:disabled_or_missing")

        # final fallback (GLOBAL only)
        if self.enable_yfinance and yf and not attempted_yfinance:
            q = await self._fetch_yfinance(symbol, market="GLOBAL")
            if q.current_price is not None:
                q.data_source = "yfinance"
                q = await self._supplement_from_yahoo_chart(q, symbol, "GLOBAL")
                return q.finalize()
            errors.append(f"yfinance:{q.error or 'no_price'}")

        # last-ditch yahoo chart (global)
        if self.enable_yahoo_chart_global:
            q = await self._fetch_yahoo_chart(symbol, market="GLOBAL")
            if q.current_price is not None:
                q = self._apply_simple_valuation(q) if self.enable_simple_valuation else q
                return q.finalize()
            errors.append(f"yahoo_chart:{q.error or 'no_price'}")

        return UnifiedQuote(
            symbol=symbol,
            market="GLOBAL",
            currency="USD",
            data_quality="MISSING",
            error="No provider data. " + (_join_errors(errors) if errors else "no_providers_enabled"),
            status="error",
        ).finalize()

    # -------------------------------------------------------------------------
    # HTTP helpers (with light retry)
    # -------------------------------------------------------------------------
    async def _http_get_text(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Optional[str]:
        for attempt in range(max(1, self._retry_attempts)):
            try:
                r = await self.client.get(url, params=params, headers=headers)
                if r.status_code == 429 or 500 <= r.status_code < 600:
                    ra = r.headers.get("Retry-After")
                    if attempt < (self._retry_attempts - 1):
                        if ra and ra.strip().isdigit():
                            await asyncio.sleep(min(2.0, float(ra.strip())))
                        else:
                            await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.35)
                        continue
                    return None
                if r.status_code >= 400:
                    return None
                return r.text
            except Exception:
                if attempt < (self._retry_attempts - 1):
                    await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.35)
                    continue
                return None
        return None

    async def _http_get_json(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Optional[Union[Dict[str, Any], List[Any]]]:
        for attempt in range(max(1, self._retry_attempts)):
            try:
                r = await self.client.get(url, params=params, headers=headers)
                if r.status_code == 429 or 500 <= r.status_code < 600:
                    ra = r.headers.get("Retry-After")
                    if attempt < (self._retry_attempts - 1):
                        if ra and ra.strip().isdigit():
                            await asyncio.sleep(min(2.0, float(ra.strip())))
                        else:
                            await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.35)
                        continue
                    return None
                if r.status_code >= 400:
                    return None
                try:
                    return r.json()
                except Exception:
                    txt = (r.text or "").strip()
                    if txt.startswith("{") or txt.startswith("["):
                        try:
                            return json.loads(txt)
                        except Exception:
                            return None
                    return None
            except Exception:
                if attempt < (self._retry_attempts - 1):
                    await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.35)
                    continue
                return None
        return None

    # -------------------------------------------------------------------------
    # Provider: Yahoo Chart (robust fallback)
    # -------------------------------------------------------------------------
    async def _fetch_yahoo_chart(self, symbol: str, market: str) -> UnifiedQuote:
        """
        Direct Yahoo chart endpoint (no yfinance). Robust for .SR and specials.
        Adds technicals (optional) using close series when available.
        """
        ysym = _yahoo_symbol(symbol)
        cache_key = f"{market}::{ysym}"

        cached = self.yahoo_chart_cache.get(cache_key)
        if isinstance(cached, UnifiedQuote):
            try:
                return cached.model_copy()
            except Exception:
                return cached

        url = YAHOO_CHART_URL.format(symbol=ysym)

        interval = _safe_str(_get_attr_or_env(["YAHOO_CHART_INTERVAL"], "1d")) or "1d"
        # default range increased to allow MA/RSI/Volatility without extra calls
        rng = _safe_str(_get_attr_or_env(["YAHOO_CHART_RANGE"], "3mo")) or "3mo"
        params = {"interval": interval, "range": rng, "includePrePost": "false"}

        data = await self._http_get_json(url, params=params)
        if not data or not isinstance(data, dict):
            q = UnifiedQuote(
                symbol=symbol,
                market=market,
                error="yahoo_chart empty response",
                data_quality="MISSING",
                status="error",
            )
            return q

        try:
            chart = data.get("chart") or {}

            # explicit yahoo error
            ch_err = chart.get("error")
            if ch_err:
                msg = None
                if isinstance(ch_err, dict):
                    msg = _safe_str(ch_err.get("description")) or _safe_str(ch_err.get("message")) or _safe_str(
                        ch_err.get("code")
                    )
                else:
                    msg = _safe_str(ch_err)
                q = UnifiedQuote(
                    symbol=symbol,
                    market=market,
                    error=f"yahoo_chart error: {msg or 'unknown'}",
                    data_source="yahoo_chart",
                    data_quality="MISSING",
                    status="error",
                )
                self.yahoo_chart_cache[cache_key] = q
                return q

            res = (chart.get("result") or [None])[0] or {}
            meta = res.get("meta") or {}

            indicators = (res.get("indicators") or {}).get("quote") or [{}]
            q0 = indicators[0] or {}

            close_series = q0.get("close") or []
            open_series = q0.get("open") or []
            high_series = q0.get("high") or []
            low_series = q0.get("low") or []
            vol_series = q0.get("volume") or []

            closes: List[float] = []
            for v in close_series:
                fv = _safe_float(v)
                if fv is not None:
                    closes.append(fv)

            vols: List[float] = []
            for v in vol_series:
                fv = _safe_float(v)
                if fv is not None:
                    vols.append(fv)

            # price
            cp = (
                _safe_float(meta.get("regularMarketPrice"))
                or _safe_float(meta.get("chartPreviousClose"))
                or (_last_non_none(close_series))
            )

            # prev close
            pc = (
                _safe_float(meta.get("previousClose"))
                or _safe_float(meta.get("chartPreviousClose"))
                or _prev_from_close_series(close_series)
            )

            # OHLC: prefer meta, else series
            op = _safe_float(meta.get("regularMarketOpen")) or _last_non_none(open_series)
            hi = _safe_float(meta.get("regularMarketDayHigh")) or _last_non_none(high_series)
            lo = _safe_float(meta.get("regularMarketDayLow")) or _last_non_none(low_series)

            vol = _safe_float(meta.get("regularMarketVolume")) or _last_non_none(vol_series)

            high_52w = _safe_float(meta.get("fiftyTwoWeekHigh"))
            low_52w = _safe_float(meta.get("fiftyTwoWeekLow"))
            mcap = _safe_float(meta.get("marketCap"))

            currency = _safe_str(meta.get("currency")) or ("SAR" if market == "KSA" else "USD")

            # technicals (optional)
            ma20 = ma50 = rsi = vol30 = macd_v = None
            avg_vol_30d = None
            if self.enable_technicals and closes:
                ma20 = _ma(closes, 20)
                ma50 = _ma(closes, 50)
                rsi = _rsi14(closes)
                vol30 = _volatility_30d(closes)
                macd_v = _macd(closes)

                if len(vols) >= 30:
                    avg_vol_30d = sum(vols[-30:]) / 30.0

            dq = "MISSING" if cp is None else ("FULL" if (pc is not None and hi is not None and lo is not None) else "PARTIAL")

            q = UnifiedQuote(
                symbol=symbol,
                market=market,
                currency=currency,
                current_price=cp,
                previous_close=pc,
                open=op,
                day_high=hi,
                day_low=lo,
                volume=vol,
                high_52w=high_52w,
                low_52w=low_52w,
                market_cap=mcap,
                avg_volume_30d=_safe_float(avg_vol_30d) if avg_vol_30d is not None else None,
                ma20=_safe_float(ma20) if ma20 is not None else None,
                ma50=_safe_float(ma50) if ma50 is not None else None,
                rsi_14=_safe_float(rsi) if rsi is not None else None,
                volatility_30d=_safe_float(vol30) if vol30 is not None else None,
                macd=_safe_float(macd_v) if macd_v is not None else None,
                data_source="yahoo_chart",
                data_quality=dq,
                error=None if cp is not None else "Yahoo chart returned no price",
                status="success" if cp is not None else "error",
            )

            self.yahoo_chart_cache[cache_key] = q
            return q

        except Exception as exc:
            q = UnifiedQuote(
                symbol=symbol,
                market=market,
                error=f"yahoo_chart error: {exc}",
                data_source="yahoo_chart",
                data_quality="MISSING",
                status="error",
            )
            return q

    # -------------------------------------------------------------------------
    # Provider: EODHD
    # -------------------------------------------------------------------------
    async def _fetch_eodhd_realtime(self, symbol: str, api_key: str) -> UnifiedQuote:
        url = EODHD_RT_URL.format(symbol=symbol)
        params = {"api_token": api_key, "fmt": "json"}
        t0 = time.perf_counter()
        try:
            data = await self._http_get_json(url, params=params)
            if not data or not isinstance(data, dict):
                return UnifiedQuote(
                    symbol=symbol, market="GLOBAL", currency="USD", error="EODHD empty response", status="error"
                )

            return UnifiedQuote(
                symbol=symbol,
                market="GLOBAL",
                currency="USD",
                current_price=_safe_float(data.get("close") or data.get("price") or data.get("previousClose")),
                previous_close=_safe_float(data.get("previousClose")),
                open=_safe_float(data.get("open")),
                day_high=_safe_float(data.get("high")),
                day_low=_safe_float(data.get("low")),
                volume=_safe_float(data.get("volume")),
                price_change=_safe_float(data.get("change")),
                percent_change=_safe_float(data.get("change_p")),
                data_source="eodhd",
                data_quality="PARTIAL",
                status="success",
            )
        except Exception as exc:
            dt = int((time.perf_counter() - t0) * 1000)
            return UnifiedQuote(
                symbol=symbol,
                market="GLOBAL",
                currency="USD",
                error=f"EODHD error ({dt}ms): {exc}",
                status="error",
            )

    async def _fetch_eodhd_fundamentals(self, symbol: str, api_key: str) -> Dict[str, Any]:
        cache_key = f"eodhd_fund::{symbol}"
        hit = self.fund_cache.get(cache_key)
        if isinstance(hit, dict):
            return hit

        url = EODHD_FUND_URL.format(symbol=symbol)
        params = {"api_token": api_key, "fmt": "json"}
        data = await self._http_get_json(url, params=params)
        if not data or not isinstance(data, dict):
            self.fund_cache[cache_key] = {}
            return {}

        general = data.get("General") or {}
        highlights = data.get("Highlights") or {}
        valuation = data.get("Valuation") or {}
        technicals = data.get("Technicals") or {}
        shares = data.get("SharesStats") or {}

        out = {
            "name": _safe_str(general.get("Name")),
            "sector": _safe_str(general.get("Sector")),
            "industry": _safe_str(general.get("Industry")),
            "listing_date": _safe_str(general.get("IPODate") or general.get("ListingDate")),
            "market_cap": _safe_float(highlights.get("MarketCapitalization") or general.get("MarketCapitalization")),
            "shares_outstanding": _safe_float(shares.get("SharesOutstanding") or general.get("SharesOutstanding")),
            "eps_ttm": _safe_float(highlights.get("EarningsShare")),
            "pe_ttm": _safe_float(highlights.get("PERatio")),
            "pb": _safe_float(valuation.get("PriceBookMRQ") or highlights.get("PriceToBookMRQ")),
            "ps": _safe_float(valuation.get("PriceSalesTTM") or highlights.get("PriceToSalesTTM")),
            "dividend_yield": _pct_if_fraction(highlights.get("DividendYield")),
            "beta": _safe_float(highlights.get("Beta")),
            "net_margin": _pct_if_fraction(highlights.get("ProfitMargin")),
            "roe": _pct_if_fraction(highlights.get("ReturnOnEquityTTM")),
            "roa": _pct_if_fraction(highlights.get("ReturnOnAssetsTTM")),
            "high_52w": _safe_float(technicals.get("52WeekHigh")),
            "low_52w": _safe_float(technicals.get("52WeekLow")),
            "ma50": _safe_float(technicals.get("50DayMA")),
            "ma20": _safe_float(technicals.get("20DayMA")),
        }

        self.fund_cache[cache_key] = out
        return out

    # -------------------------------------------------------------------------
    # Provider: Finnhub
    # -------------------------------------------------------------------------
    async def _fetch_finnhub_quote(self, symbol: str, api_key: str) -> UnifiedQuote:
        sym = _finnhub_symbol(symbol)
        params = {"symbol": sym, "token": api_key}
        try:
            data = await self._http_get_json(FINNHUB_QUOTE_URL, params=params)
            if not data or not isinstance(data, dict):
                return UnifiedQuote(
                    symbol=symbol, market="GLOBAL", currency="USD", error="Finnhub empty response", status="error"
                )

            cp = _safe_float(data.get("c"))
            pc = _safe_float(data.get("pc"))

            return UnifiedQuote(
                symbol=symbol,
                market="GLOBAL",
                currency="USD",
                current_price=cp,
                previous_close=pc,
                open=_safe_float(data.get("o")),
                day_high=_safe_float(data.get("h")),
                day_low=_safe_float(data.get("l")),
                price_change=_safe_float(data.get("d")),
                percent_change=_safe_float(data.get("dp")),
                data_source="finnhub",
                data_quality="PARTIAL" if cp is not None else "MISSING",
                status="success" if cp is not None else "error",
            )
        except Exception as exc:
            return UnifiedQuote(
                symbol=symbol, market="GLOBAL", currency="USD", error=f"Finnhub error: {exc}", status="error"
            )

    async def _fetch_finnhub_profile_and_metrics(self, symbol: str, api_key: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        sym = _finnhub_symbol(symbol)
        cache_key = f"finnhub_profile_metrics::{sym}"
        hit = self.fund_cache.get(cache_key)
        if isinstance(hit, dict) and hit.get("_combined") is True:
            return hit.get("profile", {}) or {}, hit.get("metrics", {}) or {}

        prof_params = {"symbol": sym, "token": api_key}
        met_params = {"symbol": sym, "metric": "all", "token": api_key}

        prof_data, met_data = await asyncio.gather(
            self._http_get_json(FINNHUB_PROFILE_URL, params=prof_params),
            self._http_get_json(FINNHUB_METRIC_URL, params=met_params),
            return_exceptions=True,
        )

        profile: Dict[str, Any] = {}
        metrics: Dict[str, Any] = {}

        if isinstance(prof_data, dict) and prof_data:
            profile = {
                "name": _safe_str(prof_data.get("name")),
                "currency": _safe_str(prof_data.get("currency")),
                "industry": _safe_str(prof_data.get("finnhubIndustry")),
                "listing_date": _safe_str(prof_data.get("ipo")),
                "market_cap": _safe_float(prof_data.get("marketCapitalization")),
                "shares_outstanding": _safe_float(prof_data.get("shareOutstanding")),
            }

        if isinstance(met_data, dict) and met_data:
            m = met_data.get("metric") or {}
            if isinstance(m, dict):
                metrics = {
                    "high_52w": _safe_float(m.get("52WeekHigh")),
                    "low_52w": _safe_float(m.get("52WeekLow")),
                    "pe_ttm": _safe_float(m.get("peTTM") or m.get("peAnnual")),
                    "pb": _safe_float(m.get("pbAnnual") or m.get("pbQuarterly")),
                    "ps": _safe_float(m.get("psTTM") or m.get("psAnnual")),
                    "eps_ttm": _safe_float(m.get("epsTTM")),
                    "beta": _safe_float(m.get("beta")),
                    "dividend_yield": _pct_if_fraction(
                        m.get("dividendYieldIndicatedAnnual") or m.get("dividendYieldAnnual")
                    ),
                    "roe": _pct_if_fraction(m.get("roeTTM") or m.get("roeAnnual")),
                    "roa": _pct_if_fraction(m.get("roaTTM") or m.get("roaAnnual")),
                    "net_margin": _pct_if_fraction(m.get("netMarginTTM") or m.get("netMarginAnnual")),
                    "debt_to_equity": _safe_float(
                        m.get("totalDebt/totalEquityAnnual") or m.get("totalDebt/totalEquityQuarterly")
                    ),
                    "current_ratio": _safe_float(m.get("currentRatioAnnual") or m.get("currentRatioQuarterly")),
                    "quick_ratio": _safe_float(m.get("quickRatioAnnual") or m.get("quickRatioQuarterly")),
                }

        self.fund_cache[cache_key] = {"_combined": True, "profile": profile, "metrics": metrics}
        return profile, metrics

    # -------------------------------------------------------------------------
    # Provider: FMP
    # -------------------------------------------------------------------------
    async def _fetch_fmp_quote(self, symbol: str, api_key: str) -> UnifiedQuote:
        sym = _fmp_symbol(symbol)
        url = FMP_QUOTE_URL.format(symbol=sym)
        params = {"apikey": api_key}
        try:
            data = await self._http_get_json(url, params=params)
            if not data or not isinstance(data, list):
                return UnifiedQuote(
                    symbol=symbol, market="GLOBAL", currency="USD", error="FMP empty response", status="error"
                )

            item = data[0] if data else {}
            current = _safe_float(item.get("price"))
            prev = _safe_float(item.get("previousClose"))
            return UnifiedQuote(
                symbol=symbol,
                name=_safe_str(item.get("name")),
                market="GLOBAL",
                currency=_safe_str(item.get("currency")) or "USD",
                current_price=current,
                previous_close=prev,
                price_change=_safe_float(item.get("change")),
                percent_change=_safe_float(item.get("changesPercentage")),
                day_high=_safe_float(item.get("dayHigh")),
                day_low=_safe_float(item.get("dayLow")),
                high_52w=_safe_float(item.get("yearHigh")),
                low_52w=_safe_float(item.get("yearLow")),
                volume=_safe_float(item.get("volume")),
                avg_volume_30d=_safe_float(item.get("avgVolume")),
                market_cap=_safe_float(item.get("marketCap")),
                pe_ttm=_safe_float(item.get("pe")),
                eps_ttm=_safe_float(item.get("eps")),
                data_source="fmp",
                data_quality="PARTIAL",
                status="success" if current is not None else "error",
            )
        except Exception as exc:
            return UnifiedQuote(
                symbol=symbol, market="GLOBAL", currency="USD", error=f"FMP error: {exc}", status="error"
            )

    async def _fetch_fmp_profile_and_ttm(self, symbol: str, api_key: str) -> Dict[str, Any]:
        sym = _fmp_symbol(symbol)
        cache_key = f"fmp_profile_ttm::{sym}"
        hit = self.fund_cache.get(cache_key)
        if isinstance(hit, dict):
            return hit

        params = {"apikey": api_key}
        prof_url = FMP_PROFILE_URL.format(symbol=sym)
        ratios_url = FMP_RATIOS_TTM_URL.format(symbol=sym)
        km_url = FMP_KEYMETRICS_TTM_URL.format(symbol=sym)

        prof_data, ratios_data, km_data = await asyncio.gather(
            self._http_get_json(prof_url, params=params),
            self._http_get_json(ratios_url, params=params),
            self._http_get_json(km_url, params=params),
            return_exceptions=True,
        )

        out: Dict[str, Any] = {}

        # profile: list
        if isinstance(prof_data, list) and prof_data:
            p0 = prof_data[0] or {}
            out.update(
                {
                    "name": _safe_str(p0.get("companyName") or p0.get("companyName")),
                    "industry": _safe_str(p0.get("industry")),
                    "sector": _safe_str(p0.get("sector")),
                    "currency": _safe_str(p0.get("currency")),
                    "market_cap": _safe_float(p0.get("mktCap")),
                    "beta": _safe_float(p0.get("beta")),
                    "shares_outstanding": _safe_float(p0.get("sharesOutstanding")),
                    "dividend_yield": _pct_if_fraction(p0.get("lastDiv") and (p0.get("lastDiv") / max(1e-9, (p0.get("price") or 1)))) if False else None,
                }
            )

        # ratios-ttm: list
        if isinstance(ratios_data, list) and ratios_data:
            r0 = ratios_data[0] or {}
            out.update(
                {
                    "pb": _safe_float(r0.get("priceToBookRatioTTM")),
                    "ps": _safe_float(r0.get("priceToSalesRatioTTM")),
                    "payout_ratio": _pct_if_fraction(r0.get("payoutRatioTTM")),
                    "roe": _pct_if_fraction(r0.get("returnOnEquityTTM")),
                    "roa": _pct_if_fraction(r0.get("returnOnAssetsTTM")),
                    "net_margin": _pct_if_fraction(r0.get("netProfitMarginTTM")),
                    "current_ratio": _safe_float(r0.get("currentRatioTTM")),
                    "quick_ratio": _safe_float(r0.get("quickRatioTTM")),
                    "debt_to_equity": _safe_float(r0.get("debtEquityRatioTTM")),
                }
            )

        # key-metrics-ttm: list
        if isinstance(km_data, list) and km_data:
            k0 = km_data[0] or {}
            out.update(
                {
                    "ev_ebitda": _safe_float(k0.get("enterpriseValueOverEBITDATTM")),
                    "dividend_yield": _pct_if_fraction(k0.get("dividendYieldTTM")),
                }
            )

        # clean None keys
        out = {k: v for k, v in out.items() if v is not None}
        self.fund_cache[cache_key] = out
        return out

    # -------------------------------------------------------------------------
    # Provider: yfinance
    # -------------------------------------------------------------------------
    async def _fetch_yfinance(self, symbol: str, market: str) -> UnifiedQuote:
        if not yf:
            return UnifiedQuote(
                symbol=symbol, market=market, error="yfinance not installed", data_quality="MISSING", status="error"
            )

        ysym = _yahoo_symbol(symbol)

        def _sync() -> Dict[str, Any]:
            t = yf.Ticker(ysym)
            try:
                fi = t.fast_info
                if isinstance(fi, dict):
                    return fi
                return {
                    "last_price": getattr(fi, "last_price", None),
                    "previous_close": getattr(fi, "previous_close", None),
                    "open": getattr(fi, "open", None),
                    "day_high": getattr(fi, "day_high", None),
                    "day_low": getattr(fi, "day_low", None),
                    "last_volume": getattr(fi, "last_volume", None),
                    "currency": getattr(fi, "currency", None),
                }
            except Exception:
                hist = t.history(period="5d", interval="1d")
                if hist is None or getattr(hist, "empty", True):
                    return {}
                last = hist.iloc[-1]
                prev = hist.iloc[-2] if len(hist) >= 2 else last
                return {
                    "last_price": float(last.get("Close")) if "Close" in last else None,
                    "previous_close": float(prev.get("Close")) if "Close" in prev else None,
                    "open": float(last.get("Open")) if "Open" in last else None,
                    "day_high": float(last.get("High")) if "High" in last else None,
                    "day_low": float(last.get("Low")) if "Low" in last else None,
                    "last_volume": float(last.get("Volume")) if "Volume" in last else None,
                    "currency": None,
                }

        try:
            info = await asyncio.to_thread(_sync)
            if not info:
                return UnifiedQuote(
                    symbol=symbol, market=market, error="yfinance empty response", data_quality="MISSING", status="error"
                )

            err_txt = str(info)[:800].lower()
            if "unauthorized" in err_txt or "unable to access this feature" in err_txt or "401" in err_txt:
                return UnifiedQuote(
                    symbol=symbol,
                    market=market,
                    error="yfinance blocked/Unauthorized (Yahoo 401). Disable ENABLE_YFINANCE or use other providers.",
                    data_quality="MISSING",
                    status="error",
                )

            cp = _safe_float(info.get("last_price"))
            pc = _safe_float(info.get("previous_close"))
            return UnifiedQuote(
                symbol=symbol,
                market=market,
                currency=_safe_str(info.get("currency")) or ("SAR" if market == "KSA" else "USD"),
                current_price=cp,
                previous_close=pc,
                open=_safe_float(info.get("open")),
                day_high=_safe_float(info.get("day_high")),
                day_low=_safe_float(info.get("day_low")),
                volume=_safe_float(info.get("last_volume")),
                data_source="yfinance",
                data_quality="PARTIAL" if cp is not None else "MISSING",
                status="success" if cp is not None else "error",
            )
        except Exception as exc:
            msg = str(exc)
            if "unauthorized" in msg.lower() or "401" in msg:
                msg = "yfinance blocked/Unauthorized (Yahoo 401)."
            return UnifiedQuote(
                symbol=symbol, market=market, error=f"yfinance error: {msg}", data_quality="MISSING", status="error"
            )


# =============================================================================
# Module-level singleton + API (fallback-compatible)
# =============================================================================
_ENGINE_SINGLETON: Optional[DataEngine] = None
_ENGINE_LOCK = asyncio.Lock()


async def get_engine() -> DataEngine:
    global _ENGINE_SINGLETON
    if _ENGINE_SINGLETON is None:
        async with _ENGINE_LOCK:
            if _ENGINE_SINGLETON is None:
                _ENGINE_SINGLETON = DataEngine()
    return _ENGINE_SINGLETON


async def aclose_engine() -> None:
    global _ENGINE_SINGLETON
    if _ENGINE_SINGLETON is None:
        return
    try:
        await _ENGINE_SINGLETON.aclose()
    finally:
        _ENGINE_SINGLETON = None


async def get_quote(symbol: str) -> UnifiedQuote:
    eng = await get_engine()
    return await eng.get_quote(symbol)


async def get_quotes(symbols: Sequence[str]) -> List[UnifiedQuote]:
    eng = await get_engine()
    return await eng.get_quotes(symbols)


async def get_enriched_quote(symbol: str) -> UnifiedQuote:
    return await get_quote(symbol)


async def get_enriched_quotes(symbols: Sequence[str]) -> List[UnifiedQuote]:
    return await get_quotes(symbols)


__all__ = [
    "UnifiedQuote",
    "DataEngine",
    "normalize_symbol",
    "is_ksa_symbol",
    "ENGINE_VERSION",
    "get_engine",
    "aclose_engine",
    "get_quote",
    "get_quotes",
    "get_enriched_quote",
    "get_enriched_quotes",
]
```
