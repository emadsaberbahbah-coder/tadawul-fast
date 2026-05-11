#!/usr/bin/env python3
# core/providers/yahoo_fundamentals_provider.py
"""
================================================================================
Yahoo Finance Fundamentals Provider -- v6.1.0
================================================================================
FALLBACK FUNDAMENTALS • PROFILE ENRICHMENT • HISTORY AVG-VOLUME FALLBACK
CURRENCY-SANITY GUARD • USER-AGENT ROTATION • EXPONENTIAL BACKOFF
ENGINE-COMPATIBLE • STARTUP-SAFE • SINGLEFLIGHT • CACHE-BACKED • JSON-SAFE

Purpose
-------
Fallback fundamentals/profile source. EODHD remains primary for global
equities and yahoo_chart remains primary for Yahoo-style quote/history data.
This provider fills gaps when Yahoo has richer or more readily available
metadata than the primary source.

v6.1.0 Changes (from v6.0.0)
----------------------------
Bug fixes (data accuracy):
  - CURRENCY-MISMATCH GUARD on 52W high/low. Yahoo's `fast_info` was
    returning 52W high/low from a foreign listing (BP.US -> GBX; RIO.US ->
    GBX; BRK-B.US -> BRK-A USD; CHT.US -> TWD; ASR.US -> MXN; ZTO.US -> HKD;
    PRU.L -> USD) while `current_price` was correctly in the primary
    listing's currency. v6.0.0 wrote the suspect values verbatim, producing
    rows like BRK-B with 52W high = 782,014.25 against a 475.94 price. The
    backend's `upside_synthesis_suspect` warning fired but the bad numbers
    still propagated. v6.1.0 adds `_validate_52w_bounds()` which detects
    these mismatches via the |candidate / current_price| ratio (default
    suspect thresholds: >= 8.0 or <= 0.125), drops the offender, and falls
    back to the 3-month history-derived high/low if those are in-range.
    Emits structured warnings (`week_52_high_unit_mismatch_dropped`,
    `week_52_high_used_history_fallback`, etc.) into the patch's `warnings`
    list so downstream consumers can surface them without re-deriving the
    diagnosis.
  - Bounds are also auto-corrected when high < low (`week_52_high_low_
    inverted`) and an alarm is raised when current price falls outside the
    [low, high] band by more than 5% (`current_price_outside_52w_range`).

Resilience (HTTP 403 / 429 from Yahoo):
  - USER-AGENT ROTATION. Yahoo aggressively 403s requests sharing the
    default urllib/python User-Agent header. v6.1.0 introduces an optional
    `requests.Session` carrying a rotated browser User-Agent (Chrome /
    Firefox / Safari on Win / Mac / Linux). The session is passed to
    `yf.Ticker(symbol, session=session)` on every fetch. UA is re-rotated
    on each retry. Gracefully degrades to bare `yf.Ticker(symbol)` if
    `requests` is unavailable, if the running yfinance version doesn't
    accept the `session` kwarg, or if `YF_USER_AGENT_ROTATION=0`.
  - EXPONENTIAL BACKOFF WITH JITTER. v6.0.0 retried 4 times with a fixed
    0.5s sleep. v6.1.0 uses base = min(8.0, 0.5 * 2**attempt) and adds
    25% jitter. Detects 403/429/rate_limit substrings in the error string
    and lengthens the cooldown to discourage tight-loop rate hammering.
  - Retry count is now configurable via `YF_RETRY_ATTEMPTS` (default: 4).

Observability:
  - Patch output now includes a `warnings: List[str]` field listing every
    field-level data-quality flag raised during the fetch (currency
    mismatch, inverted bounds, history fallback used, etc.). This is the
    same channel the advisor and scoring layer already read from --
    no downstream changes required to surface the new warnings.
  - `last_error_class` is now recorded on full-fail returns alongside
    `error`, so the circuit breaker's status_code argument can be inferred
    more accurately on follow-up.

Cleanup:
  - Re-imported `random` (removed in v6.0.0). Required for UA rotation and
    backoff jitter. Not used anywhere else.
  - Added `requests` as an optional dependency (already pulled in
    transitively by yfinance, so this is a free addition for any working
    install).

v6.0.0 fixes (preserved verbatim from upstream):
  - `self.enabled` is a @property backed by `_configured()` (was missing,
    raised AttributeError on call in v5.5.0)
  - Provider's `semaphore` is now actually acquired around executor calls
  - `fetch_fundamentals_batch` accepts a `concurrency` parameter and gates
    tasks through an asyncio.Semaphore
  - Rate-limiter token acquisition is INSIDE the singleflight callback
  - Lazy singleton via `get_provider()` / `close_provider()`
  - All intra-class asyncio primitives lazy-init on first async use
  - Dead code removed (TraceContext, _trace, json_dumps, unused imports)

Preserved for backward compatibility:
  - Every name in __all__ plus the dataclass field names on the provider
  - All env variable names and defaults from v6.0.0
  - Patch shape (all fundamental/price/analyst fields and legacy aliases:
    price, prev_close, open, change, change_pct, 52w_high, 52w_low,
    forward_pe, pb, ps, peg, net_margin, revenue_growth,
    dividend_yield_percent)
  - DataQuality enum values
  - Redis cache support
  - Prometheus metrics shape

New env variables (v6.1.0):
  - YF_USER_AGENT_ROTATION   (default: 1)   enable session-based UA rotation
  - YF_PRICE_SANITY_GUARD    (default: 1)   enable 52W bounds validation
  - YF_RETRY_ATTEMPTS        (default: 4)   per-fetch retry cap
  - YF_PRICE_RATIO_HIGH      (default: 8.0) suspect-ratio upper threshold
  - YF_PRICE_RATIO_LOW       (default: 0.125) suspect-ratio lower threshold
================================================================================
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import pickle
import random
import re
import time
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Tuple

# =============================================================================
# Logging
# =============================================================================

logger = logging.getLogger("core.providers.yahoo_fundamentals_provider")
logger.addHandler(logging.NullHandler())

# =============================================================================
# Constants
# =============================================================================

PROVIDER_NAME = "yahoo_fundamentals"
PROVIDER_VERSION = "6.1.0"
VERSION = PROVIDER_VERSION
PROVIDER_BATCH_SUPPORTED = True

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}
_KSA_CODE_RE = re.compile(r"^\d{3,6}$", re.IGNORECASE)
_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_K_M_B_T_RE = re.compile(r"^(-?\d+(?:\.\d+)?)([KMBT])$", re.IGNORECASE)
_K_M_B_T_MULT = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}

# v6.1.0: rotated User-Agent pool (modern browsers, ~Q1 2026).
# Yahoo's edge layer 403s the default python-urllib UA; sessions with these
# are accepted at a much higher rate. Order doesn't matter -- callers pick
# uniformly at random and rotate on retry.
_USER_AGENTS: Tuple[str, ...] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 "
    "Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.2210.144",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.2.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:121.0) Gecko/20100101 "
    "Firefox/121.0",
)

# v6.1.0: suspect-ratio thresholds for 52W bounds. If `candidate / current`
# is outside [_PRICE_RATIO_SUSPECT_LOW, _PRICE_RATIO_SUSPECT_HIGH], the
# candidate is treated as a unit/currency mismatch (e.g. GBX high vs USD
# price -> ratio ~ 100). These can be tuned via env vars.
_PRICE_RATIO_SUSPECT_HIGH_DEFAULT = 8.0
_PRICE_RATIO_SUSPECT_LOW_DEFAULT = 0.125

# v6.1.0: substring markers used to detect rate-limit / auth errors in
# yfinance exception text (it doesn't expose status codes cleanly).
_RATE_LIMIT_MARKERS: Tuple[str, ...] = (
    "403", "429", "forbidden", "rate limit", "too many requests",
    "unauthorized", "401",
)

# =============================================================================
# Optional Dependencies (prod-safe)
# =============================================================================

try:
    from redis.asyncio import Redis  # type: ignore
    _REDIS_AVAILABLE = True
except ImportError:
    Redis = None  # type: ignore[assignment]
    _REDIS_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram  # type: ignore
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    Counter = Gauge = Histogram = None  # type: ignore[assignment]
    _PROMETHEUS_AVAILABLE = False

try:
    import yfinance as yf  # type: ignore
    _HAS_YFINANCE = True
except ImportError:
    yf = None  # type: ignore[assignment]
    _HAS_YFINANCE = False

# v6.1.0: requests is required for UA-rotation sessions. It is pulled in
# transitively by yfinance, so this should always be available in any
# environment where the provider can function -- but we degrade gracefully
# if it's not.
try:
    import requests  # type: ignore
    _HAS_REQUESTS = True
except ImportError:
    requests = None  # type: ignore[assignment]
    _HAS_REQUESTS = False


# =============================================================================
# Prometheus Metrics (optional)
# =============================================================================

if _PROMETHEUS_AVAILABLE and Counter and Gauge and Histogram:
    yf_fund_requests_total = Counter(
        "yf_fund_requests_total",
        "Total Yahoo fundamentals provider requests",
        ["status"],
    )
    yf_fund_request_duration = Histogram(
        "yf_fund_request_duration_seconds",
        "Yahoo fundamentals provider request duration (seconds)",
        buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 30.0],
    )
    yf_fund_circuit_breaker_state = Gauge(
        "yf_fund_circuit_breaker_state",
        "Circuit breaker state (0=closed, 1=half_open, 2=open)",
    )
else:
    class _DummyMetric:
        def labels(self, *args: Any, **kwargs: Any) -> "_DummyMetric":
            return self

        def inc(self, *args: Any, **kwargs: Any) -> None:
            return None

        def observe(self, *args: Any, **kwargs: Any) -> None:
            return None

        def set(self, *args: Any, **kwargs: Any) -> None:
            return None

    yf_fund_requests_total = _DummyMetric()
    yf_fund_request_duration = _DummyMetric()
    yf_fund_circuit_breaker_state = _DummyMetric()


# =============================================================================
# Environment Helpers
# =============================================================================

def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    v = str(v).strip() if v is not None else ""
    return v if v else default


def _env_int(name: str, default: int) -> int:
    try:
        v = os.getenv(name)
        return int(float(str(v).strip())) if v is not None else default
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        v = os.getenv(name)
        return float(str(v).strip()) if v is not None else default
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


def _configured() -> bool:
    """Return True if the provider is enabled and yfinance is available."""
    return _env_bool("YF_ENABLED", True) and _HAS_YFINANCE


def _emit_warnings() -> bool:
    return _env_bool("YF_VERBOSE_WARNINGS", False)


def _timeout_sec() -> float:
    return max(5.0, _env_float("YF_TIMEOUT_SEC", 25.0))


def _fund_ttl_sec() -> float:
    return max(300.0, _env_float("YF_FUND_TTL_SEC", 21600.0))


def _err_ttl_sec() -> float:
    return max(5.0, _env_float("YF_ERROR_TTL_SEC", 20.0))


def _max_concurrency() -> int:
    return max(1, _env_int("YF_MAX_CONCURRENCY", 6))


def _rate_limit() -> float:
    return max(0.0, _env_float("YF_RATE_LIMIT_PER_SEC", 4.0))


def _cb_enabled() -> bool:
    return _env_bool("YF_CIRCUIT_BREAKER", True)


def _cb_fail_threshold() -> int:
    return max(2, _env_int("YF_CB_FAIL_THRESHOLD", 6))


def _cb_cooldown_sec() -> float:
    return max(5.0, _env_float("YF_CB_COOLDOWN_SEC", 30.0))


def _enable_redis() -> bool:
    return _env_bool("YF_ENABLE_REDIS", False) and _REDIS_AVAILABLE


def _redis_url() -> str:
    return _env_str("REDIS_URL", "redis://localhost:6379/0")


# v6.1.0: new env helpers

def _ua_rotation_enabled() -> bool:
    """Enable session-based User-Agent rotation (default: on)."""
    return _env_bool("YF_USER_AGENT_ROTATION", True) and _HAS_REQUESTS


def _price_sanity_enabled() -> bool:
    """Enable 52W high/low currency-mismatch validation (default: on)."""
    return _env_bool("YF_PRICE_SANITY_GUARD", True)


def _retry_attempts() -> int:
    """Per-fetch retry attempts (default: 4, minimum: 1)."""
    return max(1, _env_int("YF_RETRY_ATTEMPTS", 4))


def _price_ratio_high() -> float:
    """Upper threshold for the suspect candidate/current ratio."""
    v = _env_float("YF_PRICE_RATIO_HIGH", _PRICE_RATIO_SUSPECT_HIGH_DEFAULT)
    return v if v > 1.0 else _PRICE_RATIO_SUSPECT_HIGH_DEFAULT


def _price_ratio_low() -> float:
    """Lower threshold for the suspect candidate/current ratio."""
    v = _env_float("YF_PRICE_RATIO_LOW", _PRICE_RATIO_SUSPECT_LOW_DEFAULT)
    return v if 0.0 < v < 1.0 else _PRICE_RATIO_SUSPECT_LOW_DEFAULT


# =============================================================================
# Pure Utility Functions
# =============================================================================

def _utc_iso(dt: Optional[datetime] = None) -> str:
    """Get UTC time in ISO format."""
    d = dt or datetime.now(timezone.utc)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).isoformat()


def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    """Get Riyadh time (UTC+3) in ISO format."""
    tz = timezone(timedelta(hours=3))
    d = dt or datetime.now(tz)
    if d.tzinfo is None:
        d = d.replace(tzinfo=tz)
    return d.astimezone(tz).isoformat()


def safe_str(x: Any) -> Optional[str]:
    """Safely convert to non-empty stripped string; None for empty."""
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def safe_float(x: Any) -> Optional[float]:
    """Safely convert to float; handles Arabic digits, K/M/B/T, parens-negative."""
    if x is None:
        return None
    try:
        if isinstance(x, bool):
            return None
        if isinstance(x, (int, float)):
            f = float(x)
            return None if (math.isnan(f) or math.isinf(f)) else f

        s = str(x).strip()
        if not s:
            return None
        if s.lower() in {"n/a", "na", "null", "none", "-", "--"}:
            return None

        s = s.translate(_ARABIC_DIGITS)
        s = s.replace(",", "").strip()

        if s.endswith("%"):
            s = s[:-1].strip()

        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()

        m = _K_M_B_T_RE.match(s)
        if m:
            f = float(m.group(1)) * _K_M_B_T_MULT[m.group(2).upper()]
        else:
            f = float(s)

        return None if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return None


def safe_int(x: Any) -> Optional[int]:
    """Safely convert to int."""
    f = safe_float(x)
    return int(round(f)) if f is not None else None


def _as_fraction(x: Any) -> Optional[float]:
    """
    Convert percent-like value to ratio using the abs>1.5 heuristic.

    Yahoo returns margins/yields inconsistently (sometimes ratios like
    0.025, sometimes percents like 2.5). The heuristic: if |v| > 1.5,
    divide by 100; else return as-is.
    """
    v = safe_float(x)
    if v is None:
        return None
    return v / 100.0 if abs(v) > 1.5 else v


def _pct_from_ratio(numerator: Any, denominator: Any) -> Optional[float]:
    """Compute ratio = numerator / denominator (None-safe, zero-safe)."""
    a = safe_float(numerator)
    b = safe_float(denominator)
    if a is None or b is None or b == 0:
        return None
    return a / b


def clean_patch(p: Dict[str, Any]) -> Dict[str, Any]:
    """Remove None and whitespace-only-string values from a patch dict."""
    return {
        k: v for k, v in (p or {}).items()
        if v is not None and not (isinstance(v, str) and not v.strip())
    }


def normalize_symbol(symbol: str) -> str:
    """
    Normalize symbol to Yahoo Finance format.

    Strips prefixes (TADAWUL:, SAUDI:, KSA:, ETF:, INDEX:) and suffixes
    (.TADAWUL, .SAUDI, .KSA), then appends .SR for numeric KSA codes.

    Examples:
        "2222"           -> "2222.SR"
        "2222.SR"        -> "2222.SR"
        "TADAWUL:2222"   -> "2222.SR"
        "AAPL"           -> "AAPL"
    """
    s = (symbol or "").strip()
    if not s:
        return ""
    s = s.translate(_ARABIC_DIGITS).strip().upper()

    for prefix in ("TADAWUL:", "SAUDI:", "KSA:", "ETF:", "INDEX:"):
        if s.startswith(prefix):
            s = s.split(":", 1)[1].strip()

    for suffix in (".TADAWUL", ".SAUDI", ".KSA"):
        if s.endswith(suffix):
            s = s[: -len(suffix)].strip()

    if _KSA_CODE_RE.match(s):
        return f"{s}.SR"

    if s.endswith(".SR") and _KSA_CODE_RE.match(s[:-3]):
        return s

    return s


def _is_ksa_symbol(norm_symbol: str) -> bool:
    """Check if symbol is a normalized KSA symbol (ends in .SR with numeric code)."""
    u = (norm_symbol or "").strip().upper()
    if u.endswith(".SR"):
        code = u[:-3]
        return bool(_KSA_CODE_RE.match(code))
    return False


def map_recommendation(rec: Optional[str]) -> str:
    """Map Yahoo recommendationKey to canonical buy/sell label."""
    if not rec:
        return "HOLD"
    r = str(rec).lower()
    if "strong_buy" in r or "strongbuy" in r:
        return "STRONG_BUY"
    if "buy" in r:
        return "BUY"
    if "hold" in r or "neutral" in r:
        return "HOLD"
    if "underperform" in r or "reduce" in r:
        return "REDUCE"
    if "sell" in r:
        return "SELL"
    return "HOLD"


def _get_attr(obj: Any, *names: str) -> Any:
    """Get attribute or dict-key by multiple possible names."""
    if obj is None:
        return None
    for name in names:
        try:
            if isinstance(obj, dict) and name in obj:
                return obj.get(name)
            if hasattr(obj, name):
                return getattr(obj, name)
        except Exception:
            continue
    return None


def _pick(info: Dict[str, Any], *names: str) -> Any:
    """Pick first existing key from dict (matches on presence, not truthiness)."""
    for name in names:
        if name in info:
            return info.get(name)
    return None


def _coalesce(*vals: Any) -> Any:
    """Return first non-None non-empty-string value."""
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return v
    return None


def _infer_asset_class(info: Dict[str, Any], norm_symbol: str) -> Optional[str]:
    """Infer asset class from info dict and symbol suffix."""
    q = safe_str(_pick(info, "quoteType", "instrumentType", "typeDisp"))
    if q:
        qn = q.strip().upper().replace(" ", "_")
        mapping = {
            "EQUITY": "Equity",
            "ETF": "ETF",
            "MUTUALFUND": "Mutual Fund",
            "MUTUAL_FUND": "Mutual Fund",
            "INDEX": "Index",
            "CURRENCY": "Currency",
            "CRYPTOCURRENCY": "Crypto",
            "FUTURE": "Future",
            "FUTURES": "Future",
            "OPTION": "Option",
        }
        if qn in mapping:
            return mapping[qn]
    if norm_symbol.endswith("=X"):
        return "Currency"
    if norm_symbol.endswith("=F"):
        return "Future"
    if _is_ksa_symbol(norm_symbol):
        return "Equity"
    return None


# =============================================================================
# v6.1.0: User-Agent Rotation + Session Factory
# =============================================================================

def _pick_random_ua() -> str:
    """Pick a User-Agent string uniformly at random from the pool."""
    return random.choice(_USER_AGENTS)


def _create_yf_session() -> Optional[Any]:
    """
    Create a requests.Session pre-loaded with a rotated browser User-Agent.

    Returns None if:
      - requests is not installed, OR
      - YF_USER_AGENT_ROTATION is explicitly disabled.

    The session is suitable to pass into yfinance:
        yf.Ticker(symbol, session=_create_yf_session())

    Newer yfinance versions accept the `session` kwarg and route their
    underlying HTTP calls through it; older versions ignore it (we wrap
    the constructor in try/except to handle that).
    """
    if not _ua_rotation_enabled() or not _HAS_REQUESTS or requests is None:
        return None
    try:
        sess = requests.Session()
        sess.headers.update({
            "User-Agent": _pick_random_ua(),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "*/*;q=0.8"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        })
        return sess
    except Exception as exc:
        logger.debug("yf session creation failed: %s", exc)
        return None


def _rotate_session_ua(session: Any) -> None:
    """Rotate the User-Agent header on an existing session (in-place)."""
    if session is None:
        return
    try:
        session.headers["User-Agent"] = _pick_random_ua()
    except Exception:
        pass


def _is_rate_limit_error(err: Optional[BaseException]) -> bool:
    """Heuristic: does the error string look like a 403 / 429 / rate-limit?"""
    if err is None:
        return False
    try:
        s = str(err).lower()
    except Exception:
        return False
    return any(m in s for m in _RATE_LIMIT_MARKERS)


# =============================================================================
# v6.1.0: 52W Bounds Validation (Currency-Mismatch Guard)
# =============================================================================

def _is_suspect_price_ratio(ref: Optional[float], candidate: Optional[float]) -> bool:
    """
    Return True if `candidate` is suspiciously far from `ref` (likely a
    currency/scale mismatch -- e.g. GBX value paired with a USD reference).

    Both must be positive finite floats. The ratio thresholds default to
    [0.125, 8.0] and can be tuned via YF_PRICE_RATIO_LOW / _HIGH env vars.
    """
    if ref is None or candidate is None:
        return False
    if ref <= 0 or candidate <= 0:
        return False
    ratio = candidate / ref
    return ratio >= _price_ratio_high() or ratio <= _price_ratio_low()


def _validate_52w_bounds(
    current_price: Optional[float],
    week_52_high: Optional[float],
    week_52_low: Optional[float],
    hist_52w_high: Optional[float],
    hist_52w_low: Optional[float],
) -> Tuple[Optional[float], Optional[float], List[str]]:
    """
    Validate the 52W high/low against `current_price` for unit consistency.

    Symptoms this catches (observed in production):
      BP.US   -- 52W high 609.40 (GBX from LSE listing) vs price 43.34 USD
      RIO.US  -- 52W high 7,834.00 (GBX) vs price 105.38 USD
      BRK-B.US -- 52W high 782,014.25 (BRK-A USD) vs price 475.94 USD
      CHT.US  -- 52W high 138.00 (TWD) vs price 43.81 USD
      ASR.US  -- 52W high 690.99 (MXN) vs price 308.99 USD
      ZTO.US  -- 52W high 205.60 (HKD) vs price 25.05 USD
      PRU.L   -- 52W high 119.76 (USD ADR) vs price 1,135.00 GBX

    Behavior:
      1. If a candidate is suspect, drop it (-> None) and emit
         `week_52_<bound>_unit_mismatch_dropped`.
      2. If the history-derived fallback (3mo window, not strictly 1y but
         a strong sanity check) is in-range, use it and emit
         `week_52_<bound>_used_history_fallback`.
      3. If high < low after validation, swap them and emit
         `week_52_high_low_inverted`.
      4. If current_price is more than 5% outside the validated [low, high]
         band, emit `current_price_outside_52w_range` (informational only;
         the bounds are not rewritten).

    Returns: (validated_high, validated_low, warnings)
    """
    warnings: List[str] = []

    if not _price_sanity_enabled():
        return week_52_high, week_52_low, warnings

    cp = safe_float(current_price)
    high = safe_float(week_52_high)
    low = safe_float(week_52_low)
    hh = safe_float(hist_52w_high)
    hl = safe_float(hist_52w_low)

    # Without a usable reference price we can't validate ratios. Just pass
    # through unchanged.
    if cp is None or cp <= 0:
        return high, low, warnings

    # Validate high
    if high is not None and _is_suspect_price_ratio(cp, high):
        warnings.append("week_52_high_unit_mismatch_dropped")
        if hh is not None and not _is_suspect_price_ratio(cp, hh):
            high = hh
            warnings.append("week_52_high_used_history_fallback")
        else:
            high = None

    # Validate low
    if low is not None and _is_suspect_price_ratio(cp, low):
        warnings.append("week_52_low_unit_mismatch_dropped")
        if hl is not None and not _is_suspect_price_ratio(cp, hl):
            low = hl
            warnings.append("week_52_low_used_history_fallback")
        else:
            low = None

    # Sanity: high should be >= low
    if high is not None and low is not None and high < low:
        warnings.append("week_52_high_low_inverted")
        high, low = low, high

    # Informational: current price should sit in the validated band
    if high is not None and cp > high * 1.05:
        warnings.append("current_price_outside_52w_range")
    elif low is not None and cp < low * 0.95:
        warnings.append("current_price_outside_52w_range")

    return high, low, warnings


# =============================================================================
# Data Quality
# =============================================================================

class DataQuality(str, Enum):
    """Data quality levels."""
    EXCELLENT = "EXCELLENT"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    STALE = "STALE"
    ERROR = "ERROR"
    MISSING = "MISSING"


def data_quality_score(patch: Dict[str, Any]) -> Tuple[DataQuality, float]:
    """Score data quality based on field completeness; returns (quality, score 0-100)."""
    score = 0.0

    if safe_str(patch.get("symbol")):
        score += 8
    if safe_str(patch.get("name")):
        score += 6
    if safe_str(patch.get("currency")):
        score += 3
    if safe_str(patch.get("exchange")):
        score += 3
    if safe_str(patch.get("asset_class")):
        score += 3

    cp = safe_float(patch.get("current_price"))
    if cp is not None and cp > 0:
        score += 10
    else:
        score -= 8

    if safe_float(patch.get("market_cap")) is not None:
        score += 8

    if safe_float(patch.get("pe_ttm")) is not None:
        score += 6
    if safe_float(patch.get("pb_ratio") or patch.get("pb")) is not None:
        score += 6
    if safe_float(patch.get("ps_ratio") or patch.get("ps")) is not None:
        score += 5
    if safe_float(patch.get("peg_ratio") or patch.get("peg")) is not None:
        score += 4
    if safe_float(patch.get("ev_ebitda")) is not None:
        score += 4

    if _as_fraction(patch.get("profit_margin") or patch.get("net_margin")) is not None:
        score += 6
    if _as_fraction(patch.get("gross_margin")) is not None:
        score += 4
    if _as_fraction(patch.get("operating_margin")) is not None:
        score += 4
    if _as_fraction(patch.get("roe")) is not None:
        score += 5

    if _as_fraction(patch.get("revenue_growth_yoy") or patch.get("revenue_growth")) is not None:
        score += 6
    if safe_float(patch.get("revenue_ttm")) is not None:
        score += 5
    if safe_float(patch.get("free_cash_flow_ttm") or patch.get("free_cashflow")) is not None:
        score += 5

    if safe_float(patch.get("target_mean_price")) is not None:
        score += 5
    if safe_int(patch.get("analyst_count")) is not None:
        score += 3

    score = max(0.0, min(100.0, score))

    if score >= 85:
        return DataQuality.EXCELLENT, score
    if score >= 70:
        return DataQuality.HIGH, score
    if score >= 55:
        return DataQuality.MEDIUM, score
    if score >= 35:
        return DataQuality.LOW, score
    return DataQuality.MISSING, score


# =============================================================================
# Async Primitives (lazy locks)
# =============================================================================

@dataclass(slots=True)
class CacheStats:
    """Cache statistics."""
    hits: int = 0
    misses: int = 0
    sets: int = 0
    evictions: int = 0
    size: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {
            "hits": self.hits,
            "misses": self.misses,
            "sets": self.sets,
            "evictions": self.evictions,
            "size": self.size,
        }


class AdvancedCache:
    """Async LRU+TTL cache with optional Redis L2."""

    def __init__(self, name: str, maxsize: int, ttl: float, use_redis: bool, redis_url: str):
        self.name = name
        self.maxsize = max(50, int(maxsize))
        self.ttl = float(ttl)
        self.use_redis = bool(use_redis and _REDIS_AVAILABLE and Redis)
        self.redis_url = redis_url
        self._mem: Dict[str, Tuple[Any, float]] = {}
        self._touch: Dict[str, float] = {}
        self._lock: Optional[asyncio.Lock] = None  # lazy
        self.stats = CacheStats()
        self._redis: Any = None
        if self.use_redis:
            try:
                self._redis = Redis.from_url(self.redis_url, decode_responses=False)  # type: ignore
            except Exception as exc:
                logger.warning("Redis cache init failed (%s): %s", self.name, exc)
                self._redis = None
                self.use_redis = False

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _key(self, prefix: str) -> str:
        h = hashlib.sha256(prefix.encode("utf-8")).hexdigest()[:16]
        return f"yffund:{self.name}:{h}:{prefix}"

    def _evict_lru(self) -> None:
        if not self._touch:
            return
        oldest = min(self._touch.items(), key=lambda kv: kv[1])[0]
        self._mem.pop(oldest, None)
        self._touch.pop(oldest, None)
        self.stats.evictions += 1

    async def get(self, prefix: str) -> Optional[Any]:
        """Get value from cache (memory first, then Redis)."""
        k = self._key(prefix)
        now = time.monotonic()
        async with self._get_lock():
            if k in self._mem:
                v, exp = self._mem[k]
                if now < exp:
                    self._touch[k] = now
                    self.stats.hits += 1
                    return v
                self._mem.pop(k, None)
                self._touch.pop(k, None)

        if self.use_redis and self._redis:
            try:
                blob = await self._redis.get(k)
                if blob is not None:
                    val = pickle.loads(zlib.decompress(blob)) if blob else {}
                    async with self._get_lock():
                        if len(self._mem) >= self.maxsize:
                            self._evict_lru()
                        self._mem[k] = (val, now + self.ttl)
                        self._touch[k] = now
                        self.stats.hits += 1
                        self.stats.size = len(self._mem)
                    return val
            except Exception:
                pass

        self.stats.misses += 1
        return None

    async def set(self, prefix: str, value: Any, ttl: Optional[float] = None) -> None:
        """Set value in cache (memory + Redis)."""
        k = self._key(prefix)
        exp = time.monotonic() + float(ttl or self.ttl)
        now = time.monotonic()
        async with self._get_lock():
            if len(self._mem) >= self.maxsize and k not in self._mem:
                self._evict_lru()
            self._mem[k] = (value, exp)
            self._touch[k] = now
            self.stats.sets += 1
            self.stats.size = len(self._mem)

        if self.use_redis and self._redis:
            try:
                blob = zlib.compress(pickle.dumps(value))
                await self._redis.setex(k, int(ttl or self.ttl), blob)
            except Exception:
                pass

    async def close(self) -> None:
        """Close Redis connection if open."""
        if self.use_redis and self._redis:
            try:
                await self._redis.close()
            except Exception:
                pass
        self._redis = None

    async def size(self) -> int:
        """Current memory cache size."""
        async with self._get_lock():
            return len(self._mem)


class TokenBucket:
    """Token bucket rate limiter."""

    def __init__(self, rate_per_sec: float):
        self.rate = max(0.0, float(rate_per_sec))
        self.capacity = max(1.0, self.rate * 2.0) if self.rate > 0 else 1.0
        self.tokens = self.capacity
        self.last = time.monotonic()
        self._lock: Optional[asyncio.Lock] = None  # lazy

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def wait_and_acquire(self, tokens: float = 1.0) -> None:
        """Wait until `tokens` are available, then deduct them."""
        if self.rate <= 0:
            return
        need = float(tokens)
        while True:
            async with self._get_lock():
                now = time.monotonic()
                self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.rate)
                self.last = now
                if self.tokens >= need:
                    self.tokens -= need
                    return
                wait = (need - self.tokens) / self.rate
            await asyncio.sleep(min(1.0, max(0.01, wait)))


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    HALF_OPEN = "half_open"
    OPEN = "open"

    def to_numeric(self) -> float:
        return {CircuitState.CLOSED: 0.0, CircuitState.HALF_OPEN: 1.0, CircuitState.OPEN: 2.0}[self]


@dataclass(slots=True)
class CircuitBreakerStats:
    """Circuit breaker statistics."""
    state: CircuitState = CircuitState.CLOSED
    failures: int = 0
    successes: int = 0
    last_failure_ts: float = 0.0
    open_until_ts: float = 0.0
    cooldown_sec: float = 30.0


class AdvancedCircuitBreaker:
    """Circuit breaker with half-open probes."""

    def __init__(self, fail_threshold: int, cooldown_sec: float):
        self.fail_threshold = max(1, int(fail_threshold))
        self.stats = CircuitBreakerStats(cooldown_sec=float(cooldown_sec))
        self._lock: Optional[asyncio.Lock] = None  # lazy
        self._half_open_probe_used = False

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def allow_request(self) -> bool:
        """Return True if request is allowed through."""
        if not _cb_enabled():
            return True
        async with self._get_lock():
            now = time.monotonic()
            if self.stats.state == CircuitState.CLOSED:
                yf_fund_circuit_breaker_state.set(self.stats.state.to_numeric())
                return True
            if self.stats.state == CircuitState.OPEN:
                if now >= self.stats.open_until_ts:
                    self.stats.state = CircuitState.HALF_OPEN
                    self._half_open_probe_used = False
                    yf_fund_circuit_breaker_state.set(self.stats.state.to_numeric())
                    return True
                yf_fund_circuit_breaker_state.set(self.stats.state.to_numeric())
                return False
            if not self._half_open_probe_used:
                self._half_open_probe_used = True
                yf_fund_circuit_breaker_state.set(self.stats.state.to_numeric())
                return True
            yf_fund_circuit_breaker_state.set(self.stats.state.to_numeric())
            return False

    async def on_success(self) -> None:
        """Record a successful request."""
        if not _cb_enabled():
            return
        async with self._get_lock():
            self.stats.successes += 1
            self.stats.state = CircuitState.CLOSED
            self.stats.failures = 0
            self._half_open_probe_used = False
            yf_fund_circuit_breaker_state.set(self.stats.state.to_numeric())

    async def on_failure(self, status_code: int = 500) -> None:
        """Record a failed request (may open the breaker)."""
        if not _cb_enabled():
            return
        async with self._get_lock():
            now = time.monotonic()
            self.stats.failures += 1
            self.stats.last_failure_ts = now
            cooldown = self.stats.cooldown_sec
            if status_code in (401, 403, 429):
                cooldown = min(300.0, cooldown * 1.5)
            if self.stats.state == CircuitState.HALF_OPEN:
                self.stats.state = CircuitState.OPEN
                self.stats.open_until_ts = now + min(300.0, cooldown * 2)
            elif self.stats.failures >= self.fail_threshold:
                self.stats.state = CircuitState.OPEN
                self.stats.open_until_ts = now + cooldown
            yf_fund_circuit_breaker_state.set(self.stats.state.to_numeric())

    def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics snapshot."""
        s = self.stats
        return {
            "state": s.state.value,
            "fail_threshold": self.fail_threshold,
            "failures": s.failures,
            "successes": s.successes,
            "last_failure_ts": s.last_failure_ts,
            "open_until_ts": s.open_until_ts,
            "cooldown_sec": s.cooldown_sec,
        }


class SingleFlight:
    """Deduplicate concurrent requests for the same key."""

    def __init__(self) -> None:
        self._lock: Optional[asyncio.Lock] = None  # lazy
        self._futs: Dict[str, asyncio.Future] = {}

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def run(self, key: str, coro_fn: Callable[[], Awaitable[Any]]) -> Any:
        """Execute coroutine; concurrent callers for the same key share the result."""
        owner = False
        lock = self._get_lock()
        async with lock:
            fut = self._futs.get(key)
            if fut is None:
                fut = asyncio.get_running_loop().create_future()
                self._futs[key] = fut
                owner = True
        if not owner:
            return await fut  # type: ignore[return-value]
        try:
            res = await coro_fn()
            if not fut.done():
                fut.set_result(res)
            return res
        except Exception as exc:
            if not fut.done():
                fut.set_exception(exc)
            raise
        finally:
            async with lock:
                self._futs.pop(key, None)


# =============================================================================
# Yahoo Fundamentals Provider
# =============================================================================

@dataclass(slots=True)
class YahooFundamentalsProvider:
    """Async provider for Yahoo Finance fundamentals."""

    name: str = PROVIDER_NAME

    timeout_sec: float = field(init=False)
    semaphore: Optional[asyncio.Semaphore] = field(init=False, default=None)
    max_concurrency: int = field(init=False)
    rate_limiter: TokenBucket = field(init=False)
    circuit_breaker: AdvancedCircuitBreaker = field(init=False)
    singleflight: SingleFlight = field(init=False)
    fund_cache: AdvancedCache = field(init=False)
    err_cache: AdvancedCache = field(init=False)

    def __post_init__(self) -> None:
        self.timeout_sec = _timeout_sec()
        self.max_concurrency = _max_concurrency()
        # v6.0.0: semaphore lazy-initialized (see _get_semaphore)
        self.semaphore = None
        self.rate_limiter = TokenBucket(_rate_limit())
        self.circuit_breaker = AdvancedCircuitBreaker(
            fail_threshold=_cb_fail_threshold(),
            cooldown_sec=_cb_cooldown_sec(),
        )
        self.singleflight = SingleFlight()
        self.fund_cache = AdvancedCache(
            name="fund",
            maxsize=5000,
            ttl=_fund_ttl_sec(),
            use_redis=_enable_redis(),
            redis_url=_redis_url(),
        )
        self.err_cache = AdvancedCache(
            name="error",
            maxsize=2000,
            ttl=_err_ttl_sec(),
            use_redis=_enable_redis(),
            redis_url=_redis_url(),
        )
        logger.info(
            "YahooFundamentalsProvider v%s initialized | yfinance=%s | "
            "requests=%s | UA_rotation=%s | sanity_guard=%s | retries=%s | "
            "concurrency=%s | rate=%s/s | cb=%s/%ss",
            PROVIDER_VERSION,
            _HAS_YFINANCE,
            _HAS_REQUESTS,
            _ua_rotation_enabled(),
            _price_sanity_enabled(),
            _retry_attempts(),
            self.max_concurrency,
            _rate_limit(),
            _cb_fail_threshold(),
            _cb_cooldown_sec(),
        )

    # v6.0.0 FIX: `enabled` is a proper @property backed by `_configured()`.
    @property
    def enabled(self) -> bool:
        """Return True if YF_ENABLED and yfinance is installed."""
        return _configured()

    def _get_semaphore(self) -> asyncio.Semaphore:
        """Lazy-initialize the concurrency-gating semaphore."""
        if self.semaphore is None:
            self.semaphore = asyncio.Semaphore(self.max_concurrency)
        return self.semaphore

    # -- History helpers --------------------------------------------------

    def _history_rows(
        self,
        ticker: Any,
        period: str = "3mo",
        interval: str = "1d",
    ) -> List[Dict[str, Any]]:
        """Fetch history rows from a yfinance Ticker (blocking)."""
        rows: List[Dict[str, Any]] = []
        try:
            hist = ticker.history(period=period, interval=interval, auto_adjust=False)
            if hist is None or getattr(hist, "empty", True):
                return []
            for idx, row in hist.iterrows():
                try:
                    dt = idx.to_pydatetime() if hasattr(idx, "to_pydatetime") else idx
                except Exception:
                    dt = idx
                rows.append({
                    "date": _utc_iso(dt) if isinstance(dt, datetime) else safe_str(dt),
                    "open": safe_float(row.get("Open")),
                    "high": safe_float(row.get("High")),
                    "low": safe_float(row.get("Low")),
                    "close": safe_float(row.get("Close")),
                    "volume": safe_float(row.get("Volume")),
                })
        except Exception:
            return []
        return rows

    def _history_avg_volumes(
        self,
        rows: Iterable[Dict[str, Any]],
    ) -> Tuple[Optional[float], Optional[float]]:
        """Compute trailing 10d / 30d average volume from history rows."""
        vols = [safe_float(r.get("volume")) for r in rows]
        clean = [float(v) for v in vols if v is not None and v >= 0]
        if not clean:
            return None, None
        avg10 = sum(clean[-10:]) / min(10, len(clean))
        avg30 = sum(clean[-30:]) / min(30, len(clean))
        return (float(avg10), float(avg30))

    def _history_52w(
        self,
        rows: Iterable[Dict[str, Any]],
    ) -> Tuple[Optional[float], Optional[float]]:
        """Compute 52-week high/low from history rows."""
        highs = [safe_float(r.get("high")) for r in rows]
        lows = [safe_float(r.get("low")) for r in rows]
        hs = [float(v) for v in highs if v is not None]
        ls = [float(v) for v in lows if v is not None]
        return (max(hs) if hs else None, min(ls) if ls else None)

    # -- Blocking fetch (runs in ThreadPoolExecutor) ---------------------

    def _construct_ticker(self, norm_symbol: str, session: Any) -> Any:
        """
        Construct a yf.Ticker. Try with `session=` first (newer yfinance);
        fall back to positional-only construction if the kwarg is rejected.
        """
        if yf is None:
            return None
        if session is None:
            return yf.Ticker(norm_symbol)
        try:
            return yf.Ticker(norm_symbol, session=session)
        except TypeError:
            # Older yfinance: doesn't accept session kwarg
            return yf.Ticker(norm_symbol)
        except Exception as exc:
            logger.debug("yf.Ticker(%s, session=...) failed (%s); retrying bare",
                         norm_symbol, exc)
            return yf.Ticker(norm_symbol)

    def _blocking_fetch(self, norm_symbol: str) -> Dict[str, Any]:
        """
        Blocking yfinance fetch. Called via loop.run_in_executor.

        v6.1.0:
          - Uses a requests.Session with rotated User-Agent (if available).
          - Rotates UA on every retry.
          - Exponential backoff with 25% jitter; doubled cooldown when the
            error string looks like a rate-limit / auth response.
          - Validates 52W high/low against current_price; falls back to
            3-month history-derived bounds on suspected currency mismatch.
          - Emits a `warnings: List[str]` field summarising every field-
            level data-quality flag raised during the fetch.
        """
        if not _HAS_YFINANCE or yf is None:
            return {"error": "yfinance_not_installed",
                    "data_quality": DataQuality.ERROR.value,
                    "last_error_class": "ImportError"}

        last_err: Optional[BaseException] = None
        last_err_class: str = ""
        session = _create_yf_session()
        max_attempts = _retry_attempts()

        for attempt in range(max_attempts):
            try:
                t = self._construct_ticker(norm_symbol, session)
                if t is None:
                    return {"error": "yf_ticker_construct_failed",
                            "data_quality": DataQuality.ERROR.value,
                            "last_error_class": "RuntimeError"}

                info: Dict[str, Any] = {}
                try:
                    info = t.info or {}
                except Exception:
                    info = {}

                try:
                    fast_info = getattr(t, "fast_info", None)
                except Exception:
                    fast_info = None

                history_rows = self._history_rows(t, period="3mo", interval="1d")
                hist_avg10, hist_avg30 = self._history_avg_volumes(history_rows)
                hist_52w_high, hist_52w_low = self._history_52w(history_rows)

                now_utc = _utc_iso()
                now_riy = _riyadh_iso()

                # Price fields
                current_price = _coalesce(
                    safe_float(_get_attr(fast_info, "last_price", "lastPrice", "regularMarketPrice")),
                    safe_float(_pick(info, "currentPrice", "regularMarketPrice", "navPrice")),
                )
                previous_close = _coalesce(
                    safe_float(_get_attr(fast_info, "previous_close", "previousClose", "regularMarketPreviousClose")),
                    safe_float(_pick(info, "previousClose", "regularMarketPreviousClose", "chartPreviousClose")),
                )
                open_price = _coalesce(
                    safe_float(_get_attr(fast_info, "open", "open_price", "regularMarketOpen")),
                    safe_float(_pick(info, "open", "regularMarketOpen")),
                )
                day_high = _coalesce(
                    safe_float(_get_attr(fast_info, "day_high", "dayHigh")),
                    safe_float(_pick(info, "dayHigh", "regularMarketDayHigh")),
                )
                day_low = _coalesce(
                    safe_float(_get_attr(fast_info, "day_low", "dayLow")),
                    safe_float(_pick(info, "dayLow", "regularMarketDayLow")),
                )

                # Raw (un-validated) 52W bounds from provider
                raw_52w_high = _coalesce(
                    safe_float(_get_attr(fast_info, "fifty_two_week_high", "fiftyTwoWeekHigh", "week52High")),
                    safe_float(_pick(info, "fiftyTwoWeekHigh", "week52High")),
                    hist_52w_high,
                )
                raw_52w_low = _coalesce(
                    safe_float(_get_attr(fast_info, "fifty_two_week_low", "fiftyTwoWeekLow", "week52Low")),
                    safe_float(_pick(info, "fiftyTwoWeekLow", "week52Low")),
                    hist_52w_low,
                )

                # v6.1.0: validate 52W bounds against current_price for unit
                # consistency (currency / scale mismatch guard).
                week_52_high, week_52_low, sanity_warnings = _validate_52w_bounds(
                    current_price=safe_float(current_price),
                    week_52_high=raw_52w_high,
                    week_52_low=raw_52w_low,
                    hist_52w_high=hist_52w_high,
                    hist_52w_low=hist_52w_low,
                )

                volume = _coalesce(
                    safe_float(_get_attr(fast_info, "last_volume", "lastVolume", "regularMarketVolume")),
                    safe_float(_pick(info, "volume", "regularMarketVolume")),
                )
                market_cap = _coalesce(
                    safe_float(_get_attr(fast_info, "market_cap", "marketCap")),
                    safe_float(_pick(info, "marketCap")),
                )

                # Valuation
                pe_ttm = safe_float(_pick(info, "trailingPE"))
                pe_forward = safe_float(_pick(info, "forwardPE"))
                pb_ratio = safe_float(_pick(info, "priceToBook"))
                ps_ratio = safe_float(_pick(info, "priceToSalesTrailing12Months", "priceToSales"))
                peg_ratio = safe_float(_pick(info, "pegRatio", "trailingPegRatio"))
                ev_ebitda = safe_float(_pick(info, "enterpriseToEbitda"))
                enterprise_value = safe_float(_pick(info, "enterpriseValue"))

                # Margins (as fractions via abs>1.5 heuristic)
                gross_margin = _as_fraction(_pick(info, "grossMargins"))
                operating_margin = _as_fraction(_pick(info, "operatingMargins"))
                profit_margin = _as_fraction(_pick(info, "profitMargins", "netMargins"))
                roe = _as_fraction(_pick(info, "returnOnEquity"))
                roa = _as_fraction(_pick(info, "returnOnAssets"))

                # Growth
                revenue_growth_yoy = _as_fraction(_pick(info, "revenueGrowth"))
                earnings_growth_yoy = _as_fraction(_pick(info, "earningsGrowth"))
                revenue_ttm = safe_float(_pick(info, "totalRevenue", "revenueTTM"))
                free_cash_flow_ttm = safe_float(_pick(info, "freeCashflow", "freeCashFlow"))
                operating_cash_flow = safe_float(_pick(info, "operatingCashflow", "operatingCashFlow"))

                # Dividend + shares
                dividend_yield = _as_fraction(_pick(info, "dividendYield"))
                payout_ratio = _as_fraction(_pick(info, "payoutRatio"))
                eps_ttm = safe_float(_pick(info, "trailingEps"))
                eps_forward = safe_float(_pick(info, "forwardEps"))
                debt_to_equity = safe_float(_pick(info, "debtToEquity"))
                beta_5y = safe_float(_pick(info, "beta"))
                float_shares = safe_float(_pick(info, "floatShares"))
                shares_outstanding = safe_float(_pick(info, "sharesOutstanding"))

                # Average volumes (with history fallback)
                avg_volume_10d = _coalesce(
                    safe_float(_pick(info, "averageVolume10days", "averageDailyVolume10Day")),
                    hist_avg10,
                )
                avg_volume_30d = _coalesce(
                    safe_float(_pick(info, "averageVolume", "averageDailyVolume3Month")),
                    hist_avg30,
                )

                # Analyst
                target_mean_price = safe_float(_pick(info, "targetMeanPrice"))
                target_high_price = safe_float(_pick(info, "targetHighPrice"))
                target_low_price = safe_float(_pick(info, "targetLowPrice"))
                analyst_count = safe_int(_pick(info, "numberOfAnalystOpinions"))

                # Identity
                name = safe_str(_pick(info, "longName", "shortName", "displayName"))
                currency = safe_str(_pick(info, "currency", "financialCurrency"))
                exchange = safe_str(_pick(info, "fullExchangeName", "exchange", "exchangeName"))
                country = safe_str(_pick(info, "country"))
                sector = safe_str(_pick(info, "sector"))
                industry = safe_str(_pick(info, "industry"))
                asset_class = _infer_asset_class(info, norm_symbol)

                # Misc metrics
                book_value = safe_float(_pick(info, "bookValue"))
                current_ratio = safe_float(_pick(info, "currentRatio"))
                quick_ratio = safe_float(_pick(info, "quickRatio"))
                short_ratio = safe_float(_pick(info, "shortRatio"))
                short_percent = _as_fraction(_pick(info, "shortPercentOfFloat"))

                # Fill margins from ratios if Yahoo didn't supply direct fields
                if gross_margin is None:
                    gross_margin = _pct_from_ratio(_pick(info, "grossProfits"), revenue_ttm)
                if operating_margin is None:
                    operating_margin = _pct_from_ratio(_pick(info, "ebitda"), revenue_ttm)

                # 52-week position (fraction in [0,1]) -- computed from the
                # VALIDATED bounds, so it's now safe from currency mismatch.
                week_52_position_pct: Optional[float] = None
                cp = safe_float(current_price)
                if (cp is not None and week_52_high is not None and week_52_low is not None
                        and week_52_high != week_52_low):
                    week_52_position_pct = (cp - float(week_52_low)) / (float(week_52_high) - float(week_52_low))
                    week_52_position_pct = max(0.0, min(1.0, float(week_52_position_pct)))

                # Aggregate field-level warnings from this fetch
                fetch_warnings: List[str] = list(sanity_warnings)
                if not history_rows:
                    fetch_warnings.append("history_3mo_empty")
                if not info:
                    fetch_warnings.append("info_empty")
                if currency is None:
                    fetch_warnings.append("currency_missing_from_provider")
                if industry is None:
                    fetch_warnings.append("industry_missing_from_provider")

                out: Dict[str, Any] = {
                    "requested_symbol": norm_symbol,
                    "symbol": norm_symbol,
                    "provider_symbol": norm_symbol,
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    "data_sources": [PROVIDER_NAME],
                    "provider_version": PROVIDER_VERSION,
                    "last_updated_utc": now_utc,
                    "last_updated_riyadh": now_riy,

                    # identity/profile
                    "currency": currency,
                    "name": name,
                    "exchange": exchange,
                    "country": country,
                    "sector": sector,
                    "industry": industry,
                    "asset_class": asset_class,

                    # price/liquidity (best-effort)
                    "current_price": current_price,
                    "previous_close": previous_close,
                    "open_price": open_price,
                    "day_high": day_high,
                    "day_low": day_low,
                    "week_52_high": week_52_high,
                    "week_52_low": week_52_low,
                    "week_52_position_pct": week_52_position_pct,
                    "volume": volume,
                    "market_cap": market_cap,
                    "float_shares": float_shares,
                    "avg_volume_10d": avg_volume_10d,
                    "avg_volume_30d": avg_volume_30d,
                    "beta_5y": beta_5y,

                    # fundamentals
                    "pe_ttm": pe_ttm,
                    "pe_forward": pe_forward,
                    "eps_ttm": eps_ttm,
                    "dividend_yield": dividend_yield,
                    "payout_ratio": payout_ratio,
                    "revenue_ttm": revenue_ttm,
                    "revenue_growth_yoy": revenue_growth_yoy,
                    "gross_margin": gross_margin,
                    "operating_margin": operating_margin,
                    "profit_margin": profit_margin,
                    "debt_to_equity": debt_to_equity,
                    "free_cash_flow_ttm": free_cash_flow_ttm,

                    # valuation
                    "pb_ratio": pb_ratio,
                    "ps_ratio": ps_ratio,
                    "peg_ratio": peg_ratio,
                    "ev_ebitda": ev_ebitda,
                    "enterprise_value": enterprise_value,
                    "intrinsic_value": target_mean_price,

                    # analyst/reco
                    "target_mean_price": target_mean_price,
                    "target_high_price": target_high_price,
                    "target_low_price": target_low_price,
                    "analyst_count": analyst_count,
                    "recommendation": map_recommendation(_pick(info, "recommendationKey")),

                    # additional
                    "shares_outstanding": shares_outstanding,
                    "book_value": book_value,
                    "eps_forward": eps_forward,
                    "roe": roe,
                    "roa": roa,
                    "earnings_growth_yoy": earnings_growth_yoy,
                    "operating_cashflow": operating_cash_flow,
                    "free_cashflow": free_cash_flow_ttm,
                    "current_ratio": current_ratio,
                    "quick_ratio": quick_ratio,
                    "short_ratio": short_ratio,
                    "short_percent": short_percent,
                    "history_rows_3mo": len(history_rows),

                    # v6.1.0: structured field-level warnings
                    "warnings": fetch_warnings,
                }

                # Legacy aliases (preserved verbatim from v5.5.0 / v6.0.0)
                out["price"] = out.get("current_price")
                out["prev_close"] = out.get("previous_close")
                out["open"] = out.get("open_price")
                out["change"] = None
                out["change_pct"] = None
                if current_price is not None and previous_close is not None:
                    change = float(current_price) - float(previous_close)
                    pct = (change / float(previous_close)) if float(previous_close) != 0 else None
                    out["price_change"] = change
                    out["percent_change"] = pct
                    out["change"] = change
                    out["change_pct"] = pct
                out["52w_high"] = out.get("week_52_high")
                out["52w_low"] = out.get("week_52_low")
                out["forward_pe"] = out.get("pe_forward")
                out["pb"] = out.get("pb_ratio")
                out["ps"] = out.get("ps_ratio")
                out["peg"] = out.get("peg_ratio")
                out["net_margin"] = out.get("profit_margin")
                out["revenue_growth"] = out.get("revenue_growth_yoy")
                out["dividend_yield_percent"] = out.get("dividend_yield")

                # Forecast from analyst consensus
                if cp is not None and cp > 0 and target_mean_price is not None and target_mean_price > 0:
                    roi12 = (float(target_mean_price) / cp) - 1.0
                    out["forecast_price_12m"] = float(target_mean_price)
                    out["expected_roi_12m"] = roi12
                    out["forecast_method"] = "analyst_consensus"
                    ac = analyst_count or 1
                    out["forecast_confidence"] = min(0.95, 0.40 + (ac * 0.05))

                dq, score = data_quality_score(out)
                out["data_quality"] = dq.value
                out["data_quality_score"] = score
                return clean_patch(out)

            except Exception as exc:
                last_err = exc
                last_err_class = type(exc).__name__

                # v6.1.0: exponential backoff with jitter; doubled on rate-
                # limit signals.
                base = min(8.0, 0.5 * (2 ** attempt))
                if _is_rate_limit_error(exc):
                    base = min(16.0, base * 2.0)
                sleep_for = base + random.uniform(0.0, base * 0.25)
                time.sleep(sleep_for)

                # Rotate UA on retry to evade per-UA throttling
                _rotate_session_ua(session)

        return {
            "error": str(last_err) if last_err else "unknown_error",
            "last_error_class": last_err_class or "Unknown",
            "data_quality": DataQuality.ERROR.value,
        }

    # -- Async fetch API -----------------------------------------------------

    async def fetch_fundamentals(self, symbol: str) -> Dict[str, Any]:
        """Fetch fundamentals for a single symbol (cache + singleflight + circuit breaker)."""
        if not self.enabled or not symbol:
            return {}

        norm = normalize_symbol(symbol)
        if not norm:
            return {}

        cached = await self.fund_cache.get(norm)
        if cached:
            return cached

        if not await self.circuit_breaker.allow_request():
            return {}

        async def _do() -> Dict[str, Any]:
            # v6.0.0 FIX: rate-limiter token acquisition is INSIDE the
            # singleflight callback to avoid wasting budget on dedup'd callers.
            await self.rate_limiter.wait_and_acquire()

            # v6.0.0 FIX: actually acquire the provider's concurrency semaphore.
            async with self._get_semaphore():
                loop = asyncio.get_running_loop()
                start_time = time.monotonic()
                try:
                    res = await loop.run_in_executor(None, self._blocking_fetch, norm)
                    if "error" in res and res.get("data_quality") == DataQuality.ERROR.value:
                        # v6.1.0: infer status_code from error string so the
                        # circuit breaker can apply the extended cooldown.
                        status_code = 403 if _is_rate_limit_error(
                            Exception(str(res.get("error", "")))
                        ) else 500
                        await self.circuit_breaker.on_failure(status_code=status_code)
                        yf_fund_requests_total.labels(status="error").inc()
                        return {}
                    await self.circuit_breaker.on_success()
                    yf_fund_requests_total.labels(status="success").inc()
                    yf_fund_request_duration.observe(time.monotonic() - start_time)

                    if res.get("current_price") is not None:
                        await self.fund_cache.set(norm, res)
                    return res
                except Exception as exc:
                    await self.circuit_breaker.on_failure()
                    yf_fund_requests_total.labels(status="error").inc()
                    logger.error("Error fetching fundamentals for %s: %s", norm, exc)
                    return {}

        return await self.singleflight.run(norm, _do)

    async def fetch_fundamentals_batch(
        self,
        symbols: List[str],
        concurrency: Optional[int] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Batch fetch fundamentals for multiple symbols.

        v6.0.0: accepts a `concurrency` parameter (default: self.max_concurrency).
        Each fetch is also gated by the provider's semaphore inside
        fetch_fundamentals, so effective parallelism is the minimum of the
        two.
        """
        if not symbols:
            return {}

        batch_cap = max(1, concurrency or self.max_concurrency)
        batch_sem = asyncio.Semaphore(batch_cap)

        async def _fetch_one(sym: str) -> Tuple[str, Dict[str, Any]]:
            async with batch_sem:
                result = await self.fetch_fundamentals(sym)
                return sym, result

        tasks = [_fetch_one(sym) for sym in symbols if sym]
        gathered = await asyncio.gather(*tasks, return_exceptions=True)

        results: Dict[str, Dict[str, Any]] = {}
        for item in gathered:
            if isinstance(item, Exception):
                continue
            sym, data = item
            if data:
                results[sym] = data
        return results

    async def close(self) -> None:
        """Close provider resources (Redis connections)."""
        try:
            await self.fund_cache.close()
        except Exception as exc:
            logger.debug("fund_cache close failed: %s", exc)
        try:
            await self.err_cache.close()
        except Exception as exc:
            logger.debug("err_cache close failed: %s", exc)


# =============================================================================
# Singleton Instance (lazy)
# =============================================================================

_PROVIDER_INSTANCE: Optional[YahooFundamentalsProvider] = None
_PROVIDER_LOCK: Optional[asyncio.Lock] = None


def _get_provider_lock() -> asyncio.Lock:
    global _PROVIDER_LOCK
    if _PROVIDER_LOCK is None:
        _PROVIDER_LOCK = asyncio.Lock()
    return _PROVIDER_LOCK


async def get_provider() -> YahooFundamentalsProvider:
    """Get (or create) the singleton YahooFundamentalsProvider instance."""
    global _PROVIDER_INSTANCE
    if _PROVIDER_INSTANCE is not None:
        return _PROVIDER_INSTANCE
    async with _get_provider_lock():
        if _PROVIDER_INSTANCE is None:
            _PROVIDER_INSTANCE = YahooFundamentalsProvider()
    return _PROVIDER_INSTANCE


async def close_provider() -> None:
    """Close and reset the singleton provider."""
    global _PROVIDER_INSTANCE
    if _PROVIDER_INSTANCE is not None:
        await _PROVIDER_INSTANCE.close()
        _PROVIDER_INSTANCE = None


# =============================================================================
# Engine-Facing Functions
# =============================================================================

async def fetch_fundamentals_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Fetch fundamentals patch for a symbol."""
    if args or kwargs:
        logger.debug("fetch_fundamentals_patch(%s): ignoring args=%r kwargs=%r", symbol, args, kwargs)
    provider = await get_provider()
    return await provider.fetch_fundamentals(symbol)


async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Alias for fetch_fundamentals_patch."""
    return await fetch_fundamentals_patch(symbol, *args, **kwargs)


async def fetch_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Alias for fetch_fundamentals_patch."""
    return await fetch_fundamentals_patch(symbol, *args, **kwargs)


async def get_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Alias for fetch_fundamentals_patch."""
    return await fetch_fundamentals_patch(symbol, *args, **kwargs)


async def quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Alias for fetch_fundamentals_patch."""
    return await fetch_fundamentals_patch(symbol, *args, **kwargs)


async def enriched_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Alias for fetch_fundamentals_patch."""
    return await fetch_fundamentals_patch(symbol, *args, **kwargs)


async def fetch_quotes(
    symbols: List[str],
    concurrency: Optional[int] = None,
    **kwargs: Any,
) -> Dict[str, Dict[str, Any]]:
    """Batch fetch fundamentals for multiple symbols."""
    if kwargs:
        logger.debug("fetch_quotes: ignoring kwargs=%r", kwargs)
    provider = await get_provider()
    return await provider.fetch_fundamentals_batch(symbols, concurrency=concurrency)


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    # Metadata
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    "VERSION",
    "PROVIDER_BATCH_SUPPORTED",
    # Classes
    "YahooFundamentalsProvider",
    "DataQuality",
    # Utility
    "data_quality_score",
    "normalize_symbol",
    # Singleton control
    "get_provider",
    "close_provider",
    # Engine-facing functions
    "fetch_fundamentals_patch",
    "fetch_enriched_quote_patch",
    "fetch_quote",
    "get_quote",
    "quote",
    "enriched_quote",
    "fetch_quotes",
]
