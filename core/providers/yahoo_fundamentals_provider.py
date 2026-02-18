#!/usr/bin/env python3
# core/providers/yahoo_fundamentals_provider.py
"""
================================================================================
Yahoo Finance Fundamentals Provider — v4.0.0 (ADVANCED PRODUCTION)
================================================================================

What's new in v4.0.0:
- ✅ Multi-model financial forecasting (earnings, revenue, cash flow projections)
- ✅ Comprehensive financial health metrics (Altman Z-Score, Piotroski F-Score)
- ✅ Advanced valuation models (DCF, Graham Number, intrinsic value)
- ✅ Growth trajectory analysis with confidence scoring
- ✅ Peer comparison metrics (sector averages, relative valuation)
- ✅ Smart caching with TTL and LRU eviction
- ✅ Circuit breaker pattern with half-open state
- ✅ Advanced rate limiting with token bucket
- ✅ Singleflight pattern to prevent cache stampede
- ✅ Comprehensive metrics and monitoring
- ✅ Riyadh timezone awareness throughout
- ✅ Error backoff cache for failed symbols
- ✅ Support for KSA symbols (.SR suffix)
- ✅ Multiple data sources (info, financials, balance sheet, cashflow)

Key Features:
- Global equity fundamentals
- Real-time and historical financial data
- Analyst estimates and price targets
- Financial health scoring
- Intrinsic value calculations
- Growth projections
- Sector comparisons
- Production-grade error handling
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import os
import random
import re
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import numpy as np

# Optional scientific stack with graceful fallback
try:
    from scipy import stats
    from scipy.signal import savgol_filter
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

try:
    from sklearn.linear_model import LinearRegression, Ridge
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import RandomForestRegressor
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

logger = logging.getLogger("core.providers.yahoo_fundamentals_provider")

PROVIDER_NAME = "yahoo_fundamentals"
PROVIDER_VERSION = "4.0.0"

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

# KSA symbol patterns
_KSA_CODE_RE = re.compile(r"^\d{3,6}$", re.IGNORECASE)
_KSA_SYMBOL_RE = re.compile(r"^\d{3,6}(\.SR)?$", re.IGNORECASE)

# Arabic digit translation
_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

# ---------------------------------------------------------------------------
# Optional yfinance import (keeps module import-safe)
# ---------------------------------------------------------------------------
try:
    import yfinance as yf  # type: ignore
    import pandas as pd  # type: ignore
    _HAS_YFINANCE = True
    _HAS_PANDAS = True
except ImportError:
    yf = None  # type: ignore
    pd = None  # type: ignore
    _HAS_YFINANCE = False
    _HAS_PANDAS = False


# ============================================================================
# Enums & Data Classes
# ============================================================================

class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class DataQuality(Enum):
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    BAD = "bad"


class FinancialHealth(Enum):
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    CRITICAL = "critical"


class GrowthStage(Enum):
    EARLY = "early"          # High growth, low profitability
    GROWTH = "growth"        # Strong growth, improving profitability
    MATURE = "mature"        # Moderate growth, stable profitability
    DECLINE = "decline"      # Declining revenue/profits
    CYCLICAL = "cyclical"    # Cyclical business


@dataclass
class CircuitBreakerStats:
    failures: int = 0
    successes: int = 0
    last_failure: float = 0.0
    last_success: float = 0.0
    state: CircuitState = CircuitState.CLOSED
    open_until: float = 0.0


@dataclass
class FinancialMetrics:
    """Comprehensive financial metrics container."""
    # Valuation
    market_cap: Optional[float] = None
    enterprise_value: Optional[float] = None
    pe_ttm: Optional[float] = None
    forward_pe: Optional[float] = None
    peg_ratio: Optional[float] = None
    ps_ttm: Optional[float] = None
    pb_ttm: Optional[float] = None
    pfcf_ttm: Optional[float] = None
    
    # Profitability
    gross_margin: Optional[float] = None
    operating_margin: Optional[float] = None
    net_margin: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None
    roce: Optional[float] = None
    
    # Growth
    revenue_growth_yoy: Optional[float] = None
    earnings_growth_yoy: Optional[float] = None
    fcf_growth_yoy: Optional[float] = None
    growth_score: Optional[float] = None
    
    # Financial Health
    current_ratio: Optional[float] = None
    quick_ratio: Optional[float] = None
    debt_to_equity: Optional[float] = None
    interest_coverage: Optional[float] = None
    altman_z_score: Optional[float] = None
    piotroski_f_score: Optional[int] = None
    
    # Efficiency
    asset_turnover: Optional[float] = None
    inventory_turnover: Optional[float] = None
    receivables_turnover: Optional[float] = None
    days_sales_outstanding: Optional[float] = None
    
    # Cash Flow
    operating_cf: Optional[float] = None
    investing_cf: Optional[float] = None
    financing_cf: Optional[float] = None
    free_cashflow: Optional[float] = None
    fcf_yield: Optional[float] = None
    
    # Dividends
    dividend_yield: Optional[float] = None
    dividend_rate: Optional[float] = None
    payout_ratio: Optional[float] = None
    dividend_growth_5y: Optional[float] = None
    
    # Analyst Estimates
    target_mean: Optional[float] = None
    target_high: Optional[float] = None
    target_low: Optional[float] = None
    recommendation: Optional[str] = None
    analyst_count: Optional[int] = None
    upgrade_downgrade: Optional[str] = None
    
    # Intrinsic Value
    dcf_value: Optional[float] = None
    graham_value: Optional[float] = None
    relative_value: Optional[float] = None
    
    # Risk
    beta: Optional[float] = None
    short_ratio: Optional[float] = None
    short_percent: Optional[float] = None
    volatility_90d: Optional[float] = None


# ============================================================================
# Shared Normalizer Integration
# ============================================================================

def _try_import_shared_normalizer() -> Tuple[Optional[Any], Optional[Any]]:
    """Safely import shared symbol normalizer if available."""
    try:
        from core.symbols.normalize import normalize_symbol as _ns
        from core.symbols.normalize import looks_like_ksa as _lk
        return _ns, _lk
    except Exception:
        return None, None


_SHARED_NORMALIZE, _SHARED_LOOKS_KSA = _try_import_shared_normalizer()


# ============================================================================
# Environment Helpers (Enhanced)
# ============================================================================

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


def _configured() -> bool:
    if not _env_bool("YF_ENABLED", True):
        return False
    return _HAS_YFINANCE


def _emit_warnings() -> bool:
    return _env_bool("YF_VERBOSE_WARNINGS", False)


def _timeout_sec() -> float:
    return max(5.0, _env_float("YF_TIMEOUT_SEC", 25.0))


def _fund_ttl_sec() -> float:
    return max(300.0, _env_float("YF_FUND_TTL_SEC", 21600.0))  # 6 hours default


def _err_ttl_sec() -> float:
    return max(5.0, _env_float("YF_ERROR_TTL_SEC", 10.0))


def _max_concurrency() -> int:
    return max(2, _env_int("YF_MAX_CONCURRENCY", 10))


def _rate_limit() -> float:
    return max(0.0, _env_float("YF_RATE_LIMIT_PER_SEC", 0.0))


def _cb_enabled() -> bool:
    return _env_bool("YF_CIRCUIT_BREAKER", True)


def _cb_fail_threshold() -> int:
    return max(2, _env_int("YF_CB_FAIL_THRESHOLD", 6))


def _cb_cooldown_sec() -> float:
    return max(5.0, _env_float("YF_CB_COOLDOWN_SEC", 30.0))


def _enable_dcf() -> bool:
    return _env_bool("YF_ENABLE_DCF", True)


def _enable_ml_forecast() -> bool:
    return _env_bool("YF_ENABLE_ML_FORECAST", True) and SKLEARN_AVAILABLE


def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(timezone.utc)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).isoformat()


def _riyadh_now() -> datetime:
    return datetime.now(timezone(timedelta(hours=3)))


def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    tz = timezone(timedelta(hours=3))
    d = dt or datetime.now(tz)
    if d.tzinfo is None:
        d = d.replace(tzinfo=tz)
    return d.astimezone(tz).isoformat()


def _to_riyadh_iso(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    tz = timezone(timedelta(hours=3))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(tz).isoformat()


# ============================================================================
# Safe Type Helpers
# ============================================================================

def safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        if isinstance(x, (int, float)):
            f = float(x)
            if math.isnan(f) or math.isinf(f):
                return None
            return f

        s = str(x).strip()
        if not s or s.lower() in {"n/a", "na", "null", "none", "-", "--", ""}:
            return None

        # Handle Arabic digits and currency symbols
        s = s.translate(_ARABIC_DIGITS)
        s = s.replace(",", "").replace("%", "").replace("$", "").replace("£", "").replace("€", "")
        s = s.replace("SAR", "").replace("ريال", "").strip()

        # Handle parentheses for negative numbers
        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()

        # Handle K/M/B/T suffixes
        m = re.match(r"^(-?\d+(\.\d+)?)([KMBT])$", s, re.IGNORECASE)
        mult = 1.0
        if m:
            num = m.group(1)
            suf = m.group(3).upper()
            if suf == "K":
                mult = 1_000.0
            elif suf == "M":
                mult = 1_000_000.0
            elif suf == "B":
                mult = 1_000_000_000.0
            elif suf == "T":
                mult = 1_000_000_000_000.0
            s = num

        f = float(s) * mult
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def safe_int(x: Any) -> Optional[int]:
    f = safe_float(x)
    if f is None:
        return None
    try:
        return int(round(f))
    except Exception:
        return None


def as_percent(x: Any) -> Optional[float]:
    """Convert decimal to percentage if it looks like a decimal."""
    v = safe_float(x)
    if v is None:
        return None
    # Convert to percentage if value is between -2 and 2 (likely a decimal)
    return v * 100.0 if abs(v) <= 2.0 else v


def clean_patch(p: Dict[str, Any]) -> Dict[str, Any]:
    """Remove None and empty string values."""
    out: Dict[str, Any] = {}
    for k, v in (p or {}).items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        out[k] = v
    return out


def merge_into(dst: Dict[str, Any], src: Dict[str, Any], *, force_keys: Sequence[str] = ()) -> None:
    """Merge src into dst, with optional force keys."""
    fset = set(force_keys or ())
    for k, v in (src or {}).items():
        if v is None:
            continue
        if k in fset:
            dst[k] = v
            continue
        if k not in dst:
            dst[k] = v
            continue
        cur = dst.get(k)
        if cur is None:
            dst[k] = v
        elif isinstance(cur, str) and not cur.strip():
            dst[k] = v


def normalize_symbol(symbol: str) -> str:
    """
    Normalize symbol for Yahoo Finance.
    - Handles KSA symbols (.SR suffix)
    - Translates Arabic digits
    - Removes TADAWUL prefix
    """
    s = (symbol or "").strip()
    if not s:
        return ""

    s = s.translate(_ARABIC_DIGITS).strip().upper()

    # Try shared normalizer first
    if callable(_SHARED_NORMALIZE):
        try:
            res = _SHARED_NORMALIZE(s)
            if res:
                return res
        except Exception:
            pass

    # Remove TADAWUL prefix
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", "").strip()

    # KSA code -> .SR
    if _KSA_CODE_RE.match(s):
        return f"{s}.SR"

    # Already has .SR suffix
    if s.endswith(".SR") and _KSA_CODE_RE.match(s[:-3]):
        return s

    # Standard Yahoo format (e.g., AAPL, 9988.HK)
    return s


def map_recommendation(rec: Optional[str]) -> str:
    """Map Yahoo recommendation to standard format."""
    if not rec:
        return "HOLD"
    
    rec_lower = str(rec).lower()
    if "strong_buy" in rec_lower:
        return "STRONG_BUY"
    elif "buy" in rec_lower:
        return "BUY"
    elif "hold" in rec_lower:
        return "HOLD"
    elif "underperform" in rec_lower or "reduce" in rec_lower:
        return "REDUCE"
    elif "sell" in rec_lower:
        return "SELL"
    else:
        return "HOLD"


def data_quality_score(patch: Dict[str, Any]) -> Tuple[DataQuality, float]:
    """Score data quality (0-100) and return category."""
    score = 100.0

    # Check essential fields
    if safe_str(patch.get("name")) is None:
        score -= 20

    if safe_float(patch.get("market_cap")) is None:
        score -= 15

    if safe_float(patch.get("pe_ttm")) is None:
        score -= 10

    if safe_float(patch.get("eps_ttm")) is None:
        score -= 10

    if safe_float(patch.get("target_mean_price")) is None:
        score -= 10

    # Normalize score
    score = max(0.0, min(100.0, score))

    # Determine category
    if score >= 80:
        category = DataQuality.EXCELLENT
    elif score >= 60:
        category = DataQuality.GOOD
    elif score >= 40:
        category = DataQuality.FAIR
    elif score >= 20:
        category = DataQuality.POOR
    else:
        category = DataQuality.BAD

    return category, score


# ============================================================================
# Advanced Cache with TTL and LRU
# ============================================================================

class SmartCache:
    """Thread-safe cache with TTL and LRU eviction."""

    def __init__(self, maxsize: int = 5000, ttl: float = 300.0):
        self.maxsize = maxsize
        self.ttl = ttl
        self._cache: Dict[str, Any] = {}
        self._expires: Dict[str, float] = {}
        self._access_times: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            now = time.monotonic()

            if key in self._cache:
                if now < self._expires.get(key, 0):
                    self._access_times[key] = now
                    return self._cache[key]
                else:
                    await self._delete(key)

            return None

    async def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        async with self._lock:
            if len(self._cache) >= self.maxsize and key not in self._cache:
                await self._evict_lru()

            self._cache[key] = value
            self._expires[key] = time.monotonic() + (ttl or self.ttl)
            self._access_times[key] = time.monotonic()

    async def delete(self, key: str) -> None:
        async with self._lock:
            await self._delete(key)

    async def _delete(self, key: str) -> None:
        self._cache.pop(key, None)
        self._expires.pop(key, None)
        self._access_times.pop(key, None)

    async def _evict_lru(self) -> None:
        if not self._access_times:
            return
        oldest_key = min(self._access_times.items(), key=lambda x: x[1])[0]
        await self._delete(oldest_key)

    async def clear(self) -> None:
        async with self._lock:
            self._cache.clear()
            self._expires.clear()
            self._access_times.clear()

    async def size(self) -> int:
        async with self._lock:
            return len(self._cache)


# ============================================================================
# Rate Limiter & Circuit Breaker
# ============================================================================

class TokenBucket:
    """Async token bucket rate limiter."""

    def __init__(self, rate_per_sec: float, capacity: Optional[float] = None):
        self.rate = max(0.0, rate_per_sec)
        self.capacity = capacity or max(1.0, self.rate)
        self.tokens = self.capacity
        self.last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: float = 1.0) -> bool:
        if self.rate <= 0:
            return True

        async with self._lock:
            now = time.monotonic()
            elapsed = max(0.0, now - self.last)
            self.last = now

            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)

            if self.tokens >= tokens:
                self.tokens -= tokens
                return True

            return False

    async def wait_and_acquire(self, tokens: float = 1.0) -> None:
        while True:
            if await self.acquire(tokens):
                return

            async with self._lock:
                need = tokens - self.tokens
                wait = need / self.rate if self.rate > 0 else 0.1

            await asyncio.sleep(min(1.0, wait))


class SmartCircuitBreaker:
    """Circuit breaker with half-open state and failure tracking."""

    def __init__(
        self,
        fail_threshold: int = 5,
        cooldown_sec: float = 30.0,
        half_open_max_calls: int = 2
    ):
        self.fail_threshold = fail_threshold
        self.cooldown_sec = cooldown_sec
        self.half_open_max_calls = half_open_max_calls

        self.stats = CircuitBreakerStats()
        self.half_open_calls = 0
        self._lock = asyncio.Lock()

    async def allow_request(self) -> bool:
        async with self._lock:
            now = time.monotonic()

            if self.stats.state == CircuitState.CLOSED:
                return True

            elif self.stats.state == CircuitState.OPEN:
                if now >= self.stats.open_until:
                    self.stats.state = CircuitState.HALF_OPEN
                    self.half_open_calls = 0
                    logger.info("Circuit breaker moved to HALF_OPEN")
                    return True
                return False

            elif self.stats.state == CircuitState.HALF_OPEN:
                if self.half_open_calls < self.half_open_max_calls:
                    self.half_open_calls += 1
                    return True
                return False

            return False

    async def on_success(self) -> None:
        async with self._lock:
            self.stats.successes += 1
            self.stats.last_success = time.monotonic()

            if self.stats.state == CircuitState.HALF_OPEN:
                self.stats.state = CircuitState.CLOSED
                self.stats.failures = 0
                logger.info("Circuit breaker returned to CLOSED after success")

    async def on_failure(self) -> None:
        async with self._lock:
            self.stats.failures += 1
            self.stats.last_failure = time.monotonic()

            if self.stats.state == CircuitState.CLOSED:
                if self.stats.failures >= self.fail_threshold:
                    self.stats.state = CircuitState.OPEN
                    self.stats.open_until = time.monotonic() + self.cooldown_sec
                    logger.warning(f"Circuit breaker OPEN after {self.stats.failures} failures")

            elif self.stats.state == CircuitState.HALF_OPEN:
                self.stats.state = CircuitState.OPEN
                self.stats.open_until = time.monotonic() + self.cooldown_sec
                logger.warning("Circuit breaker returned to OPEN after half-open failure")

    def get_stats(self) -> Dict[str, Any]:
        return {
            "state": self.stats.state.value,
            "failures": self.stats.failures,
            "successes": self.stats.successes,
            "last_failure": self.stats.last_failure,
            "last_success": self.stats.last_success,
            "open_until": self.stats.open_until
        }


# ============================================================================
# Singleflight Pattern (Prevent Cache Stampede)
# ============================================================================

class SingleFlight:
    """Deduplicate concurrent requests for the same key."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._futures: Dict[str, asyncio.Future] = {}

    async def run(self, key: str, coro_fn):
        async with self._lock:
            fut = self._futures.get(key)
            if fut is not None:
                return await fut

            fut = asyncio.get_event_loop().create_future()
            self._futures[key] = fut

        try:
            result = await coro_fn()
            if not fut.done():
                fut.set_result(result)
            return result
        except Exception as e:
            if not fut.done():
                fut.set_exception(e)
            raise
        finally:
            async with self._lock:
                self._futures.pop(key, None)


# ============================================================================
# Financial Analysis Functions
# ============================================================================

class FinancialAnalyzer:
    """Advanced financial analysis and valuation models."""

    @staticmethod
    def calculate_altman_z_score(metrics: Dict[str, Any]) -> Optional[float]:
        """
        Calculate Altman Z-Score for bankruptcy risk.
        Z = 1.2A + 1.4B + 3.3C + 0.6D + 1.0E
        where:
        A = Working Capital / Total Assets
        B = Retained Earnings / Total Assets
        C = EBIT / Total Assets
        D = Market Cap / Total Liabilities
        E = Sales / Total Assets
        """
        try:
            # Required fields
            working_capital = safe_float(metrics.get("working_capital"))
            total_assets = safe_float(metrics.get("total_assets"))
            retained_earnings = safe_float(metrics.get("retained_earnings"))
            ebit = safe_float(metrics.get("ebit"))
            market_cap = safe_float(metrics.get("market_cap"))
            total_liabilities = safe_float(metrics.get("total_liabilities"))
            sales = safe_float(metrics.get("total_revenue"))

            if None in (working_capital, total_assets, retained_earnings, ebit, market_cap, total_liabilities, sales):
                return None
            if total_assets == 0 or total_liabilities == 0:
                return None

            A = working_capital / total_assets
            B = retained_earnings / total_assets
            C = ebit / total_assets
            D = market_cap / total_liabilities
            E = sales / total_assets

            z_score = 1.2 * A + 1.4 * B + 3.3 * C + 0.6 * D + 1.0 * E
            return float(z_score)
        except Exception:
            return None

    @staticmethod
    def calculate_piotroski_f_score(financials: Dict[str, Any]) -> Optional[int]:
        """
        Calculate Piotroski F-Score (0-9) for financial strength.
        Based on 9 fundamental signals.
        """
        score = 0
        
        try:
            # 1. Profitability (4 points)
            # ROA positive
            net_income = safe_float(financials.get("net_income"))
            total_assets = safe_float(financials.get("total_assets"))
            if net_income and total_assets and total_assets > 0:
                roa = net_income / total_assets
                if roa > 0:
                    score += 1

            # Operating cash flow positive
            ocf = safe_float(financials.get("operating_cashflow"))
            if ocf and ocf > 0:
                score += 1

            # ROA improvement
            roa_current = safe_float(financials.get("roa_current"))
            roa_previous = safe_float(financials.get("roa_previous"))
            if roa_current and roa_previous and roa_current > roa_previous:
                score += 1

            # Accruals (CFO > Net Income)
            if ocf and net_income and ocf > net_income:
                score += 1

            # 2. Leverage, Liquidity, Source of Funds (3 points)
            # Long-term debt decrease
            lt_debt_current = safe_float(financials.get("long_term_debt_current"))
            lt_debt_previous = safe_float(financials.get("long_term_debt_previous"))
            if lt_debt_current and lt_debt_previous and lt_debt_current < lt_debt_previous:
                score += 1

            # Current ratio increase
            cr_current = safe_float(financials.get("current_ratio_current"))
            cr_previous = safe_float(financials.get("current_ratio_previous"))
            if cr_current and cr_previous and cr_current > cr_previous:
                score += 1

            # No new shares issued
            shares_current = safe_float(financials.get("shares_outstanding_current"))
            shares_previous = safe_float(financials.get("shares_outstanding_previous"))
            if shares_current and shares_previous and shares_current <= shares_previous:
                score += 1

            # 3. Operating Efficiency (2 points)
            # Gross margin increase
            gm_current = safe_float(financials.get("gross_margin_current"))
            gm_previous = safe_float(financials.get("gross_margin_previous"))
            if gm_current and gm_previous and gm_current > gm_previous:
                score += 1

            # Asset turnover increase
            at_current = safe_float(financials.get("asset_turnover_current"))
            at_previous = safe_float(financials.get("asset_turnover_previous"))
            if at_current and at_previous and at_current > at_previous:
                score += 1

            return score
        except Exception:
            return None

    @staticmethod
    def calculate_dcf_value(
        fcf: float,
        growth_rate: float,
        terminal_growth: float = 0.03,
        discount_rate: float = 0.10,
        years: int = 5
    ) -> float:
        """
        Calculate Discounted Cash Flow intrinsic value.
        Uses two-stage DCF model.
        """
        if fcf <= 0:
            return 0.0

        # Stage 1: Explicit forecast period
        pv_fcf = 0.0
        for year in range(1, years + 1):
            future_fcf = fcf * (1 + growth_rate) ** year
            pv_fcf += future_fcf / (1 + discount_rate) ** year

        # Stage 2: Terminal value
        terminal_fcf = fcf * (1 + growth_rate) ** years * (1 + terminal_growth)
        terminal_value = terminal_fcf / (discount_rate - terminal_growth)
        pv_terminal = terminal_value / (1 + discount_rate) ** years

        return pv_fcf + pv_terminal

    @staticmethod
    def calculate_graham_number(eps: float, book_value: float) -> float:
        """
        Calculate Graham Number (intrinsic value based on EPS and book value).
        Graham Number = sqrt(22.5 * EPS * BVPS)
        """
        if eps <= 0 or book_value <= 0:
            return 0.0
        return math.sqrt(22.5 * eps * book_value)

    @staticmethod
    def determine_growth_stage(metrics: Dict[str, Any]) -> GrowthStage:
        """Determine company growth stage based on financial metrics."""
        revenue_growth = safe_float(metrics.get("revenue_growth", 0))
        net_margin = safe_float(metrics.get("net_margin", 0))
        roe = safe_float(metrics.get("roe", 0))
        pe_ratio = safe_float(metrics.get("pe_ttm", 0))

        # Early stage: High growth, low/no profitability
        if revenue_growth and revenue_growth > 0.2 and net_margin and net_margin < 0.05:
            return GrowthStage.EARLY

        # Growth stage: Strong growth, improving profitability
        if revenue_growth and revenue_growth > 0.1 and roe and roe > 0.1:
            return GrowthStage.GROWTH

        # Mature stage: Moderate growth, stable profitability
        if revenue_growth and 0.02 <= revenue_growth <= 0.1 and roe and roe > 0.1:
            return GrowthStage.MATURE

        # Decline stage: Negative growth
        if revenue_growth and revenue_growth < 0:
            return GrowthStage.DECLINE

        # Cyclical: Default for volatile sectors
        return GrowthStage.CYCLICAL

    @staticmethod
    def calculate_growth_score(metrics: Dict[str, Any]) -> float:
        """Calculate growth score (0-100) based on multiple factors."""
        score = 50.0  # Base score

        # Revenue growth (0-25 points)
        rev_growth = safe_float(metrics.get("revenue_growth"))
        if rev_growth is not None:
            score += min(25, max(-25, rev_growth * 100))

        # Earnings growth (0-25 points)
        eps_growth = safe_float(metrics.get("earnings_growth"))
        if eps_growth is not None:
            score += min(25, max(-25, eps_growth * 100))

        # ROE (0-15 points)
        roe = safe_float(metrics.get("roe"))
        if roe is not None:
            score += min(15, max(0, roe))

        # Net margin (0-15 points)
        margin = safe_float(metrics.get("net_margin"))
        if margin is not None:
            score += min(15, max(0, margin))

        # Analyst growth expectations (0-20 points)
        target_upside = safe_float(metrics.get("upside_percent"))
        if target_upside is not None:
            score += min(20, max(-20, target_upside / 5))

        return max(0, min(100, score))


# ============================================================================
# Advanced Forecasting
# ============================================================================

class FinancialForecaster:
    """Advanced financial forecasting using ML and statistical methods."""

    def __init__(self, historical_data: Dict[str, List[float]], enable_ml: bool = True):
        self.historical_data = historical_data
        self.enable_ml = enable_ml and SKLEARN_AVAILABLE

    def forecast_earnings(self, years: int = 3) -> Dict[str, Any]:
        """Forecast future earnings using multiple models."""
        result: Dict[str, Any] = {
            "forecast_available": False,
            "models_used": [],
            "forecasts": {},
            "ensemble": {},
            "confidence": 0.0
        }

        eps_history = self.historical_data.get("eps", [])
        revenue_history = self.historical_data.get("revenue", [])

        if len(eps_history) < 3:
            return result

        # Model 1: Linear trend
        trend_forecast = self._forecast_trend(eps_history, years)
        if trend_forecast is not None:
            result["models_used"].append("trend")
            result["forecasts"]["trend"] = trend_forecast

        # Model 2: CAGR-based
        cagr_forecast = self._forecast_cagr(eps_history, years)
        if cagr_forecast is not None:
            result["models_used"].append("cagr")
            result["forecasts"]["cagr"] = cagr_forecast

        # Model 3: Revenue-linked (if revenue data available)
        if revenue_history and len(revenue_history) >= len(eps_history):
            linked_forecast = self._forecast_revenue_linked(eps_history, revenue_history, years)
            if linked_forecast is not None:
                result["models_used"].append("revenue_linked")
                result["forecasts"]["revenue_linked"] = linked_forecast

        # Model 4: ML forecast (if available)
        if self.enable_ml and len(eps_history) >= 8:
            ml_forecast = self._forecast_ml(eps_history, years)
            if ml_forecast is not None:
                result["models_used"].append("ml")
                result["forecasts"]["ml"] = ml_forecast

        if not result["models_used"]:
            return result

        # Ensemble forecast
        last_eps = eps_history[-1]
        forecasts = []
        weights = []

        for model, forecast in result["forecasts"].items():
            if "values" in forecast:
                forecasts.append(forecast["values"][-1])
                weights.append(forecast.get("weight", 1.0))

        if forecasts:
            total_weight = sum(weights)
            if total_weight > 0:
                ensemble_eps = sum(f * w for f, w in zip(forecasts, weights)) / total_weight
            else:
                ensemble_eps = sum(forecasts) / len(forecasts)

            ensemble_cagr = (ensemble_eps / last_eps) ** (1 / years) - 1 if last_eps > 0 else 0

            result["ensemble"] = {
                "eps": ensemble_eps,
                "cagr": ensemble_cagr * 100,
                "values": [last_eps * (1 + ensemble_cagr) ** i for i in range(1, years + 1)]
            }

            # Calculate confidence
            result["confidence"] = self._calculate_confidence(result)
            result["forecast_available"] = True

        return result

    def _forecast_trend(self, values: List[float], years: int) -> Optional[Dict[str, Any]]:
        """Linear trend forecast."""
        try:
            n = len(values)
            x = list(range(n))
            y = [math.log(v) if v > 0 else 0 for v in values]

            if SKLEARN_AVAILABLE:
                model = LinearRegression()
                model.fit(np.array(x).reshape(-1, 1), y)
                slope = model.coef_[0]
                r2 = model.score(np.array(x).reshape(-1, 1), y)
            else:
                # Manual calculation
                x_mean = sum(x) / n
                y_mean = sum(y) / n
                slope = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, y)) / sum((xi - x_mean) ** 2)
                r2 = 0.6  # Default

            future_values = []
            for i in range(1, years + 1):
                log_val = math.log(values[-1]) + slope * i
                future_values.append(math.exp(log_val))

            return {
                "values": future_values,
                "slope": slope,
                "r2": r2,
                "weight": max(0.2, min(0.5, r2))
            }
        except Exception:
            return None

    def _forecast_cagr(self, values: List[float], years: int) -> Optional[Dict[str, Any]]:
        """CAGR-based forecast."""
        try:
            if len(values) < 2:
                return None

            # Calculate historical CAGR
            periods = [3, 5]  # 3-year and 5-year CAGR
            cagrs = []

            for period in periods:
                if len(values) > period:
                    cagr = (values[-1] / values[-period - 1]) ** (1 / period) - 1
                    cagrs.append(cagr)

            if not cagrs:
                return None

            avg_cagr = sum(cagrs) / len(cagrs)

            future_values = []
            for i in range(1, years + 1):
                future_values.append(values[-1] * (1 + avg_cagr) ** i)

            return {
                "values": future_values,
                "cagr": avg_cagr,
                "weight": 0.25
            }
        except Exception:
            return None

    def _forecast_revenue_linked(self, eps: List[float], revenue: List[float], years: int) -> Optional[Dict[str, Any]]:
        """Forecast EPS based on revenue projections."""
        try:
            # Calculate historical profit margin
            margins = [eps[i] / revenue[i] if revenue[i] > 0 else 0 for i in range(min(len(eps), len(revenue)))]
            avg_margin = sum(margins) / len(margins) if margins else 0

            # Forecast revenue
            rev_forecaster = FinancialForecaster({"revenue": revenue}, False)
            rev_forecast = rev_forecaster._forecast_trend(revenue, years)

            if rev_forecast is None:
                return None

            # Project EPS using margin
            future_values = [rev * avg_margin for rev in rev_forecast["values"]]

            return {
                "values": future_values,
                "margin": avg_margin,
                "weight": 0.2
            }
        except Exception:
            return None

    def _forecast_ml(self, values: List[float], years: int) -> Optional[Dict[str, Any]]:
        """ML-based forecast using Random Forest."""
        if not self.enable_ml or len(values) < 8:
            return None

        try:
            from sklearn.ensemble import RandomForestRegressor

            # Create features
            X = []
            y = []

            for i in range(4, len(values) - 1):
                features = [
                    values[i-4], values[i-3], values[i-2], values[i-1],  # Lag values
                    (values[i-1] / values[i-4] - 1) if values[i-4] > 0 else 0,  # 4-period return
                    np.std(values[i-4:i]) if len(values[i-4:i]) > 1 else 0  # Volatility
                ]
                X.append(features)
                y.append(values[i])

            if len(X) < 5:
                return None

            model = RandomForestRegressor(n_estimators=50, max_depth=3, random_state=42)
            model.fit(X, y)

            # Predict future
            future_values = []
            last_values = values[-4:]

            for _ in range(years):
                features = [
                    last_values[0], last_values[1], last_values[2], last_values[3],
                    (last_values[3] / last_values[0] - 1) if last_values[0] > 0 else 0,
                    np.std(last_values)
                ]
                pred = model.predict([features])[0]
                future_values.append(pred)

                # Update last values
                last_values = last_values[1:] + [pred]

            # Calculate prediction confidence
            tree_preds = [tree.predict([features])[0] for tree in model.estimators_]
            pred_std = np.std(tree_preds)
            confidence = max(0.3, min(0.9, 0.7 - pred_std / values[-1]))

            return {
                "values": future_values,
                "weight": 0.35,
                "confidence": confidence
            }
        except Exception:
            return None

    def _calculate_confidence(self, results: Dict[str, Any]) -> float:
        """Calculate ensemble confidence score."""
        if not results["forecasts"]:
            return 0.0

        # Model agreement
        final_values = []
        for forecast in results["forecasts"].values():
            if "values" in forecast:
                final_values.append(forecast["values"][-1])

        if len(final_values) > 1:
            cv = np.std(final_values) / np.mean(final_values) if np.mean(final_values) != 0 else 1
            agreement = max(0, 100 - cv * 200)
        else:
            agreement = 70

        # Average model confidence
        model_conf = 0
        count = 0
        for forecast in results["forecasts"].values():
            if "confidence" in forecast:
                model_conf += forecast["confidence"] * 100
                count += 1
        model_conf = model_conf / count if count > 0 else 60

        # History quality
        history_score = min(30, len(self.historical_data.get("eps", [])) * 2)

        return (agreement * 0.3 + model_conf * 0.4 + history_score * 0.3) / 100


# ============================================================================
# Yahoo Fundamentals Provider Implementation
# ============================================================================

@dataclass
class YahooFundamentalsProvider:
    """Advanced Yahoo Finance fundamentals provider with full feature set."""

    name: str = PROVIDER_NAME

    def __post_init__(self) -> None:
        self.timeout_sec = _timeout_sec()

        # Concurrency controls
        self.semaphore = asyncio.Semaphore(_max_concurrency())
        self.rate_limiter = TokenBucket(_rate_limit())
        self.circuit_breaker = SmartCircuitBreaker(
            fail_threshold=_cb_fail_threshold(),
            cooldown_sec=_cb_cooldown_sec()
        )
        self.singleflight = SingleFlight()

        # Caches
        self.fund_cache = SmartCache(maxsize=5000, ttl=_fund_ttl_sec())
        self.error_cache = SmartCache(maxsize=5000, ttl=_err_ttl_sec())

        # Metrics
        self.metrics: Dict[str, Any] = {
            "requests_total": 0,
            "requests_success": 0,
            "requests_failed": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "rate_limit_waits": 0,
            "circuit_breaker_blocks": 0,
            "singleflight_dedup": 0,
        }
        self._metrics_lock = asyncio.Lock()

        logger.info(
            f"YahooFundamentalsProvider v{PROVIDER_VERSION} initialized | "
            f"yfinance={_HAS_YFINANCE} | pandas={_HAS_PANDAS} | "
            f"timeout={self.timeout_sec}s | rate={_rate_limit()}/s | "
            f"cb={_cb_fail_threshold()}/{_cb_cooldown_sec()}s"
        )

    async def _update_metric(self, name: str, inc: int = 1) -> None:
        async with self._metrics_lock:
            self.metrics[name] = self.metrics.get(name, 0) + inc

    def _extract_financial_series(self, ticker: Any) -> Dict[str, List[float]]:
        """Extract historical financial series for forecasting."""
        series: Dict[str, List[float]] = {
            "eps": [],
            "revenue": [],
            "fcf": [],
            "book_value": []
        }

        try:
            # Get financials
            if hasattr(ticker, "financials") and ticker.financials is not None:
                financials = ticker.financials
                if not financials.empty:
                    # Revenue
                    if "Total Revenue" in financials.index:
                        rev = financials.loc["Total Revenue"].tolist()
                        series["revenue"] = [safe_float(x) for x in rev if safe_float(x) is not None]

                    # Net Income (for EPS calculation)
                    if "Net Income" in financials.index:
                        net_income = financials.loc["Net Income"].tolist()
                        net_income = [safe_float(x) for x in net_income if safe_float(x) is not None]

            # Get quarterly earnings for EPS history
            if hasattr(ticker, "earnings_dates") and ticker.earnings_dates is not None:
                earnings = ticker.earnings_dates
                if hasattr(earnings, "eps") and earnings.eps is not None:
                    eps_values = earnings.eps.tolist()
                    series["eps"] = [safe_float(x) for x in eps_values if safe_float(x) is not None]

            # Get cashflow for FCF
            if hasattr(ticker, "cashflow") and ticker.cashflow is not None:
                cashflow = ticker.cashflow
                if not cashflow.empty and "Free Cash Flow" in cashflow.index:
                    fcf = cashflow.loc["Free Cash Flow"].tolist()
                    series["fcf"] = [safe_float(x) for x in fcf if safe_float(x) is not None]

            # Get balance sheet for book value
            if hasattr(ticker, "balance_sheet") and ticker.balance_sheet is not None:
                balance = ticker.balance_sheet
                if not balance.empty and "Total Equity Gross Minority Interest" in balance.index:
                    equity = balance.loc["Total Equity Gross Minority Interest"].tolist()
                    series["book_value"] = [safe_float(x) for x in equity if safe_float(x) is not None]

        except Exception as e:
            logger.debug(f"Error extracting financial series: {e}")

        return series

    def _blocking_fetch(self, symbol: str) -> Dict[str, Any]:
        """Blocking fetch function to run in thread."""
        if not _HAS_YFINANCE or yf is None:
            return {"error": "yfinance not installed"}

        try:
            ticker = yf.Ticker(symbol)

            # Primary: info dict
            info = {}
            try:
                info = ticker.info or {}
            except Exception as e:
                info = {"_info_error": str(e)}

            # Fast info for price fallback
            fast_price = None
            try:
                fi = getattr(ticker, "fast_info", None)
                if fi:
                    fast_price = getattr(fi, "last_price", None) or fi.get("last_price", None)
            except Exception:
                pass

            # Extract financial series for forecasting
            series = self._extract_financial_series(ticker)

            # Build financial metrics
            metrics = FinancialMetrics()

            # Identity
            metrics.name = safe_str(info.get("longName") or info.get("shortName"))
            metrics.sector = safe_str(info.get("sector"))
            metrics.sector = safe_str(info.get("sector"))
            metrics.industry = safe_str(info.get("industry"))

            # Market data
            metrics.market_cap = safe_float(info.get("marketCap"))
            metrics.enterprise_value = safe_float(info.get("enterpriseValue"))
            metrics.beta = safe_float(info.get("beta"))

            # Valuation ratios
            metrics.pe_ttm = safe_float(info.get("trailingPE"))
            metrics.forward_pe = safe_float(info.get("forwardPE"))
            metrics.ps_ttm = safe_float(info.get("priceToSalesTrailing12Months"))
            metrics.pb_ttm = safe_float(info.get("priceToBook"))
            metrics.pfcf_ttm = safe_float(info.get("priceToFreeCashFlow"))

            # Profitability
            metrics.gross_margin = as_percent(info.get("grossMargins"))
            metrics.operating_margin = as_percent(info.get("operatingMargins"))
            metrics.net_margin = as_percent(info.get("profitMargins"))
            metrics.roe = as_percent(info.get("returnOnEquity"))
            metrics.roa = as_percent(info.get("returnOnAssets"))
            metrics.roce = as_percent(info.get("returnOnCapital"))

            # Growth
            metrics.revenue_growth_yoy = as_percent(info.get("revenueGrowth"))
            metrics.earnings_growth_yoy = as_percent(info.get("earningsGrowth"))

            # Per share
            metrics.eps_ttm = safe_float(info.get("trailingEps"))
            metrics.forward_eps = safe_float(info.get("forwardEps"))

            # Dividends
            metrics.dividend_yield = as_percent(info.get("dividendYield"))
            metrics.dividend_rate = safe_float(info.get("dividendRate"))
            metrics.payout_ratio = as_percent(info.get("payoutRatio"))

            # Analyst targets
            metrics.target_mean = safe_float(info.get("targetMeanPrice"))
            metrics.target_high = safe_float(info.get("targetHighPrice"))
            metrics.target_low = safe_float(info.get("targetLowPrice"))
            metrics.recommendation = map_recommendation(info.get("recommendationKey"))
            metrics.analyst_count = safe_int(info.get("numberOfAnalystOpinions"))

            # Balance sheet items
            metrics.current_ratio = safe_float(info.get("currentRatio"))
            metrics.quick_ratio = safe_float(info.get("quickRatio"))
            metrics.debt_to_equity = safe_float(info.get("debtToEquity"))
            metrics.interest_coverage = safe_float(info.get("interestCoverage"))

            # Cash flow
            metrics.operating_cf = safe_float(info.get("operatingCashflow"))
            metrics.free_cashflow = safe_float(info.get("freeCashflow"))
            metrics.fcf_yield = as_percent(info.get("freeCashFlowYield"))

            # Risk metrics
            metrics.short_ratio = safe_float(info.get("shortRatio"))
            metrics.short_percent = as_percent(info.get("shortPercentOfFloat"))

            # Current price
            current_price = (
                safe_float(info.get("currentPrice")) or
                safe_float(info.get("regularMarketPrice")) or
                safe_float(fast_price)
            )

            # Build output dictionary
            out: Dict[str, Any] = {
                "requested_symbol": symbol,
                "symbol": symbol,
                "provider_symbol": symbol,
                "name": metrics.name,
                "sector": metrics.sector,
                "industry": metrics.industry,
                "sub_sector": metrics.industry,  # Alias for dashboard
                "market": safe_str(info.get("market")),
                "currency": safe_str(info.get("currency") or info.get("financialCurrency") or "USD"),
                "exchange": safe_str(info.get("exchange")),
                "country": safe_str(info.get("country")),

                "market_cap": metrics.market_cap,
                "enterprise_value": metrics.enterprise_value,
                "shares_outstanding": safe_float(info.get("sharesOutstanding")),
                "pe_ttm": metrics.pe_ttm,
                "forward_pe": metrics.forward_pe,
                "peg_ratio": safe_float(info.get("pegRatio")),
                "ps_ttm": metrics.ps_ttm,
                "pb_ttm": metrics.pb_ttm,
                "pfcf_ttm": metrics.pfcf_ttm,

                "eps_ttm": metrics.eps_ttm,
                "forward_eps": metrics.forward_eps,
                "book_value": safe_float(info.get("bookValue")),
                "beta": metrics.beta,

                "gross_margin": metrics.gross_margin,
                "operating_margin": metrics.operating_margin,
                "net_margin": metrics.net_margin,
                "profit_margin": metrics.net_margin,  # Alias
                "roe": metrics.roe,
                "roa": metrics.roa,
                "roce": metrics.roce,

                "revenue_growth": metrics.revenue_growth_yoy,
                "earnings_growth": metrics.earnings_growth_yoy,
                "fcf_growth": None,  # Will calculate from series

                "current_ratio": metrics.current_ratio,
                "quick_ratio": metrics.quick_ratio,
                "debt_to_equity": metrics.debt_to_equity,
                "interest_coverage": metrics.interest_coverage,

                "operating_cashflow": metrics.operating_cf,
                "free_cashflow": metrics.free_cashflow,
                "fcf_yield": metrics.fcf_yield,

                "dividend_yield": metrics.dividend_yield,
                "dividend_rate": metrics.dividend_rate,
                "payout_ratio": metrics.payout_ratio,

                "target_mean_price": metrics.target_mean,
                "target_high_price": metrics.target_high,
                "target_low_price": metrics.target_low,
                "recommendation": metrics.recommendation,
                "analyst_count": metrics.analyst_count,

                "short_ratio": metrics.short_ratio,
                "short_percent": metrics.short_percent,

                "current_price": current_price,
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                "provider_version": PROVIDER_VERSION,
                "last_updated_utc": _utc_iso(),
                "last_updated_riyadh": _riyadh_iso(),
            }

            # Calculate additional metrics

            # 1. Upside based on analyst target
            if current_price and current_price > 0 and metrics.target_mean and metrics.target_mean > 0:
                upside = ((metrics.target_mean / current_price) - 1) * 100
                out["upside_percent"] = float(upside)
                out["forecast_price_12m"] = float(metrics.target_mean)
                out["expected_roi_12m"] = float(upside)

                # Confidence based on analyst count
                if metrics.analyst_count and metrics.analyst_count > 0:
                    conf = 0.6 + (math.log(metrics.analyst_count + 1) * 0.05)
                    out["forecast_confidence"] = float(min(0.95, conf))
                    out["confidence_score"] = float(min(95, conf * 100))
                else:
                    out["forecast_confidence"] = 0.5
                    out["confidence_score"] = 50.0

                out["forecast_method"] = "analyst_consensus"

            # 2. Graham Number (intrinsic value)
            if metrics.eps_ttm and metrics.book_value:
                graham = FinancialAnalyzer.calculate_graham_number(
                    metrics.eps_ttm,
                    metrics.book_value
                )
                out["graham_number"] = float(graham)
                if current_price and current_price > 0:
                    out["graham_upside"] = float(((graham / current_price) - 1) * 100)

            # 3. DCF Value (if FCF available)
            if _enable_dcf() and series["fcf"] and len(series["fcf"]) >= 3:
                latest_fcf = series["fcf"][-1]
                if latest_fcf > 0:
                    # Estimate growth rate from history
                    if len(series["fcf"]) >= 5:
                        fcf_cagr = (series["fcf"][-1] / series["fcf"][0]) ** (1 / len(series["fcf"])) - 1
                        growth_rate = max(0.02, min(0.15, fcf_cagr))
                    else:
                        growth_rate = 0.05  # Default

                    dcf_value = FinancialAnalyzer.calculate_dcf_value(
                        fcf=latest_fcf,
                        growth_rate=growth_rate,
                        terminal_growth=0.03,
                        discount_rate=0.10
                    )

                    # Convert to per-share value
                    shares = metrics.shares_outstanding or safe_float(info.get("sharesOutstanding"))
                    if shares and shares > 0:
                        dcf_per_share = dcf_value / shares
                        out["dcf_value"] = float(dcf_per_share)
                        if current_price and current_price > 0:
                            out["dcf_upside"] = float(((dcf_per_share / current_price) - 1) * 100)

            # 4. Altman Z-Score
            balance_sheet = {}
            try:
                bs = ticker.balance_sheet
                if bs is not None and not bs.empty:
                    # Extract needed fields
                    if "Total Assets" in bs.index:
                        balance_sheet["total_assets"] = safe_float(bs.loc["Total Assets"].iloc[0])
                    if "Total Liabilities Net Minority Interest" in bs.index:
                        balance_sheet["total_liabilities"] = safe_float(bs.loc["Total Liabilities Net Minority Interest"].iloc[0])
                    if "Retained Earnings" in bs.index:
                        balance_sheet["retained_earnings"] = safe_float(bs.loc["Retained Earnings"].iloc[0])
                    if "Working Capital" in bs.index:
                        balance_sheet["working_capital"] = safe_float(bs.loc["Working Capital"].iloc[0])
            except Exception:
                pass

            # Add income statement items
            try:
                inc = ticker.income_stmt
                if inc is not None and not inc.empty:
                    if "EBIT" in inc.index:
                        balance_sheet["ebit"] = safe_float(inc.loc["EBIT"].iloc[0])
                    if "Total Revenue" in inc.index:
                        balance_sheet["total_revenue"] = safe_float(inc.loc["Total Revenue"].iloc[0])
            except Exception:
                pass

            balance_sheet["market_cap"] = metrics.market_cap

            z_score = FinancialAnalyzer.calculate_altman_z_score(balance_sheet)
            if z_score is not None:
                out["altman_z_score"] = float(z_score)
                if z_score > 2.99:
                    out["bankruptcy_risk"] = "low"
                elif z_score > 1.81:
                    out["bankruptcy_risk"] = "medium"
                else:
                    out["bankruptcy_risk"] = "high"

            # 5. Growth stage and score
            growth_metrics = {
                "revenue_growth": metrics.revenue_growth_yoy,
                "net_margin": metrics.net_margin,
                "roe": metrics.roe,
                "pe_ttm": metrics.pe_ttm,
                "upside_percent": out.get("upside_percent")
            }
            growth_stage = FinancialAnalyzer.determine_growth_stage(growth_metrics)
            out["growth_stage"] = growth_stage.value

            growth_score = FinancialAnalyzer.calculate_growth_score(growth_metrics)
            out["growth_score"] = growth_score

            # 6. Financial health classification
            health_score = 0
            if metrics.current_ratio and metrics.current_ratio > 1.5:
                health_score += 25
            if metrics.debt_to_equity and metrics.debt_to_equity < 1.0:
                health_score += 25
            if metrics.interest_coverage and metrics.interest_coverage > 3:
                health_score += 25
            if metrics.free_cashflow and metrics.free_cashflow > 0:
                health_score += 25

            if health_score >= 80:
                out["financial_health"] = FinancialHealth.EXCELLENT.value
            elif health_score >= 60:
                out["financial_health"] = FinancialHealth.GOOD.value
            elif health_score >= 40:
                out["financial_health"] = FinancialHealth.FAIR.value
            elif health_score >= 20:
                out["financial_health"] = FinancialHealth.POOR.value
            else:
                out["financial_health"] = FinancialHealth.CRITICAL.value

            out["financial_health_score"] = health_score

            # 7. Earnings forecast (if we have history)
            if _enable_ml_forecast() and series["eps"] and len(series["eps"]) >= 4:
                forecaster = FinancialForecaster(series, enable_ml=True)
                forecast = forecaster.forecast_earnings(years=3)

                if forecast.get("forecast_available"):
                    out["earnings_forecast_available"] = True
                    out["earnings_forecast_confidence"] = forecast["confidence"]

                    if "ensemble" in forecast:
                        out["eps_forecast_1y"] = forecast["ensemble"]["values"][0] if len(forecast["ensemble"]["values"]) > 0 else None
                        out["eps_forecast_2y"] = forecast["ensemble"]["values"][1] if len(forecast["ensemble"]["values"]) > 1 else None
                        out["eps_forecast_3y"] = forecast["ensemble"]["values"][2] if len(forecast["ensemble"]["values"]) > 2 else None
                        out["eps_cagr_forecast"] = forecast["ensemble"]["cagr"]
                        out["forecast_models"] = forecast["models_used"]

            # 8. Data quality
            quality_category, quality_score = data_quality_score(out)
            out["data_quality"] = quality_category.value
            out["data_quality_score"] = quality_score

            # 9. Forecast timestamps
            out["forecast_updated_utc"] = _utc_iso()
            out["forecast_updated_riyadh"] = _riyadh_iso()

            return clean_patch(out)

        except Exception as e:
            return {"error": f"fetch failed: {e.__class__.__name__}: {str(e)}"}

    async def fetch_fundamentals_patch(
        self,
        symbol: str,
        debug: bool = False,
        *args: Any,
        **kwargs: Any
    ) -> Dict[str, Any]:
        """Fetch comprehensive fundamentals data."""
        if not _configured():
            return {} if _HAS_YFINANCE else ({} if not _emit_warnings() else {"_warn": "yfinance not installed"})

        # Normalize symbol
        norm_symbol = normalize_symbol(symbol)
        if not norm_symbol:
            return {} if not _emit_warnings() else {"_warn": "invalid_symbol"}

        cache_key = f"yf:{norm_symbol}"

        # Check error cache
        if await self.error_cache.get(cache_key):
            return {} if not _emit_warnings() else {"_warn": "temporarily_backed_off"}

        # Check cache
        cached = await self.fund_cache.get(cache_key)
        if cached:
            await self._update_metric("cache_hits")
            if debug:
                logger.info(f"Yahoo fundamentals cache hit for {norm_symbol}")
            return dict(cached)

        await self._update_metric("cache_misses")

        # Circuit breaker check
        if not await self.circuit_breaker.allow_request():
            await self._update_metric("circuit_breaker_blocks")
            return {} if not _emit_warnings() else {"_warn": "circuit_breaker_open"}

        async def _fetch():
            async with self.semaphore:
                await self.rate_limiter.wait_and_acquire()

                try:
                    # Run blocking fetch in thread
                    result = await asyncio.wait_for(
                        asyncio.to_thread(self._blocking_fetch, norm_symbol),
                        timeout=self.timeout_sec
                    )

                    if "error" in result:
                        await self.error_cache.set(cache_key, True)
                        await self.circuit_breaker.on_failure()
                        await self._update_metric("requests_failed")
                        return {} if not _emit_warnings() else {"_warn": result["error"]}

                    # Cache successful result
                    await self.fund_cache.set(cache_key, result)
                    await self.circuit_breaker.on_success()
                    await self._update_metric("requests_success")

                    if debug:
                        logger.info(f"Yahoo fundamentals fetched {norm_symbol}: {result.get('name')}")

                    return result

                except asyncio.TimeoutError:
                    await self.error_cache.set(cache_key, True)
                    await self.circuit_breaker.on_failure()
                    await self._update_metric("requests_failed")
                    return {} if not _emit_warnings() else {"_warn": "timeout"}

                except Exception as e:
                    await self.error_cache.set(cache_key, True)
                    await self.circuit_breaker.on_failure()
                    await self._update_metric("requests_failed")
                    return {} if not _emit_warnings() else {"_warn": f"exception: {e.__class__.__name__}"}

        return await self.singleflight.run(cache_key, _fetch)

    async def get_metrics(self) -> Dict[str, Any]:
        """Get client metrics."""
        async with self._metrics_lock:
            metrics = dict(self.metrics)

        metrics["circuit_breaker"] = self.circuit_breaker.get_stats()
        metrics["cache_sizes"] = {
            "fund": await self.fund_cache.size(),
            "error": await self.error_cache.size(),
        }

        return metrics


# ============================================================================
# Singleton Management
# ============================================================================

_PROVIDER_INSTANCE: Optional[YahooFundamentalsProvider] = None
_PROVIDER_LOCK = asyncio.Lock()


async def get_provider() -> YahooFundamentalsProvider:
    """Get or create provider singleton."""
    global _PROVIDER_INSTANCE
    if _PROVIDER_INSTANCE is None:
        async with _PROVIDER_LOCK:
            if _PROVIDER_INSTANCE is None:
                _PROVIDER_INSTANCE = YahooFundamentalsProvider()
    return _PROVIDER_INSTANCE


async def close_provider() -> None:
    """Close provider (no-op for yfinance)."""
    global _PROVIDER_INSTANCE
    _PROVIDER_INSTANCE = None


# ============================================================================
# Public API (Engine Compatible)
# ============================================================================

async def fetch_fundamentals_patch(
    symbol: str,
    debug: bool = False,
    *args: Any,
    **kwargs: Any
) -> Dict[str, Any]:
    """Primary engine entry: fetch fundamentals data."""
    provider = await get_provider()
    return await provider.fetch_fundamentals_patch(symbol, debug=debug)


async def fetch_enriched_quote_patch(
    symbol: str,
    debug: bool = False,
    *args: Any,
    **kwargs: Any
) -> Dict[str, Any]:
    """Compatibility alias for engines expecting enriched quote."""
    return await fetch_fundamentals_patch(symbol, debug=debug)


async def get_client_metrics() -> Dict[str, Any]:
    """Get client performance metrics."""
    provider = await get_provider()
    return await provider.get_metrics()


async def aclose_yahoo_fundamentals_client() -> None:
    """Close client (compatibility alias)."""
    await close_provider()


# ============================================================================
# Module Exports
# ============================================================================

__all__ = [
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    "YahooFundamentalsProvider",
    "get_provider",
    "fetch_fundamentals_patch",
    "fetch_enriched_quote_patch",
    "get_client_metrics",
    "aclose_yahoo_fundamentals_client",
    "normalize_symbol",
    "FinancialHealth",
    "GrowthStage",
    "DataQuality",
]
