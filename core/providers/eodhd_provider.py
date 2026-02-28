#!/usr/bin/env python3
# core/providers/eodhd_provider.py
"""
================================================================================
EODHD Provider — v4.1.0 (GLOBAL-STABLE / RENDER-SAFE / EODHD PRIMARY)
================================================================================

Purpose of this revision (GLOBAL-first):
- ✅ Fix GLOBAL fetch failures when symbols are sent like "AAPL" (EODHD expects "AAPL.US" in many cases)
- ✅ Strict, safe symbol normalization with configurable exchange suffix
- ✅ Render-safe: all heavy ML/science deps are OPTIONAL (no startup crashes)
- ✅ Stronger, production-grade HTTP retries + rate-limit handling (429)
- ✅ Returns a consistent "enriched quote patch" with core fields:
     name, currency, current_price, prev_close, change, change_pct, etc.
- ✅ Optional fundamentals + history enrichment (52W high/low, avg vol, volatility)

Key env vars (works with your revised core/config.py):
- EODHD_API_KEY (or EODHD_API_TOKEN / EODHD_KEY)
- EODHD_BASE_URL (default: https://eodhistoricaldata.com/api)
- EODHD_DEFAULT_EXCHANGE (default: US)
- EODHD_APPEND_EXCHANGE_SUFFIX (default: true)  -> "AAPL" => "AAPL.US"
- KSA_DISALLOW_EODHD (default: false here; engine may enforce too)
- ALLOW_EODHD_KSA / EODHD_ALLOW_KSA (override KSA block)
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import httpx

logger = logging.getLogger("core.providers.eodhd_provider")

PROVIDER_NAME = "eodhd"
PROVIDER_VERSION = "4.1.0"

# EODHD official base
DEFAULT_BASE_URL = "https://eodhistoricaldata.com/api"

# -----------------------------
# Optional JSON perf (orjson)
# -----------------------------
try:
    import orjson  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)

    _HAS_ORJSON = True
except Exception:
    import json

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, bytes):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)

    _HAS_ORJSON = False

# -----------------------------
# Optional numpy (never required)
# -----------------------------
try:
    import numpy as np  # type: ignore

    _HAS_NUMPY = True
except Exception:
    np = None  # type: ignore
    _HAS_NUMPY = False

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

UA_DEFAULT = "TFB-EODHD/4.1.0 (Render)"

# ============================================================================
# Env helpers (safe)
# ============================================================================
def _env_str(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in _TRUTHY:
        return True
    if raw in _FALSY:
        return False
    return default


def _env_int(name: str, default: int, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    try:
        v = int(float((os.getenv(name) or str(default)).strip()))
    except Exception:
        v = default
    if lo is not None and v < lo:
        v = lo
    if hi is not None and v > hi:
        v = hi
    return v


def _env_float(name: str, default: float, lo: Optional[float] = None, hi: Optional[float] = None) -> float:
    try:
        v = float((os.getenv(name) or str(default)).strip())
    except Exception:
        v = default
    if lo is not None and v < lo:
        v = lo
    if hi is not None and v > hi:
        v = hi
    return v


def _token() -> str:
    return _env_str("EODHD_API_KEY") or _env_str("EODHD_API_TOKEN") or _env_str("EODHD_KEY")


def _base_url() -> str:
    return _env_str("EODHD_BASE_URL", DEFAULT_BASE_URL).rstrip("/")


def _default_exchange() -> str:
    # Align with core/config.py
    ex = _env_str("EODHD_DEFAULT_EXCHANGE") or _env_str("EODHD_SYMBOL_SUFFIX_DEFAULT") or "US"
    return ex.strip().upper() or "US"


def _append_exchange_suffix() -> bool:
    return _env_bool("EODHD_APPEND_EXCHANGE_SUFFIX", True)


def _ksa_blocked_by_default() -> bool:
    # If engine blocks KSA, it can also enforce it; provider is defensive
    return _env_bool("KSA_DISALLOW_EODHD", False)


def _allow_ksa_override() -> bool:
    return _env_bool("ALLOW_EODHD_KSA", False) or _env_bool("EODHD_ALLOW_KSA", False)


def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(timezone.utc)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).isoformat()


def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(timezone.utc)
    tz = timezone(timedelta(hours=3))
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(tz).isoformat()


# ============================================================================
# Data coercion
# ============================================================================
def safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        if isinstance(v, (int, float)):
            x = float(v)
        else:
            s = str(v).strip().replace(",", "")
            if not s:
                return None
            x = float(s)
        if math.isnan(x) or math.isinf(x):
            return None
        return x
    except Exception:
        return None


def safe_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    try:
        s = str(v).strip()
        return s if s else None
    except Exception:
        return None


def _clean_patch(p: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in (p or {}).items() if v is not None}


# ============================================================================
# Symbol normalization (KEY FIX for GLOBAL)
# ============================================================================
_US_LIKE_RE = re.compile(r"^[A-Z0-9][A-Z0-9\-\_\.]{0,11}$")  # allow BRK-B style
_KSA_RE = re.compile(r"^\d{3,4}\.SR$", re.IGNORECASE)

def normalize_eodhd_symbol(symbol: str) -> str:
    """
    Normalize a symbol for EODHD.
    - If already exchange-qualified (contains '.') -> keep
    - If plain US ticker like AAPL -> append .US (configurable)
    - Keep KSA (.SR) as-is (but provider may block KSA by default)
    """
    s = (symbol or "").strip().upper()
    if not s:
        return ""

    # Preserve already-qualified tickers: AAPL.US, 7203.T, VOD.LSE, 2222.SR, etc.
    if "." in s:
        return s

    # Don't touch exotic notations
    if "=" in s or "^" in s or "/" in s:
        return s

    if not _append_exchange_suffix():
        return s

    # If it looks like a standard equity ticker, append default exchange suffix
    if _US_LIKE_RE.match(s):
        return f"{s}.{_default_exchange()}"

    return s


# ============================================================================
# Small TTL cache (async-safe)
# ============================================================================
@dataclass
class _CacheItem:
    exp: float
    val: Any


class _TTLCache:
    def __init__(self, maxsize: int, ttl_sec: float):
        self.maxsize = max(128, int(maxsize))
        self.ttl_sec = max(1.0, float(ttl_sec))
        self._d: Dict[str, _CacheItem] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Any:
        now = time.monotonic()
        async with self._lock:
            it = self._d.get(key)
            if not it:
                return None
            if it.exp > now:
                return it.val
            self._d.pop(key, None)
            return None

    async def set(self, key: str, val: Any, ttl_sec: Optional[float] = None) -> None:
        now = time.monotonic()
        ttl = self.ttl_sec if ttl_sec is None else max(1.0, float(ttl_sec))
        async with self._lock:
            if len(self._d) >= self.maxsize and key not in self._d:
                # simple eviction: remove one arbitrary key (fast)
                self._d.pop(next(iter(self._d.keys())), None)
            self._d[key] = _CacheItem(exp=now + ttl, val=val)


# ============================================================================
# EODHD Client
# ============================================================================
class EODHDClient:
    def __init__(self) -> None:
        self.api_key = _token()
        self.base_url = _base_url()

        # Timeouts / retries
        self.timeout_sec = _env_float("EODHD_TIMEOUT_SEC", 15.0, lo=3.0, hi=120.0)
        self.retry_attempts = _env_int("EODHD_RETRY_ATTEMPTS", 4, lo=0, hi=10)
        self.retry_base_delay = _env_float("EODHD_RETRY_BASE_DELAY", 0.6, lo=0.0, hi=10.0)

        # Rate limiting (soft)
        self.max_concurrency = _env_int("EODHD_MAX_CONCURRENCY", 20, lo=1, hi=100)
        self._sem = asyncio.Semaphore(self.max_concurrency)

        # Caches
        self.quote_cache = _TTLCache(maxsize=6000, ttl_sec=_env_float("EODHD_QUOTE_TTL_SEC", 12.0, lo=1.0, hi=600.0))
        self.fund_cache = _TTLCache(maxsize=3000, ttl_sec=_env_float("EODHD_FUND_TTL_SEC", 21600.0, lo=60.0, hi=86400.0))
        self.hist_cache = _TTLCache(maxsize=2000, ttl_sec=_env_float("EODHD_HISTORY_TTL_SEC", 1800.0, lo=60.0, hi=86400.0))

        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.timeout_sec),
            headers={"User-Agent": _env_str("EODHD_USER_AGENT", UA_DEFAULT)},
            limits=httpx.Limits(max_keepalive_connections=30, max_connections=60),
            http2=True,
        )

    def _base_params(self) -> Dict[str, str]:
        # EODHD uses api_token
        return {"api_token": self.api_key, "fmt": "json"}

    async def _request_json(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Tuple[Optional[Any], Optional[str]]:
        if not self.api_key:
            return None, "EODHD_API_KEY missing"

        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        p = {**self._base_params(), **(params or {})}

        last_err: Optional[str] = None

        async with self._sem:
            for attempt in range(max(1, self.retry_attempts + 1)):
                try:
                    r = await self._client.get(url, params=p)
                    sc = int(r.status_code)

                    # Rate limit
                    if sc == 429:
                        ra = r.headers.get("Retry-After")
                        wait = float(ra) if ra and ra.isdigit() else min(10.0, 1.0 + attempt)
                        last_err = "HTTP 429"
                        await asyncio.sleep(wait)
                        continue

                    # Server errors -> retry
                    if 500 <= sc < 600:
                        last_err = f"HTTP {sc}"
                        # full jitter backoff
                        base = self.retry_base_delay * (2 ** (attempt - 1))
                        await asyncio.sleep(min(10.0, random.uniform(0, base + 0.2)))
                        continue

                    # 404 is a valid "not found"
                    if sc == 404:
                        return None, "HTTP 404 not_found"

                    if sc >= 400:
                        return None, f"HTTP {sc}"

                    try:
                        data = json_loads(r.content)
                        return data, None
                    except Exception:
                        return None, "invalid_json_payload"

                except httpx.RequestError as e:
                    last_err = f"network_error:{e.__class__.__name__}"
                    base = self.retry_base_delay * (2 ** (attempt - 1))
                    await asyncio.sleep(min(10.0, random.uniform(0, base + 0.2)))

                except Exception as e:
                    last_err = f"unexpected_error:{e.__class__.__name__}"
                    break

        return None, last_err or "request_failed"

    # -----------------------------
    # Fetchers
    # -----------------------------
    async def fetch_quote(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        sym = normalize_eodhd_symbol(symbol)
        if not sym:
            return {}, "invalid_symbol"

        # KSA blocking
        if _KSA_RE.match(sym) and _ksa_blocked_by_default() and not _allow_ksa_override():
            return {}, "ksa_blocked"

        ck = f"q:{sym}"
        cached = await self.quote_cache.get(ck)
        if cached:
            return cached, None

        data, err = await self._request_json(f"real-time/{sym}", params={})
        if err or not isinstance(data, dict):
            return {}, err or "bad_payload"

        # Typical EODHD fields
        close = safe_float(data.get("close"))
        prev = safe_float(data.get("previousClose"))
        chg = safe_float(data.get("change"))
        chg_p = safe_float(data.get("change_p"))

        patch = _clean_patch(
            {
                "symbol": symbol,
                "symbol_normalized": sym,
                "data_source": PROVIDER_NAME,
                "current_price": close,
                "previous_close": prev,
                "change": chg,
                "change_pct": chg_p,
                "day_open": safe_float(data.get("open")),
                "day_high": safe_float(data.get("high")),
                "day_low": safe_float(data.get("low")),
                "volume": safe_float(data.get("volume")),
                "currency": safe_str(data.get("currency")) or None,
                "last_updated_utc": _utc_iso(),
                "last_updated_riyadh": _riyadh_iso(),
            }
        )

        await self.quote_cache.set(ck, patch)
        return patch, None

    async def fetch_fundamentals(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        sym = normalize_eodhd_symbol(symbol)
        if not sym:
            return {}, "invalid_symbol"

        if _KSA_RE.match(sym) and _ksa_blocked_by_default() and not _allow_ksa_override():
            return {}, "ksa_blocked"

        ck = f"f:{sym}"
        cached = await self.fund_cache.get(ck)
        if cached:
            return cached, None

        data, err = await self._request_json(f"fundamentals/{sym}", params={})
        if err or not isinstance(data, dict):
            return {}, err or "bad_payload"

        general = data.get("General") or {}
        highlights = data.get("Highlights") or {}
        valuation = data.get("Valuation") or {}
        shares = data.get("SharesStats") or {}

        # EODHD sometimes returns CurrencyCode in General
        currency = safe_str(general.get("CurrencyCode")) or safe_str(general.get("Currency")) or None

        patch = _clean_patch(
            {
                "symbol": symbol,
                "symbol_normalized": sym,
                "data_source": PROVIDER_NAME,
                "name": safe_str(general.get("Name")),
                "exchange": safe_str(general.get("Exchange")),
                "country": safe_str(general.get("CountryName")) or safe_str(general.get("CountryISO")),
                "currency": currency,
                "sector": safe_str(general.get("Sector")),
                "sub_sector": safe_str(general.get("Industry")),
                # fundamentals
                "market_cap": safe_float(highlights.get("MarketCapitalization")) or safe_float(general.get("MarketCapitalization")),
                "eps_ttm": safe_float(highlights.get("EarningsShare")),
                "pe_ttm": safe_float(valuation.get("TrailingPE")) or safe_float(highlights.get("PERatio")),
                "forward_pe": safe_float(valuation.get("ForwardPE")),
                "pb": safe_float(valuation.get("PriceBookMRQ")),
                "dividend_yield": (
                    (safe_float(highlights.get("DividendYield")) * 100.0)
                    if safe_float(highlights.get("DividendYield")) is not None
                    else None
                ),
                "shares_outstanding": safe_float(shares.get("SharesOutstanding")),
                "last_updated_utc": _utc_iso(),
                "last_updated_riyadh": _riyadh_iso(),
            }
        )

        await self.fund_cache.set(ck, patch)
        return patch, None

    async def fetch_history_stats(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        """
        Pull EOD history and compute lightweight stats:
        - 52W high/low + position %
        - avg volume 30D
        - volatility 30D (stdev of daily returns)
        """
        sym = normalize_eodhd_symbol(symbol)
        if not sym:
            return {}, "invalid_symbol"

        if _KSA_RE.match(sym) and _ksa_blocked_by_default() and not _allow_ksa_override():
            return {}, "ksa_blocked"

        ck = f"h:{sym}"
        cached = await self.hist_cache.get(ck)
        if cached:
            return cached, None

        days = _env_int("EODHD_HISTORY_DAYS", 420, lo=60, hi=5000)
        from_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
        data, err = await self._request_json(f"eod/{sym}", params={"from": from_date})
        if err or not isinstance(data, list):
            return {}, err or "bad_payload"

        closes: List[float] = []
        vols: List[float] = []

        for row in data:
            if not isinstance(row, dict):
                continue
            c = safe_float(row.get("close"))
            if c is not None:
                closes.append(c)
            v = safe_float(row.get("volume"))
            if v is not None:
                vols.append(v)

        if len(closes) < 20:
            patch = {"symbol": symbol, "symbol_normalized": sym, "data_source": PROVIDER_NAME}
            await self.hist_cache.set(ck, patch)
            return patch, None

        # 52W window ~ 252 trading days
        win = min(252, len(closes))
        last = closes[-1]
        lo_52 = min(closes[-win:])
        hi_52 = max(closes[-win:])
        pos_52 = None
        if hi_52 != lo_52:
            pos_52 = (last - lo_52) / (hi_52 - lo_52) * 100.0

        # avg volume 30D
        avg_vol_30 = None
        if len(vols) >= 30:
            avg_vol_30 = sum(vols[-30:]) / 30.0

        # volatility 30D
        vol_30 = None
        if len(closes) >= 31:
            rets = []
            for i in range(len(closes) - 30, len(closes)):
                if i <= 0:
                    continue
                prev = closes[i - 1]
                cur = closes[i]
                if prev != 0:
                    rets.append((cur / prev) - 1.0)

            if rets:
                if _HAS_NUMPY:
                    vol_30 = float(np.std(np.array(rets, dtype=float)))  # type: ignore
                else:
                    m = sum(rets) / len(rets)
                    vol_30 = math.sqrt(sum((x - m) ** 2 for x in rets) / max(1, (len(rets) - 1)))

        patch = _clean_patch(
            {
                "symbol": symbol,
                "symbol_normalized": sym,
                "data_source": PROVIDER_NAME,
                "low_52w": lo_52,
                "high_52w": hi_52,
                "position_52w_pct": pos_52,
                "avg_vol_30d": avg_vol_30,
                "volatility_30d": vol_30,
                "last_updated_utc": _utc_iso(),
                "last_updated_riyadh": _riyadh_iso(),
            }
        )

        await self.hist_cache.set(ck, patch)
        return patch, None

    # -----------------------------
    # Public: enriched patch
    # -----------------------------
    async def fetch_enriched_quote_patch(self, symbol: str) -> Dict[str, Any]:
        """
        Returns a dict patch to merge into the unified enriched quote model.
        Never throws. Always includes timestamps. Returns structured error fields on failure.
        """
        now_utc = _utc_iso()
        now_riy = _riyadh_iso()

        sym = (symbol or "").strip().upper()
        sym_norm = normalize_eodhd_symbol(sym)

        # KSA block
        if _KSA_RE.match(sym_norm) and _ksa_blocked_by_default() and not _allow_ksa_override():
            return {
                "symbol": sym,
                "symbol_normalized": sym_norm,
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                "data_quality": "BLOCKED",
                "error": "ksa_blocked",
                "last_updated_utc": now_utc,
                "last_updated_riyadh": now_riy,
            }

        enable_fund = _env_bool("EODHD_ENABLE_FUNDAMENTALS", True)
        enable_hist = _env_bool("EODHD_ENABLE_HISTORY", True)

        tasks = [self.fetch_quote(sym)]
        if enable_fund:
            tasks.append(self.fetch_fundamentals(sym))
        if enable_hist:
            tasks.append(self.fetch_history_stats(sym))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        merged: Dict[str, Any] = {
            "symbol": sym,
            "symbol_normalized": sym_norm,
            "provider": PROVIDER_NAME,
            "data_source": PROVIDER_NAME,
            "last_updated_utc": now_utc,
            "last_updated_riyadh": now_riy,
        }

        errors: List[str] = []

        for r in results:
            if isinstance(r, Exception):
                errors.append(f"exception:{r.__class__.__name__}")
                continue
            patch, err = r  # type: ignore
            if err:
                errors.append(err)
            if isinstance(patch, dict) and patch:
                for k, v in patch.items():
                    if v is not None and (k not in merged or merged.get(k) in (None, "", [])):
                        merged[k] = v

        # If we still don’t have core quote fields, mark fetch_failed
        if merged.get("current_price") is None:
            merged["data_quality"] = "MISSING"
            merged["error"] = "fetch_failed"
            merged["error_detail"] = ",".join(sorted(set(errors))) if errors else "no_data"

        else:
            merged["data_quality"] = "OK"

        # Currency fallback from fundamentals might arrive; ensure symbol fields exist
        merged["symbol"] = sym
        merged["symbol_normalized"] = sym_norm

        return _clean_patch(merged)

    async def aclose(self) -> None:
        try:
            await self._client.aclose()
        except Exception:
            pass


# ============================================================================
# Singleton + public API
# ============================================================================
_INSTANCE: Optional[EODHDClient] = None
_INSTANCE_LOCK = asyncio.Lock()

async def get_client() -> EODHDClient:
    global _INSTANCE
    if _INSTANCE is None:
        async with _INSTANCE_LOCK:
            if _INSTANCE is None:
                _INSTANCE = EODHDClient()
    return _INSTANCE

async def fetch_enriched_quote_patch(symbol: str, *args, **kwargs) -> Dict[str, Any]:
    client = await get_client()
    return await client.fetch_enriched_quote_patch(symbol)

__all__ = [
    "fetch_enriched_quote_patch",
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    "EODHDClient",
    "get_client",
    "normalize_eodhd_symbol",
]
