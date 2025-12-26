# core/providers/eodhd_provider.py
"""
core/providers/eodhd_provider.py
===============================================================
EODHD Provider / Client (v1.3.0) — PROD SAFE + KSA SAFE

Purpose
- A standalone, import-safe async client for EODHD endpoints.
- Designed to align 1:1 with fields used by core/data_engine_v2.py (UnifiedQuote).
- Never crashes app startup (no network calls at import-time).
- KSA-SAFE: will refuse .SR (and numeric-only) symbols by default.

What it returns
- Realtime: a PATCH dict aligned to UnifiedQuote keys
  (current_price, previous_close, open, day_high, day_low, volume, price_change, percent_change, ...)
- Fundamentals: a PATCH dict aligned to UnifiedQuote keys
  (name, sector, industry, listing_date, market_cap, shares_outstanding, eps_ttm, pe_ttm, pb, ps, ...)

Env / Settings
- EODHD_API_KEY (or settings.eodhd_api_key) is typically supplied by the engine.
- EODHD_BASE_URL (optional) default: https://eodhd.com/api
- HTTP_TIMEOUT_SEC, HTTP_RETRY_ATTEMPTS (optional)
- EODHD_RT_TTL_SEC, EODHD_FUND_TTL_SEC (optional) micro-cache inside this provider
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
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, Union

import httpx
from cachetools import TTLCache

logger = logging.getLogger("core.providers.eodhd_provider")

PROVIDER_VERSION = "1.3.0"

DEFAULT_BASE_URL = "https://eodhd.com/api"
DEFAULT_TIMEOUT_SEC = 25.0
DEFAULT_RETRY_ATTEMPTS = 3

EODHD_RT_PATH = "/real-time/{symbol}"
EODHD_FUND_PATH = "/fundamentals/{symbol}"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_TRUTHY = {"1", "true", "yes", "on", "y", "t"}
_FALSY = {"0", "false", "no", "off", "n", "f"}


# -----------------------------------------------------------------------------
# Settings shim (PROD SAFE)
# -----------------------------------------------------------------------------
try:
    from core.config import get_settings  # type: ignore

    _settings = get_settings()
except Exception:  # pragma: no cover
    class _FallbackSettings:
        pass

    _settings = _FallbackSettings()  # type: ignore


def _get_attr_or_env(name: str, default: Any = None) -> Any:
    try:
        if hasattr(_settings, name):
            v = getattr(_settings, name)
            if v is not None:
                return v
    except Exception:
        pass
    v = os.getenv(name) or os.getenv(name.upper()) or os.getenv(name.lower())
    return v if v is not None else default


def _get_int(name: str, default: int) -> int:
    raw = _get_attr_or_env(name, None)
    if raw is None:
        return default
    try:
        x = int(str(raw).strip())
        return x if x > 0 else default
    except Exception:
        return default


def _get_float(name: str, default: float) -> float:
    raw = _get_attr_or_env(name, None)
    if raw is None:
        return default
    try:
        x = float(str(raw).strip())
        return x if x > 0 else default
    except Exception:
        return default


def _get_bool(name: str, default: bool) -> bool:
    raw = _get_attr_or_env(name, None)
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


_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


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
            mult = 1_000.0 if suf == "K" else 1_000_000.0 if suf == "M" else 1_000_000_000.0
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


def _is_ksa_symbol(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    return s.endswith(".SR") or s.isdigit()


def _to_eodhd_symbol(symbol: str) -> str:
    """
    EODHD commonly expects:
    - US: AAPL.US
    - Other exchanges: 7203.TSE etc.
    If caller passes 'AAPL' we will convert to AAPL.US.
    """
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    # keep indices/commodities/forex as-is; engine usually won't route these to EODHD anyway
    if any(ch in s for ch in ("^", "=", "/")):
        return s
    if "." in s:
        return s
    return f"{s}.US"


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# -----------------------------------------------------------------------------
# Client
# -----------------------------------------------------------------------------
class EodhdClient:
    """
    Async EODHD client. Safe-by-default and can be shared across requests.

    Notes:
    - You should NOT instantiate this at import-time in app startup paths unless needed.
    - Use get_eodhd_client() (lazy singleton) for typical use.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        timeout_sec: float = DEFAULT_TIMEOUT_SEC,
        retry_attempts: int = DEFAULT_RETRY_ATTEMPTS,
    ) -> None:
        self.api_key = (api_key or "").strip() or None
        self.base_url = (base_url or DEFAULT_BASE_URL).rstrip("/")
        self.timeout_sec = float(timeout_sec) if timeout_sec and timeout_sec > 0 else DEFAULT_TIMEOUT_SEC
        self.retry_attempts = int(retry_attempts) if retry_attempts and retry_attempts > 0 else DEFAULT_RETRY_ATTEMPTS

        timeout = httpx.Timeout(self.timeout_sec, connect=min(10.0, self.timeout_sec))
        self._client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.8,ar;q=0.6",
            },
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=40),
        )

        # Micro-cache inside provider (optional; engine also has caches)
        rt_ttl = _get_float("EODHD_RT_TTL_SEC", 15.0)
        fund_ttl = _get_float("EODHD_FUND_TTL_SEC", 21600.0)
        self._rt_cache: TTLCache = TTLCache(maxsize=4000, ttl=max(5.0, float(rt_ttl)))
        self._fund_cache: TTLCache = TTLCache(maxsize=2000, ttl=max(120.0, float(fund_ttl)))

    async def aclose(self) -> None:
        await self._client.aclose()

    # -------------------------------------------------------------------------
    # Low-level HTTP
    # -------------------------------------------------------------------------
    async def _get_json(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Optional[Union[Dict[str, Any], list]]:
        for attempt in range(max(1, self.retry_attempts)):
            try:
                r = await self._client.get(url, params=params)
                if r.status_code == 429 or 500 <= r.status_code < 600:
                    if attempt < (self.retry_attempts - 1):
                        ra = r.headers.get("Retry-After")
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
                if attempt < (self.retry_attempts - 1):
                    await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.35)
                    continue
                return None
        return None

    def _api_token(self, api_key: Optional[str]) -> Optional[str]:
        k = (api_key or "").strip() or (self.api_key or "").strip()
        return k or None

    # -------------------------------------------------------------------------
    # Public: Realtime
    # -------------------------------------------------------------------------
    async def fetch_realtime_patch(
        self,
        symbol: str,
        api_key: Optional[str] = None,
        *,
        allow_ksa: bool = False,
    ) -> Tuple[Dict[str, Any], Optional[str]]:
        """
        Returns (patch_dict, error_str).
        patch_dict aligns to UnifiedQuote keys.

        If error_str is not None, patch_dict may still be empty.
        """
        sym_in = (symbol or "").strip()
        if not sym_in:
            return {}, "EODHD: empty symbol"

        if _is_ksa_symbol(sym_in) and not allow_ksa:
            return {}, "EODHD: KSA symbol refused (.SR not supported)"

        sym = _to_eodhd_symbol(sym_in)
        token = self._api_token(api_key)
        if not token:
            return {}, "EODHD: missing api key"

        cache_key = f"rt::{sym}"
        hit = self._rt_cache.get(cache_key)
        if isinstance(hit, dict) and hit:
            return dict(hit), None

        url = f"{self.base_url}{EODHD_RT_PATH.format(symbol=sym)}"
        params = {"api_token": token, "fmt": "json"}

        t0 = time.perf_counter()
        data = await self._get_json(url, params=params)
        dt_ms = int((time.perf_counter() - t0) * 1000)

        if not data or not isinstance(data, dict):
            return {}, f"EODHD: empty response ({dt_ms}ms)"

        # EODHD realtime fields are commonly:
        # close, previousClose, open, high, low, volume, change, change_p, timestamp
        cp = _safe_float(data.get("close") or data.get("price") or data.get("previousClose"))
        patch: Dict[str, Any] = {
            "current_price": cp,
            "previous_close": _safe_float(data.get("previousClose")),
            "open": _safe_float(data.get("open")),
            "day_high": _safe_float(data.get("high")),
            "day_low": _safe_float(data.get("low")),
            "volume": _safe_float(data.get("volume")),
            "price_change": _safe_float(data.get("change")),
            "percent_change": _safe_float(data.get("change_p")),
            "data_source": "eodhd",
            "last_updated_utc": _utc_iso(),
        }

        # if EODHD returns numeric timestamp
        ts = _safe_float(data.get("timestamp"))
        if ts:
            try:
                patch["eodhd_timestamp_utc"] = datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
            except Exception:
                pass

        # Cache only if we got a usable price
        if patch.get("current_price") is not None:
            self._rt_cache[cache_key] = dict(patch)
            return patch, None

        return {}, f"EODHD: no price in response ({dt_ms}ms)"

    # -------------------------------------------------------------------------
    # Public: Fundamentals
    # -------------------------------------------------------------------------
    async def fetch_fundamentals_patch(
        self,
        symbol: str,
        api_key: Optional[str] = None,
        *,
        allow_ksa: bool = False,
    ) -> Tuple[Dict[str, Any], Optional[str]]:
        """
        Returns (patch_dict, error_str).
        patch_dict aligns to UnifiedQuote keys.
        """
        sym_in = (symbol or "").strip()
        if not sym_in:
            return {}, "EODHD: empty symbol"

        if _is_ksa_symbol(sym_in) and not allow_ksa:
            return {}, "EODHD: KSA symbol refused (.SR not supported)"

        sym = _to_eodhd_symbol(sym_in)
        token = self._api_token(api_key)
        if not token:
            return {}, "EODHD: missing api key"

        cache_key = f"fund::{sym}"
        hit = self._fund_cache.get(cache_key)
        if isinstance(hit, dict) and hit:
            return dict(hit), None

        url = f"{self.base_url}{EODHD_FUND_PATH.format(symbol=sym)}"
        params = {"api_token": token, "fmt": "json"}

        data = await self._get_json(url, params=params)
        if not data or not isinstance(data, dict):
            return {}, "EODHD: fundamentals empty response"

        general = data.get("General") or {}
        highlights = data.get("Highlights") or {}
        valuation = data.get("Valuation") or {}
        technicals = data.get("Technicals") or {}
        shares = data.get("SharesStats") or {}

        patch: Dict[str, Any] = {
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
            "last_updated_utc": _utc_iso(),
        }

        # Cache even if partially filled (fundamentals are naturally sparse)
        self._fund_cache[cache_key] = dict(patch)
        return patch, None


# -----------------------------------------------------------------------------
# Lazy singleton (PROD SAFE)
# -----------------------------------------------------------------------------
_CLIENT_SINGLETON: Optional[EodhdClient] = None
_CLIENT_LOCK = asyncio.Lock()


async def get_eodhd_client(api_key: Optional[str] = None) -> EodhdClient:
    """
    Lazy singleton. If api_key is provided and singleton has none, we keep the
    singleton but do not mutate its api_key to avoid surprising cross-tenant behavior.
    Prefer passing api_key per-call.
    """
    global _CLIENT_SINGLETON
    if _CLIENT_SINGLETON is None:
        async with _CLIENT_LOCK:
            if _CLIENT_SINGLETON is None:
                base_url = _safe_str(_get_attr_or_env("EODHD_BASE_URL", DEFAULT_BASE_URL)) or DEFAULT_BASE_URL
                timeout_sec = _get_float("HTTP_TIMEOUT_SEC", DEFAULT_TIMEOUT_SEC)
                retries = _get_int("HTTP_RETRY_ATTEMPTS", DEFAULT_RETRY_ATTEMPTS)
                _CLIENT_SINGLETON = EodhdClient(
                    api_key=_safe_str(api_key),
                    base_url=base_url,
                    timeout_sec=timeout_sec,
                    retry_attempts=retries,
                )
                logger.info("EODHD client init v%s | base=%s | timeout=%.1fs | retries=%s", PROVIDER_VERSION, base_url, timeout_sec, retries)
    return _CLIENT_SINGLETON


async def aclose_eodhd_client() -> None:
    global _CLIENT_SINGLETON
    if _CLIENT_SINGLETON is None:
        return
    try:
        await _CLIENT_SINGLETON.aclose()
    finally:
        _CLIENT_SINGLETON = None


# -----------------------------------------------------------------------------
# Convenience functions (engine-friendly)
# -----------------------------------------------------------------------------
async def fetch_realtime_patch(symbol: str, api_key: Optional[str]) -> Tuple[Dict[str, Any], Optional[str]]:
    c = await get_eodhd_client()
    return await c.fetch_realtime_patch(symbol, api_key=api_key, allow_ksa=False)


async def fetch_fundamentals_patch(symbol: str, api_key: Optional[str]) -> Tuple[Dict[str, Any], Optional[str]]:
    c = await get_eodhd_client()
    return await c.fetch_fundamentals_patch(symbol, api_key=api_key, allow_ksa=False)


__all__ = [
    "EodhdClient",
    "get_eodhd_client",
    "aclose_eodhd_client",
    "fetch_realtime_patch",
    "fetch_fundamentals_patch",
    "PROVIDER_VERSION",
]
