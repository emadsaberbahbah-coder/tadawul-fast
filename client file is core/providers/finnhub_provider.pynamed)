```python
# core/providers/finnhub_provider.py
"""
core/providers/finnhub_provider.py
===============================================================
Finnhub Provider / Client (v1.4.0) — PROD SAFE + Async + Engine-Aligned

Purpose
- Standalone, import-safe async client for Finnhub endpoints.
- Aligns 1:1 with UnifiedQuote fields used in core/data_engine_v2.py (v2.7.x).
- Never crashes app startup (no network calls at import time).
- Provides:
    1) Quote (FINNHUB /quote)
    2) Profile (FINNHUB /stock/profile2)
    3) Metrics (FINNHUB /stock/metric?metric=all)

What it returns
- Quote: PATCH dict aligned to UnifiedQuote keys.
- Profile+Metrics: PATCH dict aligned to UnifiedQuote keys.
- Combined helper: fetch_quote_and_enrichment_patch()

Env / Settings
- FINNHUB_API_KEY (or settings.finnhub_api_key) is typically passed by engine.
- FINNHUB_BASE_URL (optional) default: https://finnhub.io/api/v1
- HTTP_TIMEOUT_SEC, HTTP_RETRY_ATTEMPTS (optional)
- FINNHUB_QUOTE_TTL_SEC, FINNHUB_META_TTL_SEC (optional) micro-caches inside this provider.
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

logger = logging.getLogger("core.providers.finnhub_provider")

PROVIDER_VERSION = "1.4.0"

DEFAULT_BASE_URL = "https://finnhub.io/api/v1"
DEFAULT_TIMEOUT_SEC = 25.0
DEFAULT_RETRY_ATTEMPTS = 3

QUOTE_PATH = "/quote"
PROFILE_PATH = "/stock/profile2"
METRIC_PATH = "/stock/metric"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_TRUTHY = {"1", "true", "yes", "on", "y", "t"}
_FALSY = {"0", "false", "no", "off", "n", "f"}

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


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


def _safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


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
        s = s.replace("%", "").replace(",", "").strip()

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


def _pct_if_fraction(x: Any) -> Optional[float]:
    v = _safe_float(x)
    if v is None:
        return None
    return v * 100.0 if abs(v) <= 1.0 else v


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _finnhub_symbol(symbol: str) -> str:
    """
    Finnhub uses plain tickers for US (AAPL), not AAPL.US.
    Keep special symbols unchanged. Keep .SR unchanged (but Finnhub generally won't support).
    """
    s = (symbol or "").strip().upper()
    if not s:
        return s
    if any(ch in s for ch in ("=", "^")):
        return s
    if s.endswith(".US"):
        return s[:-3]
    return s


def _is_ksa_symbol(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    return s.endswith(".SR") or s.isdigit()


# -----------------------------------------------------------------------------
# Client
# -----------------------------------------------------------------------------
class FinnhubClient:
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

        quote_ttl = _get_float("FINNHUB_QUOTE_TTL_SEC", 15.0)
        meta_ttl = _get_float("FINNHUB_META_TTL_SEC", 21600.0)
        self._quote_cache: TTLCache = TTLCache(maxsize=5000, ttl=max(5.0, float(quote_ttl)))
        self._meta_cache: TTLCache = TTLCache(maxsize=3000, ttl=max(120.0, float(meta_ttl)))

    async def aclose(self) -> None:
        await self._client.aclose()

    def _token(self, api_key: Optional[str]) -> Optional[str]:
        k = (api_key or "").strip() or (self.api_key or "").strip()
        return k or None

    async def _get_json(self, path: str, params: Dict[str, Any]) -> Optional[Union[Dict[str, Any], list]]:
        url = f"{self.base_url}{path}"
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

    # -------------------------------------------------------------------------
    # Quote
    # -------------------------------------------------------------------------
    async def fetch_quote_patch(
        self,
        symbol: str,
        api_key: Optional[str] = None,
        *,
        allow_ksa: bool = False,
    ) -> Tuple[Dict[str, Any], Optional[str]]:
        sym_in = (symbol or "").strip()
        if not sym_in:
            return {}, "Finnhub: empty symbol"

        if _is_ksa_symbol(sym_in) and not allow_ksa:
            return {}, "Finnhub: KSA symbol refused (.SR not supported)"

        sym = _finnhub_symbol(sym_in)
        token = self._token(api_key)
        if not token:
            return {}, "Finnhub: missing api key"

        cache_key = f"q::{sym}"
        hit = self._quote_cache.get(cache_key)
        if isinstance(hit, dict) and hit:
            return dict(hit), None

        params = {"symbol": sym, "token": token}
        t0 = time.perf_counter()
        data = await self._get_json(QUOTE_PATH, params=params)
        dt_ms = int((time.perf_counter() - t0) * 1000)

        if not data or not isinstance(data, dict):
            return {}, f"Finnhub: empty response ({dt_ms}ms)"

        # Finnhub quote schema:
        # c=current, pc=prev close, o=open, h=high, l=low, d=change, dp=percent change, t=timestamp
        cp = _safe_float(data.get("c"))
        pc = _safe_float(data.get("pc"))
        patch: Dict[str, Any] = {
            "current_price": cp,
            "previous_close": pc,
            "open": _safe_float(data.get("o")),
            "day_high": _safe_float(data.get("h")),
            "day_low": _safe_float(data.get("l")),
            "price_change": _safe_float(data.get("d")),
            "percent_change": _safe_float(data.get("dp")),
            "data_source": "finnhub",
            "last_updated_utc": _utc_iso(),
        }

        ts = _safe_float(data.get("t"))
        if ts:
            try:
                patch["finnhub_timestamp_utc"] = datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
            except Exception:
                pass

        if patch.get("current_price") is not None:
            self._quote_cache[cache_key] = dict(patch)
            return patch, None

        return {}, f"Finnhub: no price in response ({dt_ms}ms)"

    # -------------------------------------------------------------------------
    # Profile + Metrics
    # -------------------------------------------------------------------------
    async def fetch_profile_patch(
        self,
        symbol: str,
        api_key: Optional[str] = None,
        *,
        allow_ksa: bool = False,
    ) -> Tuple[Dict[str, Any], Optional[str]]:
        sym_in = (symbol or "").strip()
        if not sym_in:
            return {}, "Finnhub: empty symbol"

        if _is_ksa_symbol(sym_in) and not allow_ksa:
            return {}, "Finnhub: KSA symbol refused (.SR not supported)"

        sym = _finnhub_symbol(sym_in)
        token = self._token(api_key)
        if not token:
            return {}, "Finnhub: missing api key"

        cache_key = f"profile::{sym}"
        hit = self._meta_cache.get(cache_key)
        if isinstance(hit, dict) and hit:
            return dict(hit), None

        params = {"symbol": sym, "token": token}
        data = await self._get_json(PROFILE_PATH, params=params)
        if not data or not isinstance(data, dict):
            return {}, "Finnhub: profile empty response"

        patch: Dict[str, Any] = {
            "name": _safe_str(data.get("name")),
            "currency": _safe_str(data.get("currency")),
            "industry": _safe_str(data.get("finnhubIndustry")),
            "listing_date": _safe_str(data.get("ipo")),
            "market_cap": _safe_float(data.get("marketCapitalization")),
            "shares_outstanding": _safe_float(data.get("shareOutstanding")),
            "last_updated_utc": _utc_iso(),
        }

        self._meta_cache[cache_key] = dict(patch)
        return patch, None

    async def fetch_metrics_patch(
        self,
        symbol: str,
        api_key: Optional[str] = None,
        *,
        allow_ksa: bool = False,
    ) -> Tuple[Dict[str, Any], Optional[str]]:
        sym_in = (symbol or "").strip()
        if not sym_in:
            return {}, "Finnhub: empty symbol"

        if _is_ksa_symbol(sym_in) and not allow_ksa:
            return {}, "Finnhub: KSA symbol refused (.SR not supported)"

        sym = _finnhub_symbol(sym_in)
        token = self._token(api_key)
        if not token:
            return {}, "Finnhub: missing api key"

        cache_key = f"metrics::{sym}"
        hit = self._meta_cache.get(cache_key)
        if isinstance(hit, dict) and hit:
            return dict(hit), None

        params = {"symbol": sym, "metric": "all", "token": token}
        data = await self._get_json(METRIC_PATH, params=params)
        if not data or not isinstance(data, dict):
            return {}, "Finnhub: metrics empty response"

        m = data.get("metric") or {}
        if not isinstance(m, dict) or not m:
            return {}, "Finnhub: metrics missing"

        patch: Dict[str, Any] = {
            "high_52w": _safe_float(m.get("52WeekHigh")),
            "low_52w": _safe_float(m.get("52WeekLow")),
            "pe_ttm": _safe_float(m.get("peTTM") or m.get("peAnnual")),
            "pb": _safe_float(m.get("pbAnnual") or m.get("pbQuarterly")),
            "ps": _safe_float(m.get("psTTM") or m.get("psAnnual")),
            "eps_ttm": _safe_float(m.get("epsTTM")),
            "beta": _safe_float(m.get("beta")),
            "dividend_yield": _pct_if_fraction(m.get("dividendYieldIndicatedAnnual") or m.get("dividendYieldAnnual")),
            "roe": _pct_if_fraction(m.get("roeTTM") or m.get("roeAnnual")),
            "roa": _pct_if_fraction(m.get("roaTTM") or m.get("roaAnnual")),
            "net_margin": _pct_if_fraction(m.get("netMarginTTM") or m.get("netMarginAnnual")),
            "debt_to_equity": _safe_float(
                m.get("totalDebt/totalEquityAnnual") or m.get("totalDebt/totalEquityQuarterly")
            ),
            "current_ratio": _safe_float(m.get("currentRatioAnnual") or m.get("currentRatioQuarterly")),
            "quick_ratio": _safe_float(m.get("quickRatioAnnual") or m.get("quickRatioQuarterly")),
            "last_updated_utc": _utc_iso(),
        }

        self._meta_cache[cache_key] = dict(patch)
        return patch, None

    async def fetch_profile_and_metrics_patch(
        self,
        symbol: str,
        api_key: Optional[str] = None,
        *,
        allow_ksa: bool = False,
    ) -> Tuple[Dict[str, Any], Optional[str]]:
        """
        Concurrently fetch profile+metrics and merge into one PATCH dict.
        """
        prof, met = await asyncio.gather(
            self.fetch_profile_patch(symbol, api_key=api_key, allow_ksa=allow_ksa),
            self.fetch_metrics_patch(symbol, api_key=api_key, allow_ksa=allow_ksa),
            return_exceptions=True,
        )

        patch: Dict[str, Any] = {}
        errs = []

        if isinstance(prof, Exception):
            errs.append(f"profile:{prof}")
        else:
            p_patch, p_err = prof
            if p_patch:
                patch.update({k: v for k, v in p_patch.items() if v is not None})
            if p_err:
                errs.append(p_err)

        if isinstance(met, Exception):
            errs.append(f"metrics:{met}")
        else:
            m_patch, m_err = met
            if m_patch:
                patch.update({k: v for k, v in m_patch.items() if v is not None})
            if m_err:
                errs.append(m_err)

        if errs and not patch:
            return {}, "Finnhub: " + " | ".join([e for e in errs if e])
        return patch, None

    async def fetch_quote_and_enrichment_patch(
        self,
        symbol: str,
        api_key: Optional[str] = None,
        *,
        allow_ksa: bool = False,
        enrich: bool = True,
    ) -> Tuple[Dict[str, Any], Optional[str]]:
        """
        One-call helper:
        - Quote patch always attempted
        - If enrich=True, profile+metrics are also fetched and merged
        """
        q_patch, q_err = await self.fetch_quote_patch(symbol, api_key=api_key, allow_ksa=allow_ksa)
        if not enrich:
            return q_patch, q_err

        meta_patch, meta_err = await self.fetch_profile_and_metrics_patch(symbol, api_key=api_key, allow_ksa=allow_ksa)

        patch: Dict[str, Any] = {}
        patch.update({k: v for k, v in (q_patch or {}).items() if v is not None})
        patch.update({k: v for k, v in (meta_patch or {}).items() if v is not None})

        if patch:
            return patch, None

        # If nothing, return best error
        return {}, meta_err or q_err or "Finnhub: unknown error"


# -----------------------------------------------------------------------------
# Lazy singleton (PROD SAFE)
# -----------------------------------------------------------------------------
_CLIENT_SINGLETON: Optional[FinnhubClient] = None
_CLIENT_LOCK = asyncio.Lock()


async def get_finnhub_client(api_key: Optional[str] = None) -> FinnhubClient:
    global _CLIENT_SINGLETON
    if _CLIENT_SINGLETON is None:
        async with _CLIENT_LOCK:
            if _CLIENT_SINGLETON is None:
                base_url = _safe_str(_get_attr_or_env("FINNHUB_BASE_URL", DEFAULT_BASE_URL)) or DEFAULT_BASE_URL
                timeout_sec = _get_float("HTTP_TIMEOUT_SEC", DEFAULT_TIMEOUT_SEC)
                retries = _get_int("HTTP_RETRY_ATTEMPTS", DEFAULT_RETRY_ATTEMPTS)
                _CLIENT_SINGLETON = FinnhubClient(
                    api_key=_safe_str(api_key),
                    base_url=base_url,
                    timeout_sec=timeout_sec,
                    retry_attempts=retries,
                )
                logger.info(
                    "Finnhub client init v%s | base=%s | timeout=%.1fs | retries=%s",
                    PROVIDER_VERSION,
                    base_url,
                    timeout_sec,
                    retries,
                )
    return _CLIENT_SINGLETON


async def aclose_finnhub_client() -> None:
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
async def fetch_quote_patch(symbol: str, api_key: Optional[str]) -> Tuple[Dict[str, Any], Optional[str]]:
    c = await get_finnhub_client()
    return await c.fetch_quote_patch(symbol, api_key=api_key, allow_ksa=False)


async def fetch_profile_and_metrics_patch(symbol: str, api_key: Optional[str]) -> Tuple[Dict[str, Any], Optional[str]]:
    c = await get_finnhub_client()
    return await c.fetch_profile_and_metrics_patch(symbol, api_key=api_key, allow_ksa=False)


async def fetch_quote_and_enrichment_patch(symbol: str, api_key: Optional[str]) -> Tuple[Dict[str, Any], Optional[str]]:
    c = await get_finnhub_client()
    return await c.fetch_quote_and_enrichment_patch(symbol, api_key=api_key, allow_ksa=False, enrich=True)


__all__ = [
    "FinnhubClient",
    "get_finnhub_client",
    "aclose_finnhub_client",
    "fetch_quote_patch",
    "fetch_profile_and_metrics_patch",
    "fetch_quote_and_enrichment_patch",
    "PROVIDER_VERSION",
]
```
