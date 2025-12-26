"""
core/providers/yahoo_chart_provider.py
===============================================================
Yahoo Chart Provider (v1.1.0) — KSA-safe quote fallback (NO yfinance)

Purpose
- Fetch KSA quotes from Yahoo's public chart endpoint:
  https://query1.finance.yahoo.com/v8/finance/chart/{symbol}

Guarantees
- PROD SAFE: no network at import time
- Async, retry + TTL cache
- Returns PATCH dict aligned to UnifiedQuote fields used by DataEngineV2
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, Union

import httpx
from cachetools import TTLCache

logger = logging.getLogger("core.providers.yahoo_chart_provider")

PROVIDER_VERSION = "1.1.0"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

DEFAULT_TIMEOUT_SEC = 20.0
DEFAULT_RETRY_ATTEMPTS = 3


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            f = float(x)
        else:
            s = str(x).strip()
            if not s or s.lower() in {"na", "n/a", "null", "none", "-"}:
                return None
            s = s.replace(",", "")
            f = float(s)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def _safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s or None


def _normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    # If numeric only => assume KSA .SR
    if s.isdigit():
        return f"{s}.SR"
    if s.endswith(".SR"):
        return s
    # If looks like KSA code without suffix
    if s.replace(".", "").isdigit() and "." not in s:
        return f"{s}.SR"
    return s


def _get_env_int(name: str, default: int) -> int:
    try:
        v = int(str(os.getenv(name, "")).strip() or "0")
        return v if v > 0 else default
    except Exception:
        return default


def _get_env_float(name: str, default: float) -> float:
    try:
        v = float(str(os.getenv(name, "")).strip() or "0")
        return v if v > 0 else default
    except Exception:
        return default


# -----------------------------------------------------------------------------
# Client (lazy singleton)
# -----------------------------------------------------------------------------
_CLIENT: Optional[httpx.AsyncClient] = None
_LOCK = asyncio.Lock()

_cache_ttl = _get_env_float("YAHOO_CHART_TTL_SEC", 8.0)
_CACHE: TTLCache = TTLCache(maxsize=8000, ttl=max(3.0, float(_cache_ttl)))


async def _get_client() -> httpx.AsyncClient:
    global _CLIENT
    if _CLIENT is None:
        async with _LOCK:
            if _CLIENT is None:
                timeout_sec = _get_env_float("YAHOO_CHART_TIMEOUT_SEC", DEFAULT_TIMEOUT_SEC)
                timeout = httpx.Timeout(timeout_sec, connect=min(10.0, timeout_sec))
                _CLIENT = httpx.AsyncClient(
                    timeout=timeout,
                    follow_redirects=True,
                    headers={
                        "User-Agent": USER_AGENT,
                        "Accept": "application/json,text/plain;q=0.9,*/*;q=0.8",
                        "Accept-Language": "en-US,en;q=0.8,ar;q=0.6",
                    },
                    limits=httpx.Limits(max_keepalive_connections=20, max_connections=40),
                )
                logger.info("YahooChart client init v%s | timeout=%.1fs", PROVIDER_VERSION, timeout_sec)
    return _CLIENT


async def aclose_yahoo_chart_client() -> None:
    global _CLIENT
    if _CLIENT is None:
        return
    try:
        await _CLIENT.aclose()
    finally:
        _CLIENT = None


async def _get_json(url: str, params: Optional[Dict[str, Any]] = None) -> Optional[Union[Dict[str, Any], list]]:
    retries = _get_env_int("YAHOO_CHART_RETRY_ATTEMPTS", DEFAULT_RETRY_ATTEMPTS)
    client = await _get_client()

    for attempt in range(max(1, retries)):
        try:
            r = await client.get(url, params=params)
            if r.status_code == 429 or 500 <= r.status_code < 600:
                if attempt < (retries - 1):
                    await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.25)
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
            if attempt < (retries - 1):
                await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.25)
                continue
            return None
    return None


def _pick(d: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in d:
            return d.get(k)
    return None


def _extract_last_quote(chart: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract best-effort quote fields from Yahoo chart response.
    """
    result = (((chart or {}).get("chart") or {}).get("result") or [])
    if not result or not isinstance(result, list) or not isinstance(result[0], dict):
        return {}

    r0 = result[0]
    meta = r0.get("meta") or {}
    indicators = r0.get("indicators") or {}
    quote_list = indicators.get("quote") or []
    q0 = quote_list[0] if quote_list and isinstance(quote_list[0], dict) else {}

    # Arrays for OHLCV (may include None)
    opens = q0.get("open") or []
    highs = q0.get("high") or []
    lows = q0.get("low") or []
    closes = q0.get("close") or []
    vols = q0.get("volume") or []

    def _last_num(arr):
        if not isinstance(arr, list) or not arr:
            return None
        for v in reversed(arr):
            f = _safe_float(v)
            if f is not None:
                return f
        return None

    last_close = _safe_float(meta.get("regularMarketPrice")) or _last_num(closes)
    prev_close = _safe_float(meta.get("previousClose"))
    opn = _safe_float(meta.get("regularMarketOpen")) or _last_num(opens)
    day_high = _safe_float(meta.get("regularMarketDayHigh")) or _last_num(highs)
    day_low = _safe_float(meta.get("regularMarketDayLow")) or _last_num(lows)
    vol = _last_num(vols)

    currency = _safe_str(meta.get("currency"))
    name = _safe_str(meta.get("shortName")) or _safe_str(meta.get("longName"))

    out: Dict[str, Any] = {
        "current_price": last_close,
        "previous_close": prev_close,
        "open": opn,
        "day_high": day_high,
        "day_low": day_low,
        "volume": vol,
        "currency": currency,
        "name": name,
    }
    return out


# -----------------------------------------------------------------------------
# Public API used by DataEngineV2
# -----------------------------------------------------------------------------
async def fetch_quote_patch(symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    sym = _normalize_symbol(symbol)
    if not sym:
        return {}, "yahoo_chart: empty symbol"

    cache_key = f"q::{sym}"
    hit = _CACHE.get(cache_key)
    if isinstance(hit, dict) and hit:
        return dict(hit), None

    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
    params = {"interval": "1d", "range": "5d"}

    t0 = time.perf_counter()
    data = await _get_json(url, params=params)
    dt_ms = int((time.perf_counter() - t0) * 1000)
    if not data or not isinstance(data, dict):
        return {}, f"yahoo_chart: empty response ({dt_ms}ms)"

    extracted = _extract_last_quote(data)
    px = _safe_float(extracted.get("current_price"))
    if px is None:
        return {}, f"yahoo_chart: no price in chart payload ({dt_ms}ms)"

    prev = _safe_float(extracted.get("previous_close"))
    chg = None
    chg_p = None
    if prev is not None:
        chg = px - prev
        if prev != 0:
            chg_p = (chg / prev) * 100.0

    patch: Dict[str, Any] = {
        "market": "KSA" if sym.endswith(".SR") else "GLOBAL",
        "currency": extracted.get("currency") or ("SAR" if sym.endswith(".SR") else None),
        "name": extracted.get("name"),
        "current_price": px,
        "previous_close": prev,
        "open": _safe_float(extracted.get("open")),
        "day_high": _safe_float(extracted.get("day_high")),
        "day_low": _safe_float(extracted.get("day_low")),
        "volume": _safe_float(extracted.get("volume")),
        "price_change": chg,
        "percent_change": chg_p,
        "data_source": "yahoo_chart",
        "last_updated_utc": _utc_iso(),
    }

    _CACHE[cache_key] = dict(patch)
    return patch, None


__all__ = ["fetch_quote_patch", "aclose_yahoo_chart_client", "PROVIDER_VERSION"]
