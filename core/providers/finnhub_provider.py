"""
core/providers/finnhub_provider.py
------------------------------------------------------------
Finnhub Provider (GLOBAL fallback) — v1.3.0 (PROD SAFE)

Exposes engine-compatible callables:
- fetch_quote_patch
- fetch_enriched_quote_patch
- fetch_quote_and_enrichment_patch

Env vars
- FINNHUB_API_TOKEN (or FINNHUB_TOKEN)
- FINNHUB_BASE_URL (default: https://finnhub.io/api/v1)
- FINNHUB_TIMEOUT_S (default: 8.0)
"""

from __future__ import annotations

import os
import math
from typing import Any, Dict, Optional, Tuple

import httpx

PROVIDER_VERSION = "1.3.0"
PROVIDER_NAME = "finnhub"

BASE_URL = (os.getenv("FINNHUB_BASE_URL") or "https://finnhub.io/api/v1").rstrip("/")
TIMEOUT_S = float(os.getenv("FINNHUB_TIMEOUT_S") or "8.0")

UA = os.getenv(
    "FINNHUB_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
)


def _token() -> Optional[str]:
    for k in ("FINNHUB_API_TOKEN", "FINNHUB_TOKEN"):
        v = (os.getenv(k) or "").strip()
        if v:
            return v
    return None


def _is_nan(x: Any) -> bool:
    try:
        return x is None or (isinstance(x, float) and math.isnan(x))
    except Exception:
        return False


def _to_float(x: Any) -> Optional[float]:
    try:
        if _is_nan(x) or x is None:
            return None
        return float(x)
    except Exception:
        return None


async def _get_json(path: str, params: Dict[str, Any]) -> Tuple[Optional[dict], Optional[str]]:
    tok = _token()
    if not tok:
        return None, "not configured (FINNHUB_API_TOKEN)"
    params = dict(params)
    params["token"] = tok

    url = f"{BASE_URL}{path}"
    headers = {"User-Agent": UA, "Accept": "application/json,text/plain,*/*"}

    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_S, headers=headers) as client:
            r = await client.get(url, params=params)
            if r.status_code != 200:
                return None, f"HTTP {r.status_code}"
            return r.json(), None
    except Exception as e:
        return None, f"{e.__class__.__name__}: {e}"


def _base(symbol: str) -> Dict[str, Any]:
    return {
        "status": "success",
        "symbol": symbol,
        "market": "GLOBAL",
        "data_source": PROVIDER_NAME,
        "data_quality": "OK",
        "error": "",
        "provider_version": PROVIDER_VERSION,
        "currency": "",
        "name": "",
        "current_price": None,
        "previous_close": None,
        "open": None,
        "day_high": None,
        "day_low": None,
        "volume": None,
        "price_change": None,
        "percent_change": None,
        "value_traded": None,
    }


def _fill_derived(out: Dict[str, Any]) -> None:
    cur = _to_float(out.get("current_price"))
    prev = _to_float(out.get("previous_close"))
    vol = _to_float(out.get("volume"))

    if out.get("price_change") is None and cur is not None and prev is not None:
        out["price_change"] = cur - prev

    if out.get("percent_change") is None and cur is not None and prev not in (None, 0.0):
        out["percent_change"] = (cur - prev) / prev * 100.0

    if out.get("value_traded") is None and cur is not None and vol is not None:
        out["value_traded"] = cur * vol


async def _fetch_quote(symbol: str) -> Dict[str, Any]:
    out = _base(symbol)

    # Finnhub quote endpoint
    js, err = await _get_json("/quote", {"symbol": symbol})
    if js is None:
        out["status"] = "error"
        out["data_quality"] = "BAD"
        out["error"] = f"{PROVIDER_NAME}: {err}"
        return out

    # Fields: c=current, pc=prev close, o=open, h=high, l=low
    out["current_price"] = _to_float(js.get("c"))
    out["previous_close"] = _to_float(js.get("pc"))
    out["open"] = _to_float(js.get("o"))
    out["day_high"] = _to_float(js.get("h"))
    out["day_low"] = _to_float(js.get("l"))

    _fill_derived(out)
    out["data_quality"] = "OK" if out.get("current_price") is not None else "BAD"
    return out


async def _fetch_profile(symbol: str) -> Dict[str, Any]:
    patch: Dict[str, Any] = {}
    js, err = await _get_json("/stock/profile2", {"symbol": symbol})
    if js is None or not isinstance(js, dict) or not js:
        return patch

    patch["name"] = (js.get("name") or "") or ""
    patch["currency"] = (js.get("currency") or "") or ""
    patch["industry"] = (js.get("finnhubIndustry") or "") or ""
    return patch


async def _fetch(symbol: str, want_profile: bool = True) -> Dict[str, Any]:
    out = await _fetch_quote(symbol)
    if out.get("status") == "error":
        return out

    if want_profile:
        p = await _fetch_profile(symbol)
        for k, v in p.items():
            if v and not out.get(k):
                out[k] = v

    return out


# Engine discovery callables
async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _fetch(symbol, want_profile=False)


async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _fetch(symbol, want_profile=True)


async def fetch_quote_and_enrichment_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _fetch(symbol, want_profile=True)
