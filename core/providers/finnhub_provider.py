# core/providers/finnhub_provider.py  (FULL REPLACEMENT)
"""
core/providers/finnhub_provider.py
------------------------------------------------------------
Finnhub Provider (GLOBAL enrichment fallback) — v1.4.0 (ENV-ALIAS HARDENED)

Why this revision (matches your Render env names)
- ✅ Accepts FINNHUB_API_KEY (your current Render variable)
- ✅ Still accepts FINNHUB_API_TOKEN / FINNHUB_TOKEN (legacy aliases)
- ✅ Uses Finnhub standard query param: token=<...> (works with all clients)
- ✅ Optional header support for clients is NOT needed here because we call Finnhub directly.

Exposes engine-compatible callables:
- fetch_quote_patch
- fetch_enriched_quote_patch
- fetch_quote_and_enrichment_patch

Env vars (supported)
- FINNHUB_API_KEY (preferred in your Render)
- FINNHUB_API_TOKEN / FINNHUB_TOKEN (legacy)
- FINNHUB_BASE_URL (default: https://finnhub.io/api/v1)
- FINNHUB_TIMEOUT_S (default: 8.0)
- FINNHUB_ENABLE_PROFILE (default: true)

Notes
- Finnhub quote endpoint does NOT provide volume.
- This provider focuses on quote + profile identity fields to fill blanks when EODHD is missing.
"""

from __future__ import annotations

import math
import os
from typing import Any, Dict, Optional, Tuple

import httpx

PROVIDER_VERSION = "1.4.0"
PROVIDER_NAME = "finnhub"

BASE_URL = (os.getenv("FINNHUB_BASE_URL") or "https://finnhub.io/api/v1").rstrip("/")
TIMEOUT_S = float(os.getenv("FINNHUB_TIMEOUT_S") or "8.0")
ENABLE_PROFILE = (os.getenv("FINNHUB_ENABLE_PROFILE") or "true").strip().lower() in {"1", "true", "yes", "y", "on"}

UA = os.getenv(
    "FINNHUB_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
)


def _token() -> Optional[str]:
    # ✅ Align with your Render name first, then legacy aliases
    for k in ("FINNHUB_API_KEY", "FINNHUB_API_TOKEN", "FINNHUB_TOKEN"):
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
        return None, "not configured (FINNHUB_API_KEY)"

    url = f"{BASE_URL}{path}"
    headers = {"User-Agent": UA, "Accept": "application/json,text/plain,*/*"}

    q = dict(params)
    q["token"] = tok

    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_S, headers=headers) as client:
            r = await client.get(url, params=q)
            if r.status_code != 200:
                hint = ""
                if r.status_code in (401, 403):
                    hint = " (auth failed: check FINNHUB_API_KEY)"
                return None, f"HTTP {r.status_code}{hint}"
            try:
                return r.json(), None
            except Exception:
                return None, "invalid JSON response"
    except Exception as e:
        return None, f"{e.__class__.__name__}: {e}"


def _base(symbol: str) -> Dict[str, Any]:
    # Keep shape consistent with your engine merge policy
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
        "sector": "",
        "industry": "",
        "sub_sector": "",
        "listing_date": "",
        "current_price": None,
        "previous_close": None,
        "open": None,
        "day_high": None,
        "day_low": None,
        "volume": None,        # Finnhub quote doesn't return volume; kept for schema compatibility
        "price_change": None,
        "percent_change": None,
        "value_traded": None,  # computed if volume is later filled by another provider (unlikely)
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


async def _fetch_quote(symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    # Finnhub quote endpoint:
    # c=current, d=change, dp=percent change, h=high, l=low, o=open, pc=prev close
    js, err = await _get_json("/quote", {"symbol": symbol})
    if js is None or not isinstance(js, dict):
        return {}, err or "quote failed"

    patch: Dict[str, Any] = {
        "current_price": _to_float(js.get("c")),
        "previous_close": _to_float(js.get("pc")),
        "open": _to_float(js.get("o")),
        "day_high": _to_float(js.get("h")),
        "day_low": _to_float(js.get("l")),
    }

    # Prefer Finnhub direct change fields if present (no confusion about percent format)
    d = _to_float(js.get("d"))
    dp = _to_float(js.get("dp"))
    if d is not None:
        patch["price_change"] = d
    if dp is not None:
        patch["percent_change"] = dp

    patch = {k: v for k, v in patch.items() if v is not None}
    return patch, None


async def _fetch_profile(symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    js, err = await _get_json("/stock/profile2", {"symbol": symbol})
    if js is None or not isinstance(js, dict) or not js:
        return {}, err

    patch: Dict[str, Any] = {
        "name": (js.get("name") or "") or "",
        "currency": (js.get("currency") or "") or "",
        "industry": (js.get("finnhubIndustry") or "") or "",
        "sector": (js.get("gsector") or "") or "",
        "sub_sector": (js.get("gsubind") or "") or "",
        "listing_date": (js.get("ipo") or "") or "",
    }

    patch = {k: v for k, v in patch.items() if isinstance(v, str) and v.strip()}
    return patch, None


async def _fetch(symbol: str, want_profile: bool = True) -> Dict[str, Any]:
    out = _base(symbol)

    q_patch, q_err = await _fetch_quote(symbol)
    if q_err:
        out["status"] = "error"
        out["data_quality"] = "BAD"
        out["error"] = f"{PROVIDER_NAME}: {q_err}"
        return out

    out.update(q_patch)
    _fill_derived(out)

    if want_profile and ENABLE_PROFILE:
        p_patch, _ = await _fetch_profile(symbol)
        # Only fill blanks (EODHD should be primary for richness)
        for k, v in p_patch.items():
            if v and not out.get(k):
                out[k] = v

    # Quality: Finnhub quote must at least have current_price
    out["data_quality"] = "OK" if _to_float(out.get("current_price")) is not None else "BAD"
    if out["data_quality"] == "BAD":
        out["status"] = "error"
        out["error"] = out["error"] or f"{PROVIDER_NAME}: missing current_price"

    return out


# Engine discovery callables
async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _fetch(symbol, want_profile=False)


async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _fetch(symbol, want_profile=True)


async def fetch_quote_and_enrichment_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _fetch(symbol, want_profile=True)
