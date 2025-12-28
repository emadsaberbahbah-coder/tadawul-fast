# core/providers/finnhub_provider.py  (FULL REPLACEMENT)
"""
core/providers/finnhub_provider.py
------------------------------------------------------------
Finnhub Provider (GLOBAL enrichment fallback) — v1.6.0 (PROD SAFE + ENGINE ALIGNED)

Aligned with your Render env names
- ✅ Uses FINNHUB_API_KEY (preferred)
- ✅ Supports legacy aliases: FINNHUB_API_TOKEN / FINNHUB_TOKEN
- ✅ Uses standard query param token=<...>

Role
- GLOBAL fallback/enrichment (quote + optional profile) when EODHD is primary.
- KSA symbols are explicitly rejected to prevent wrong routing.

Key improvements vs v1.5.0
- ✅ Engine-aligned: exported callables return Dict[str, Any] (no tuples)
- ✅ AsyncClient reused (no new client per request) + aclose hook
- ✅ Retry/backoff on 429 + transient 5xx
- ✅ Detects Finnhub "zero payload" for invalid symbols (c=o=h=l=pc=0)
- ✅ Symbol normalization: strips ".US" safely; rejects Yahoo special (^GSPC, GC=F, EURUSD=X)
- ✅ Optional TTLCache to reduce rate-limit risk (FINNHUB_TTL_SEC, min 3s)
- ✅ Never crashes if env vars missing; returns clean error object

Env vars (supported)
- FINNHUB_API_KEY (preferred)
- FINNHUB_API_TOKEN / FINNHUB_TOKEN (legacy)
- FINNHUB_BASE_URL (default: https://finnhub.io/api/v1)
- FINNHUB_TIMEOUT_SEC (default: falls back to HTTP_TIMEOUT_SEC then 8.0)
- FINNHUB_RETRY_ATTEMPTS (default: 2, min 1)
- FINNHUB_ENABLE_PROFILE (default: true)
- FINNHUB_HEADERS_JSON (optional JSON dict)
- FINNHUB_UA (optional override)
- FINNHUB_TTL_SEC (default: 10, min 3)
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import httpx
from cachetools import TTLCache

logger = logging.getLogger("core.providers.finnhub_provider")

PROVIDER_VERSION = "1.6.0"
PROVIDER_NAME = "finnhub"

DEFAULT_BASE_URL = "https://finnhub.io/api/v1"
DEFAULT_TIMEOUT_SEC = 8.0
DEFAULT_RETRY_ATTEMPTS = 2

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

USER_AGENT_DEFAULT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Reuse one client
_CLIENT: Optional[httpx.AsyncClient] = None
_LOCK = asyncio.Lock()


# -----------------------------------------------------------------------------
# Safe env parsing (never crash on bad env values)
# -----------------------------------------------------------------------------
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
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _token() -> Optional[str]:
    # ✅ Render uses FINNHUB_API_KEY
    for k in ("FINNHUB_API_KEY", "FINNHUB_API_TOKEN", "FINNHUB_TOKEN"):
        v = (os.getenv(k) or "").strip()
        if v:
            return v
    return None


def _base_url() -> str:
    return _env_str("FINNHUB_BASE_URL", DEFAULT_BASE_URL).rstrip("/")


def _timeout_sec() -> float:
    # prefer FINNHUB_TIMEOUT_SEC; fallback to HTTP_TIMEOUT_SEC; then default
    t = _env_float("FINNHUB_TIMEOUT_SEC", _env_float("HTTP_TIMEOUT_SEC", DEFAULT_TIMEOUT_SEC))
    return t if t > 0 else DEFAULT_TIMEOUT_SEC


def _retry_attempts() -> int:
    r = _env_int("FINNHUB_RETRY_ATTEMPTS", _env_int("MAX_RETRIES", DEFAULT_RETRY_ATTEMPTS))
    return max(1, r)


def _enable_profile() -> bool:
    return _env_bool("FINNHUB_ENABLE_PROFILE", True)


def _ua() -> str:
    return _env_str("FINNHUB_UA", USER_AGENT_DEFAULT)


def _ttl_seconds() -> float:
    ttl = _env_float("FINNHUB_TTL_SEC", 10.0)
    if ttl <= 0:
        ttl = 10.0
    return max(3.0, ttl)


_CACHE: TTLCache = TTLCache(maxsize=5000, ttl=_ttl_seconds())


def _get_headers() -> Dict[str, str]:
    h = {
        "User-Agent": _ua(),
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.8",
    }
    raw = _env_str("FINNHUB_HEADERS_JSON", "")
    if raw:
        try:
            obj = json.loads(raw)
            if isinstance(obj, dict):
                for k, v in obj.items():
                    h[str(k)] = str(v)
        except Exception:
            pass
    return h


async def _get_client() -> httpx.AsyncClient:
    global _CLIENT
    if _CLIENT is None:
        async with _LOCK:
            if _CLIENT is None:
                t = _timeout_sec()
                timeout = httpx.Timeout(t, connect=min(10.0, t))
                _CLIENT = httpx.AsyncClient(
                    timeout=timeout,
                    follow_redirects=True,
                    headers=_get_headers(),
                )
                logger.info("Finnhub client init v%s | base=%s | timeout=%.1fs", PROVIDER_VERSION, _base_url(), t)
    return _CLIENT


async def aclose_finnhub_client() -> None:
    global _CLIENT
    if _CLIENT is None:
        return
    try:
        await _CLIENT.aclose()
    finally:
        _CLIENT = None


# -----------------------------------------------------------------------------
# Symbol rules
# -----------------------------------------------------------------------------
_KSA_RE = re.compile(r"^\d{3,5}(\.SR)?$", re.IGNORECASE)


def _is_ksa_symbol(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    if not s:
        return False
    if s.endswith(".SR"):
        return True
    if _KSA_RE.match(s):
        return True
    return False


def _is_yahoo_special(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    if not s:
        return False
    # Yahoo-style symbols are not supported by Finnhub in your routing design
    return any(ch in s for ch in ("^", "=", "/")) or s.endswith("=X")


def _normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if s.endswith(".US"):
        s = s[:-3]
    return s


# -----------------------------------------------------------------------------
# Numeric helpers
# -----------------------------------------------------------------------------
def _is_nan(x: Any) -> bool:
    try:
        return x is None or (isinstance(x, float) and math.isnan(x))
    except Exception:
        return False


def _to_float(x: Any) -> Optional[float]:
    try:
        if _is_nan(x) or x is None:
            return None
        s = str(x).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


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
        "last_updated_utc": _utc_iso(),
        "currency": "",
        "name": "",
        "sector": "",
        "industry": "",
        "sub_sector": "",
        "listing_date": "",
        # quote-ish
        "current_price": None,
        "previous_close": None,
        "open": None,
        "day_high": None,
        "day_low": None,
        "volume": None,  # Finnhub /quote doesn't provide volume (kept for schema compatibility)
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


def _quote_looks_empty(js: Dict[str, Any]) -> bool:
    """
    Finnhub often returns 0 for invalid/unknown symbols:
      {c:0, d:0, dp:0, h:0, l:0, o:0, pc:0, t:0}
    Treat that as empty.
    """
    keys = ("c", "h", "l", "o", "pc")
    vals = []
    for k in keys:
        f = _to_float(js.get(k))
        vals.append(0.0 if f is None else float(f))
    return all(v == 0.0 for v in vals)


async def _get_json(path: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    tok = _token()
    if not tok:
        return None

    base = _base_url()
    url = f"{base}{path}"

    q = dict(params)
    q["token"] = tok

    retries = _retry_attempts()
    client = await _get_client()

    for attempt in range(retries):
        try:
            r = await client.get(url, params=q)

            # Retry on rate-limit / transient server errors
            if r.status_code == 429 or 500 <= r.status_code < 600:
                if attempt < (retries - 1):
                    await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.25)
                    continue
                return None

            if r.status_code != 200:
                return None

            try:
                js = r.json()
                if isinstance(js, dict):
                    return js
                return None
            except Exception:
                return None

        except Exception:
            if attempt < (retries - 1):
                await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.25)
                continue
            return None

    return None


# -----------------------------------------------------------------------------
# Finnhub calls
# -----------------------------------------------------------------------------
async def _fetch_quote_patch(symbol: str) -> Dict[str, Any]:
    """
    Finnhub quote endpoint:
      c=current, d=change, dp=percent change, h=high, l=low, o=open, pc=prev close
    """
    js = await _get_json("/quote", {"symbol": symbol})
    if not js or not isinstance(js, dict):
        return {}

    if _quote_looks_empty(js):
        return {}

    patch: Dict[str, Any] = {
        "current_price": _to_float(js.get("c")),
        "previous_close": _to_float(js.get("pc")),
        "open": _to_float(js.get("o")),
        "day_high": _to_float(js.get("h")),
        "day_low": _to_float(js.get("l")),
    }

    d = _to_float(js.get("d"))
    dp = _to_float(js.get("dp"))
    if d is not None:
        patch["price_change"] = d
    if dp is not None:
        patch["percent_change"] = dp

    return {k: v for k, v in patch.items() if v is not None}


async def _fetch_profile_patch(symbol: str) -> Dict[str, Any]:
    js = await _get_json("/stock/profile2", {"symbol": symbol})
    if not js or not isinstance(js, dict):
        return {}

    patch: Dict[str, Any] = {
        "name": (js.get("name") or "") or "",
        "currency": (js.get("currency") or "") or "",
        "industry": (js.get("finnhubIndustry") or "") or "",
        "sector": (js.get("gsector") or "") or "",
        "sub_sector": (js.get("gsubind") or "") or "",
        "listing_date": (js.get("ipo") or "") or "",
    }

    return {k: v for k, v in patch.items() if isinstance(v, str) and v.strip()}


async def _fetch(symbol_raw: str, want_profile: bool = True) -> Dict[str, Any]:
    sym_in = (symbol_raw or "").strip()
    sym = _normalize_symbol(sym_in)

    out = _base(sym_in)

    # Hard guard: avoid misrouting KSA to Finnhub
    if _is_ksa_symbol(sym_in):
        out["status"] = "error"
        out["data_quality"] = "BAD"
        out["error"] = f"{PROVIDER_NAME}: KSA symbol not supported"
        return out

    # Guard: avoid Yahoo-special symbols in this provider
    if _is_yahoo_special(sym_in):
        out["status"] = "error"
        out["data_quality"] = "BAD"
        out["error"] = f"{PROVIDER_NAME}: symbol format not supported"
        return out

    # Token required
    if not _token():
        out["status"] = "error"
        out["data_quality"] = "BAD"
        out["error"] = f"{PROVIDER_NAME}: not configured (FINNHUB_API_KEY)"
        return out

    # Cache key (quote vs enriched)
    ck = f"finnhub::{('enriched' if want_profile else 'quote')}::{sym}"
    hit = _CACHE.get(ck)
    if isinstance(hit, dict) and hit:
        return dict(hit)

    # Quote required
    q_patch = await _fetch_quote_patch(sym)
    if not q_patch or _to_float(q_patch.get("current_price")) is None:
        out["status"] = "error"
        out["data_quality"] = "BAD"
        out["error"] = f"{PROVIDER_NAME}: quote failed or empty"
        out["last_updated_utc"] = _utc_iso()
        _CACHE[ck] = dict(out)
        return out

    out.update(q_patch)
    _fill_derived(out)

    # Optional profile enrichment (never fails the quote)
    if want_profile and _enable_profile():
        p_patch = await _fetch_profile_patch(sym)
        for k, v in p_patch.items():
            # Only fill blanks (keep EODHD richer identity as primary)
            if v and not out.get(k):
                out[k] = v

    # Final quality
    out["last_updated_utc"] = _utc_iso()
    out["data_quality"] = "OK" if _to_float(out.get("current_price")) is not None else "BAD"
    if out["data_quality"] == "BAD":
        out["status"] = "error"
        out["error"] = out["error"] or f"{PROVIDER_NAME}: missing current_price"

    _CACHE[ck] = dict(out)
    return out


# -----------------------------------------------------------------------------
# Engine discovery callables (return Dict patch, no tuples)
# -----------------------------------------------------------------------------
async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _fetch(symbol, want_profile=False)


async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _fetch(symbol, want_profile=True)


async def fetch_quote_and_enrichment_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _fetch(symbol, want_profile=True)


__all__ = [
    "fetch_quote_patch",
    "fetch_enriched_quote_patch",
    "fetch_quote_and_enrichment_patch",
    "aclose_finnhub_client",
    "PROVIDER_VERSION",
]
