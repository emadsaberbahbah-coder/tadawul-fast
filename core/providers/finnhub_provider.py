# core/providers/finnhub_provider.py
"""
core/providers/finnhub_provider.py
------------------------------------------------------------
Finnhub Provider (GLOBAL enrichment fallback) — v1.7.0 (PROD SAFE + ENGINE ALIGNED)

Aligned with your Render env names
- ✅ Uses FINNHUB_API_KEY (preferred)
- ✅ Supports legacy aliases: FINNHUB_API_TOKEN / FINNHUB_TOKEN
- ✅ Uses standard query param token=<...>

Role
- GLOBAL fallback/enrichment (quote + optional profile) when EODHD is primary.
- KSA symbols are explicitly rejected to prevent wrong routing.

Key improvements vs v1.6.0
- ✅ Engine-aligned: exported callables return Dict[str, Any] (no tuples)
- ✅ AsyncClient reused (no new client per request) + aclose hook
- ✅ Retry/backoff on 429 + transient 5xx
- ✅ Detects Finnhub "zero payload" for invalid symbols (c=o=h=l=pc=0)
- ✅ Symbol normalization: strips ".US" safely; rejects Yahoo special (^GSPC, GC=F, EURUSD=X)
- ✅ Optional TTL cache (FINNHUB_TTL_SEC, min 3s). Import-safe fallback if cachetools missing.
- ✅ Never crashes if env vars missing; returns clean error object
- ✅ CLEAN PATCH policy for engine merges:
    - Default: return patch-style dict (no "status"/"data_quality") for success
    - If not configured / invalid symbol: returns {"error": "..."} only (keeps engine fallback working)

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
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import httpx

logger = logging.getLogger("core.providers.finnhub_provider")

PROVIDER_VERSION = "1.7.0"
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

# ---------------------------------------------------------------------------
# TTL cache (import-safe if cachetools missing)
# ---------------------------------------------------------------------------
try:
    from cachetools import TTLCache  # type: ignore

    _HAS_CACHETOOLS = True
except Exception:  # pragma: no cover
    _HAS_CACHETOOLS = False

    class TTLCache(dict):  # type: ignore
        def __init__(self, maxsize: int = 1024, ttl: float = 60.0) -> None:
            super().__init__()
            self._maxsize = max(1, int(maxsize))
            self._ttl = max(1.0, float(ttl))
            self._exp: Dict[str, float] = {}

        def get(self, key: str, default: Any = None) -> Any:  # type: ignore
            now = time.time()
            exp = self._exp.get(key)
            if exp is not None and exp < now:
                try:
                    super().pop(key, None)
                except Exception:
                    pass
                self._exp.pop(key, None)
                return default
            return super().get(key, default)

        def __setitem__(self, key: str, value: Any) -> None:  # type: ignore
            if len(self) >= self._maxsize:
                try:
                    oldest_key = next(iter(self.keys()))
                    super().pop(oldest_key, None)
                    self._exp.pop(oldest_key, None)
                except Exception:
                    pass
            super().__setitem__(key, value)
            self._exp[key] = time.time() + self._ttl


# ---------------------------------------------------------------------------
# Safe env parsing (never crash on bad env values)
# ---------------------------------------------------------------------------
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
                _CLIENT = httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=_get_headers())
                logger.info(
                    "Finnhub client init v%s | base=%s | timeout=%.1fs | cachetools=%s",
                    PROVIDER_VERSION,
                    _base_url(),
                    t,
                    _HAS_CACHETOOLS,
                )
    return _CLIENT


async def aclose_finnhub_client() -> None:
    global _CLIENT
    c = _CLIENT
    _CLIENT = None
    if c is not None:
        try:
            await c.aclose()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Symbol rules
# ---------------------------------------------------------------------------
_KSA_RE = re.compile(r"^\d{3,6}(\.SR)?$", re.IGNORECASE)


def _is_ksa_symbol(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    if not s:
        return False
    if s.endswith(".SR"):
        return True
    return bool(_KSA_RE.match(s))


def _is_yahoo_special(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    if not s:
        return False
    # Yahoo-style symbols are not supported by Finnhub in your routing design
    if any(ch in s for ch in ("^", "=", "/")):
        return True
    if s.endswith("=X"):
        return True
    return False


def _normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if s.endswith(".US"):
        s = s[:-3]
    return s


# ---------------------------------------------------------------------------
# Numeric helpers
# ---------------------------------------------------------------------------
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


def _clean_patch(p: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (p or {}).items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        out[k] = v
    return out


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
    for k in keys:
        f = _to_float(js.get(k))
        if f not in (None, 0.0):
            return False
    return True


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
async def _get_json(path: str, params: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    tok = _token()
    if not tok:
        return None, "not configured (FINNHUB_API_KEY)"

    base = _base_url()
    url = f"{base}{path}"

    q = dict(params)
    q["token"] = tok

    retries = _retry_attempts()
    client = await _get_client()

    last_err: Optional[str] = None

    for attempt in range(retries):
        try:
            r = await client.get(url, params=q)

            # Retry on rate-limit / transient server errors
            if r.status_code == 429 or 500 <= r.status_code < 600:
                last_err = f"HTTP {r.status_code}"
                if attempt < (retries - 1):
                    await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.25)
                    continue
                return None, last_err

            if r.status_code != 200:
                # best-effort message
                msg = ""
                try:
                    js2 = r.json()
                    if isinstance(js2, dict):
                        msg = str(js2.get("error") or js2.get("message") or "").strip()
                except Exception:
                    msg = ""
                return None, f"HTTP {r.status_code}" + (f": {msg}" if msg else "")

            try:
                js = r.json()
            except Exception:
                return None, "invalid JSON response"

            if not isinstance(js, dict):
                return None, "unexpected JSON type"

            return js, None

        except Exception as e:
            last_err = f"{e.__class__.__name__}: {e}"
            if attempt < (retries - 1):
                await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.25)
                continue
            return None, last_err

    return None, last_err or "request failed"


# ---------------------------------------------------------------------------
# Finnhub calls
# ---------------------------------------------------------------------------
async def _fetch_quote_patch(symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Finnhub quote endpoint:
      c=current, d=change, dp=percent change, h=high, l=low, o=open, pc=prev close
    """
    js, err = await _get_json("/quote", {"symbol": symbol})
    if js is None:
        return {}, err

    if _quote_looks_empty(js):
        return {}, "empty quote"

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

    return _clean_patch(patch), None


async def _fetch_profile_patch(symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    js, err = await _get_json("/stock/profile2", {"symbol": symbol})
    if js is None:
        return {}, err

    patch: Dict[str, Any] = {
        "name": (js.get("name") or "") or "",
        "currency": (js.get("currency") or "") or "",
        "industry": (js.get("finnhubIndustry") or "") or "",
        "sector": (js.get("gsector") or "") or "",
        "sub_sector": (js.get("gsubind") or "") or "",
        "listing_date": (js.get("ipo") or "") or "",
    }

    return _clean_patch(patch), None


async def _fetch(symbol_raw: str, want_profile: bool = True) -> Dict[str, Any]:
    sym_in = (symbol_raw or "").strip()
    if not sym_in:
        return {"error": f"{PROVIDER_NAME}: empty symbol"}

    # Hard guard: avoid misrouting KSA to Finnhub
    if _is_ksa_symbol(sym_in):
        return {"error": f"{PROVIDER_NAME}: KSA symbol not supported"}

    # Guard: avoid Yahoo-special symbols in this provider
    if _is_yahoo_special(sym_in):
        return {"error": f"{PROVIDER_NAME}: symbol format not supported"}

    # Token required
    if not _token():
        return {"error": f"{PROVIDER_NAME}: not configured (FINNHUB_API_KEY)"}

    sym = _normalize_symbol(sym_in)

    ck = f"finnhub::{('enriched' if want_profile else 'quote')}::{sym}"
    hit = _CACHE.get(ck)
    if isinstance(hit, dict) and hit:
        return dict(hit)

    # Quote required
    q_patch, q_err = await _fetch_quote_patch(sym)
    if not q_patch or _to_float(q_patch.get("current_price")) is None:
        out = {"error": f"{PROVIDER_NAME}: quote failed ({q_err or 'empty'})"}
        _CACHE[ck] = dict(out)
        return out

    out: Dict[str, Any] = dict(q_patch)
    _fill_derived(out)

    # Optional profile enrichment (never fails quote)
    if want_profile and _enable_profile():
        p_patch, _ = await _fetch_profile_patch(sym)
        # fill blanks only
        for k, v in p_patch.items():
            if not out.get(k) and v is not None:
                out[k] = v

    out["data_source"] = PROVIDER_NAME
    out["provider_version"] = PROVIDER_VERSION
    out["last_updated_utc"] = _utc_iso()

    out = _clean_patch(out)
    _CACHE[ck] = dict(out)
    return out


# ---------------------------------------------------------------------------
# Engine discovery callables (return Dict patch, no tuples)
# ---------------------------------------------------------------------------
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
    "PROVIDER_NAME",
]
