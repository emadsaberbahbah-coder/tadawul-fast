#!/usr/bin/env python3
"""
core/providers/argaam_provider.py
===============================================================
Argaam Provider (KSA Market Data) — v4.1.0 (RELIABILITY + ROUTER COMPAT)
PROD SAFE + ASYNC + ENGINE PATCH FORMAT

Why this revision:
- Fixes KSA router health: adds a real provider object + get_provider() factory
- Fixes engine behavior: NEVER returns {} on failure (returns {"error": "..."} instead)
- Works whether caller is:
    - DataEngineV4 (expects fetch_enriched_quote_patch / fetch_patch style functions)
    - KSA router (expects get_provider()/provider object with get_quote/quote)
- Supports URL templates via env (no hard-coded Argaam endpoints):
    ARGAAM_QUOTE_URL   e.g. "https://.../quote?symbol={symbol}"
    ARGAAM_PROFILE_URL e.g. "https://.../profile?symbol={symbol}"
    ARGAAM_HISTORY_URL e.g. "https://.../history?symbol={symbol}"
  Notes:
    - {symbol} will be filled with BOTH "2222" and "2222.SR" variants automatically.
    - If URLs are not configured, provider returns a clear error (not empty dict).

Exports:
- fetch_enriched_quote_patch(symbol)  -> dict patch
- fetch_patch(symbol)                -> alias
- get_provider() / provider          -> router compatibility
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
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

# ---------------------------------------------------------------------------
# Optional deps (keep PROD safe)
# ---------------------------------------------------------------------------
try:
    import httpx  # type: ignore
    _HTTPX_AVAILABLE = True
except Exception:
    httpx = None  # type: ignore
    _HTTPX_AVAILABLE = False

try:
    import orjson  # type: ignore
    def _json_loads(b: Union[str, bytes]) -> Any:
        return orjson.loads(b)
except Exception:
    def _json_loads(b: Union[str, bytes]) -> Any:
        return json.loads(b)

logger = logging.getLogger("core.providers.argaam_provider")

PROVIDER_NAME = "argaam"
PROVIDER_VERSION = "4.1.0"

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DEFAULT_TIMEOUT_SEC = float(os.getenv("ARGAAM_TIMEOUT_SEC", "20") or "20")
DEFAULT_RETRY_ATTEMPTS = int(os.getenv("ARGAAM_RETRY_ATTEMPTS", "3") or "3")
DEFAULT_MAX_CONCURRENCY = int(os.getenv("ARGAAM_MAX_CONCURRENCY", "10") or "10")
DEFAULT_CACHE_TTL_SEC = float(os.getenv("ARGAAM_CACHE_TTL_SEC", "20") or "20")

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled"}
_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_KSA_CODE_RE = re.compile(r"^\d{3,6}$")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# URL templates (must be set in env to actually fetch data)
ARGAAM_QUOTE_URL = (os.getenv("ARGAAM_QUOTE_URL") or "").strip()
ARGAAM_PROFILE_URL = (os.getenv("ARGAAM_PROFILE_URL") or "").strip()
ARGAAM_HISTORY_URL = (os.getenv("ARGAAM_HISTORY_URL") or "").strip()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in _TRUTHY

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
        if not s or s.lower() in {"-", "—", "n/a", "na", "null", "none"}:
            return None

        s = s.translate(_ARABIC_DIGITS)
        s = s.replace("٬", ",").replace("٫", ".")
        s = s.replace("−", "-")
        s = s.replace("%", "")
        s = s.replace(",", "")
        s = s.replace("SAR", "").replace("ريال", "").strip()

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

def normalize_ksa_symbol(symbol: str) -> str:
    raw = (symbol or "").strip()
    if not raw:
        return ""
    raw = raw.translate(_ARABIC_DIGITS).strip().upper()

    if raw.startswith("TADAWUL:"):
        raw = raw.split(":", 1)[1].strip()
    if raw.endswith(".TADAWUL"):
        raw = raw[:-len(".TADAWUL")].strip()

    if raw.endswith(".SR"):
        code = raw[:-3].strip()
        return f"{code}.SR" if _KSA_CODE_RE.match(code) else ""

    if _KSA_CODE_RE.match(raw):
        return f"{raw}.SR"
    return ""

def _ksa_code(symbol_norm: str) -> str:
    # "2222.SR" -> "2222"
    s = (symbol_norm or "").strip().upper()
    return s[:-3] if s.endswith(".SR") else s

def _unwrap_common_envelopes(data: Any) -> Any:
    cur = data
    for _ in range(4):
        if isinstance(cur, dict):
            for k in ("data", "result", "payload", "response", "items", "results"):
                if k in cur and isinstance(cur[k], (dict, list)):
                    cur = cur[k]
                    break
            else:
                break
        else:
            break
    return cur

def _first_existing(d: Dict[str, Any], keys: List[str]) -> Any:
    for k in keys:
        if k in d and d[k] is not None and d[k] != "":
            return d[k]
    return None

def _normalize_patch_keys(patch: Dict[str, Any]) -> Dict[str, Any]:
    # Engine expects current_price / previous_close etc.
    aliases = {
        "price": "current_price",
        "last_price": "current_price",
        "last": "current_price",
        "close": "previous_close",
        "prev_close": "previous_close",
        "change": "price_change",
        "change_pct": "percent_change",
        "change_percent": "percent_change",
        "pct_change": "percent_change",
        "open": "day_open",
        "high": "day_high",
        "low": "day_low",
    }
    out = dict(patch or {})
    for src, dst in aliases.items():
        if src in out and (dst not in out or out.get(dst) in (None, "", 0)):
            out[dst] = out.get(src)
    # keep coherence
    if out.get("current_price") is not None and out.get("price") in (None, "", 0):
        out["price"] = out.get("current_price")
    if out.get("price") is not None and out.get("current_price") in (None, "", 0):
        out["current_price"] = out.get("price")
    return out

def _error_patch(symbol: str, message: str, *, requested_symbol: Optional[str] = None) -> Dict[str, Any]:
    return {
        "symbol": normalize_ksa_symbol(symbol) or symbol,
        "requested_symbol": requested_symbol or symbol,
        "provider": PROVIDER_NAME,
        "provider_version": PROVIDER_VERSION,
        "data_quality": "MISSING",
        "error": message,
        "last_updated_utc": _now_utc_iso(),
    }

# ---------------------------------------------------------------------------
# Simple TTL cache (in-memory)
# ---------------------------------------------------------------------------

@dataclass
class _CacheItem:
    exp: float
    value: Any

class _TTLCache:
    def __init__(self, max_size: int = 2000) -> None:
        self._max = max_size
        self._d: Dict[str, _CacheItem] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Any:
        now = time.monotonic()
        async with self._lock:
            it = self._d.get(key)
            if not it:
                return None
            if now >= it.exp:
                self._d.pop(key, None)
                return None
            return it.value

    async def set(self, key: str, value: Any, ttl: float) -> None:
        now = time.monotonic()
        async with self._lock:
            if len(self._d) >= self._max:
                # random eviction (cheap + safe)
                for k in random.sample(list(self._d.keys()), k=min(200, len(self._d))):
                    self._d.pop(k, None)
            self._d[key] = _CacheItem(exp=now + max(1.0, ttl), value=value)

# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class ArgaamClient:
    def __init__(self) -> None:
        self.client_id = str(uuid.uuid4())[:8]
        self.quote_url = ARGAAM_QUOTE_URL
        self.profile_url = ARGAAM_PROFILE_URL
        self.history_url = ARGAAM_HISTORY_URL

        self.timeout_sec = DEFAULT_TIMEOUT_SEC
        self.retry_attempts = max(1, DEFAULT_RETRY_ATTEMPTS)
        self._sem = asyncio.Semaphore(max(1, DEFAULT_MAX_CONCURRENCY))
        self._cache = _TTLCache(max_size=int(os.getenv("ARGAAM_CACHE_MAX", "2000") or "2000"))

        self._client = None
        if _HTTPX_AVAILABLE:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout_sec),
                limits=httpx.Limits(max_keepalive_connections=20, max_connections=40),
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json",
                    "X-Client-ID": self.client_id,
                },
            )

    def _is_configured(self) -> bool:
        return bool(self.quote_url)

    def _build_url_candidates(self, template: str, symbol_norm: str) -> List[str]:
        """
        Some APIs want "2222", others want "2222.SR".
        We generate both candidates and try in order.
        """
        if not template:
            return []
        code = _ksa_code(symbol_norm)
        full = symbol_norm
        urls = []
        # {symbol} placeholder
        if "{symbol}" in template:
            urls.append(template.replace("{symbol}", code))
            if full != code:
                urls.append(template.replace("{symbol}", full))
        else:
            # If user put a raw base URL without placeholder, try appending
            urls.append(template.rstrip("/") + "/" + code)
            if full != code:
                urls.append(template.rstrip("/") + "/" + full)
        # de-dupe
        seen = set()
        out = []
        for u in urls:
            if u and u not in seen:
                seen.add(u)
                out.append(u)
        return out

    async def _get_json(self, url: str, cache_key: str, ttl: float) -> Tuple[Optional[Any], Optional[str]]:
        # cache
        cached = await self._cache.get(cache_key)
        if cached is not None:
            return cached, None

        if not self._client:
            return None, "httpx_not_available"

        async with self._sem:
            last_err: Optional[str] = None
            for attempt in range(self.retry_attempts):
                try:
                    r = await self._client.get(url)
                    sc = int(getattr(r, "status_code", 0) or 0)

                    if sc == 429:
                        # backoff + retry
                        wait = min(15.0, (2 ** attempt) + random.uniform(0, 1.0))
                        await asyncio.sleep(wait)
                        last_err = "rate_limited_429"
                        continue

                    if 500 <= sc < 600:
                        wait = min(10.0, (2 ** attempt) + random.uniform(0, 1.0))
                        await asyncio.sleep(wait)
                        last_err = f"server_error_{sc}"
                        continue

                    if sc >= 400:
                        return None, f"http_{sc}"

                    try:
                        payload = _unwrap_common_envelopes(_json_loads(r.content))
                    except Exception:
                        return None, "invalid_json"

                    await self._cache.set(cache_key, payload, ttl)
                    return payload, None

                except Exception as e:
                    last_err = f"network_error:{e.__class__.__name__}"
                    wait = min(10.0, (2 ** attempt) + random.uniform(0, 1.0))
                    await asyncio.sleep(wait)

            return None, last_err or "max_retries_exceeded"

    def _parse_quote_patch(self, symbol_norm: str, quote_data: Any, profile_data: Any = None) -> Dict[str, Any]:
        """
        Turn unknown provider payload into UnifiedQuote-like patch.
        We use heuristics: look for common keys in dicts.
        """
        out: Dict[str, Any] = {
            "symbol": symbol_norm,
            "symbol_normalized": symbol_norm,
            "provider": PROVIDER_NAME,
            "provider_version": PROVIDER_VERSION,
            "last_updated_utc": _now_utc_iso(),
        }

        q = quote_data if isinstance(quote_data, dict) else {}
        p = profile_data if isinstance(profile_data, dict) else {}

        # common fields
        name = _first_existing(p, ["name", "companyName", "company_name", "longName"]) or _first_existing(q, ["name", "companyName"])
        sector = _first_existing(p, ["sector"]) or _first_existing(q, ["sector"])
        currency = _first_existing(q, ["currency"]) or _first_existing(p, ["currency"]) or "SAR"

        price_raw = _first_existing(q, ["current_price", "price", "last", "lastPrice", "closePrice", "tradePrice"])
        prev_raw = _first_existing(q, ["previous_close", "prev_close", "previousClose", "close", "yesterdayClose"])
        ch_raw = _first_existing(q, ["price_change", "change", "chg"])
        pct_raw = _first_existing(q, ["percent_change", "change_percent", "changePct", "pct_change"])

        out["name"] = name
        out["sector"] = sector
        out["currency"] = currency
        out["current_price"] = _safe_float(price_raw)
        out["price"] = out["current_price"]
        out["previous_close"] = _safe_float(prev_raw)
        out["price_change"] = _safe_float(ch_raw)
        # pct sometimes already percent (e.g. 1.23) or ratio (0.0123). keep as-is; enriched router can format.
        out["percent_change"] = _safe_float(pct_raw)

        out["volume"] = _safe_float(_first_existing(q, ["volume", "vol", "tradedVolume"]))

        # KSA identification
        out["exchange"] = out.get("exchange") or "TADAWUL"
        out["market"] = out.get("market") or "KSA"

        # data quality (minimal)
        out["data_quality"] = "GOOD" if out.get("current_price") is not None else "MISSING"
        return _normalize_patch_keys(out)

    async def get_enriched_patch(self, symbol: str) -> Dict[str, Any]:
        symbol_norm = normalize_ksa_symbol(symbol)
        if not symbol_norm:
            return _error_patch(symbol, "invalid_ksa_symbol")

        if not self._is_configured():
            return _error_patch(symbol_norm, "ARGAAM_QUOTE_URL not configured (set env var)")

        quote_urls = self._build_url_candidates(self.quote_url, symbol_norm)
        prof_urls = self._build_url_candidates(self.profile_url, symbol_norm) if self.profile_url else []
        # history currently unused for patch in this reliability version (kept for future)
        # hist_urls = self._build_url_candidates(self.history_url, symbol_norm) if self.history_url else []

        # quote (required)
        quote_data = None
        quote_err = None
        for u in quote_urls:
            quote_data, quote_err = await self._get_json(u, f"q:{symbol_norm}:{u}", ttl=DEFAULT_CACHE_TTL_SEC)
            if quote_data is not None:
                break

        if quote_data is None:
            return _error_patch(symbol_norm, f"quote_fetch_failed: {quote_err or 'unknown'}")

        # profile (optional)
        profile_data = None
        if prof_urls:
            for u in prof_urls:
                profile_data, _ = await self._get_json(u, f"p:{symbol_norm}:{u}", ttl=max(60.0, DEFAULT_CACHE_TTL_SEC))
                if profile_data is not None:
                    break

        patch = self._parse_quote_patch(symbol_norm, quote_data, profile_data)
        patch["requested_symbol"] = symbol
        patch["data_sources"] = [PROVIDER_NAME]
        return patch

    async def close(self) -> None:
        try:
            if self._client:
                await self._client.aclose()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Provider object for KSA router compatibility
# ---------------------------------------------------------------------------

class ArgaamProvider:
    """
    Router expects a provider object that has methods like:
    - get_quote(symbol=...)
    - quote(symbol=...)
    - fetch_quote(symbol=...)
    This class provides them all.
    """
    name = PROVIDER_NAME
    version = PROVIDER_VERSION

    def __init__(self) -> None:
        self._client: Optional[ArgaamClient] = None
        self._lock = asyncio.Lock()

    async def _get_client(self) -> ArgaamClient:
        async with self._lock:
            if self._client is None:
                self._client = ArgaamClient()
            return self._client

    async def get_quote(self, symbol: str) -> Dict[str, Any]:
        c = await self._get_client()
        return await c.get_enriched_patch(symbol)

    async def quote(self, symbol: str) -> Dict[str, Any]:
        return await self.get_quote(symbol)

    async def fetch_quote(self, symbol: str) -> Dict[str, Any]:
        return await self.get_quote(symbol)

    async def fetch_enriched_quote_patch(self, symbol: str) -> Dict[str, Any]:
        # DataEngine compatibility
        return await self.get_quote(symbol)

    async def close(self) -> None:
        if self._client:
            await self._client.close()
            self._client = None


# ---------------------------------------------------------------------------
# Singleton exports
# ---------------------------------------------------------------------------

_CLIENT_INSTANCE: Optional[ArgaamClient] = None
_CLIENT_LOCK = asyncio.Lock()

_PROVIDER_INSTANCE: Optional[ArgaamProvider] = None
_PROVIDER_LOCK = asyncio.Lock()

async def get_client() -> ArgaamClient:
    global _CLIENT_INSTANCE
    if _CLIENT_INSTANCE is None:
        async with _CLIENT_LOCK:
            if _CLIENT_INSTANCE is None:
                _CLIENT_INSTANCE = ArgaamClient()
    return _CLIENT_INSTANCE

async def close_client() -> None:
    global _CLIENT_INSTANCE
    if _CLIENT_INSTANCE:
        await _CLIENT_INSTANCE.close()
        _CLIENT_INSTANCE = None

async def get_provider() -> ArgaamProvider:
    global _PROVIDER_INSTANCE
    if _PROVIDER_INSTANCE is None:
        async with _PROVIDER_LOCK:
            if _PROVIDER_INSTANCE is None:
                _PROVIDER_INSTANCE = ArgaamProvider()
    return _PROVIDER_INSTANCE

# Provide a module-level provider object too (router sometimes checks "provider" attr)
provider = ArgaamProvider()

# ---------------------------------------------------------------------------
# Engine-facing functions (DataEngineV4)
# ---------------------------------------------------------------------------

async def fetch_enriched_quote_patch(symbol: str, *args, **kwargs) -> Dict[str, Any]:
    """
    Main function used by DataEngineV4.
    Must return a dict patch. On failure MUST include "error".
    """
    c = await get_client()
    patch = await c.get_enriched_patch(symbol)
    # Ensure engine sees failure if needed
    if not isinstance(patch, dict) or not patch:
        return _error_patch(symbol, "empty_patch")
    return patch

# aliases (engine tries multiple names)
fetch_patch = fetch_enriched_quote_patch
fetch_quote_patch = fetch_enriched_quote_patch

__all__ = [
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    "normalize_ksa_symbol",
    "fetch_enriched_quote_patch",
    "fetch_patch",
    "fetch_quote_patch",
    "get_client",
    "close_client",
    "ArgaamProvider",
    "get_provider",
    "provider",
]
