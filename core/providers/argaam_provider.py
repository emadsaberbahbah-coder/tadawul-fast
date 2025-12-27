# core/providers/argaam_provider.py  (FULL REPLACEMENT)
"""
core/providers/argaam_provider.py
===============================================================
Argaam Provider (KSA optional enrichment) — v1.2.0 (PROD SAFE + ENGINE ALIGNED)

What this fixes vs your old version
- ✅ Engine-aligned signatures: returns Dict[str, Any] (NOT tuple)
- ✅ Never crashes if ARGAAM_* env vars are missing (clean error object instead)
- ✅ Supports URL templates with {symbol} and {code}
- ✅ Defensive parsing (only fills fields that actually exist in response)
- ✅ No misleading values; data_quality reflects whether we got real quote fields

Supported env vars (optional)
- ARGAAM_QUOTE_URL              e.g. https://.../quote?symbol={symbol}  or .../{code}
- ARGAAM_PROFILE_URL            e.g. https://.../profile?symbol={symbol}
- ARGAAM_HEADERS_JSON           JSON dict for headers (optional)
- ARGAAM_TIMEOUT_SEC            default: fallback to HTTP_TIMEOUT then 20
- ARGAAM_RETRY_ATTEMPTS         default: 2
- ARGAAM_TTL_SEC                default: 15 (min 5)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Union

import httpx
from cachetools import TTLCache

logger = logging.getLogger("core.providers.argaam_provider")

PROVIDER_NAME = "argaam"
PROVIDER_VERSION = "1.2.0"

DEFAULT_TIMEOUT_SEC = 20.0
DEFAULT_RETRY_ATTEMPTS = 2

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_CLIENT: Optional[httpx.AsyncClient] = None
_LOCK = asyncio.Lock()

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

def _ttl_seconds() -> float:
    # TTLCache must be > 0; enforce minimum
    ttl = _env_float("ARGAAM_TTL_SEC", 15.0)
    if ttl <= 0:
        ttl = 15.0
    return max(5.0, ttl)

_CACHE: TTLCache = TTLCache(maxsize=5000, ttl=_ttl_seconds())


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s or None


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def _pick(d: Any, *keys: str) -> Any:
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d:
            return d.get(k)
    return None


def _get_headers() -> Dict[str, str]:
    h = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json,text/plain,text/html;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.8,ar;q=0.6",
    }
    raw = _safe_str(os.getenv("ARGAAM_HEADERS_JSON", ""))
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
                # If ARGAAM_TIMEOUT_SEC not set, fall back to HTTP_TIMEOUT then default
                timeout_sec = _env_float("ARGAAM_TIMEOUT_SEC", _env_float("HTTP_TIMEOUT", DEFAULT_TIMEOUT_SEC))
                timeout = httpx.Timeout(timeout_sec, connect=min(10.0, timeout_sec))
                _CLIENT = httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=_get_headers())
                logger.info("Argaam client init v%s | timeout=%.1fs", PROVIDER_VERSION, timeout_sec)
    return _CLIENT


async def aclose_argaam_client() -> None:
    global _CLIENT
    if _CLIENT is None:
        return
    try:
        await _CLIENT.aclose()
    finally:
        _CLIENT = None


def _format_url(tpl: str, symbol: str) -> str:
    s = (symbol or "").strip().upper()
    # code: remove .SR if present
    code = s[:-3] if s.endswith(".SR") else s
    return (tpl or "").replace("{symbol}", s).replace("{code}", code)


def _base(symbol: str) -> Dict[str, Any]:
    return {
        "status": "success",
        "symbol": symbol,
        "market": "KSA",
        "currency": "SAR",
        "data_source": PROVIDER_NAME,
        "data_quality": "OK",
        "error": "",
        "provider_version": PROVIDER_VERSION,
        "last_updated_utc": _utc_iso(),
        # quote-ish fields (optional)
        "current_price": None,
        "previous_close": None,
        "open": None,
        "day_high": None,
        "day_low": None,
        "volume": None,
        "price_change": None,
        "percent_change": None,
        # identity (optional)
        "name": "",
        "sector": "",
        "industry": "",
        "sub_sector": "",
    }


def _quality(out: Dict[str, Any]) -> None:
    # OK if at least current_price exists; otherwise BAD
    if _to_float(out.get("current_price")) is None:
        out["data_quality"] = "BAD"
        if out.get("error"):
            out["status"] = "error"
    else:
        out["data_quality"] = "OK"


async def _get_json(url: str) -> Union[Dict[str, Any], list, None]:
    retries = _env_int("ARGAAM_RETRY_ATTEMPTS", DEFAULT_RETRY_ATTEMPTS)
    retries = max(1, retries)

    client = await _get_client()

    for attempt in range(retries):
        try:
            r = await client.get(url)

            # Retry on rate-limit / transient server errors
            if r.status_code == 429 or 500 <= r.status_code < 600:
                if attempt < (retries - 1):
                    await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.25)
                    continue
                return None

            if r.status_code >= 400:
                return None

            # JSON only (avoid misleading HTML parsing here)
            try:
                return r.json()
            except Exception:
                return None

        except Exception:
            if attempt < (retries - 1):
                await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.25)
                continue
            return None

    return None


def _map_quote_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Best-effort mapping for common JSON payload shapes.
    Only sets values when present.
    """
    patch: Dict[str, Any] = {}

    # Common key guesses
    patch["current_price"] = _to_float(_pick(data, "last", "last_price", "price", "close", "c", "LastPrice"))
    patch["previous_close"] = _to_float(_pick(data, "previous_close", "prev_close", "pc", "PreviousClose"))
    patch["open"] = _to_float(_pick(data, "open", "o", "Open"))
    patch["day_high"] = _to_float(_pick(data, "high", "day_high", "h", "High"))
    patch["day_low"] = _to_float(_pick(data, "low", "day_low", "l", "Low"))
    patch["volume"] = _to_float(_pick(data, "volume", "v", "Volume"))

    patch["price_change"] = _to_float(_pick(data, "change", "d", "price_change", "Change"))
    patch["percent_change"] = _to_float(_pick(data, "change_pct", "change_percent", "dp", "percent_change", "ChangePercent"))

    # Identity (if provided)
    name = _safe_str(_pick(data, "name", "company", "company_name", "CompanyName"))
    if name:
        patch["name"] = name
    sector = _safe_str(_pick(data, "sector", "Sector"))
    if sector:
        patch["sector"] = sector
    industry = _safe_str(_pick(data, "industry", "Industry"))
    if industry:
        patch["industry"] = industry
    sub_sector = _safe_str(_pick(data, "sub_sector", "subSector", "SubSector"))
    if sub_sector:
        patch["sub_sector"] = sub_sector

    # Remove Nones / empty strings
    clean: Dict[str, Any] = {}
    for k, v in patch.items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        clean[k] = v
    return clean


async def _fetch_quote_patch(symbol: str) -> Dict[str, Any]:
    out = _base(symbol)

    tpl = _safe_str(os.getenv("ARGAAM_QUOTE_URL", ""))
    if not tpl:
        out["status"] = "error"
        out["data_quality"] = "BAD"
        out["error"] = "argaam: not configured (ARGAAM_QUOTE_URL)"
        return out

    sym = (symbol or "").strip().upper()
    if not sym:
        out["status"] = "error"
        out["data_quality"] = "BAD"
        out["error"] = "argaam: empty symbol"
        return out

    ck = f"quote::{sym}"
    hit = _CACHE.get(ck)
    if isinstance(hit, dict) and hit:
        return dict(hit)

    url = _format_url(tpl, sym)
    data = await _get_json(url)
    if not data or not isinstance(data, dict):
        out["status"] = "error"
        out["data_quality"] = "BAD"
        out["error"] = "argaam: empty response"
        return out

    out.update(_map_quote_payload(data))
    out["last_updated_utc"] = _utc_iso()
    _quality(out)

    _CACHE[ck] = dict(out)
    return out


async def _fetch_profile_patch(symbol: str) -> Dict[str, Any]:
    out = _base(symbol)

    tpl = _safe_str(os.getenv("ARGAAM_PROFILE_URL", ""))
    if not tpl:
        out["status"] = "error"
        out["data_quality"] = "BAD"
        out["error"] = "argaam: not configured (ARGAAM_PROFILE_URL)"
        return out

    sym = (symbol or "").strip().upper()
    if not sym:
        out["status"] = "error"
        out["data_quality"] = "BAD"
        out["error"] = "argaam: empty symbol"
        return out

    ck = f"profile::{sym}"
    hit = _CACHE.get(ck)
    if isinstance(hit, dict) and hit:
        return dict(hit)

    url = _format_url(tpl, sym)
    data = await _get_json(url)
    if not data or not isinstance(data, dict):
        out["status"] = "error"
        out["data_quality"] = "BAD"
        out["error"] = "argaam: empty response"
        return out

    # Profile may include identity fields; reuse mapper safely
    patch = _map_quote_payload(data)

    # Prefer identity keys only; do not claim prices from profile endpoints unless present
    identity_only: Dict[str, Any] = {}
    for k in ("name", "sector", "industry", "sub_sector", "currency"):
        if k in patch:
            identity_only[k] = patch[k]

    out.update(identity_only)
    out["last_updated_utc"] = _utc_iso()

    # Profile alone may not include current_price; keep BAD if missing
    _quality(out)

    _CACHE[ck] = dict(out)
    return out


# ----------------------------------------------------------------------
# Engine-compatible exported callables (return Dict patch, no tuples)
# ----------------------------------------------------------------------
async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _fetch_quote_patch(symbol)


async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    # Try quote first; if it fails, try profile as best-effort identity enrichment
    q = await _fetch_quote_patch(symbol)
    if q.get("status") != "error":
        return q

    p = await _fetch_profile_patch(symbol)
    # Keep the quote error if both fail
    if p.get("status") == "error":
        return q
    return p


async def fetch_quote_and_enrichment_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    # Same as enriched; engine merges anyway
    return await fetch_enriched_quote_patch(symbol)


__all__ = [
    "fetch_quote_patch",
    "fetch_enriched_quote_patch",
    "fetch_quote_and_enrichment_patch",
    "aclose_argaam_client",
    "PROVIDER_VERSION",
]
