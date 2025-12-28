# core/providers/argaam_provider.py  (FULL REPLACEMENT)
"""
core/providers/argaam_provider.py
===============================================================
Argaam Provider (KSA optional enrichment) — v1.3.0
(PROD SAFE + ENGINE PATCH-STYLE + SILENT WHEN NOT CONFIGURED)

Why v1.3.0
- ✅ **Silent when not configured**: if ARGAAM_QUOTE_URL and ARGAAM_PROFILE_URL are both missing,
  returns empty patch with no error (prevents noisy warnings in DataEngineV2).
- ✅ Engine-friendly: exports PATCH functions that return **(patch, err)** (DataEngineV2 supports both).
- ✅ Returns ONLY patch fields (no "status"/"error" keys inside patch) to avoid polluting engine output.
- ✅ Better numeric parsing (Arabic digits, commas, %, parentheses).
- ✅ Defensive mapping for dict OR list payload shapes.
- ✅ No network calls at import-time; AsyncClient is created lazily.

Supported env vars (optional)
- ARGAAM_QUOTE_URL              e.g. https://.../quote?symbol={symbol}  or .../{code}
- ARGAAM_PROFILE_URL            e.g. https://.../profile?symbol={symbol}
- ARGAAM_HEADERS_JSON           JSON dict for headers (optional)
- ARGAAM_TIMEOUT_SEC            default: fallback to HTTP_TIMEOUT then 20
- ARGAAM_RETRY_ATTEMPTS         default: 2
- ARGAAM_TTL_SEC                default: 15 (min 5)

Exports
- fetch_quote_patch(symbol) -> (patch: dict, err: str|None)
- fetch_enriched_quote_patch(symbol) -> (patch, err)
- fetch_quote_and_enrichment_patch(symbol) -> (patch, err)
- aclose_argaam_client()
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
from typing import Any, Dict, Optional, Tuple, Union, List

import httpx
from cachetools import TTLCache

logger = logging.getLogger("core.providers.argaam_provider")

PROVIDER_NAME = "argaam"
PROVIDER_VERSION = "1.3.0"

DEFAULT_TIMEOUT_SEC = 20.0
DEFAULT_RETRY_ATTEMPTS = 2

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

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
        s = s.replace("%", "").replace(",", "").replace("+", "").strip()

        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()

        # optional K/M/B suffix
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
                timeout_sec = _env_float("ARGAAM_TIMEOUT_SEC", _env_float("HTTP_TIMEOUT", DEFAULT_TIMEOUT_SEC))
                timeout = httpx.Timeout(timeout_sec, connect=min(10.0, timeout_sec))
                _CLIENT = httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=_get_headers())
                logger.info("Argaam client init v%s | timeout=%.1fs", PROVIDER_VERSION, timeout_sec)
    return _CLIENT

async def aclose_argaam_client() -> None:
    global _CLIENT
    c = _CLIENT
    _CLIENT = None
    if c is not None:
        try:
            await c.aclose()
        except Exception:
            pass

def _format_url(tpl: str, symbol: str) -> str:
    s = (symbol or "").strip().upper()
    code = s[:-3] if s.endswith(".SR") else s
    return (tpl or "").replace("{symbol}", s).replace("{code}", code)

def _configured() -> bool:
    return bool(_safe_str(os.getenv("ARGAAM_QUOTE_URL", "")) or _safe_str(os.getenv("ARGAAM_PROFILE_URL", "")))

def _base_patch(symbol: str) -> Dict[str, Any]:
    """
    PATCH-style only. Do NOT include 'status'/'error' here (engine handles warnings + status).
    """
    return {
        "symbol": (symbol or "").strip().upper(),
        "market": "KSA",
        "currency": "SAR",
        "data_source": PROVIDER_NAME,
        "provider_version": PROVIDER_VERSION,
        "last_updated_utc": _utc_iso(),
        # quote-ish fields
        "current_price": None,
        "previous_close": None,
        "open": None,
        "day_high": None,
        "day_low": None,
        "volume": None,
        "price_change": None,
        "percent_change": None,
        # identity (optional)
        "name": None,
        "sector": None,
        "industry": None,
        "sub_sector": None,
    }

def _clean_patch(p: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (p or {}).items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        out[k] = v
    return out

async def _http_get_json(url: str) -> Tuple[Optional[Union[dict, list]], Optional[str]]:
    retries = max(1, _env_int("ARGAAM_RETRY_ATTEMPTS", DEFAULT_RETRY_ATTEMPTS))
    client = await _get_client()

    last_err: Optional[str] = None

    for attempt in range(retries):
        try:
            r = await client.get(url)

            sc = int(r.status_code)
            if sc == 429 or 500 <= sc < 600:
                last_err = f"HTTP {sc}"
                if attempt < retries - 1:
                    await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.25)
                    continue
                return None, last_err

            if sc >= 400:
                # keep short error only
                return None, f"HTTP {sc}"

            try:
                js = r.json()
            except Exception:
                return None, "invalid JSON"

            if not isinstance(js, (dict, list)):
                return None, "unexpected JSON type"

            return js, None

        except Exception as e:
            last_err = f"{e.__class__.__name__}: {e}"
            if attempt < retries - 1:
                await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.25)
                continue
            return None, last_err

    return None, last_err or "request failed"

def _coerce_dict(data: Union[dict, list]) -> dict:
    if isinstance(data, dict):
        return data
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return data[0]
    return {}

def _map_quote_payload(data: dict) -> Dict[str, Any]:
    """
    Best-effort mapping for common JSON payload shapes.
    Only sets values when present.
    """
    patch: Dict[str, Any] = {}

    patch["current_price"] = _safe_float(_pick(data, "last", "last_price", "price", "close", "c", "LastPrice"))
    patch["previous_close"] = _safe_float(_pick(data, "previous_close", "prev_close", "pc", "PreviousClose"))
    patch["open"] = _safe_float(_pick(data, "open", "o", "Open"))
    patch["day_high"] = _safe_float(_pick(data, "high", "day_high", "h", "High"))
    patch["day_low"] = _safe_float(_pick(data, "low", "day_low", "l", "Low"))
    patch["volume"] = _safe_float(_pick(data, "volume", "v", "Volume"))

    patch["price_change"] = _safe_float(_pick(data, "change", "d", "price_change", "Change"))
    patch["percent_change"] = _safe_float(
        _pick(data, "change_pct", "change_percent", "dp", "percent_change", "ChangePercent")
    )

    name = _safe_str(_pick(data, "name", "company", "company_name", "CompanyName", "shortName", "longName"))
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

    return _clean_patch(patch)

async def _fetch_quote_patch(symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    sym = (symbol or "").strip().upper()
    if not sym:
        return {}, "argaam: empty symbol"

    # ✅ Silent skip when not configured (prevents noisy warnings)
    tpl = _safe_str(os.getenv("ARGAAM_QUOTE_URL", ""))
    if not tpl:
        return {}, None

    ck = f"quote::{sym}"
    hit = _CACHE.get(ck)
    if isinstance(hit, dict) and hit:
        return dict(hit), None

    url = _format_url(tpl, sym)
    js, err = await _http_get_json(url)
    if js is None:
        return {}, f"argaam quote failed: {err}"

    root = _coerce_dict(js)
    mapped = _map_quote_payload(root)

    # must have price to be considered useful
    if _safe_float(mapped.get("current_price")) is None:
        return {}, "argaam quote returned no price"

    base = _base_patch(sym)
    base.update(mapped)
    base["last_updated_utc"] = _utc_iso()

    patch = _clean_patch(base)
    _CACHE[ck] = dict(patch)
    return patch, None

async def _fetch_profile_patch(symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    sym = (symbol or "").strip().upper()
    if not sym:
        return {}, "argaam: empty symbol"

    tpl = _safe_str(os.getenv("ARGAAM_PROFILE_URL", ""))
    if not tpl:
        return {}, None  # silent if not configured

    ck = f"profile::{sym}"
    hit = _CACHE.get(ck)
    if isinstance(hit, dict) and hit:
        return dict(hit), None

    url = _format_url(tpl, sym)
    js, err = await _http_get_json(url)
    if js is None:
        return {}, f"argaam profile failed: {err}"

    root = _coerce_dict(js)
    mapped = _map_quote_payload(root)

    # identity-only
    identity: Dict[str, Any] = {}
    for k in ("name", "sector", "industry", "sub_sector", "currency"):
        if k in mapped:
            identity[k] = mapped[k]

    if not identity:
        return {}, "argaam profile had no identity fields"

    base = _base_patch(sym)
    for k in ("current_price", "previous_close", "open", "day_high", "day_low", "volume", "price_change", "percent_change"):
        base.pop(k, None)  # ensure profile doesn't claim quote fields
    base.update(identity)
    base["last_updated_utc"] = _utc_iso()

    patch = _clean_patch(base)
    _CACHE[ck] = dict(patch)
    return patch, None


# ----------------------------------------------------------------------
# Engine-compatible exported callables
# ----------------------------------------------------------------------
async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Tuple[Dict[str, Any], Optional[str]]:
    return await _fetch_quote_patch(symbol)

async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Tuple[Dict[str, Any], Optional[str]]:
    # quote first, then profile for identity (best-effort)
    q_patch, q_err = await _fetch_quote_patch(symbol)
    if q_patch:
        return q_patch, q_err

    p_patch, p_err = await _fetch_profile_patch(symbol)
    if p_patch:
        return p_patch, p_err

    # If nothing and not configured => keep silent (no err) to avoid noisy warnings
    if not _configured():
        return {}, None

    # If configured but failed => return the more relevant error
    return {}, (q_err or p_err or "argaam: unavailable")

async def fetch_quote_and_enrichment_patch(symbol: str, *args: Any, **kwargs: Any) -> Tuple[Dict[str, Any], Optional[str]]:
    return await fetch_enriched_quote_patch(symbol)

__all__ = [
    "fetch_quote_patch",
    "fetch_enriched_quote_patch",
    "fetch_quote_and_enrichment_patch",
    "aclose_argaam_client",
    "PROVIDER_VERSION",
]
