# core/providers/argaam_provider.py
"""
core/providers/argaam_provider.py
===============================================================
Argaam Provider (KSA optional enrichment) — v1.4.1
(PROD SAFE + ENGINE PATCH-STYLE + SILENT WHEN NOT CONFIGURED)

Why v1.4.1
- ✅ Keeps v1.4.0 guarantees
- ✅ Fix: TTL cache is created with a static ttl at import-time; now also recreates cache if TTL env changes
- ✅ Fix: accept both HTTP_TIMEOUT and HTTP_TIMEOUT_SEC consistently
- ✅ Fix: safe handling for JSON responses wrapped under common top-level keys (e.g., {"data": {...}})
- ✅ Better: more tolerant percent parsing and negative formats

Key guarantees
- Silent when not configured: if ARGAAM_QUOTE_URL and ARGAAM_PROFILE_URL are both missing,
  returns empty patch with no error (prevents noisy warnings in DataEngineV2).
- Import-safe even if cachetools is missing (has a tiny TTL cache fallback).
- Engine-friendly: exports PATCH functions that return (patch, err) tuples.
- Patch is CLEAN: no "status"/"error" keys in the patch; no meta keys that can overwrite canonical output.
- Strict KSA-only: non-KSA symbols are silently ignored (prevents misrouting / pollution).
- Defensive mapping: supports dict OR list payload shapes; bounded deep-find for nested JSON.
- No network calls at import-time; AsyncClient is created lazily.
- Enriched call merges profile identity into quote patch (fills blanks only).

Supported env vars (optional)
- ARGAAM_QUOTE_URL              e.g. https://.../quote?symbol={symbol}  or .../{code}
- ARGAAM_PROFILE_URL            e.g. https://.../profile?symbol={symbol}
- ARGAAM_HEADERS_JSON           JSON dict for headers (optional)
- ARGAAM_TIMEOUT_SEC            default: fallback to HTTP_TIMEOUT_SEC / HTTP_TIMEOUT then 20
- ARGAAM_RETRY_ATTEMPTS         default: 2 (min 1)
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
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import httpx

logger = logging.getLogger("core.providers.argaam_provider")

PROVIDER_NAME = "argaam"
PROVIDER_VERSION = "1.4.1"

DEFAULT_TIMEOUT_SEC = 20.0
DEFAULT_RETRY_ATTEMPTS = 2

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

# KSA symbol: 3-6 digits, optionally .SR
_KSA_CODE_RE = re.compile(r"^\d{3,6}$")
_KSA_SYMBOL_RE = re.compile(r"^\d{3,6}(\.SR)?$", re.IGNORECASE)

_CLIENT: Optional[httpx.AsyncClient] = None
_LOCK = asyncio.Lock()

# -----------------------------------------------------------------------------


def _safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s or None


# -----------------------------------------------------------------------------
# Env helpers (never crash)
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


def _ttl_seconds() -> float:
    ttl = _env_float("ARGAAM_TTL_SEC", 15.0)
    if ttl <= 0:
        ttl = 15.0
    return max(5.0, ttl)


def _timeout_seconds() -> float:
    # priority: ARGAAM_TIMEOUT_SEC, then HTTP_TIMEOUT_SEC, then HTTP_TIMEOUT
    for key in ("ARGAAM_TIMEOUT_SEC", "HTTP_TIMEOUT_SEC", "HTTP_TIMEOUT"):
        t = _env_float(key, 0.0)
        if t and t > 0:
            return float(t)
    return DEFAULT_TIMEOUT_SEC


def _configured() -> bool:
    return bool(_safe_str(os.getenv("ARGAAM_QUOTE_URL", "")) or _safe_str(os.getenv("ARGAAM_PROFILE_URL", "")))


# -----------------------------------------------------------------------------
# Tiny TTL cache fallback if cachetools missing (import-safe)
# -----------------------------------------------------------------------------
try:
    from cachetools import TTLCache as _TTLCache  # type: ignore

    _HAS_CACHETOOLS = True
except Exception:  # pragma: no cover
    _HAS_CACHETOOLS = False

    class _TTLCache(dict):  # type: ignore
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
            # naive eviction
            if len(self) >= self._maxsize:
                try:
                    oldest_key = next(iter(self.keys()))
                    super().pop(oldest_key, None)
                    self._exp.pop(oldest_key, None)
                except Exception:
                    pass
            super().__setitem__(key, value)
            self._exp[key] = time.time() + self._ttl

        def _set_ttl(self, ttl: float) -> None:
            # allow TTL update for env changes
            try:
                self._ttl = max(1.0, float(ttl))
            except Exception:
                pass


_CACHE_MAXSIZE = 5000
_CACHE_TTL = _ttl_seconds()
_CACHE: _TTLCache = _TTLCache(maxsize=_CACHE_MAXSIZE, ttl=_CACHE_TTL)  # type: ignore


def _ensure_cache_ttl_current() -> None:
    global _CACHE_TTL, _CACHE
    ttl_now = _ttl_seconds()
    if abs(ttl_now - _CACHE_TTL) < 0.0001:
        return
    _CACHE_TTL = ttl_now
    # cachetools TTLCache ttl is not meant to be mutated reliably, so rebuild cleanly
    try:
        _CACHE = _TTLCache(maxsize=_CACHE_MAXSIZE, ttl=_CACHE_TTL)  # type: ignore
    except Exception:
        # fallback: best effort update for our tiny cache
        try:
            _CACHE._set_ttl(_CACHE_TTL)  # type: ignore
        except Exception:
            pass


# -----------------------------------------------------------------------------
# Symbol helpers
# -----------------------------------------------------------------------------
def _is_ksa_symbol(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    if not s:
        return False
    if s.endswith(".SR"):
        code = s[:-3]
        return bool(_KSA_CODE_RE.match(code))
    return bool(_KSA_SYMBOL_RE.match(s) or _KSA_CODE_RE.match(s))


def _normalize_ksa_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    if s.endswith(".SR"):
        code = s[:-3]
        return f"{code}.SR" if _KSA_CODE_RE.match(code) else ""
    if _KSA_CODE_RE.match(s):
        return f"{s}.SR"
    # accept raw "1234sr" styles? nope, keep strict
    return ""


# -----------------------------------------------------------------------------
# Parsing helpers
# -----------------------------------------------------------------------------
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
        # normalize percent and separators
        s = s.replace("%", "").replace(",", "").replace("+", "").strip()

        # parentheses negative
        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()

        # allow trailing percent sign already removed; allow whitespace
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


def _format_url(tpl: str, symbol: str) -> str:
    s = (symbol or "").strip().upper()
    code = s[:-3] if s.endswith(".SR") else s
    return (tpl or "").replace("{symbol}", s).replace("{code}", code)


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
                timeout_sec = _timeout_seconds()
                timeout = httpx.Timeout(timeout_sec, connect=min(10.0, timeout_sec))
                _CLIENT = httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=_get_headers())
                logger.info(
                    "Argaam client init v%s | timeout=%.1fs | cachetools=%s",
                    PROVIDER_VERSION,
                    timeout_sec,
                    _HAS_CACHETOOLS,
                )
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


def _clean_patch(p: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (p or {}).items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        out[k] = v
    return out


# -----------------------------------------------------------------------------
# Bounded deep-find (nested JSON safe)
# -----------------------------------------------------------------------------
def _find_first_value(obj: Any, keys: Sequence[str], *, max_depth: int = 6, max_nodes: int = 2500) -> Any:
    if obj is None:
        return None
    keyset = {str(k).strip().lower() for k in keys if k}
    if not keyset:
        return None

    q: List[Tuple[Any, int]] = [(obj, 0)]
    seen: set[int] = set()
    nodes = 0

    while q:
        x, d = q.pop(0)
        if x is None:
            continue

        xid = id(x)
        if xid in seen:
            continue
        seen.add(xid)

        nodes += 1
        if nodes > max_nodes:
            return None
        if d > max_depth:
            continue

        if isinstance(x, dict):
            for k, v in x.items():
                if str(k).strip().lower() in keyset:
                    return v
            for v in x.values():
                q.append((v, d + 1))
            continue

        if isinstance(x, list):
            for it in x:
                q.append((it, d + 1))
            continue

    return None


def _pick_num(obj: Any, *keys: str) -> Optional[float]:
    return _safe_float(_find_first_value(obj, keys))


def _pick_pct(obj: Any, *keys: str) -> Optional[float]:
    return _pct_if_fraction(_find_first_value(obj, keys))


def _pick_str(obj: Any, *keys: str) -> Optional[str]:
    return _safe_str(_find_first_value(obj, keys))


def _unwrap_common_envelopes(js: Union[dict, list]) -> Union[dict, list]:
    """
    Some endpoints wrap payloads like {"data": {...}} or {"result": {...}}.
    This unwraps a few common keys, bounded (no loops).
    """
    cur: Any = js
    for _ in range(2):
        if isinstance(cur, dict):
            for k in ("data", "result", "payload", "quote", "profile"):
                if k in cur and isinstance(cur[k], (dict, list)):
                    cur = cur[k]
                    break
            else:
                break
        else:
            break
    return cur


def _coerce_dict(data: Union[dict, list]) -> dict:
    if isinstance(data, dict):
        return data
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return data[0]
    return {}


# -----------------------------------------------------------------------------
# HTTP
# -----------------------------------------------------------------------------
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
                return None, f"HTTP {sc}"

            try:
                js = r.json()
            except Exception:
                return None, "invalid JSON"

            if not isinstance(js, (dict, list)):
                return None, "unexpected JSON type"

            js = _unwrap_common_envelopes(js)
            return js, None

        except Exception as e:
            last_err = f"{e.__class__.__name__}: {e}"
            if attempt < retries - 1:
                await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.25)
                continue
            return None, last_err

    return None, last_err or "request failed"


# -----------------------------------------------------------------------------
# Mapping
# -----------------------------------------------------------------------------
def _map_quote_payload(root: Any) -> Dict[str, Any]:
    """
    Best-effort mapping for common JSON payload shapes.
    Returns ONLY useful quote/identity fields (no status/error/meta).
    """
    patch: Dict[str, Any] = {}

    patch["current_price"] = _pick_num(root, "last", "last_price", "price", "close", "c", "LastPrice", "tradingPrice")
    patch["previous_close"] = _pick_num(root, "previous_close", "prev_close", "pc", "PreviousClose", "prevClose")
    patch["open"] = _pick_num(root, "open", "o", "Open", "openPrice")
    patch["day_high"] = _pick_num(root, "high", "day_high", "h", "High", "dayHigh", "sessionHigh")
    patch["day_low"] = _pick_num(root, "low", "day_low", "l", "Low", "dayLow", "sessionLow")
    patch["volume"] = _pick_num(root, "volume", "v", "Volume", "tradedVolume", "qty", "quantity")

    patch["price_change"] = _pick_num(root, "change", "d", "price_change", "Change", "diff", "delta")
    patch["percent_change"] = _pick_pct(
        root,
        "change_pct",
        "change_percent",
        "dp",
        "percent_change",
        "ChangePercent",
        "pctChange",
        "changePercent",
    )

    name = _pick_str(
        root,
        "name",
        "company",
        "company_name",
        "CompanyName",
        "shortName",
        "longName",
        "securityName",
        "issuerName",
    )
    if name:
        patch["name"] = name

    sector = _pick_str(root, "sector", "Sector", "sectorName")
    if sector:
        patch["sector"] = sector

    industry = _pick_str(root, "industry", "Industry", "industryName")
    if industry:
        patch["industry"] = industry

    sub_sector = _pick_str(root, "sub_sector", "subSector", "SubSector", "subSectorName", "subIndustry")
    if sub_sector:
        patch["sub_sector"] = sub_sector

    return _clean_patch(patch)


# -----------------------------------------------------------------------------
# Fetchers (PATCH + ERR)
# -----------------------------------------------------------------------------
async def _fetch_quote_patch(symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    sym_in = (symbol or "").strip().upper()
    if not sym_in:
        return {}, "argaam: empty symbol"

    # KSA-only, silently skip globals
    sym = _normalize_ksa_symbol(sym_in)
    if not sym:
        return {}, None

    # Silent if not configured
    tpl = _safe_str(os.getenv("ARGAAM_QUOTE_URL", ""))
    if not tpl:
        return {}, None

    _ensure_cache_ttl_current()

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

    if _safe_float(mapped.get("current_price")) is None:
        return {}, "argaam quote returned no price"

    patch = dict(mapped)
    _CACHE[ck] = dict(patch)
    return patch, None


async def _fetch_profile_patch(symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    sym_in = (symbol or "").strip().upper()
    if not sym_in:
        return {}, "argaam: empty symbol"

    # KSA-only, silently skip globals
    sym = _normalize_ksa_symbol(sym_in)
    if not sym:
        return {}, None

    # Silent if not configured
    tpl = _safe_str(os.getenv("ARGAAM_PROFILE_URL", ""))
    if not tpl:
        return {}, None

    _ensure_cache_ttl_current()

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
    for k in ("name", "sector", "industry", "sub_sector"):
        v = mapped.get(k)
        if isinstance(v, str) and v.strip():
            identity[k] = v

    if not identity:
        return {}, "argaam profile had no identity fields"

    patch = dict(identity)
    _CACHE[ck] = dict(patch)
    return patch, None


# -----------------------------------------------------------------------------
# Engine-compatible exported callables
# -----------------------------------------------------------------------------
async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Tuple[Dict[str, Any], Optional[str]]:
    return await _fetch_quote_patch(symbol)


async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Quote first; if quote exists, merge profile identity into it (fill blanks only).
    If both empty and not configured -> silent {} + None.
    """
    q_patch, q_err = await _fetch_quote_patch(symbol)

    # If we have quote, try profile for identity and merge into quote (best-effort)
    if q_patch:
        p_patch, p_err = await _fetch_profile_patch(symbol)
        if p_patch:
            for k, v in p_patch.items():
                if k not in q_patch and v is not None:
                    q_patch[k] = v
        return _clean_patch(q_patch), (q_err or p_err)

    # No quote -> try profile
    p_patch, p_err = await _fetch_profile_patch(symbol)
    if p_patch:
        return p_patch, p_err

    # Silent if not configured at all
    if not _configured():
        return {}, None

    # If configured but failed, return best available error
    return {}, (q_err or p_err or "argaam: unavailable")


async def fetch_quote_and_enrichment_patch(symbol: str, *args: Any, **kwargs: Any) -> Tuple[Dict[str, Any], Optional[str]]:
    return await fetch_enriched_quote_patch(symbol)


__all__ = [
    "fetch_quote_patch",
    "fetch_enriched_quote_patch",
    "fetch_quote_and_enrichment_patch",
    "aclose_argaam_client",
    "PROVIDER_VERSION",
    "PROVIDER_NAME",
]
