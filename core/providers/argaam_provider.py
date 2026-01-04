# core/providers/argaam_provider.py  (FULL REPLACEMENT)
"""
core/providers/argaam_provider.py
===============================================================
Argaam Provider (KSA optional enrichment) — v1.6.0
(PROD SAFE + ENGINE PATCH-STYLE + SILENT WHEN NOT CONFIGURED)

What’s improved in v1.6.0
- ✅ Engine-aligned aliases added (older engines call different names):
    • fetch_enriched_patch
    • fetch_quote_and_fundamentals_patch (alias; Argaam has no true fundamentals)
- ✅ Profile fallback for quote:
    If ARGAAM_QUOTE_URL is missing but ARGAAM_PROFILE_URL exists, we can still try
    the profile endpoint to extract quote-like fields (when payload includes them).
- ✅ Better symbol normalization (accepts: "1234", "1234.SR", "TADAWUL:1234", ".TADAWUL")
- ✅ Richer mapping (adds: week_52_high/low if present, value_traded if present)
- ✅ Optional hard-disable via ARGAAM_ENABLED=false
- ✅ Still silent when not configured:
    If ARGAAM_QUOTE_URL and ARGAAM_PROFILE_URL are both missing OR ARGAAM_ENABLED=false,
    returns empty patch with no error (prevents noisy warnings in DataEngineV2).

Key guarantees (unchanged)
- Import-safe even if cachetools is missing (tiny TTL cache fallback).
- PATCH functions return (patch, err) tuples.
- Patch is CLEAN: no "status"/"error" keys inside patch; no meta keys that can overwrite canonical output.
- Strict KSA-only: non-KSA symbols are silently ignored.
- No network calls at import-time; AsyncClient is created lazily.
- Enriched call merges profile identity into quote patch (fills blanks only).

Supported env vars (optional)
- ARGAAM_ENABLED               default true (set to false to disable silently)
- ARGAAM_QUOTE_URL             e.g. https://.../quote?symbol={symbol}  or .../{code}
- ARGAAM_PROFILE_URL           e.g. https://.../profile?symbol={symbol}
- ARGAAM_HEADERS_JSON          JSON dict for headers (optional)
- ARGAAM_TIMEOUT_SEC           default: fallback to HTTP_TIMEOUT_SEC / HTTP_TIMEOUT then 20
- ARGAAM_RETRY_ATTEMPTS        default: 2 (min 1)
- ARGAAM_TTL_SEC               default: 15 (min 5)

Exports
- fetch_quote_patch(symbol) -> (patch: dict, err: str|None)
- fetch_enriched_quote_patch(symbol) -> (patch, err)
- fetch_quote_and_enrichment_patch(symbol) -> (patch, err)
- fetch_enriched_patch(symbol) -> (patch, err)                     # alias
- fetch_quote_and_fundamentals_patch(symbol) -> (patch, err)        # alias
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
PROVIDER_VERSION = "1.6.0"

DEFAULT_TIMEOUT_SEC = 20.0
DEFAULT_RETRY_ATTEMPTS = 2

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

# KSA code: 3-6 digits
_KSA_CODE_RE = re.compile(r"^\d{3,6}$", re.IGNORECASE)
_KSA_SYMBOL_RE = re.compile(r"^\d{3,6}(\.SR)?$", re.IGNORECASE)

_CLIENT: Optional[httpx.AsyncClient] = None
_LOCK = asyncio.Lock()

# -----------------------------------------------------------------------------
# Small helpers
# -----------------------------------------------------------------------------
def _safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s or None


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


def _env_bool(name: str, default: bool = True) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in {"0", "false", "no", "n", "off", "f"}:
        return False
    if raw in {"1", "true", "yes", "y", "on", "t"}:
        return True
    return default


def _ttl_seconds() -> float:
    ttl = _env_float("ARGAAM_TTL_SEC", 15.0)
    if ttl <= 0:
        ttl = 15.0
    return max(5.0, float(ttl))


def _timeout_seconds() -> float:
    # priority: ARGAAM_TIMEOUT_SEC, then HTTP_TIMEOUT_SEC, then HTTP_TIMEOUT
    for key in ("ARGAAM_TIMEOUT_SEC", "HTTP_TIMEOUT_SEC", "HTTP_TIMEOUT"):
        t = _env_float(key, 0.0)
        if t and t > 0:
            return float(t)
    return DEFAULT_TIMEOUT_SEC


def _configured() -> bool:
    if not _env_bool("ARGAAM_ENABLED", True):
        return False
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
            try:
                self._ttl = max(1.0, float(ttl))
            except Exception:
                pass


_CACHE_MAXSIZE = 6000
_CACHE_TTL = _ttl_seconds()
_CACHE: _TTLCache = _TTLCache(maxsize=_CACHE_MAXSIZE, ttl=_CACHE_TTL)  # type: ignore


def _ensure_cache_ttl_current() -> None:
    """
    If ARGAAM_TTL_SEC changes during runtime, rebuild cache to avoid stale TTL behavior.
    """
    global _CACHE_TTL, _CACHE
    ttl_now = _ttl_seconds()
    if abs(ttl_now - _CACHE_TTL) < 0.0001:
        return
    _CACHE_TTL = ttl_now
    try:
        _CACHE = _TTLCache(maxsize=_CACHE_MAXSIZE, ttl=_CACHE_TTL)  # type: ignore
    except Exception:
        try:
            _CACHE._set_ttl(_CACHE_TTL)  # type: ignore
        except Exception:
            pass


# -----------------------------------------------------------------------------
# Symbol helpers (KSA strict)
# -----------------------------------------------------------------------------
def _normalize_ksa_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        return ""

    # common wrappers
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()

    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", "").strip()

    # normalize ".SR"
    if s.endswith(".SR"):
        code = s[:-3].strip()
        return f"{code}.SR" if _KSA_CODE_RE.match(code) else ""

    # raw digits
    if _KSA_CODE_RE.match(s):
        return f"{s}.SR"

    return ""


def _is_ksa_symbol(symbol: str) -> bool:
    return bool(_normalize_ksa_symbol(symbol))


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
        s = s.replace("−", "-")  # unicode minus
        s = s.replace("%", "").replace(",", "").replace("+", "").strip()

        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()

        # K/M/B suffix
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
    # If API returns 0.0123 for 1.23% treat as fraction
    return v * 100.0 if abs(v) <= 1.0 else v


def _format_url(tpl: str, symbol: str) -> str:
    """
    Supports templates with {symbol} and/or {code}.
    symbol must be normalized "####.SR".
    """
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
def _find_first_value(obj: Any, keys: Sequence[str], *, max_depth: int = 7, max_nodes: int = 3000) -> Any:
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
    Unwraps a few common wrappers like {"data": {...}} / {"result": {...}}.
    Bounded (prevents loops).
    """
    cur: Any = js
    for _ in range(3):
        if isinstance(cur, dict):
            for k in ("data", "result", "payload", "quote", "profile", "response"):
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
    if isinstance(data, list):
        # return first dict-like item
        for it in data:
            if isinstance(it, dict):
                return it
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
# Mapping (best-effort)
# -----------------------------------------------------------------------------
def _map_payload(root: Any) -> Dict[str, Any]:
    """
    Best-effort mapping for common JSON payload shapes.
    Returns ONLY useful fields (no status/error/meta).
    """
    patch: Dict[str, Any] = {}

    # Quote-ish
    patch["current_price"] = _pick_num(
        root,
        "last",
        "last_price",
        "price",
        "close",
        "c",
        "LastPrice",
        "tradingPrice",
        "regularMarketPrice",
    )
    patch["previous_close"] = _pick_num(root, "previous_close", "prev_close", "pc", "PreviousClose", "prevClose")
    patch["open"] = _pick_num(root, "open", "o", "Open", "openPrice")
    patch["day_high"] = _pick_num(root, "high", "day_high", "h", "High", "dayHigh", "sessionHigh")
    patch["day_low"] = _pick_num(root, "low", "day_low", "l", "Low", "dayLow", "sessionLow")
    patch["volume"] = _pick_num(root, "volume", "v", "Volume", "tradedVolume", "qty", "quantity")

    # Optional extras (if present)
    patch["week_52_high"] = _pick_num(root, "week_52_high", "fiftyTwoWeekHigh", "52w_high", "yearHigh")
    patch["week_52_low"] = _pick_num(root, "week_52_low", "fiftyTwoWeekLow", "52w_low", "yearLow")
    patch["value_traded"] = _pick_num(root, "value_traded", "tradedValue", "turnover", "value", "tradeValue")

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

    # Identity-ish (optional)
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

    # KSA currency hint (only if missing elsewhere)
    if patch.get("currency") is None:
        patch["currency"] = "SAR"

    return _clean_patch(patch)


def _identity_only(mapped: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k in ("name", "sector", "industry", "sub_sector"):
        v = mapped.get(k)
        if isinstance(v, str) and v.strip():
            out[k] = v
    return out


# -----------------------------------------------------------------------------
# Fetchers (PATCH + ERR)
# -----------------------------------------------------------------------------
async def _fetch_quote_patch(symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Primary quote patch. If quote URL missing, optionally falls back to profile URL.
    """
    if not _configured():
        return {}, None

    sym_in = (symbol or "").strip().upper()
    if not sym_in:
        return {}, "argaam: empty symbol"

    sym = _normalize_ksa_symbol(sym_in)
    if not sym:
        return {}, None  # strict KSA-only, silent for globals

    _ensure_cache_ttl_current()
    ck = f"quote::{sym}"
    hit = _CACHE.get(ck)
    if isinstance(hit, dict) and hit:
        return dict(hit), None

    quote_tpl = _safe_str(os.getenv("ARGAAM_QUOTE_URL", ""))
    profile_tpl = _safe_str(os.getenv("ARGAAM_PROFILE_URL", ""))

    # 1) Quote URL path
    if quote_tpl:
        url = _format_url(quote_tpl, sym)
        js, err = await _http_get_json(url)
        if js is None:
            return {}, f"argaam quote failed: {err}"

        root = _coerce_dict(js)
        mapped = _map_payload(root)

        if _safe_float(mapped.get("current_price")) is None:
            return {}, "argaam quote returned no price"

        patch = dict(mapped)
        _CACHE[ck] = dict(patch)
        return patch, None

    # 2) Fallback: profile URL (if it happens to contain quote fields)
    if profile_tpl:
        url = _format_url(profile_tpl, sym)
        js, err = await _http_get_json(url)
        if js is None:
            return {}, f"argaam profile-as-quote failed: {err}"

        root = _coerce_dict(js)
        mapped = _map_payload(root)

        if _safe_float(mapped.get("current_price")) is None:
            return {}, "argaam profile-as-quote returned no price"

        patch = dict(mapped)
        _CACHE[ck] = dict(patch)
        return patch, "argaam: used profile endpoint as quote fallback"

    # No URLs (should not happen because _configured() checked, but keep safe)
    return {}, None


async def _fetch_profile_identity_patch(symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Identity patch (name/sector/industry/sub_sector) from profile endpoint.
    Silent when missing profile URL.
    """
    if not _configured():
        return {}, None

    sym_in = (symbol or "").strip().upper()
    if not sym_in:
        return {}, "argaam: empty symbol"

    sym = _normalize_ksa_symbol(sym_in)
    if not sym:
        return {}, None

    profile_tpl = _safe_str(os.getenv("ARGAAM_PROFILE_URL", ""))
    if not profile_tpl:
        return {}, None

    _ensure_cache_ttl_current()
    ck = f"profile::{sym}"
    hit = _CACHE.get(ck)
    if isinstance(hit, dict) and hit:
        return dict(hit), None

    url = _format_url(profile_tpl, sym)
    js, err = await _http_get_json(url)
    if js is None:
        return {}, f"argaam profile failed: {err}"

    root = _coerce_dict(js)
    mapped = _map_payload(root)
    identity = _identity_only(mapped)

    if not identity:
        return {}, "argaam profile had no identity fields"

    _CACHE[ck] = dict(identity)
    return identity, None


# -----------------------------------------------------------------------------
# Engine-compatible exported callables (PATCH + ERR tuple)
# -----------------------------------------------------------------------------
async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Tuple[Dict[str, Any], Optional[str]]:
    return await _fetch_quote_patch(symbol)


async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Quote first; if quote exists, merge profile identity into it (fill blanks only).
    If both empty and not configured -> silent {} + None.
    """
    if not _configured():
        return {}, None

    q_patch, q_err = await _fetch_quote_patch(symbol)

    if q_patch:
        p_patch, p_err = await _fetch_profile_identity_patch(symbol)
        if p_patch:
            for k, v in p_patch.items():
                if (k not in q_patch or q_patch.get(k) in (None, "")) and v is not None:
                    q_patch[k] = v
        return _clean_patch(q_patch), (q_err or p_err)

    # No quote -> still try identity (useful for sheets even without price)
    p_patch, p_err = await _fetch_profile_identity_patch(symbol)
    if p_patch:
        return p_patch, p_err

    # Configured but nothing returned
    return {}, (q_err or p_err or "argaam: unavailable")


async def fetch_quote_and_enrichment_patch(symbol: str, *args: Any, **kwargs: Any) -> Tuple[Dict[str, Any], Optional[str]]:
    return await fetch_enriched_quote_patch(symbol)


# Extra aliases for compatibility with engines that probe these names
async def fetch_enriched_patch(symbol: str, *args: Any, **kwargs: Any) -> Tuple[Dict[str, Any], Optional[str]]:
    return await fetch_enriched_quote_patch(symbol)


async def fetch_quote_and_fundamentals_patch(symbol: str, *args: Any, **kwargs: Any) -> Tuple[Dict[str, Any], Optional[str]]:
    # Argaam doesn’t provide true fundamentals here; alias to enriched quote patch
    return await fetch_enriched_quote_patch(symbol)


__all__ = [
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    "fetch_quote_patch",
    "fetch_enriched_quote_patch",
    "fetch_quote_and_enrichment_patch",
    "fetch_enriched_patch",
    "fetch_quote_and_fundamentals_patch",
    "aclose_argaam_client",
]
