#!/usr/bin/env python3
"""
core/providers/argaam_provider.py
===============================================================
Argaam Provider (KSA Market Data) — v4.4.0
PROFILE + CLASSIFICATION + HISTORY FALLBACK + ROUTER/ENGINE COMPAT

What this revision improves
---------------------------
- Keeps env-driven URL templates (no hard-coded endpoints)
- Uses profile + quote + optional history together
- Adds stronger KSA classification/profile mapping:
    - exchange / market / country / currency / asset_class
    - sector / industry / name
- Adds history-derived fallback for:
    - current_price / previous_close / open_price / day_high / day_low / volume
    - avg_volume_10d / avg_volume_30d
    - week_52_high / week_52_low / week_52_position_pct
- Returns engine-friendly patches with explicit error payloads on failure
- Broadens compatibility methods for routers / engines / legacy callers

Required env templates
----------------------
- ARGAAM_QUOTE_URL
- ARGAAM_PROFILE_URL   (optional but recommended)
- ARGAAM_HISTORY_URL   (optional but recommended)

Notes
-----
- Templates may use {symbol}; both "2222" and "2222.SR" are tried.
- This provider is best used as KSA profile/classification enrichment over the
  Tadawul price/history path, but it now also contributes meaningful fallback
  history fields when Argaam history is configured.
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
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

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
PROVIDER_VERSION = "4.4.0"
VERSION = PROVIDER_VERSION

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DEFAULT_TIMEOUT_SEC = float(os.getenv("ARGAAM_TIMEOUT_SEC", "20") or "20")
DEFAULT_RETRY_ATTEMPTS = int(os.getenv("ARGAAM_RETRY_ATTEMPTS", "3") or "3")
DEFAULT_MAX_CONCURRENCY = int(os.getenv("ARGAAM_MAX_CONCURRENCY", "10") or "10")
DEFAULT_CACHE_TTL_SEC = float(os.getenv("ARGAAM_CACHE_TTL_SEC", "20") or "20")
DEFAULT_HISTORY_TTL_SEC = float(os.getenv("ARGAAM_HISTORY_CACHE_TTL_SEC", "120") or "120")

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled"}
_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_KSA_CODE_RE = re.compile(r"^\d{3,6}$")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

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

        m = re.match(r"^(-?\d+(?:\.\d+)?)([KMB])$", s, re.IGNORECASE)
        mult = 1.0
        if m:
            num = m.group(1)
            suf = m.group(2).upper()
            mult = 1_000.0 if suf == "K" else 1_000_000.0 if suf == "M" else 1_000_000_000.0
            s = num

        f = float(s) * mult
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def _safe_str(val: Any) -> Optional[str]:
    if val is None:
        return None
    try:
        s = str(val).strip()
        return s or None
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
    s = (symbol_norm or "").strip().upper()
    return s[:-3] if s.endswith(".SR") else s


def _unwrap_common_envelopes(data: Any) -> Any:
    cur = data
    for _ in range(5):
        if isinstance(cur, dict):
            for k in ("data", "result", "payload", "response", "items", "results", "quote", "quotes"):
                if k in cur and isinstance(cur[k], (dict, list)):
                    cur = cur[k]
                    break
            else:
                break
        else:
            break
    return cur


def _iter_dict_candidates(obj: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            if isinstance(v, dict):
                yield v
            elif isinstance(v, list):
                for item in v:
                    if isinstance(item, dict):
                        yield item
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, dict):
                yield item


def _first_existing(d: Dict[str, Any], keys: List[str]) -> Any:
    for k in keys:
        if k in d and d[k] is not None and d[k] != "":
            return d[k]
    return None


def _find_value_any(data: Any, keys: List[str]) -> Any:
    keyset = {k.lower() for k in keys}
    for cand in _iter_dict_candidates(data):
        for k, v in cand.items():
            if str(k).strip().lower() in keyset and v not in (None, ""):
                return v
    return None


def _normalize_patch_keys(patch: Dict[str, Any]) -> Dict[str, Any]:
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
        "open": "open_price",
        "day_open": "open_price",
        "high": "day_high",
        "low": "day_low",
        "fifty_two_week_high": "week_52_high",
        "fifty_two_week_low": "week_52_low",
        "avg_vol_10d": "avg_volume_10d",
        "avg_vol_30d": "avg_volume_30d",
        "company_name": "name",
    }
    out = dict(patch or {})
    for src, dst in aliases.items():
        if src in out and (dst not in out or out.get(dst) in (None, "", 0)):
            out[dst] = out.get(src)
    if out.get("current_price") is not None and out.get("price") in (None, "", 0):
        out["price"] = out.get("current_price")
    if out.get("price") is not None and out.get("current_price") in (None, "", 0):
        out["current_price"] = out.get("price")
    if out.get("country") in (None, ""):
        out["country"] = "Saudi Arabia"
    if out.get("currency") in (None, ""):
        out["currency"] = "SAR"
    if out.get("exchange") in (None, ""):
        out["exchange"] = "TADAWUL"
    if out.get("market") in (None, ""):
        out["market"] = "KSA"
    return out


def _week_52_position_pct(price: Optional[float], lo: Optional[float], hi: Optional[float]) -> Optional[float]:
    try:
        if price is None or lo is None or hi is None:
            return None
        if hi <= lo:
            return None
        return round(((float(price) - float(lo)) / (float(hi) - float(lo))) * 100.0, 4)
    except Exception:
        return None


def _error_patch(symbol: str, message: str, *, requested_symbol: Optional[str] = None) -> Dict[str, Any]:
    return {
        "symbol": normalize_ksa_symbol(symbol) or symbol,
        "requested_symbol": requested_symbol or symbol,
        "provider": PROVIDER_NAME,
        "provider_version": PROVIDER_VERSION,
        "data_quality": "MISSING",
        "error": message,
        "last_updated_utc": _now_utc_iso(),
        "country": "Saudi Arabia",
        "currency": "SAR",
        "exchange": "TADAWUL",
        "market": "KSA",
    }


def _as_history_rows(history_data: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    raw = _unwrap_common_envelopes(history_data)
    if isinstance(raw, dict):
        for k in ("history", "historical", "candles", "rows", "prices", "series"):
            if isinstance(raw.get(k), list):
                raw = raw[k]
                break
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                rows.append(item)
    elif isinstance(raw, dict):
        rows.append(raw)
    return rows


def _history_stats(history_data: Any) -> Dict[str, Any]:
    rows = _as_history_rows(history_data)
    if not rows:
        return {}

    closes: List[float] = []
    highs: List[float] = []
    lows: List[float] = []
    opens: List[float] = []
    vols: List[float] = []

    for r in rows:
        c = _safe_float(_first_existing(r, ["close", "Close", "c", "last", "price", "value"]))
        h = _safe_float(_first_existing(r, ["high", "High", "h"]))
        l = _safe_float(_first_existing(r, ["low", "Low", "l"]))
        o = _safe_float(_first_existing(r, ["open", "Open", "o"]))
        v = _safe_float(_first_existing(r, ["volume", "Volume", "v", "qty", "tradedVolume"]))
        if c is not None:
            closes.append(c)
        if h is not None:
            highs.append(h)
        if l is not None:
            lows.append(l)
        if o is not None:
            opens.append(o)
        if v is not None and v >= 0:
            vols.append(v)

    out: Dict[str, Any] = {}
    if closes:
        out["current_price"] = closes[-1]
        if len(closes) >= 2:
            out["previous_close"] = closes[-2]
    if opens:
        out["open_price"] = opens[-1]
    if highs:
        out["day_high"] = highs[-1]
        out["week_52_high"] = max(highs[-252:] if len(highs) >= 252 else highs)
    if lows:
        out["day_low"] = lows[-1]
        out["week_52_low"] = min(lows[-252:] if len(lows) >= 252 else lows)
    if vols:
        out["volume"] = vols[-1]
        last10 = vols[-10:] if len(vols) >= 10 else vols
        last30 = vols[-30:] if len(vols) >= 30 else vols
        out["avg_volume_10d"] = sum(last10) / len(last10) if last10 else None
        out["avg_volume_30d"] = sum(last30) / len(last30) if last30 else None

    out["week_52_position_pct"] = _week_52_position_pct(
        out.get("current_price"),
        out.get("week_52_low"),
        out.get("week_52_high"),
    )
    return {k: v for k, v in out.items() if v is not None}

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

    def _has_any_config(self) -> bool:
        return bool(self.quote_url or self.profile_url or self.history_url)

    def _build_url_candidates(self, template: str, symbol_norm: str) -> List[str]:
        if not template:
            return []
        code = _ksa_code(symbol_norm)
        full = symbol_norm
        urls: List[str] = []
        if "{symbol}" in template:
            urls.append(template.replace("{symbol}", code))
            if full != code:
                urls.append(template.replace("{symbol}", full))
        else:
            urls.append(template.rstrip("/") + "/" + code)
            if full != code:
                urls.append(template.rstrip("/") + "/" + full)
        seen = set()
        out = []
        for u in urls:
            if u and u not in seen:
                seen.add(u)
                out.append(u)
        return out

    async def _get_json(self, url: str, cache_key: str, ttl: float) -> Tuple[Optional[Any], Optional[str]]:
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

    async def _fetch_first(self, urls: List[str], prefix: str, ttl: float) -> Tuple[Optional[Any], Optional[str], Optional[str]]:
        data = None
        err = None
        used = None
        for u in urls:
            data, err = await self._get_json(u, f"{prefix}:{u}", ttl=ttl)
            if data is not None:
                used = u
                break
        return data, err, used

    def _parse_profile_patch(self, symbol_norm: str, profile_data: Any) -> Dict[str, Any]:
        p = profile_data if isinstance(profile_data, dict) else {}
        out: Dict[str, Any] = {
            "symbol": symbol_norm,
            "symbol_normalized": symbol_norm,
            "provider": PROVIDER_NAME,
            "provider_version": PROVIDER_VERSION,
            "exchange": "TADAWUL",
            "market": "KSA",
            "country": "Saudi Arabia",
            "currency": "SAR",
            "asset_class": "Equity",
            "last_updated_utc": _now_utc_iso(),
        }
        out["name"] = _safe_str(_find_value_any(p, ["name", "companyname", "company_name", "longname", "securityname", "displayname"]))
        out["sector"] = _safe_str(_find_value_any(p, ["sector", "sectorname"]))
        out["industry"] = _safe_str(_find_value_any(p, ["industry", "industryname", "subsector", "subsectorname"]))
        out["currency"] = _safe_str(_find_value_any(p, ["currency"])) or "SAR"
        out["country"] = _safe_str(_find_value_any(p, ["country"])) or "Saudi Arabia"
        out["exchange"] = _safe_str(_find_value_any(p, ["exchange", "marketname"])) or "TADAWUL"
        out["asset_class"] = _safe_str(_find_value_any(p, ["assetclass", "asset_class", "type", "instrumenttype", "securitytype"])) or "Equity"
        return {k: v for k, v in out.items() if v not in (None, "")}

    def _parse_quote_patch(self, symbol_norm: str, quote_data: Any, profile_data: Any = None, history_data: Any = None) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "symbol": symbol_norm,
            "symbol_normalized": symbol_norm,
            "provider": PROVIDER_NAME,
            "provider_version": PROVIDER_VERSION,
            "last_updated_utc": _now_utc_iso(),
            "exchange": "TADAWUL",
            "market": "KSA",
            "country": "Saudi Arabia",
            "currency": "SAR",
            "asset_class": "Equity",
        }

        q = quote_data if isinstance(quote_data, dict) else {}
        p = profile_data if isinstance(profile_data, dict) else {}

        # profile / classification
        out.update(self._parse_profile_patch(symbol_norm, p))

        # common fields from quote
        out["name"] = out.get("name") or _safe_str(_find_value_any(q, ["name", "companyname", "company_name", "longname"]))
        out["sector"] = out.get("sector") or _safe_str(_find_value_any(q, ["sector"]))
        out["industry"] = out.get("industry") or _safe_str(_find_value_any(q, ["industry"]))

        price_raw = _find_value_any(q, ["current_price", "price", "last", "lastprice", "closeprice", "tradeprice", "lasttradepriceonly"])
        prev_raw = _find_value_any(q, ["previous_close", "prev_close", "previousclose", "close", "yesterdayclose"])
        open_raw = _find_value_any(q, ["open_price", "open", "openingprice", "dayopen"])
        high_raw = _find_value_any(q, ["day_high", "high", "highprice", "sessionhigh"])
        low_raw = _find_value_any(q, ["day_low", "low", "lowprice", "sessionlow"])
        vol_raw = _find_value_any(q, ["volume", "vol", "tradedvolume", "quantity", "qty"])
        ch_raw = _find_value_any(q, ["price_change", "change", "chg"])
        pct_raw = _find_value_any(q, ["percent_change", "change_percent", "changepercent", "changepct", "pct_change"])

        out["current_price"] = _safe_float(price_raw)
        out["price"] = out.get("current_price")
        out["previous_close"] = _safe_float(prev_raw)
        out["open_price"] = _safe_float(open_raw)
        out["day_high"] = _safe_float(high_raw)
        out["day_low"] = _safe_float(low_raw)
        out["volume"] = _safe_float(vol_raw)
        out["price_change"] = _safe_float(ch_raw)
        out["percent_change"] = _safe_float(pct_raw)

        out["market_cap"] = _safe_float(_find_value_any(q, ["marketcap", "market_cap"])) or _safe_float(_find_value_any(p, ["marketcap", "market_cap"]))
        out["float_shares"] = _safe_float(_find_value_any(q, ["floatshares", "float_shares"])) or _safe_float(_find_value_any(p, ["floatshares", "float_shares"]))

        # optional direct 52W and liquidity
        out["week_52_high"] = _safe_float(_find_value_any(q, ["week_52_high", "52weekhigh", "yearhigh", "fiftytwoweekhigh"]))
        out["week_52_low"] = _safe_float(_find_value_any(q, ["week_52_low", "52weeklow", "yearlow", "fiftytwoweeklow"]))
        out["avg_volume_10d"] = _safe_float(_find_value_any(q, ["avg_volume_10d", "averagevolume10days", "avgvol10d"]))
        out["avg_volume_30d"] = _safe_float(_find_value_any(q, ["avg_volume_30d", "averagevolume", "avgvolume", "avgvol30d"]))

        hist_patch = _history_stats(history_data) if history_data is not None else {}
        for k, v in hist_patch.items():
            if out.get(k) in (None, "", 0):
                out[k] = v

        if out.get("week_52_position_pct") is None:
            out["week_52_position_pct"] = _week_52_position_pct(
                out.get("current_price"),
                out.get("week_52_low"),
                out.get("week_52_high"),
            )

        if out.get("price_change") is None and out.get("current_price") is not None and out.get("previous_close") is not None:
            out["price_change"] = float(out["current_price"]) - float(out["previous_close"])
        if out.get("percent_change") is None and out.get("price_change") is not None and out.get("previous_close") not in (None, 0):
            try:
                out["percent_change"] = (float(out["price_change"]) / float(out["previous_close"])) * 100.0
            except Exception:
                pass

        out["data_quality"] = "GOOD" if out.get("current_price") is not None else "MISSING"
        return _normalize_patch_keys({k: v for k, v in out.items() if v is not None})

    async def get_enriched_patch(self, symbol: str) -> Dict[str, Any]:
        symbol_norm = normalize_ksa_symbol(symbol)
        if not symbol_norm:
            return _error_patch(symbol, "invalid_ksa_symbol")

        if not self._has_any_config():
            return _error_patch(symbol_norm, "ARGAAM provider URLs not configured (set env vars)")

        quote_urls = self._build_url_candidates(self.quote_url, symbol_norm) if self.quote_url else []
        prof_urls = self._build_url_candidates(self.profile_url, symbol_norm) if self.profile_url else []
        hist_urls = self._build_url_candidates(self.history_url, symbol_norm) if self.history_url else []

        quote_data, quote_err, quote_used = (None, None, None)
        if quote_urls:
            quote_data, quote_err, quote_used = await self._fetch_first(quote_urls, f"q:{symbol_norm}", DEFAULT_CACHE_TTL_SEC)

        profile_data, _, profile_used = (None, None, None)
        if prof_urls:
            profile_data, _, profile_used = await self._fetch_first(prof_urls, f"p:{symbol_norm}", max(60.0, DEFAULT_CACHE_TTL_SEC))

        history_data, _, history_used = (None, None, None)
        if hist_urls:
            history_data, _, history_used = await self._fetch_first(hist_urls, f"h:{symbol_norm}", DEFAULT_HISTORY_TTL_SEC)

        if quote_data is None and profile_data is None and history_data is None:
            return _error_patch(symbol_norm, f"fetch_failed: {quote_err or 'no_data'}", requested_symbol=symbol)

        patch = self._parse_quote_patch(symbol_norm, quote_data, profile_data, history_data)
        patch["requested_symbol"] = symbol
        patch["data_sources"] = [PROVIDER_NAME]
        patch["provider_origin"] = {
            "quote_url_used": quote_used,
            "profile_url_used": profile_used,
            "history_url_used": history_used,
        }
        if quote_data is None:
            patch["warning"] = (patch.get("warning") or "quote_missing_used_profile_and_or_history_fallback")
            if patch.get("data_quality") == "GOOD":
                patch["data_quality"] = "DEGRADED"
        return patch

    async def close(self) -> None:
        try:
            if self._client:
                await self._client.aclose()
        except Exception:
            pass

# ---------------------------------------------------------------------------
# Provider object for router compatibility
# ---------------------------------------------------------------------------

class ArgaamProvider:
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

    async def fetch_quote_patch(self, symbol: str) -> Dict[str, Any]:
        return await self.get_quote(symbol)

    async def fetch_patch(self, symbol: str) -> Dict[str, Any]:
        return await self.get_quote(symbol)

    async def fetch_enriched_quote_patch(self, symbol: str) -> Dict[str, Any]:
        return await self.get_quote(symbol)

    async def fetch_profile(self, symbol: str) -> Dict[str, Any]:
        return await self.get_quote(symbol)

    async def fetch_profile_patch(self, symbol: str) -> Dict[str, Any]:
        return await self.get_quote(symbol)

    async def fetch_history_patch(self, symbol: str) -> Dict[str, Any]:
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


provider = ArgaamProvider()

# ---------------------------------------------------------------------------
# Engine-facing functions
# ---------------------------------------------------------------------------

async def fetch_enriched_quote_patch(symbol: str, *args, **kwargs) -> Dict[str, Any]:
    c = await get_client()
    patch = await c.get_enriched_patch(symbol)
    if not isinstance(patch, dict) or not patch:
        return _error_patch(symbol, "empty_patch")
    return patch


fetch_patch = fetch_enriched_quote_patch
fetch_quote_patch = fetch_enriched_quote_patch
fetch_quote = fetch_enriched_quote_patch
quote = fetch_enriched_quote_patch
get_quote = fetch_enriched_quote_patch
fetch_profile_patch = fetch_enriched_quote_patch
fetch_history_patch = fetch_enriched_quote_patch

__all__ = [
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    "VERSION",
    "normalize_ksa_symbol",
    "fetch_enriched_quote_patch",
    "fetch_patch",
    "fetch_quote_patch",
    "fetch_quote",
    "quote",
    "get_quote",
    "fetch_profile_patch",
    "fetch_history_patch",
    "get_client",
    "close_client",
    "ArgaamProvider",
    "get_provider",
    "provider",
]
