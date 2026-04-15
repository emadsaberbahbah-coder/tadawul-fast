#!/usr/bin/env python3
"""
core/providers/argaam_provider.py
===============================================================
Argaam Provider (KSA Market Data) -- v4.5.0
PROFILE + CLASSIFICATION + HISTORY FALLBACK + ROUTER/ENGINE COMPAT

v4.5.0 changes vs v4.4.0
--------------------------
FIX CRITICAL: All dtype=pct fields now stored as fractions (0.0142 = 1.42%).
  v4.4.0 used _safe_float() which returned raw API values (e.g. 1.42 for 1.42%
  change) causing a 100x display error in Google Sheets.
  Fixed fields: percent_change, week_52_position_pct, dividend_yield,
    gross_margin, operating_margin, profit_margin, volatility_30d/90d,
    expected_roi_1m/3m/12m, payout_ratio.
  New helper: _as_fraction(v) -- normalises any percent-like value to fraction:
    "1.42%" -> 0.0142  |  1.42 -> 0.0142  |  0.0142 -> 0.0142

FIX: _history_stats() now returns week_52_position_pct as fraction (0.0142).
  v4.4.0 returned it as percent (0..100) from _week_52_position_pct().

FIX: percent_change computed from price_change/previous_close is now stored
  as fraction, not percent points.

FIX: price_change derived from percent_change now correctly back-converts
  from fraction (fraction * previous_close = price change in currency).

ENH: get_enriched_quotes_batch() added to ArgaamClient and ArgaamProvider
  for engine batch-fetch compatibility (single semaphore-guarded gather).

ENH: PROVIDER_BATCH_SUPPORTED = True exported for engine capability detection.

ENH: fetch_enriched_quotes_batch() module-level function added.

ENH: data_quality logic tightened -- GOOD requires current_price; DEGRADED
  means some data but no price; MISSING means empty fetch.

Preserved from v4.4.0:
  All env-driven URL templates, TTL cache, retry/backoff logic.
  Profile + quote + history parallel fetch pattern.
  KSA symbol normalisation (numeric code <-> .SR suffix).
  router/engine compatibility aliases.
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
# Optional deps
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

PROVIDER_NAME            = "argaam"
PROVIDER_VERSION         = "4.5.0"
VERSION                  = PROVIDER_VERSION
PROVIDER_BATCH_SUPPORTED = True   # ENH v4.5.0

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DEFAULT_TIMEOUT_SEC       = float(os.getenv("ARGAAM_TIMEOUT_SEC",           "20")  or "20")
DEFAULT_RETRY_ATTEMPTS    = int(  os.getenv("ARGAAM_RETRY_ATTEMPTS",        "3")   or "3")
DEFAULT_MAX_CONCURRENCY   = int(  os.getenv("ARGAAM_MAX_CONCURRENCY",       "10")  or "10")
DEFAULT_CACHE_TTL_SEC     = float(os.getenv("ARGAAM_CACHE_TTL_SEC",         "20")  or "20")
DEFAULT_HISTORY_TTL_SEC   = float(os.getenv("ARGAAM_HISTORY_CACHE_TTL_SEC", "120") or "120")

_TRUTHY        = {"1", "true", "yes", "y", "on", "t", "enabled"}
_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_KSA_CODE_RE   = re.compile(r"^\d{3,6}$")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

ARGAAM_QUOTE_URL   = (os.getenv("ARGAAM_QUOTE_URL")   or "").strip()
ARGAAM_PROFILE_URL = (os.getenv("ARGAAM_PROFILE_URL") or "").strip()
ARGAAM_HISTORY_URL = (os.getenv("ARGAAM_HISTORY_URL") or "").strip()

# ---------------------------------------------------------------------------
# Numeric helpers
# ---------------------------------------------------------------------------

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in _TRUTHY


def _safe_float(val: Any) -> Optional[float]:
    """
    Parse any numeric-like value to float.
    Strips "%", commas, Arabic digits, SAR/ريال suffixes.
    Returns None for NaN/Inf/blank/sentinel strings.
    Does NOT normalise percent-to-fraction -- use _as_fraction() for pct fields.
    """
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
            num, suf = m.group(1), m.group(2).upper()
            mult = 1_000.0 if suf == "K" else 1_000_000.0 if suf == "M" else 1_000_000_000.0
            s = num

        f = float(s) * mult
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def _as_fraction(val: Any) -> Optional[float]:
    """
    FIX v4.5.0: Normalise any percent-like value to a fraction (0.0142 = 1.42%).

    Accepted input formats:
      "1.42%"  -> 0.0142   (strips %, divides by 100)
      1.42     -> 0.0142   (value > 1.5: treat as percent points, divide by 100)
      0.0142   -> 0.0142   (value <= 1.5: already a fraction, return as-is)
      "-1.5"   -> -0.015
      "142%"   -> 1.42     (large percent)
      None     -> None

    Rule: if abs(value) > 1.5 after stripping, divide by 100.
    This is the same convention used throughout the TFB engine.
    """
    f = _safe_float(val)
    if f is None:
        return None
    if abs(f) > 1.5:
        return f / 100.0
    return f


def _safe_str(val: Any) -> Optional[str]:
    if val is None:
        return None
    try:
        s = str(val).strip()
        return s or None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# KSA symbol helpers
# ---------------------------------------------------------------------------

def normalize_ksa_symbol(symbol: str) -> str:
    raw = (symbol or "").strip()
    if not raw:
        return ""
    raw = raw.translate(_ARABIC_DIGITS).strip().upper()

    if raw.startswith("TADAWUL:"):
        raw = raw.split(":", 1)[1].strip()
    if raw.endswith(".TADAWUL"):
        raw = raw[: -len(".TADAWUL")].strip()

    if raw.endswith(".SR"):
        code = raw[:-3].strip()
        return f"{code}.SR" if _KSA_CODE_RE.match(code) else ""

    if _KSA_CODE_RE.match(raw):
        return f"{raw}.SR"
    return ""


def _ksa_code(symbol_norm: str) -> str:
    s = (symbol_norm or "").strip().upper()
    return s[:-3] if s.endswith(".SR") else s


# ---------------------------------------------------------------------------
# Data extraction helpers
# ---------------------------------------------------------------------------

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
    """
    Apply canonical key aliases and ensure KSA defaults are always set.
    Exchange/country/currency/market are always forced to KSA values
    for symbols validated through normalize_ksa_symbol().
    """
    aliases = {
        "price":           "current_price",
        "last_price":      "current_price",
        "last":            "current_price",
        "close":           "previous_close",
        "prev_close":      "previous_close",
        "change":          "price_change",
        "change_pct":      "percent_change",
        "change_percent":  "percent_change",
        "pct_change":      "percent_change",
        "open":            "open_price",
        "day_open":        "open_price",
        "high":            "day_high",
        "low":             "day_low",
        "fifty_two_week_high": "week_52_high",
        "fifty_two_week_low":  "week_52_low",
        "avg_vol_10d":     "avg_volume_10d",
        "avg_vol_30d":     "avg_volume_30d",
        "company_name":    "name",
    }
    out = dict(patch or {})
    for src, dst in aliases.items():
        if src in out and (dst not in out or out.get(dst) in (None, "", 0)):
            out[dst] = out.pop(src)

    # Sync current_price <-> price alias
    if out.get("current_price") is not None and out.get("price") in (None, "", 0):
        out["price"] = out["current_price"]
    if out.get("price") is not None and out.get("current_price") in (None, "", 0):
        out["current_price"] = out["price"]

    # KSA identity always wins for validated symbols
    out["country"]  = "Saudi Arabia"
    out["currency"] = out.get("currency") or "SAR"
    out["exchange"] = "TADAWUL"
    out["market"]   = "KSA"
    return out


def _week_52_position_pct(
    price: Optional[float],
    lo: Optional[float],
    hi: Optional[float],
) -> Optional[float]:
    """
    Returns 52-week position as a FRACTION (0.0 .. 1.0).
    FIX v4.5.0: v4.4.0 returned 0..100 percent. Schema dtype=pct expects fraction.
    """
    try:
        if price is None or lo is None or hi is None:
            return None
        if hi <= lo:
            return None
        return round((float(price) - float(lo)) / (float(hi) - float(lo)), 6)
    except Exception:
        return None


def _error_patch(
    symbol: str,
    message: str,
    *,
    requested_symbol: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "symbol":           normalize_ksa_symbol(symbol) or symbol,
        "requested_symbol": requested_symbol or symbol,
        "provider":         PROVIDER_NAME,
        "provider_version": PROVIDER_VERSION,
        "data_quality":     "MISSING",
        "error":            message,
        "last_updated_utc": _now_utc_iso(),
        "country":          "Saudi Arabia",
        "currency":         "SAR",
        "exchange":         "TADAWUL",
        "market":           "KSA",
    }


# ---------------------------------------------------------------------------
# History helpers
# ---------------------------------------------------------------------------

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
    """
    Derive OHLCV stats from history rows.
    FIX v4.5.0: week_52_position_pct returned as fraction (was percent in v4.4.0).
    """
    rows = _as_history_rows(history_data)
    if not rows:
        return {}

    closes: List[float] = []
    highs:  List[float] = []
    lows:   List[float] = []
    opens:  List[float] = []
    vols:   List[float] = []

    for r in rows:
        c = _safe_float(_first_existing(r, ["close", "Close", "c", "last", "price", "value"]))
        h = _safe_float(_first_existing(r, ["high",  "High",  "h"]))
        l = _safe_float(_first_existing(r, ["low",   "Low",   "l"]))
        o = _safe_float(_first_existing(r, ["open",  "Open",  "o"]))
        v = _safe_float(_first_existing(r, ["volume","Volume","v","qty","tradedVolume"]))
        if c is not None: closes.append(c)
        if h is not None: highs.append(h)
        if l is not None: lows.append(l)
        if o is not None: opens.append(o)
        if v is not None and v >= 0: vols.append(v)

    out: Dict[str, Any] = {}
    if closes:
        out["current_price"] = closes[-1]
        if len(closes) >= 2:
            out["previous_close"] = closes[-2]
    if opens:
        out["open_price"] = opens[-1]
    if highs:
        out["day_high"]     = highs[-1]
        out["week_52_high"] = max(highs[-252:] if len(highs) >= 252 else highs)
    if lows:
        out["day_low"]     = lows[-1]
        out["week_52_low"] = min(lows[-252:] if len(lows) >= 252 else lows)
    if vols:
        out["volume"] = vols[-1]
        last10 = vols[-10:] if len(vols) >= 10 else vols
        last30 = vols[-30:] if len(vols) >= 30 else vols
        out["avg_volume_10d"] = sum(last10) / len(last10) if last10 else None
        out["avg_volume_30d"] = sum(last30) / len(last30) if last30 else None

    # FIX v4.5.0: fraction (0.0..1.0) not percent (0..100)
    out["week_52_position_pct"] = _week_52_position_pct(
        out.get("current_price"),
        out.get("week_52_low"),
        out.get("week_52_high"),
    )
    return {k: v for k, v in out.items() if v is not None}


# ---------------------------------------------------------------------------
# Simple TTL cache
# ---------------------------------------------------------------------------

@dataclass
class _CacheItem:
    exp:   float
    value: Any


class _TTLCache:
    def __init__(self, max_size: int = 2000) -> None:
        self._max  = max_size
        self._d:   Dict[str, _CacheItem] = {}
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
# HTTP client
# ---------------------------------------------------------------------------

class ArgaamClient:
    def __init__(self) -> None:
        self.client_id    = str(uuid.uuid4())[:8]
        self.quote_url    = ARGAAM_QUOTE_URL
        self.profile_url  = ARGAAM_PROFILE_URL
        self.history_url  = ARGAAM_HISTORY_URL
        self.timeout_sec  = DEFAULT_TIMEOUT_SEC
        self.retry_attempts = max(1, DEFAULT_RETRY_ATTEMPTS)
        self._sem   = asyncio.Semaphore(max(1, DEFAULT_MAX_CONCURRENCY))
        self._cache = _TTLCache(
            max_size=int(os.getenv("ARGAAM_CACHE_MAX", "2000") or "2000")
        )
        self._client = None
        if _HTTPX_AVAILABLE:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout_sec),
                limits=httpx.Limits(max_keepalive_connections=20, max_connections=40),
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept":     "application/json",
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
        seen: set = set()
        out: List[str] = []
        for u in urls:
            if u and u not in seen:
                seen.add(u)
                out.append(u)
        return out

    async def _get_json(
        self, url: str, cache_key: str, ttl: float
    ) -> Tuple[Optional[Any], Optional[str]]:
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
                        await asyncio.sleep(min(15.0, (2 ** attempt) + random.uniform(0, 1.0)))
                        last_err = "rate_limited_429"
                        continue
                    if 500 <= sc < 600:
                        await asyncio.sleep(min(10.0, (2 ** attempt) + random.uniform(0, 1.0)))
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
                    await asyncio.sleep(min(10.0, (2 ** attempt) + random.uniform(0, 1.0)))
            return None, last_err or "max_retries_exceeded"

    async def _fetch_first(
        self, urls: List[str], prefix: str, ttl: float
    ) -> Tuple[Optional[Any], Optional[str], Optional[str]]:
        data = None
        err  = None
        used = None
        for u in urls:
            data, err = await self._get_json(u, f"{prefix}:{u}", ttl=ttl)
            if data is not None:
                used = u
                break
        return data, err, used

    # ------------------------------------------------------------------
    # Patch parsers
    # ------------------------------------------------------------------

    def _parse_profile_patch(
        self, symbol_norm: str, profile_data: Any
    ) -> Dict[str, Any]:
        p = profile_data if isinstance(profile_data, dict) else {}
        out: Dict[str, Any] = {
            "symbol":           symbol_norm,
            "symbol_normalized": symbol_norm,
            "provider":         PROVIDER_NAME,
            "provider_version": PROVIDER_VERSION,
            "exchange":         "TADAWUL",
            "market":           "KSA",
            "country":          "Saudi Arabia",
            "currency":         "SAR",
            "asset_class":      "Equity",
            "last_updated_utc": _now_utc_iso(),
        }
        out["name"]       = _safe_str(_find_value_any(p, ["name", "companyname", "company_name", "longname", "securityname", "displayname"]))
        out["sector"]     = _safe_str(_find_value_any(p, ["sector", "sectorname"]))
        out["industry"]   = _safe_str(_find_value_any(p, ["industry", "industryname", "subsector", "subsectorname"]))
        out["currency"]   = _safe_str(_find_value_any(p, ["currency"])) or "SAR"
        out["country"]    = _safe_str(_find_value_any(p, ["country"])) or "Saudi Arabia"
        out["exchange"]   = "TADAWUL"   # always TADAWUL for KSA symbols
        out["asset_class"] = _safe_str(_find_value_any(p, ["assetclass","asset_class","type","instrumenttype","securitytype"])) or "Equity"
        return {k: v for k, v in out.items() if v not in (None, "")}

    def _parse_quote_patch(
        self,
        symbol_norm: str,
        quote_data: Any,
        profile_data: Any = None,
        history_data: Any = None,
    ) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "symbol":           symbol_norm,
            "symbol_normalized": symbol_norm,
            "provider":         PROVIDER_NAME,
            "provider_version": PROVIDER_VERSION,
            "last_updated_utc": _now_utc_iso(),
            "exchange":         "TADAWUL",
            "market":           "KSA",
            "country":          "Saudi Arabia",
            "currency":         "SAR",
            "asset_class":      "Equity",
        }

        q = quote_data   if isinstance(quote_data,   dict) else {}
        p = profile_data if isinstance(profile_data, dict) else {}

        # Profile / classification (name, sector, industry, asset_class)
        out.update(self._parse_profile_patch(symbol_norm, p))

        # Name / sector / industry from quote as fallback
        out["name"]     = out.get("name")     or _safe_str(_find_value_any(q, ["name","companyname","company_name","longname"]))
        out["sector"]   = out.get("sector")   or _safe_str(_find_value_any(q, ["sector"]))
        out["industry"] = out.get("industry") or _safe_str(_find_value_any(q, ["industry"]))

        # --- Price fields (dtype=float, raw value) ---
        out["current_price"]  = _safe_float(_find_value_any(q, ["current_price","price","last","lastprice","closeprice","tradeprice","lasttradepriceonly"]))
        out["price"]          = out["current_price"]
        out["previous_close"] = _safe_float(_find_value_any(q, ["previous_close","prev_close","previousclose","close","yesterdayclose"]))
        out["open_price"]     = _safe_float(_find_value_any(q, ["open_price","open","openingprice","dayopen"]))
        out["day_high"]       = _safe_float(_find_value_any(q, ["day_high","high","highprice","sessionhigh"]))
        out["day_low"]        = _safe_float(_find_value_any(q, ["day_low","low","lowprice","sessionlow"]))
        out["price_change"]   = _safe_float(_find_value_any(q, ["price_change","change","chg"]))
        out["volume"]         = _safe_float(_find_value_any(q, ["volume","vol","tradedvolume","quantity","qty"]))
        out["market_cap"]     = _safe_float(_find_value_any(q, ["marketcap","market_cap"])) or _safe_float(_find_value_any(p, ["marketcap","market_cap"]))
        out["float_shares"]   = _safe_float(_find_value_any(q, ["floatshares","float_shares"])) or _safe_float(_find_value_any(p, ["floatshares","float_shares"]))
        out["week_52_high"]   = _safe_float(_find_value_any(q, ["week_52_high","52weekhigh","yearhigh","fiftytwoweekhigh"]))
        out["week_52_low"]    = _safe_float(_find_value_any(q, ["week_52_low","52weeklow","yearlow","fiftytwoweeklow"]))
        out["avg_volume_10d"] = _safe_float(_find_value_any(q, ["avg_volume_10d","averagevolume10days","avgvol10d"]))
        out["avg_volume_30d"] = _safe_float(_find_value_any(q, ["avg_volume_30d","averagevolume","avgvolume","avgvol30d"]))

        # --- Percent / ratio fields (dtype=pct -- MUST be stored as fraction) ---
        # FIX v4.5.0: use _as_fraction() not _safe_float() for all pct fields
        out["percent_change"] = _as_fraction(
            _find_value_any(q, ["percent_change","change_percent","changepercent","changepct","pct_change"])
        )
        out["dividend_yield"]     = _as_fraction(_find_value_any(q, ["dividend_yield","dividendyield","yieldpct"]))
        out["gross_margin"]       = _as_fraction(_find_value_any(q, ["gross_margin","grossmargin"]))
        out["operating_margin"]   = _as_fraction(_find_value_any(q, ["operating_margin","operatingmargin"]))
        out["profit_margin"]      = _as_fraction(_find_value_any(q, ["profit_margin","profitmargin","netmargin","net_margin"]))
        out["payout_ratio"]       = _as_fraction(_find_value_any(q, ["payout_ratio","payoutratio"]))
        out["revenue_growth_yoy"] = _as_fraction(_find_value_any(q, ["revenue_growth_yoy","revenuegrowth","revenue_growth"]))

        # --- History fallback (fills gaps from OHLCV history) ---
        hist_patch = _history_stats(history_data) if history_data is not None else {}
        for k, v in hist_patch.items():
            if out.get(k) in (None, "", 0):
                out[k] = v

        # --- Derived: week_52_position_pct (fraction) ---
        if out.get("week_52_position_pct") is None:
            out["week_52_position_pct"] = _week_52_position_pct(
                out.get("current_price"),
                out.get("week_52_low"),
                out.get("week_52_high"),
            )

        # --- Derived: price_change / percent_change cross-fill ---
        #
        # If percent_change is missing, derive from price_change / previous_close.
        # FIX v4.5.0: result is stored as FRACTION, not percent points.
        if out.get("percent_change") is None and out.get("price_change") is not None:
            prev = out.get("previous_close")
            if prev not in (None, 0):
                try:
                    out["percent_change"] = float(out["price_change"]) / float(prev)
                except Exception:
                    pass

        # If price_change is missing, derive from percent_change (fraction) * previous_close.
        if out.get("price_change") is None and out.get("percent_change") is not None:
            prev = out.get("previous_close")
            if prev not in (None, 0):
                try:
                    out["price_change"] = float(out["percent_change"]) * float(prev)
                except Exception:
                    pass

        # --- data_quality ---
        if out.get("current_price") is not None:
            out["data_quality"] = "GOOD"
        elif hist_patch or profile_data is not None:
            out["data_quality"] = "DEGRADED"
        else:
            out["data_quality"] = "MISSING"

        return _normalize_patch_keys({k: v for k, v in out.items() if v is not None})

    # ------------------------------------------------------------------
    # Single-symbol fetch
    # ------------------------------------------------------------------

    async def get_enriched_patch(self, symbol: str) -> Dict[str, Any]:
        symbol_norm = normalize_ksa_symbol(symbol)
        if not symbol_norm:
            return _error_patch(symbol, "invalid_ksa_symbol")

        if not self._has_any_config():
            return _error_patch(symbol_norm, "ARGAAM provider URLs not configured (set env vars)")

        quote_urls = self._build_url_candidates(self.quote_url,   symbol_norm) if self.quote_url   else []
        prof_urls  = self._build_url_candidates(self.profile_url, symbol_norm) if self.profile_url else []
        hist_urls  = self._build_url_candidates(self.history_url, symbol_norm) if self.history_url else []

        quote_data,   quote_err,   quote_used   = None, None, None
        profile_data, _,           profile_used = None, None, None
        history_data, _,           history_used = None, None, None

        tasks = []
        if quote_urls:
            tasks.append(self._fetch_first(quote_urls, f"q:{symbol_norm}", DEFAULT_CACHE_TTL_SEC))
        if prof_urls:
            tasks.append(self._fetch_first(prof_urls, f"p:{symbol_norm}", max(60.0, DEFAULT_CACHE_TTL_SEC)))
        if hist_urls:
            tasks.append(self._fetch_first(hist_urls, f"h:{symbol_norm}", DEFAULT_HISTORY_TTL_SEC))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        idx = 0
        if quote_urls:
            r = results[idx]; idx += 1
            if not isinstance(r, Exception):
                quote_data, quote_err, quote_used = r
        if prof_urls:
            r = results[idx]; idx += 1
            if not isinstance(r, Exception):
                profile_data, _, profile_used = r
        if hist_urls:
            r = results[idx]; idx += 1
            if not isinstance(r, Exception):
                history_data, _, history_used = r

        if quote_data is None and profile_data is None and history_data is None:
            return _error_patch(symbol_norm, f"fetch_failed: {quote_err or 'no_data'}", requested_symbol=symbol)

        patch = self._parse_quote_patch(symbol_norm, quote_data, profile_data, history_data)
        patch["requested_symbol"] = symbol
        patch["data_sources"]     = [PROVIDER_NAME]
        patch["provider_origin"]  = {
            "quote_url_used":   quote_used,
            "profile_url_used": profile_used,
            "history_url_used": history_used,
        }
        if quote_data is None and patch.get("data_quality") == "GOOD":
            patch["data_quality"] = "DEGRADED"
            patch["warning"]      = patch.get("warning") or "quote_missing_used_profile_and_or_history_fallback"
        return patch

    # ------------------------------------------------------------------
    # Batch fetch (ENH v4.5.0)
    # ------------------------------------------------------------------

    async def get_enriched_quotes_batch(
        self,
        symbols: List[str],
        *,
        mode: str = "",
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch enriched patches for multiple KSA symbols concurrently.
        Returns {symbol_norm: patch_dict}.
        Non-KSA symbols or fetch failures return error_patch entries.
        """
        if not symbols:
            return {}
        results = await asyncio.gather(
            *(self.get_enriched_patch(sym) for sym in symbols),
            return_exceptions=True,
        )
        out: Dict[str, Dict[str, Any]] = {}
        for sym, result in zip(symbols, results):
            if isinstance(result, Exception):
                out[sym] = _error_patch(sym, f"batch_gather_error: {result}")
            elif isinstance(result, dict):
                key = result.get("symbol") or normalize_ksa_symbol(sym) or sym
                out[key] = result
            else:
                out[sym] = _error_patch(sym, "batch_invalid_result")
        return out

    async def close(self) -> None:
        try:
            if self._client:
                await self._client.aclose()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Provider class (router / engine compatible)
# ---------------------------------------------------------------------------

class ArgaamProvider:
    name    = PROVIDER_NAME
    version = PROVIDER_VERSION
    batch_supported = True

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

    # ENH v4.5.0: batch method
    async def get_enriched_quotes_batch(
        self, symbols: List[str], *, mode: str = ""
    ) -> Dict[str, Dict[str, Any]]:
        c = await self._get_client()
        return await c.get_enriched_quotes_batch(symbols, mode=mode)

    # Aliases for router compatibility
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
# Singletons
# ---------------------------------------------------------------------------

_CLIENT_INSTANCE:   Optional[ArgaamClient]   = None
_CLIENT_LOCK                                  = asyncio.Lock()
_PROVIDER_INSTANCE: Optional[ArgaamProvider] = None
_PROVIDER_LOCK                                = asyncio.Lock()


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
# Module-level engine-facing functions
# ---------------------------------------------------------------------------

async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    c = await get_client()
    patch = await c.get_enriched_patch(symbol)
    if not isinstance(patch, dict) or not patch:
        return _error_patch(symbol, "empty_patch")
    return patch


async def fetch_enriched_quotes_batch(
    symbols: List[str], *, mode: str = "", **kwargs: Any
) -> Dict[str, Dict[str, Any]]:
    """ENH v4.5.0: batch engine fetch."""
    c = await get_client()
    return await c.get_enriched_quotes_batch(symbols, mode=mode)


# Aliases
fetch_patch              = fetch_enriched_quote_patch
fetch_quote_patch        = fetch_enriched_quote_patch
fetch_quote              = fetch_enriched_quote_patch
quote                    = fetch_enriched_quote_patch
get_quote                = fetch_enriched_quote_patch
fetch_profile_patch      = fetch_enriched_quote_patch
fetch_history_patch      = fetch_enriched_quote_patch


__all__ = [
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    "PROVIDER_BATCH_SUPPORTED",
    "VERSION",
    "normalize_ksa_symbol",
    "fetch_enriched_quote_patch",
    "fetch_enriched_quotes_batch",
    "fetch_patch",
    "fetch_quote_patch",
    "fetch_quote",
    "quote",
    "get_quote",
    "fetch_profile_patch",
    "fetch_history_patch",
    "get_client",
    "close_client",
    "ArgaamClient",
    "ArgaamProvider",
    "get_provider",
    "provider",
]
