```python
# core/providers/tadawul_provider.py  (FULL REPLACEMENT)
"""
core/providers/tadawul_provider.py
===============================================================
Tadawul Provider / Client (v1.7.0) — KSA Fundamentals + PROD SAFE + Async (HARDENED)

What’s improved vs v1.6.0 (avoid errors + missing data)
- ✅ Import-safe settings: NO get_settings() at import-time (prevents startup crashes).
- ✅ Multi-endpoint fallback: supports TADAWUL_URLS_JSON and tries multiple URLs until one works.
- ✅ Better HTTP diagnostics: provider_status_code + provider_latency_ms + compact error strings.
- ✅ Quote parsing hardened + computes change/% when missing.
- ✅ Fundamentals parsing expanded (more aliases + deep search, bounded).
- ✅ Always returns a BASE patch (data_source/provider_version/last_updated_utc/market/currency),
  even when the endpoint is missing or response is partial.

Important
- This provider does NOT assume an official Tadawul public API.
  It relies on env/settings URLs YOU provide.

Environment / Settings
- TADAWUL_QUOTE_URL         (optional): quote endpoint template (supports {code} or {symbol})
- TADAWUL_FUNDAMENTALS_URL  (optional): fundamentals/profile endpoint template
- TADAWUL_PROFILE_URL       (optional): alternative to fundamentals url
- TADAWUL_URLS_JSON         (optional): JSON list/dict of multiple endpoints
    Example:
    [
      {"name":"quote","url":"https://.../quote/{code}"},
      {"name":"fund","url":"https://.../fundamentals/{code}"}
    ]
- TADAWUL_HEADERS_JSON      (optional): JSON dict of request headers (cookies, auth, etc.)
- HTTP_TIMEOUT_SEC          (optional) default 25
- HTTP_RETRY_ATTEMPTS       (optional) default 3
- TADAWUL_QUOTE_TTL_SEC     (optional) default 15
- TADAWUL_FUND_TTL_SEC      (optional) default 21600

Usage
- patch, err = await fetch_fundamentals_patch("1120.SR")
- patch2, err2 = await fetch_quote_patch("1120.SR")
- combined, err = await fetch_quote_and_fundamentals_patch("1120.SR")
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
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import httpx
from cachetools import TTLCache

logger = logging.getLogger("core.providers.tadawul_provider")

PROVIDER_VERSION = "1.7.0"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

DEFAULT_TIMEOUT_SEC = 25.0
DEFAULT_RETRY_ATTEMPTS = 3

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


# -----------------------------------------------------------------------------
# Lazy settings shim (PROD SAFE) — no import-time get_settings()
# -----------------------------------------------------------------------------
_SETTINGS_OBJ: Optional[Any] = None
_SETTINGS_LOCK = asyncio.Lock()


async def _get_settings_obj() -> Optional[Any]:
    global _SETTINGS_OBJ
    if _SETTINGS_OBJ is not None:
        return _SETTINGS_OBJ
    async with _SETTINGS_LOCK:
        if _SETTINGS_OBJ is not None:
            return _SETTINGS_OBJ
        try:
            from core.config import get_settings  # type: ignore

            _SETTINGS_OBJ = get_settings()
            return _SETTINGS_OBJ
        except Exception:
            _SETTINGS_OBJ = None
            return None


def _get_env_any(name: str) -> Optional[str]:
    v = os.getenv(name)
    if v is not None:
        return v
    v = os.getenv(name.upper())
    if v is not None:
        return v
    v = os.getenv(name.lower())
    if v is not None:
        return v
    return None


async def _get_attr_or_env(name: str, default: Any = None) -> Any:
    s = await _get_settings_obj()
    if s is not None:
        try:
            if hasattr(s, name):
                v = getattr(s, name)
                if v is not None:
                    return v
        except Exception:
            pass
    v = _get_env_any(name)
    return v if v is not None else default


async def _get_int(name: str, default: int) -> int:
    raw = await _get_attr_or_env(name, None)
    if raw is None:
        return default
    try:
        x = int(str(raw).strip())
        return x if x > 0 else default
    except Exception:
        return default


async def _get_float(name: str, default: float) -> float:
    raw = await _get_attr_or_env(name, None)
    if raw is None:
        return default
    try:
        x = float(str(raw).strip())
        return x if x > 0 else default
    except Exception:
        return default


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clamp(s: Any, n: int = 320) -> str:
    try:
        t = str(s or "")
        return t if len(t) <= n else (t[:n] + "…")
    except Exception:
        return ""


def _safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def _safe_float(val: Any) -> Optional[float]:
    """
    Robust numeric parser:
    - supports Arabic digits
    - strips %, +, commas, currency tokens
    - supports suffix K/M/B
    - supports (1.23) negatives
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
        if not s or s in {"-", "—", "N/A", "NA", "null", "None"}:
            return None

        s = s.translate(_ARABIC_DIGITS)
        s = s.replace("٬", ",").replace("٫", ".")
        s = (
            s.replace("SAR", "")
            .replace("USD", "")
            .replace("ريال", "")
            .replace("$", "")
            .replace("﷼", "")
            .strip()
        )
        s = s.replace("+", "").strip()

        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()

        s = s.replace("%", "").strip()
        s = s.replace(",", "")

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


def _ksa_code(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    if s.endswith(".SR"):
        s = s[:-3]
    if "." in s:
        s = s.split(".", 1)[0]
    return s


def _normalize_ksa_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    if s.isdigit():
        return f"{s}.SR"
    if s.endswith(".SR"):
        return s
    if re.match(r"^\d{3,6}$", s):
        return f"{s}.SR"
    # keep as-is (engine should not route non-KSA here, but don't break)
    return s


def _format_url(template: str, symbol: str) -> str:
    code = _ksa_code(symbol)
    return (template or "").replace("{code}", code).replace("{symbol}", symbol)


def _base_patch(symbol: str) -> Dict[str, Any]:
    sym = _normalize_ksa_symbol(symbol)
    return {
        "market": "KSA",
        "currency": "SAR",
        "data_source": "tadawul",
        "provider_version": PROVIDER_VERSION,
        "provider_symbol": sym or (symbol or "").strip(),
        "provider_code": _ksa_code(sym or symbol),
        "last_updated_utc": _utc_iso(),
    }


# -----------------------------------------------------------------------------
# Deep-find helpers (bounded)
# -----------------------------------------------------------------------------
def _find_first_value(obj: Any, keys: Sequence[str], *, max_depth: int = 5) -> Any:
    """
    Search nested dict/list for any of `keys` (case-insensitive), depth-limited.
    Returns the first matched value.
    """
    if obj is None:
        return None

    keyset = {k.lower() for k in keys if k}
    seen_ids = set()

    def _walk(x: Any, depth: int) -> Any:
        if x is None or depth < 0:
            return None
        xid = id(x)
        if xid in seen_ids:
            return None
        seen_ids.add(xid)

        if isinstance(x, dict):
            for k, v in x.items():
                if str(k).strip().lower() in keyset:
                    return v
            for v in x.values():
                r = _walk(v, depth - 1)
                if r is not None:
                    return r
            return None

        if isinstance(x, list):
            for it in x:
                r = _walk(it, depth - 1)
                if r is not None:
                    return r
            return None

        return None

    return _walk(obj, max_depth)


def _pick_num(obj: Any, *keys: str, max_depth: int = 5) -> Optional[float]:
    return _safe_float(_find_first_value(obj, keys, max_depth=max_depth))


def _pick_pct(obj: Any, *keys: str, max_depth: int = 5) -> Optional[float]:
    return _pct_if_fraction(_find_first_value(obj, keys, max_depth=max_depth))


def _pick_str(obj: Any, *keys: str, max_depth: int = 5) -> Optional[str]:
    return _safe_str(_find_first_value(obj, keys, max_depth=max_depth))


def _coerce_dict(data: Any) -> Dict[str, Any]:
    """
    Normalize response payload into a dict-like root:
    - dict -> dict
    - list[dict] -> first item
    - other -> {}
    """
    if isinstance(data, dict):
        return data
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return data[0]
    return {}


def _has_payload(patch: Dict[str, Any]) -> bool:
    base = {
        "market",
        "currency",
        "data_source",
        "provider_version",
        "provider_symbol",
        "provider_code",
        "last_updated_utc",
        "provider_status_code",
        "provider_latency_ms",
    }
    for k, v in (patch or {}).items():
        if k in base:
            continue
        if v is not None and v != "":
            return True
    return False


# -----------------------------------------------------------------------------
# Client
# -----------------------------------------------------------------------------
class TadawulClient:
    def __init__(
        self,
        *,
        quote_url: Optional[str],
        fundamentals_url: Optional[str],
        profile_url: Optional[str],
        endpoints_json: Optional[str],
        headers_json: Optional[str],
        timeout_sec: float,
        retry_attempts: int,
        quote_ttl_sec: float,
        fund_ttl_sec: float,
    ) -> None:
        self.quote_url = _safe_str(quote_url)
        self.fundamentals_url = _safe_str(fundamentals_url)
        self.profile_url = _safe_str(profile_url)

        self.timeout_sec = float(timeout_sec) if timeout_sec and timeout_sec > 0 else DEFAULT_TIMEOUT_SEC
        self.retry_attempts = int(retry_attempts) if retry_attempts and retry_attempts > 0 else DEFAULT_RETRY_ATTEMPTS

        timeout = httpx.Timeout(self.timeout_sec, connect=min(10.0, self.timeout_sec))
        self._client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.8,ar;q=0.6",
            },
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=40),
        )

        # Headers (auth/cookies)
        self._headers: Dict[str, str] = {}
        raw_hdr = _safe_str(headers_json)
        if raw_hdr:
            try:
                obj = json.loads(raw_hdr)
                if isinstance(obj, dict):
                    self._headers.update({str(k): str(v) for k, v in obj.items()})
            except Exception:
                pass

        # Optional multi-endpoint config
        self._endpoints: List[Dict[str, str]] = self._load_endpoints_json(_safe_str(endpoints_json))

        self._quote_cache: TTLCache = TTLCache(maxsize=6000, ttl=max(5.0, float(quote_ttl_sec or 15.0)))
        self._fund_cache: TTLCache = TTLCache(maxsize=4000, ttl=max(120.0, float(fund_ttl_sec or 21600.0)))

    async def aclose(self) -> None:
        await self._client.aclose()

    def _load_endpoints_json(self, raw: Optional[str]) -> List[Dict[str, str]]:
        """
        TADAWUL_URLS_JSON:
          - list of {name,url}
          - or dict {quote:..., fund:..., fundamentals:..., profile:...}
        """
        if not raw:
            return []
        try:
            data = json.loads(raw)
            out: List[Dict[str, str]] = []
            if isinstance(data, dict):
                for k, v in data.items():
                    if isinstance(v, str) and v.strip():
                        out.append({"name": str(k).strip().lower(), "url": v.strip()})
                return out
            if isinstance(data, list):
                for it in data:
                    if isinstance(it, dict) and isinstance(it.get("url"), str) and it["url"].strip():
                        out.append(
                            {
                                "name": (_safe_str(it.get("name")) or "unnamed").strip().lower(),
                                "url": str(it["url"]).strip(),
                            }
                        )
                return out
        except Exception:
            return []
        return []

    def _iter_endpoint_urls(self, kind: str) -> List[str]:
        """
        Returns a list of endpoint templates (strings) to try, in order.
        """
        k = (kind or "").strip().lower()
        urls: List[str] = []

        # First: endpoints_json matching name
        for ep in self._endpoints:
            if (ep.get("name") or "").strip().lower() == k:
                u = _safe_str(ep.get("url"))
                if u:
                    urls.append(u)

        # Then: fallback aliases for fundamentals
        if k in {"fund", "fundamentals", "profile"}:
            for alias in ("fund", "fundamentals", "profile"):
                for ep in self._endpoints:
                    if (ep.get("name") or "").strip().lower() == alias:
                        u = _safe_str(ep.get("url"))
                        if u and u not in urls:
                            urls.append(u)

        # Finally: direct env URL fields
        if k == "quote" and self.quote_url and self.quote_url not in urls:
            urls.append(self.quote_url)

        if k in {"fund", "fundamentals"}:
            for u in (self.fundamentals_url, self.profile_url):
                if u and u not in urls:
                    urls.append(u)

        if k == "profile" and self.profile_url and self.profile_url not in urls:
            urls.append(self.profile_url)

        return [u for u in urls if u]

    async def _get_json(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Optional[Union[Dict[str, Any], List[Any]]], Optional[str], int, int]:
        """
        Returns: (data, error, status_code, latency_ms)
        Never raises.
        Also tries to parse JSON from text when response isn't clean JSON.
        """
        last_status = 0
        last_err: Optional[str] = None

        for attempt in range(max(1, self.retry_attempts)):
            t0 = time.perf_counter()
            try:
                r = await self._client.get(url, params=params, headers=(self._headers or None))
                dt_ms = int((time.perf_counter() - t0) * 1000)
                last_status = int(getattr(r, "status_code", 0) or 0)

                if last_status == 429 or (500 <= last_status < 600):
                    if attempt < (self.retry_attempts - 1):
                        ra = (r.headers.get("Retry-After") or "").strip()
                        if ra.isdigit():
                            await asyncio.sleep(min(3.0, float(ra)))
                        else:
                            await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.35)
                        continue
                    return None, f"http {last_status}", last_status, dt_ms

                if last_status >= 400:
                    return None, f"http {last_status}", last_status, dt_ms

                # Try JSON directly
                try:
                    return r.json(), None, last_status, dt_ms
                except Exception:
                    txt = (r.text or "").strip()
                    if not txt:
                        return None, "empty body", last_status, dt_ms

                    # Try parse full text
                    if txt.startswith("{") or txt.startswith("["):
                        try:
                            return json.loads(txt), None, last_status, dt_ms
                        except Exception:
                            pass

                    # Try extract a JSON object/array substring (best-effort)
                    # e.g. HTML page with embedded JSON
                    m = re.search(r"(\{.*\}|\[.*\])", txt, flags=re.DOTALL)
                    if m:
                        chunk = m.group(1)
                        try:
                            return json.loads(chunk), None, last_status, dt_ms
                        except Exception:
                            return None, "invalid embedded json", last_status, dt_ms

                    return None, "non-json response", last_status, dt_ms

            except Exception as exc:
                dt_ms = int((time.perf_counter() - t0) * 1000)
                last_err = str(exc)
                if attempt < (self.retry_attempts - 1):
                    await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.35)
                    continue
                return None, _clamp(last_err, 500), last_status, dt_ms

        return None, last_err or "request failed", last_status, 0

    # -------------------------------------------------------------------------
    # Quote (price/volume) patch
    # -------------------------------------------------------------------------
    async def fetch_quote_patch(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        sym = _normalize_ksa_symbol(symbol)
        patch = _base_patch(sym or symbol)

        url_templates = self._iter_endpoint_urls("quote")
        if not url_templates:
            return patch, "Tadawul: quote url not configured (TADAWUL_QUOTE_URL / TADAWUL_URLS_JSON)"

        cache_key = f"quote::{sym}"
        hit = self._quote_cache.get(cache_key)
        if isinstance(hit, dict) and hit:
            return dict(hit), None

        last_err = None
        last_status = 0
        last_dt = 0
        data: Optional[Union[Dict[str, Any], List[Any]]] = None

        for tpl in url_templates:
            url = _format_url(tpl, sym)
            data, err, status, dt_ms = await self._get_json(url)
            last_err, last_status, last_dt = err, status, dt_ms
            if data:
                break

        patch["provider_status_code"] = last_status
        patch["provider_latency_ms"] = last_dt

        if not data:
            return patch, f"Tadawul: quote empty response ({last_err or 'no data'}) ({last_dt}ms)"

        root = _coerce_dict(data)

        # Common quote keys (best-effort)
        price = _pick_num(
            root,
            "price",
            "last",
            "last_price",
            "lastPrice",
            "tradingPrice",
            "close",
            "currentPrice",
            "current_price",
            "regularMarketPrice",
        )
        prev = _pick_num(root, "previous_close", "previousClose", "prevClose", "prev_close", "previousClosePrice")
        opn = _pick_num(root, "open", "openPrice", "regularMarketOpen")
        hi = _pick_num(root, "high", "day_high", "dayHigh", "regularMarketDayHigh")
        lo = _pick_num(root, "low", "day_low", "dayLow", "regularMarketDayLow")
        vol = _pick_num(root, "volume", "tradedVolume", "qty", "volumeTraded", "totalVolume")

        chg = _pick_num(root, "change", "price_change", "diff", "delta", "d")
        chg_p = _pick_pct(
            root,
            "change_percent",
            "percent_change",
            "changeP",
            "changePercent",
            "pctChange",
            "dp",
        )

        # Compute change/% if missing but have price & prev
        if chg is None and price is not None and prev is not None:
            chg = price - prev
        if chg_p is None and price is not None and prev not in (None, 0.0):
            try:
                chg_p = (price - prev) / prev * 100.0
            except Exception:
                chg_p = None

        name = _pick_str(root, "name", "company", "companyName", "company_name", "securityName", "issuerName")
        currency = _pick_str(root, "currency") or "SAR"

        patch.update(
            {
                "currency": currency,
                "name": name,
                "current_price": price,
                "previous_close": prev,
                "open": opn,
                "day_high": hi,
                "day_low": lo,
                "volume": vol,
                "price_change": chg,
                "percent_change": chg_p,
            }
        )

        # Cache only if contains usable price
        if patch.get("current_price") is not None:
            self._quote_cache[cache_key] = dict(patch)
            return patch, None

        return patch, f"Tadawul: quote returned no price ({last_dt}ms)"

    # -------------------------------------------------------------------------
    # Fundamentals patch (market cap / shares / free-float / ratios)
    # -------------------------------------------------------------------------
    async def fetch_fundamentals_patch(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        sym = _normalize_ksa_symbol(symbol)
        patch = _base_patch(sym or symbol)

        url_templates = self._iter_endpoint_urls("fund") or self._iter_endpoint_urls("fundamentals") or self._iter_endpoint_urls("profile")
        if not url_templates:
            return patch, "Tadawul: fundamentals url not configured (TADAWUL_FUNDAMENTALS_URL / TADAWUL_URLS_JSON)"

        cache_key = f"fund::{sym}"
        hit = self._fund_cache.get(cache_key)
        if isinstance(hit, dict) and hit:
            return dict(hit), None

        last_err = None
        last_status = 0
        last_dt = 0
        data: Optional[Union[Dict[str, Any], List[Any]]] = None

        for tpl in url_templates:
            url = _format_url(tpl, sym)
            data, err, status, dt_ms = await self._get_json(url)
            last_err, last_status, last_dt = err, status, dt_ms
            if data:
                break

        patch["provider_status_code"] = last_status
        patch["provider_latency_ms"] = last_dt

        if not data:
            return patch, f"Tadawul: fundamentals empty response ({last_err or 'no data'})"

        root = _coerce_dict(data)

        # Identity / classification
        name = _pick_str(
            root,
            "name",
            "companyName",
            "company_name",
            "securityName",
            "issuerName",
            "shortName",
            "longName",
            "instrumentName",
        )
        sector = _pick_str(root, "sector", "Sector", "sectorName", "sector_name")
        industry = _pick_str(root, "industry", "Industry", "industryName", "industry_name")
        sub_sector = _pick_str(root, "sub_sector", "subSector", "subSectorName", "sub_sector_name", "subIndustry")

        listing_date = _pick_str(root, "listing_date", "ipoDate", "IPODate", "listingDate", "ListingDate", "ipo")

        # Market cap / shares / float
        market_cap = _pick_num(root, "market_cap", "marketCap", "MarketCap", "mcap", "marketCapitalization")
        shares_out = _pick_num(
            root,
            "shares_outstanding",
            "sharesOutstanding",
            "SharesOutstanding",
            "outstandingShares",
            "issuedShares",
        )

        free_float_pct = _pick_pct(
            root,
            "free_float",
            "freeFloat",
            "freeFloatPercent",
            "free_float_percent",
            "freeFloatPct",
            "freeFloatPercentage",
        )
        free_float_shares = _pick_num(root, "free_float_shares", "freeFloatShares", "freeShares", "freeFloatQty")

        # Derive missing float pieces when possible
        if free_float_pct is None and shares_out is not None and free_float_shares is not None and shares_out != 0:
            free_float_pct = (free_float_shares / shares_out) * 100.0
        if free_float_shares is None and shares_out is not None and free_float_pct is not None:
            free_float_shares = shares_out * (free_float_pct / 100.0)

        free_float_mcap = None
        if market_cap is not None and free_float_pct is not None:
            free_float_mcap = market_cap * (free_float_pct / 100.0)

        # Valuation / profitability / leverage (best-effort)
        eps_ttm = _pick_num(root, "eps", "eps_ttm", "epsTTM", "earningsPerShare", "EarningsPerShare")
        pe_ttm = _pick_num(root, "pe", "pe_ttm", "peTTM", "priceEarnings", "PERatio", "p_e")
        pb = _pick_num(root, "pb", "p_b", "priceToBook", "PriceToBook", "priceBook", "PBRatio")
        ps = _pick_num(root, "ps", "p_s", "priceToSales", "PriceToSales", "PSRatio")

        dividend_yield = _pick_pct(root, "dividend_yield", "dividendYield", "DividendYield")
        dividend_rate = _pick_num(root, "dividend_rate", "dividendRate", "DividendRate", "dividendPerShare", "dps")
        payout_ratio = _pick_pct(root, "payout_ratio", "payoutRatio", "PayoutRatio")

        roe = _pick_pct(root, "roe", "ROE", "returnOnEquity", "ReturnOnEquity")
        roa = _pick_pct(root, "roa", "ROA", "returnOnAssets", "ReturnOnAssets")

        beta = _pick_num(root, "beta", "Beta")
        debt_to_equity = _pick_num(root, "debt_to_equity", "debtToEquity", "DebtToEquity", "D/E")
        current_ratio = _pick_num(root, "current_ratio", "currentRatio", "CurrentRatio")
        quick_ratio = _pick_num(root, "quick_ratio", "quickRatio", "QuickRatio")

        high_52w = _pick_num(root, "high_52w", "52WeekHigh", "fiftyTwoWeekHigh", "yearHigh")
        low_52w = _pick_num(root, "low_52w", "52WeekLow", "fiftyTwoWeekLow", "yearLow")
        ma20 = _pick_num(root, "ma20", "MA20", "20DayMA", "movingAverage20")
        ma50 = _pick_num(root, "ma50", "MA50", "50DayMA", "movingAverage50")

        patch.update(
            {
                "name": name,
                "sector": sector,
                "industry": industry,
                "sub_sector": sub_sector,
                "listing_date": listing_date,
                "market_cap": market_cap,
                "shares_outstanding": shares_out,
                "free_float": free_float_pct,
                "free_float_shares": free_float_shares,
                "free_float_market_cap": free_float_mcap,
                "eps_ttm": eps_ttm,
                "pe_ttm": pe_ttm,
                "pb": pb,
                "ps": ps,
                "dividend_yield": dividend_yield,
                "dividend_rate": dividend_rate,
                "payout_ratio": payout_ratio,
                "roe": roe,
                "roa": roa,
                "beta": beta,
                "debt_to_equity": debt_to_equity,
                "current_ratio": current_ratio,
                "quick_ratio": quick_ratio,
                "high_52w": high_52w,
                "low_52w": low_52w,
                "ma20": ma20,
                "ma50": ma50,
            }
        )

        if not _has_payload(patch):
            return patch, "Tadawul: fundamentals present but no identity/mcap/shares/ratios found"

        # Cache even if partial
        self._fund_cache[cache_key] = dict(patch)
        return patch, None

    async def fetch_quote_and_fundamentals_patch(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        """
        Concurrent quote + fundamentals; merge into one PATCH.
        Always returns a base patch; error only if both are empty/failed.
        """
        q_res, f_res = await asyncio.gather(
            self.fetch_quote_patch(symbol),
            self.fetch_fundamentals_patch(symbol),
            return_exceptions=True,
        )

        patch: Dict[str, Any] = _base_patch(symbol)
        errs: List[str] = []

        if isinstance(f_res, Exception):
            errs.append(f"fund:{_clamp(f_res)}")
        else:
            f_patch, f_err = f_res
            if isinstance(f_patch, dict) and f_patch:
                patch.update({k: v for k, v in f_patch.items() if v is not None})
            if f_err:
                errs.append(f_err)

        if isinstance(q_res, Exception):
            errs.append(f"quote:{_clamp(q_res)}")
        else:
            q_patch, q_err = q_res
            if isinstance(q_patch, dict) and q_patch:
                patch.update({k: v for k, v in q_patch.items() if v is not None})
            if q_err:
                errs.append(q_err)

        if _has_payload(patch):
            return patch, None

        return patch, ("Tadawul: " + " | ".join([e for e in errs if e])) if errs else "Tadawul: unknown error"


# -----------------------------------------------------------------------------
# Lazy singleton (PROD SAFE)
# -----------------------------------------------------------------------------
_CLIENT_SINGLETON: Optional[TadawulClient] = None
_CLIENT_LOCK = asyncio.Lock()


async def get_tadawul_client() -> TadawulClient:
    global _CLIENT_SINGLETON
    if _CLIENT_SINGLETON is not None:
        return _CLIENT_SINGLETON

    async with _CLIENT_LOCK:
        if _CLIENT_SINGLETON is not None:
            return _CLIENT_SINGLETON

        timeout_sec = await _get_float("HTTP_TIMEOUT_SEC", DEFAULT_TIMEOUT_SEC)
        retries = await _get_int("HTTP_RETRY_ATTEMPTS", DEFAULT_RETRY_ATTEMPTS)

        quote_ttl = await _get_float("TADAWUL_QUOTE_TTL_SEC", 15.0)
        fund_ttl = await _get_float("TADAWUL_FUND_TTL_SEC", 21600.0)

        quote_url = await _get_attr_or_env("TADAWUL_QUOTE_URL", None)
        fund_url = await _get_attr_or_env("TADAWUL_FUNDAMENTALS_URL", None)
        profile_url = await _get_attr_or_env("TADAWUL_PROFILE_URL", None)
        urls_json = await _get_attr_or_env("TADAWUL_URLS_JSON", None)
        headers_json = await _get_attr_or_env("TADAWUL_HEADERS_JSON", None)

        _CLIENT_SINGLETON = TadawulClient(
            quote_url=_safe_str(quote_url),
            fundamentals_url=_safe_str(fund_url),
            profile_url=_safe_str(profile_url),
            endpoints_json=_safe_str(urls_json),
            headers_json=_safe_str(headers_json),
            timeout_sec=float(timeout_sec),
            retry_attempts=int(retries),
            quote_ttl_sec=float(quote_ttl),
            fund_ttl_sec=float(fund_ttl),
        )

        logger.info(
            "Tadawul client init v%s | quote_url=%s | fund_url=%s | profile_url=%s | endpoints_json=%s | timeout=%.1fs | retries=%s",
            PROVIDER_VERSION,
            bool(_CLIENT_SINGLETON.quote_url),
            bool(_CLIENT_SINGLETON.fundamentals_url),
            bool(_CLIENT_SINGLETON.profile_url),
            bool(_CLIENT_SINGLETON._endpoints),
            float(timeout_sec),
            int(retries),
        )

    return _CLIENT_SINGLETON


async def aclose_tadawul_client() -> None:
    global _CLIENT_SINGLETON
    if _CLIENT_SINGLETON is None:
        return
    try:
        await _CLIENT_SINGLETON.aclose()
    finally:
        _CLIENT_SINGLETON = None


# -----------------------------------------------------------------------------
# Convenience functions (engine-friendly)
# -----------------------------------------------------------------------------
async def fetch_quote_patch(symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    c = await get_tadawul_client()
    return await c.fetch_quote_patch(symbol)


async def fetch_fundamentals_patch(symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    c = await get_tadawul_client()
    return await c.fetch_fundamentals_patch(symbol)


async def fetch_quote_and_fundamentals_patch(symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    c = await get_tadawul_client()
    return await c.fetch_quote_and_fundamentals_patch(symbol)


__all__ = [
    "TadawulClient",
    "get_tadawul_client",
    "aclose_tadawul_client",
    "fetch_quote_patch",
    "fetch_fundamentals_patch",
    "fetch_quote_and_fundamentals_patch",
    "PROVIDER_VERSION",
]
```
