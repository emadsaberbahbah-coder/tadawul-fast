# core/providers/tadawul_provider.py
"""
core/providers/tadawul_provider.py
===============================================================
Tadawul Provider / Client (v1.6.0) — KSA Fundamentals + PROD SAFE + Async

Goal
- Provide KSA market-cap / shares / free-float / listing date / basic fundamentals
  via *your configured* Tadawul JSON endpoint(s).
- Generic + defensive: Tadawul payloads differ by source; this client maps common keys
  and also searches nested structures.

Alignment
- Returns PATCH dicts aligned to UnifiedQuote fields in core/data_engine_v2.py (v2.7.x):
  name, sector, industry, sub_sector, listing_date,
  shares_outstanding, free_float, market_cap, free_float_market_cap,
  eps_ttm, pe_ttm, pb, ps, dividend_yield, dividend_rate, payout_ratio,
  roe, roa, beta, debt_to_equity, current_ratio, quick_ratio,
  high_52w, low_52w, ma20, ma50, etc. (best-effort)
- Designed to be used by DataEngine as an optional enrichment layer.

Important
- This provider does NOT assume an "official" Tadawul public API.
  It relies on env/settings URLs YOU provide.

Environment / Settings
- TADAWUL_QUOTE_URL         (optional): quote endpoint template (supports {code} or {symbol})
- TADAWUL_FUNDAMENTALS_URL  (optional): fundamentals/profile endpoint template
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

PROVIDER_VERSION = "1.6.0"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

DEFAULT_TIMEOUT_SEC = 25.0
DEFAULT_RETRY_ATTEMPTS = 3

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


# -----------------------------------------------------------------------------
# Settings shim (PROD SAFE)
# -----------------------------------------------------------------------------
try:
    from core.config import get_settings  # type: ignore

    _settings = get_settings()
except Exception:  # pragma: no cover
    class _FallbackSettings:
        pass

    _settings = _FallbackSettings()  # type: ignore


def _get_attr_or_env(name: str, default: Any = None) -> Any:
    try:
        if hasattr(_settings, name):
            v = getattr(_settings, name)
            if v is not None:
                return v
    except Exception:
        pass
    v = os.getenv(name) or os.getenv(name.upper()) or os.getenv(name.lower())
    return v if v is not None else default


def _get_int(name: str, default: int) -> int:
    raw = _get_attr_or_env(name, None)
    if raw is None:
        return default
    try:
        x = int(str(raw).strip())
        return x if x > 0 else default
    except Exception:
        return default


def _get_float(name: str, default: float) -> float:
    raw = _get_attr_or_env(name, None)
    if raw is None:
        return default
    try:
        x = float(str(raw).strip())
        return x if x > 0 else default
    except Exception:
        return default


def _safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def _safe_float(val: Any) -> Optional[float]:
    """
    Robust numeric parser:
    - supports Arabic digits
    - strips %, +, commas
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
        s = s.replace("SAR", "").replace("ريال", "").replace("USD", "").strip()
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


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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
    # If user passes "1120" etc, normalize to .SR; otherwise keep as-is.
    if re.match(r"^\d{3,6}$", s):
        return f"{s}.SR"
    return s


def _format_url(template: str, symbol: str) -> str:
    code = _ksa_code(symbol)
    return (template or "").replace("{code}", code).replace("{symbol}", symbol)


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
            # direct match first
            for k, v in x.items():
                if str(k).strip().lower() in keyset:
                    return v
            # then recurse
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
    v = _find_first_value(obj, keys, max_depth=max_depth)
    return _safe_float(v)


def _pick_pct(obj: Any, *keys: str, max_depth: int = 5) -> Optional[float]:
    v = _find_first_value(obj, keys, max_depth=max_depth)
    return _pct_if_fraction(v)


def _pick_str(obj: Any, *keys: str, max_depth: int = 5) -> Optional[str]:
    v = _find_first_value(obj, keys, max_depth=max_depth)
    return _safe_str(v)


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


# -----------------------------------------------------------------------------
# Client
# -----------------------------------------------------------------------------
class TadawulClient:
    def __init__(
        self,
        quote_url: Optional[str] = None,
        fundamentals_url: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout_sec: float = DEFAULT_TIMEOUT_SEC,
        retry_attempts: int = DEFAULT_RETRY_ATTEMPTS,
    ) -> None:
        self.quote_url = _safe_str(quote_url or _get_attr_or_env("TADAWUL_QUOTE_URL"))
        self.fundamentals_url = _safe_str(
            fundamentals_url or _get_attr_or_env("TADAWUL_FUNDAMENTALS_URL") or _get_attr_or_env("TADAWUL_PROFILE_URL")
        )

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
        if headers:
            self._headers.update({str(k): str(v) for k, v in headers.items()})

        hdr_json = _safe_str(_get_attr_or_env("TADAWUL_HEADERS_JSON"))
        if hdr_json:
            try:
                obj = json.loads(hdr_json)
                if isinstance(obj, dict):
                    self._headers.update({str(k): str(v) for k, v in obj.items()})
            except Exception:
                pass

        quote_ttl = _get_float("TADAWUL_QUOTE_TTL_SEC", 15.0)
        fund_ttl = _get_float("TADAWUL_FUND_TTL_SEC", 21600.0)
        self._quote_cache: TTLCache = TTLCache(maxsize=5000, ttl=max(5.0, float(quote_ttl)))
        self._fund_cache: TTLCache = TTLCache(maxsize=3000, ttl=max(120.0, float(fund_ttl)))

        # Optional multi-endpoint config
        self._endpoints = self._load_endpoints_json()

    async def aclose(self) -> None:
        await self._client.aclose()

    def _load_endpoints_json(self) -> List[Dict[str, str]]:
        """
        Optional: TADAWUL_URLS_JSON
          - list of {name,url} objects
          - or dict {quote:..., fund:...}
        """
        raw = _safe_str(_get_attr_or_env("TADAWUL_URLS_JSON"))
        if not raw:
            return []
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                out = []
                for k, v in data.items():
                    if isinstance(v, str) and v.strip():
                        out.append({"name": str(k).strip().lower(), "url": v.strip()})
                return out
            if isinstance(data, list):
                out = []
                for it in data:
                    if isinstance(it, dict) and isinstance(it.get("url"), str) and it["url"].strip():
                        out.append(
                            {
                                "name": _safe_str(it.get("name")) or "unnamed",
                                "url": str(it["url"]).strip(),
                            }
                        )
                return out
        except Exception:
            return []
        return []

    def _endpoint_url(self, kind: str) -> Optional[str]:
        k = (kind or "").strip().lower()
        for ep in self._endpoints:
            if (ep.get("name") or "").strip().lower() == k and ep.get("url"):
                return _safe_str(ep.get("url"))
        if k == "quote":
            return self.quote_url
        if k in {"fund", "fundamentals", "profile"}:
            return self.fundamentals_url
        return None

    async def _get_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> Optional[Union[Dict[str, Any], List[Any]]]:
        for attempt in range(max(1, self.retry_attempts)):
            try:
                r = await self._client.get(url, params=params, headers=(self._headers or None))
                if r.status_code == 429 or 500 <= r.status_code < 600:
                    if attempt < (self.retry_attempts - 1):
                        ra = r.headers.get("Retry-After")
                        if ra and ra.strip().isdigit():
                            await asyncio.sleep(min(2.0, float(ra.strip())))
                        else:
                            await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.35)
                        continue
                    return None
                if r.status_code >= 400:
                    return None
                try:
                    return r.json()
                except Exception:
                    txt = (r.text or "").strip()
                    if txt.startswith("{") or txt.startswith("["):
                        try:
                            return json.loads(txt)
                        except Exception:
                            return None
                    return None
            except Exception:
                if attempt < (self.retry_attempts - 1):
                    await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.35)
                    continue
                return None
        return None

    # -------------------------------------------------------------------------
    # Quote (price/volume) patch (optional)
    # -------------------------------------------------------------------------
    async def fetch_quote_patch(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        sym = _normalize_ksa_symbol(symbol)
        if not sym:
            return {}, "Tadawul: empty symbol"

        tpl = self._endpoint_url("quote")
        if not tpl:
            return {}, "Tadawul: quote url not configured (TADAWUL_QUOTE_URL)"

        cache_key = f"quote::{sym}"
        hit = self._quote_cache.get(cache_key)
        if isinstance(hit, dict) and hit:
            return dict(hit), None

        url = _format_url(tpl, sym)
        t0 = time.perf_counter()
        data = await self._get_json(url)
        dt_ms = int((time.perf_counter() - t0) * 1000)
        if not data:
            return {}, f"Tadawul: quote empty response ({dt_ms}ms)"

        root = _coerce_dict(data)

        # Common quote keys (best-effort)
        price = (
            _pick_num(root, "price", "last", "last_price", "lastPrice", "tradingPrice", "close", "currentPrice")
            or _pick_num(root, "regularMarketPrice")
        )
        prev = _pick_num(root, "previous_close", "previousClose", "prevClose", "prev_close", "previousClosePrice")
        op = _pick_num(root, "open", "openPrice", "regularMarketOpen")
        hi = _pick_num(root, "high", "day_high", "dayHigh", "regularMarketDayHigh")
        lo = _pick_num(root, "low", "day_low", "dayLow", "regularMarketDayLow")
        vol = _pick_num(root, "volume", "tradedVolume", "qty", "volumeTraded")

        chg = _pick_num(root, "change", "price_change", "diff", "delta")
        chg_p = _pick_pct(root, "change_percent", "percent_change", "changeP", "changePercent", "pctChange", "dp")

        name = _pick_str(root, "name", "company", "companyName", "company_name", "securityName", "issuerName")
        currency = _pick_str(root, "currency") or "SAR"

        patch: Dict[str, Any] = {
            "market": "KSA",
            "currency": currency,
            "name": name,
            "current_price": price,
            "previous_close": prev,
            "open": op,
            "day_high": hi,
            "day_low": lo,
            "volume": vol,
            "price_change": chg,
            "percent_change": chg_p,
            "data_source": "tadawul",
            "last_updated_utc": _utc_iso(),
        }

        # Only cache if it contains a usable price
        if patch.get("current_price") is not None:
            self._quote_cache[cache_key] = dict(patch)
            return patch, None

        return {}, f"Tadawul: quote returned no price ({dt_ms}ms)"

    # -------------------------------------------------------------------------
    # Fundamentals patch (market cap / shares / free-float / ratios)
    # -------------------------------------------------------------------------
    async def fetch_fundamentals_patch(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        sym = _normalize_ksa_symbol(symbol)
        if not sym:
            return {}, "Tadawul: empty symbol"

        tpl = self._endpoint_url("fund") or self._endpoint_url("fundamentals") or self._endpoint_url("profile")
        if not tpl:
            return {}, "Tadawul: fundamentals url not configured (TADAWUL_FUNDAMENTALS_URL)"

        cache_key = f"fund::{sym}"
        hit = self._fund_cache.get(cache_key)
        if isinstance(hit, dict) and hit:
            return dict(hit), None

        url = _format_url(tpl, sym)
        data = await self._get_json(url)
        if not data:
            return {}, "Tadawul: fundamentals empty response"

        root = _coerce_dict(data)

        # Identity / classification
        name = _pick_str(root, "name", "companyName", "company_name", "securityName", "issuerName", "shortName", "longName")
        sector = _pick_str(root, "sector", "Sector", "sectorName", "sector_name")
        industry = _pick_str(root, "industry", "Industry", "industryName", "industry_name")
        sub_sector = _pick_str(root, "sub_sector", "subSector", "subSectorName", "sub_sector_name", "subIndustry")

        listing_date = _pick_str(root, "listing_date", "ipoDate", "IPODate", "listingDate", "ListingDate")

        # Market cap / shares / float
        market_cap = _pick_num(root, "market_cap", "marketCap", "MarketCap", "mcap", "marketCapitalization")
        shares_out = _pick_num(root, "shares_outstanding", "sharesOutstanding", "SharesOutstanding", "outstandingShares")
        free_float_pct = _pick_pct(
            root,
            "free_float",
            "freeFloat",
            "freeFloatPercent",
            "free_float_percent",
            "freeFloatPct",
            "freeFloatPercentage",
        )

        free_float_mcap = None
        if market_cap is not None and free_float_pct is not None:
            free_float_mcap = market_cap * (free_float_pct / 100.0)

        # Valuation / profitability / leverage (best-effort)
        eps_ttm = _pick_num(root, "eps", "eps_ttm", "epsTTM", "earningsPerShare", "EarningsPerShare")
        pe_ttm = _pick_num(root, "pe", "pe_ttm", "peTTM", "priceEarnings", "PERatio", "p_e")
        pb = _pick_num(root, "pb", "p_b", "priceToBook", "PriceToBook", "priceBook", "PBRatio")
        ps = _pick_num(root, "ps", "p_s", "priceToSales", "PriceToSales", "PSRatio")
        dividend_yield = _pick_pct(root, "dividend_yield", "dividendYield", "DividendYield")
        dividend_rate = _pick_num(root, "dividend_rate", "dividendRate", "DividendRate", "dividendPerShare")
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

        patch: Dict[str, Any] = {
            "market": "KSA",
            "currency": "SAR",
            "name": name,
            "sector": sector,
            "industry": industry,
            "sub_sector": sub_sector,
            "listing_date": listing_date,
            "market_cap": market_cap,
            "shares_outstanding": shares_out,
            "free_float": free_float_pct,
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
            "data_source": "tadawul",
            "last_updated_utc": _utc_iso(),
        }

        # Cache even if partial, but avoid caching empty dict
        non_null = {k: v for k, v in patch.items() if v is not None}
        if len(non_null) <= 3:  # only market/currency/data_source etc.
            return {}, "Tadawul: fundamentals present but no mcap/shares/identity fields found"

        self._fund_cache[cache_key] = dict(patch)
        return patch, None

    async def fetch_quote_and_fundamentals_patch(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        """
        Concurrent quote + fundamentals; merge into one PATCH.
        """
        q_res, f_res = await asyncio.gather(
            self.fetch_quote_patch(symbol),
            self.fetch_fundamentals_patch(symbol),
            return_exceptions=True,
        )

        patch: Dict[str, Any] = {}
        errs: List[str] = []

        if isinstance(f_res, Exception):
            errs.append(f"fund:{f_res}")
        else:
            f_patch, f_err = f_res
            if f_patch:
                patch.update({k: v for k, v in f_patch.items() if v is not None})
            if f_err:
                errs.append(f_err)

        if isinstance(q_res, Exception):
            errs.append(f"quote:{q_res}")
        else:
            q_patch, q_err = q_res
            if q_patch:
                patch.update({k: v for k, v in q_patch.items() if v is not None})
            if q_err:
                errs.append(q_err)

        if patch:
            return patch, None
        return {}, "Tadawul: " + " | ".join([e for e in errs if e]) if errs else "Tadawul: unknown error"


# -----------------------------------------------------------------------------
# Lazy singleton (PROD SAFE)
# -----------------------------------------------------------------------------
_CLIENT_SINGLETON: Optional[TadawulClient] = None
_CLIENT_LOCK = asyncio.Lock()


def _load_headers() -> Dict[str, str]:
    hdrs: Dict[str, str] = {}
    raw = _safe_str(_get_attr_or_env("TADAWUL_HEADERS_JSON"))
    if raw:
        try:
            obj = json.loads(raw)
            if isinstance(obj, dict):
                hdrs.update({str(k): str(v) for k, v in obj.items()})
        except Exception:
            pass
    return hdrs


async def get_tadawul_client() -> TadawulClient:
    global _CLIENT_SINGLETON
    if _CLIENT_SINGLETON is None:
        async with _CLIENT_LOCK:
            if _CLIENT_SINGLETON is None:
                timeout_sec = _get_float("HTTP_TIMEOUT_SEC", DEFAULT_TIMEOUT_SEC)
                retries = _get_int("HTTP_RETRY_ATTEMPTS", DEFAULT_RETRY_ATTEMPTS)
                _CLIENT_SINGLETON = TadawulClient(
                    quote_url=_safe_str(_get_attr_or_env("TADAWUL_QUOTE_URL")),
                    fundamentals_url=_safe_str(_get_attr_or_env("TADAWUL_FUNDAMENTALS_URL")),
                    headers=_load_headers(),
                    timeout_sec=timeout_sec,
                    retry_attempts=retries,
                )
                logger.info(
                    "Tadawul client init v%s | quote_url=%s | fund_url=%s | timeout=%.1fs | retries=%s",
                    PROVIDER_VERSION,
                    bool(_CLIENT_SINGLETON.quote_url),
                    bool(_CLIENT_SINGLETON.fundamentals_url),
                    timeout_sec,
                    retries,
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
