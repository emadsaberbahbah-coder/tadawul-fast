```python
# core/providers/eodhd_provider.py  (FULL REPLACEMENT)
"""
core/providers/eodhd_provider.py
===============================================================
EODHD Provider / Client (v1.4.0) — PROD SAFE + KSA SAFE (HARDENED)

What’s improved vs v1.3.0 (your “avoid missing data” request)
- ✅ Import-safe settings: NO get_settings() at import-time (prevents startup crashes).
- ✅ Realtime parsing expanded (more key aliases + computed change/% if missing).
- ✅ Fundamentals parsing expanded (more fields mapped into UnifiedQuote-friendly keys).
- ✅ Always includes last_updated_utc + data_source in returned patches (even partial).
- ✅ Better error strings (http status / latency) without raising.
- ✅ Micro-cache kept (TTLCache) but safer cache criteria + cache keys.

KSA-SAFE:
- Refuses .SR and numeric-only symbols by default.
- allow_ksa=True must be explicitly passed to override (engine should NOT do this).

Notes:
- This provider returns PATCH dicts; the engine merges them into UnifiedQuote.
- Extra keys are safe (engine model will ignore if not used).
- Never performs network calls at import-time.
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
from typing import Any, Dict, Optional, Tuple, Union

import httpx
from cachetools import TTLCache

logger = logging.getLogger("core.providers.eodhd_provider")

PROVIDER_VERSION = "1.4.0"

DEFAULT_BASE_URL = "https://eodhd.com/api"
DEFAULT_TIMEOUT_SEC = 25.0
DEFAULT_RETRY_ATTEMPTS = 3

EODHD_RT_PATH = "/real-time/{symbol}"
EODHD_FUND_PATH = "/fundamentals/{symbol}"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_TRUTHY = {"1", "true", "yes", "on", "y", "t"}
_FALSY = {"0", "false", "no", "off", "n", "f"}


# -----------------------------------------------------------------------------
# Settings shim (PROD SAFE) — LAZY (no import-time get_settings call)
# -----------------------------------------------------------------------------
_SETTINGS_OBJ: Optional[Any] = None
_SETTINGS_LOCK = asyncio.Lock()


async def _get_settings_obj() -> Optional[Any]:
    """
    Lazy settings resolve. Never raises. Never blocks startup import.
    """
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


async def _get_bool(name: str, default: bool) -> bool:
    raw = await _get_attr_or_env(name, None)
    if isinstance(raw, bool):
        return raw
    if raw is None:
        return default
    s = str(raw).strip().lower()
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default


_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def _safe_float(val: Any) -> Optional[float]:
    """
    Robust numeric parser:
    - supports Arabic digits
    - strips %, +, currency tokens, commas
    - supports suffix K/M/B (e.g. 1.2B)
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

        mult = 1.0
        m = re.match(r"^(-?\d+(\.\d+)?)([KMB])$", s, re.IGNORECASE)
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


def _is_ksa_symbol(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    return s.endswith(".SR") or s.isdigit()


def _to_eodhd_symbol(symbol: str) -> str:
    """
    EODHD commonly expects:
    - US: AAPL.US
    - Other exchanges: 7203.TSE etc.
    If caller passes 'AAPL' we convert to AAPL.US (engine’s global default).
    """
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    # keep indices/commodities/forex/crypto-ish as-is (engine usually won't route these here)
    if any(ch in s for ch in ("^", "=", "/", ":")):
        return s
    if "-" in s and "." not in s:
        # avoid incorrectly forcing .US on crypto/paired tickers like BTC-USD
        return s
    if "." in s:
        return s
    return f"{s}.US"


def _pick_first_float(d: Dict[str, Any], keys: Tuple[str, ...]) -> Optional[float]:
    for k in keys:
        if k in d and d.get(k) not in (None, "", "null"):
            v = _safe_float(d.get(k))
            if v is not None:
                return v
    return None


# -----------------------------------------------------------------------------
# Client
# -----------------------------------------------------------------------------
class EodhdClient:
    """
    Async EODHD client. Safe-by-default and can be shared across requests.

    Notes:
    - Do NOT instantiate at import-time in app startup paths unless needed.
    - Use get_eodhd_client() (lazy singleton) for typical use.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        timeout_sec: float = DEFAULT_TIMEOUT_SEC,
        retry_attempts: int = DEFAULT_RETRY_ATTEMPTS,
        *,
        rt_ttl_sec: float = 15.0,
        fund_ttl_sec: float = 21600.0,
    ) -> None:
        self.api_key = (api_key or "").strip() or None
        self.base_url = (base_url or DEFAULT_BASE_URL).rstrip("/")
        self.timeout_sec = float(timeout_sec) if timeout_sec and timeout_sec > 0 else DEFAULT_TIMEOUT_SEC
        self.retry_attempts = int(retry_attempts) if retry_attempts and retry_attempts > 0 else DEFAULT_RETRY_ATTEMPTS

        timeout = httpx.Timeout(self.timeout_sec, connect=min(10.0, self.timeout_sec))
        self._client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.8,ar;q=0.6",
            },
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=40),
        )

        # Micro-cache inside provider (engine also caches, but this reduces quota pressure)
        self._rt_cache: TTLCache = TTLCache(maxsize=5000, ttl=max(5.0, float(rt_ttl_sec)))
        self._fund_cache: TTLCache = TTLCache(maxsize=2500, ttl=max(120.0, float(fund_ttl_sec)))

    async def aclose(self) -> None:
        await self._client.aclose()

    def _api_token(self, api_key: Optional[str]) -> Optional[str]:
        k = (api_key or "").strip() or (self.api_key or "").strip()
        return k or None

    # -------------------------------------------------------------------------
    # Low-level HTTP
    # -------------------------------------------------------------------------
    async def _get_json(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Optional[Union[Dict[str, Any], list]], Optional[str], int, int]:
        """
        Returns: (data, error, status_code, latency_ms)
        Never raises.
        """
        last_err: Optional[str] = None
        last_status = 0

        for attempt in range(max(1, self.retry_attempts)):
            t0 = time.perf_counter()
            try:
                r = await self._client.get(url, params=params)
                dt_ms = int((time.perf_counter() - t0) * 1000)
                last_status = int(getattr(r, "status_code", 0) or 0)

                # Retry on rate limit / transient server failures
                if last_status in (429,) or (500 <= last_status < 600):
                    ra = (r.headers.get("Retry-After") or "").strip()
                    if attempt < (self.retry_attempts - 1):
                        if ra.isdigit():
                            await asyncio.sleep(min(3.0, float(ra)))
                        else:
                            await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.35)
                        continue
                    return None, f"http {last_status}", last_status, dt_ms

                if last_status >= 400:
                    # do not retry 4xx by default (except 429 already handled)
                    return None, f"http {last_status}", last_status, dt_ms

                try:
                    return r.json(), None, last_status, dt_ms
                except Exception:
                    txt = (r.text or "").strip()
                    if txt.startswith("{") or txt.startswith("["):
                        try:
                            return json.loads(txt), None, last_status, dt_ms
                        except Exception:
                            return None, "invalid json", last_status, dt_ms
                    return None, "empty/invalid body", last_status, dt_ms

            except Exception as exc:
                dt_ms = int((time.perf_counter() - t0) * 1000)
                last_err = str(exc)
                if attempt < (self.retry_attempts - 1):
                    await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.35)
                    continue
                return None, _clamp(last_err, 500), last_status, dt_ms

        return None, last_err or "request failed", last_status, 0

    # -------------------------------------------------------------------------
    # Public: Realtime
    # -------------------------------------------------------------------------
    async def fetch_realtime_patch(
        self,
        symbol: str,
        api_key: Optional[str] = None,
        *,
        allow_ksa: bool = False,
    ) -> Tuple[Dict[str, Any], Optional[str]]:
        """
        Returns (patch_dict, error_str).
        patch_dict aligns to UnifiedQuote keys.

        IMPORTANT:
        - Always sets: data_source, provider_version, last_updated_utc
        - If error_str is not None, patch may still contain partial fields.
        """
        sym_in = (symbol or "").strip()
        if not sym_in:
            return (
                {"data_source": "eodhd", "provider_version": PROVIDER_VERSION, "last_updated_utc": _utc_iso()},
                "EODHD: empty symbol",
            )

        if _is_ksa_symbol(sym_in) and not allow_ksa:
            return (
                {"data_source": "eodhd", "provider_version": PROVIDER_VERSION, "last_updated_utc": _utc_iso()},
                "EODHD: KSA symbol refused (.SR/numeric not supported)",
            )

        sym = _to_eodhd_symbol(sym_in)
        token = self._api_token(api_key)
        if not token:
            return (
                {"data_source": "eodhd", "provider_version": PROVIDER_VERSION, "last_updated_utc": _utc_iso()},
                "EODHD: missing api key",
            )

        cache_key = f"rt::{sym}"
        hit = self._rt_cache.get(cache_key)
        if isinstance(hit, dict) and hit:
            return dict(hit), None

        url = f"{self.base_url}{EODHD_RT_PATH.format(symbol=sym)}"
        params = {"api_token": token, "fmt": "json"}

        data, err, status, dt_ms = await self._get_json(url, params=params)
        if not data or not isinstance(data, dict):
            patch = {
                "data_source": "eodhd",
                "provider_version": PROVIDER_VERSION,
                "last_updated_utc": _utc_iso(),
                "provider_latency_ms": dt_ms,
                "provider_status_code": status,
            }
            return patch, f"EODHD: realtime empty ({err or 'no data'}) ({dt_ms}ms)"

        # EODHD realtime fields commonly:
        # close, previousClose, open, high, low, volume, change, change_p, timestamp
        # But we also accept aliases defensively.
        cp = _pick_first_float(
            data,
            (
                "close",
                "price",
                "last",
                "last_price",
                "regularMarketPrice",
                "currentPrice",
            ),
        )
        prev = _pick_first_float(
            data,
            (
                "previousClose",
                "previous_close",
                "prevClose",
                "regularMarketPreviousClose",
            ),
        )

        opn = _pick_first_float(data, ("open", "regularMarketOpen"))
        high = _pick_first_float(data, ("high", "dayHigh", "regularMarketDayHigh"))
        low = _pick_first_float(data, ("low", "dayLow", "regularMarketDayLow"))
        vol = _pick_first_float(data, ("volume", "regularMarketVolume"))

        chg = _pick_first_float(data, ("change", "price_change", "regularMarketChange"))
        pct = _pick_first_float(data, ("change_p", "percent_change", "changePercent", "regularMarketChangePercent"))

        # Compute change/% if missing but have cp & prev
        if chg is None and cp is not None and prev is not None:
            chg = cp - prev
        if pct is None and cp is not None and prev not in (None, 0.0):
            try:
                pct = (cp - prev) / prev * 100.0
            except Exception:
                pct = None

        patch: Dict[str, Any] = {
            "current_price": cp,
            "previous_close": prev,
            "open": opn,
            "day_high": high,
            "day_low": low,
            "volume": vol,
            "price_change": chg,
            "percent_change": pct,
            "data_source": "eodhd",
            "provider_version": PROVIDER_VERSION,
            "last_updated_utc": _utc_iso(),
            "provider_latency_ms": dt_ms,
            "provider_status_code": status,
            "provider_symbol": sym,
        }

        # If EODHD returns numeric timestamp
        ts = _safe_float(data.get("timestamp"))
        if ts:
            try:
                patch["eodhd_timestamp_utc"] = datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
            except Exception:
                pass

        # Cache only if we got a usable price
        if patch.get("current_price") is not None:
            self._rt_cache[cache_key] = dict(patch)
            return patch, None

        return patch, f"EODHD: realtime returned no price ({dt_ms}ms)"

    # -------------------------------------------------------------------------
    # Public: Fundamentals
    # -------------------------------------------------------------------------
    async def fetch_fundamentals_patch(
        self,
        symbol: str,
        api_key: Optional[str] = None,
        *,
        allow_ksa: bool = False,
    ) -> Tuple[Dict[str, Any], Optional[str]]:
        """
        Returns (patch_dict, error_str).
        patch_dict aligns to UnifiedQuote keys.

        IMPORTANT:
        - Always sets: data_source, provider_version, last_updated_utc
        - Fundamentals are naturally sparse; we cache partial results.
        """
        sym_in = (symbol or "").strip()
        if not sym_in:
            return (
                {"data_source": "eodhd", "provider_version": PROVIDER_VERSION, "last_updated_utc": _utc_iso()},
                "EODHD: empty symbol",
            )

        if _is_ksa_symbol(sym_in) and not allow_ksa:
            return (
                {"data_source": "eodhd", "provider_version": PROVIDER_VERSION, "last_updated_utc": _utc_iso()},
                "EODHD: KSA symbol refused (.SR/numeric not supported)",
            )

        sym = _to_eodhd_symbol(sym_in)
        token = self._api_token(api_key)
        if not token:
            return (
                {"data_source": "eodhd", "provider_version": PROVIDER_VERSION, "last_updated_utc": _utc_iso()},
                "EODHD: missing api key",
            )

        cache_key = f"fund::{sym}"
        hit = self._fund_cache.get(cache_key)
        if isinstance(hit, dict) and hit:
            return dict(hit), None

        url = f"{self.base_url}{EODHD_FUND_PATH.format(symbol=sym)}"
        params = {"api_token": token, "fmt": "json"}

        data, err, status, dt_ms = await self._get_json(url, params=params)
        if not data or not isinstance(data, dict):
            patch = {
                "data_source": "eodhd",
                "provider_version": PROVIDER_VERSION,
                "last_updated_utc": _utc_iso(),
                "provider_latency_ms": dt_ms,
                "provider_status_code": status,
                "provider_symbol": sym,
            }
            return patch, f"EODHD: fundamentals empty ({err or 'no data'})"

        general = data.get("General") or {}
        highlights = data.get("Highlights") or {}
        valuation = data.get("Valuation") or {}
        technicals = data.get("Technicals") or {}
        shares = data.get("SharesStats") or {}
        splits = data.get("SplitsDividends") or {}

        # Currency / exchange
        currency = _safe_str(general.get("CurrencyCode") or general.get("Currency") or general.get("CurrencyName"))
        exchange = _safe_str(general.get("Exchange") or general.get("PrimaryExchange") or general.get("ExchangeShortName"))
        country = _safe_str(general.get("CountryName") or general.get("Country"))

        # Dividend info
        div_yield = highlights.get("DividendYield")
        div_rate = highlights.get("DividendShare") or highlights.get("DividendPerShare") or splits.get("ForwardAnnualDividendRate")
        payout = highlights.get("PayoutRatio")
        ex_div = splits.get("ExDividendDate")
        div_date = splits.get("DividendDate")

        patch: Dict[str, Any] = {
            # Identity
            "name": _safe_str(general.get("Name")),
            "sector": _safe_str(general.get("Sector") or general.get("GicSector")),
            "industry": _safe_str(general.get("Industry") or general.get("GicIndustry")),
            "sub_industry": _safe_str(general.get("GicSubIndustry")),
            "exchange": exchange,
            "country": country,
            "currency": currency,
            "isin": _safe_str(general.get("ISIN")),
            "cusip": _safe_str(general.get("CUSIP")),
            "website": _safe_str(general.get("WebURL") or general.get("Website")),
            "description": _safe_str(general.get("Description")),
            "listing_date": _safe_str(general.get("IPODate") or general.get("ListingDate")),
            # Size / shares
            "market_cap": _safe_float(highlights.get("MarketCapitalization") or general.get("MarketCapitalization")),
            "shares_outstanding": _safe_float(shares.get("SharesOutstanding") or general.get("SharesOutstanding")),
            "float_shares": _safe_float(shares.get("FloatShares")),
            "shares_short": _safe_float(shares.get("SharesShort")),
            "short_ratio": _safe_float(shares.get("ShortRatio")),
            # Earnings / valuation (best-effort)
            "eps_ttm": _safe_float(highlights.get("EarningsShare")),
            "pe_ttm": _safe_float(highlights.get("PERatio") or valuation.get("TrailingPE")),
            "forward_pe": _safe_float(valuation.get("ForwardPE")),
            "peg": _safe_float(highlights.get("PEGRatio")),
            "pb": _safe_float(valuation.get("PriceBookMRQ") or highlights.get("PriceToBookMRQ")),
            "ps": _safe_float(valuation.get("PriceSalesTTM") or highlights.get("PriceToSalesTTM")),
            "ev_ebitda": _safe_float(valuation.get("EnterpriseValueEbitda")),
            # Dividend
            "dividend_yield": _pct_if_fraction(div_yield),
            "dividend_rate": _safe_float(div_rate),
            "payout_ratio": _pct_if_fraction(payout),
            "ex_dividend_date": _safe_str(ex_div),
            "dividend_date": _safe_str(div_date),
            # Profitability / risk
            "beta": _safe_float(highlights.get("Beta")),
            "net_margin": _pct_if_fraction(highlights.get("ProfitMargin")),
            "operating_margin": _pct_if_fraction(highlights.get("OperatingMarginTTM")),
            "gross_margin": _pct_if_fraction(highlights.get("GrossMarginTTM")),
            "roe": _pct_if_fraction(highlights.get("ReturnOnEquityTTM")),
            "roa": _pct_if_fraction(highlights.get("ReturnOnAssetsTTM")),
            # Financial magnitude
            "revenue_ttm": _safe_float(highlights.get("RevenueTTM")),
            "ebitda": _safe_float(highlights.get("EBITDA")),
            "profit_ttm": _safe_float(highlights.get("ProfitTTM")),
            # Technical anchors
            "high_52w": _safe_float(technicals.get("52WeekHigh")),
            "low_52w": _safe_float(technicals.get("52WeekLow")),
            "ma20": _safe_float(technicals.get("20DayMA")),
            "ma50": _safe_float(technicals.get("50DayMA")),
            "ma200": _safe_float(technicals.get("200DayMA")),
            # Meta
            "data_source": "eodhd",
            "provider_version": PROVIDER_VERSION,
            "last_updated_utc": _utc_iso(),
            "provider_latency_ms": dt_ms,
            "provider_status_code": status,
            "provider_symbol": sym,
        }

        # Cache fundamentals even if partial (expected behavior)
        self._fund_cache[cache_key] = dict(patch)
        return patch, None


# -----------------------------------------------------------------------------
# Lazy singleton (PROD SAFE)
# -----------------------------------------------------------------------------
_CLIENT_SINGLETON: Optional[EodhdClient] = None
_CLIENT_LOCK = asyncio.Lock()


async def get_eodhd_client(api_key: Optional[str] = None) -> EodhdClient:
    """
    Lazy singleton. If api_key is provided and singleton has none, we keep the
    singleton but do not mutate its api_key to avoid surprising cross-tenant behavior.
    Prefer passing api_key per-call.
    """
    global _CLIENT_SINGLETON
    if _CLIENT_SINGLETON is not None:
        return _CLIENT_SINGLETON

    async with _CLIENT_LOCK:
        if _CLIENT_SINGLETON is not None:
            return _CLIENT_SINGLETON

        base_url = (await _get_attr_or_env("EODHD_BASE_URL", DEFAULT_BASE_URL)) or DEFAULT_BASE_URL
        base_url = str(base_url).strip() or DEFAULT_BASE_URL

        timeout_sec = await _get_float("HTTP_TIMEOUT_SEC", DEFAULT_TIMEOUT_SEC)
        retries = await _get_int("HTTP_RETRY_ATTEMPTS", DEFAULT_RETRY_ATTEMPTS)

        rt_ttl = await _get_float("EODHD_RT_TTL_SEC", 15.0)
        fund_ttl = await _get_float("EODHD_FUND_TTL_SEC", 21600.0)

        _CLIENT_SINGLETON = EodhdClient(
            api_key=_safe_str(api_key),
            base_url=_safe_str(base_url) or DEFAULT_BASE_URL,
            timeout_sec=float(timeout_sec),
            retry_attempts=int(retries),
            rt_ttl_sec=float(rt_ttl),
            fund_ttl_sec=float(fund_ttl),
        )

        logger.info(
            "EODHD client init v%s | base=%s | timeout=%.1fs | retries=%s | rt_ttl=%.1fs | fund_ttl=%.1fs",
            PROVIDER_VERSION,
            base_url,
            float(timeout_sec),
            int(retries),
            float(rt_ttl),
            float(fund_ttl),
        )

    return _CLIENT_SINGLETON


async def aclose_eodhd_client() -> None:
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
async def fetch_realtime_patch(symbol: str, api_key: Optional[str]) -> Tuple[Dict[str, Any], Optional[str]]:
    c = await get_eodhd_client()
    return await c.fetch_realtime_patch(symbol, api_key=api_key, allow_ksa=False)


async def fetch_fundamentals_patch(symbol: str, api_key: Optional[str]) -> Tuple[Dict[str, Any], Optional[str]]:
    c = await get_eodhd_client()
    return await c.fetch_fundamentals_patch(symbol, api_key=api_key, allow_ksa=False)


__all__ = [
    "EodhdClient",
    "get_eodhd_client",
    "aclose_eodhd_client",
    "fetch_realtime_patch",
    "fetch_fundamentals_patch",
    "PROVIDER_VERSION",
]
```
