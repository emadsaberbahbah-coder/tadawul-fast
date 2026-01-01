# core/providers/eodhd_provider.py
"""
core/providers/eodhd_provider.py
------------------------------------------------------------
EODHD Provider — v1.9.0 (HARDENED + ENGINE-PATCH + KSA-OPTIONAL + CLIENT REUSE)

Key points
- ✅ Uses EODHD_API_KEY (preferred in Render)
- ✅ Also accepts EODHD_API_TOKEN / EODHD_TOKEN (legacy)
- ✅ Default base URL: https://eodhistoricaldata.com/api
- ✅ No network calls at import-time
- ✅ Lazy singleton AsyncClient (reuse connections)
- ✅ Defensive JSON parsing + clear errors
- ✅ Clean PATCH outputs (no null/empty noise unless useful)

KSA handling (IMPORTANT)
- Default: KSA is BLOCKED (safe) to avoid routing confusion.
- Enable KSA via: ALLOW_EODHD_KSA=true
  (also accepts EODHD_ALLOW_KSA=true)

Exports (engine discovery)
- fetch_quote_patch
- fetch_enriched_quote_patch
- fetch_quote_and_enrichment_patch
- aclose_eodhd_client

Env vars (supported)
- EODHD_API_KEY (preferred)
- EODHD_API_TOKEN / EODHD_TOKEN (legacy)
- EODHD_BASE_URL (default: https://eodhistoricaldata.com/api)
- EODHD_ENABLE_FUNDAMENTALS (default: true)
- ALLOW_EODHD_KSA / EODHD_ALLOW_KSA (default: false)
- EODHD_TIMEOUT_S (default: 8.5)  (fallbacks to HTTP_TIMEOUT_SEC / HTTP_TIMEOUT if present)
- EODHD_UA (optional)
"""

from __future__ import annotations

import asyncio
import math
import os
from typing import Any, Dict, Optional, Tuple

import httpx

PROVIDER_VERSION = "1.9.0"
PROVIDER_NAME = "eodhd"

BASE_URL = (os.getenv("EODHD_BASE_URL") or "https://eodhistoricaldata.com/api").rstrip("/")

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


def _timeout_default() -> float:
    for k in ("EODHD_TIMEOUT_S", "HTTP_TIMEOUT_SEC", "HTTP_TIMEOUT"):
        v = (os.getenv(k) or "").strip()
        if v:
            try:
                t = float(v)
                if t > 0:
                    return t
            except Exception:
                pass
    return 8.5


TIMEOUT_S = _timeout_default()

ENABLE_FUNDAMENTALS = (os.getenv("EODHD_ENABLE_FUNDAMENTALS") or "true").strip().lower() in _TRUTHY

ALLOW_EODHD_KSA = (
    (os.getenv("ALLOW_EODHD_KSA") or os.getenv("EODHD_ALLOW_KSA") or "false").strip().lower() in _TRUTHY
)

UA = os.getenv(
    "EODHD_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
)

_CLIENT: Optional[httpx.AsyncClient] = None
_LOCK = asyncio.Lock()


async def _get_client() -> httpx.AsyncClient:
    global _CLIENT
    if _CLIENT is None:
        async with _LOCK:
            if _CLIENT is None:
                headers = {"User-Agent": UA, "Accept": "application/json,text/plain,*/*"}
                timeout = httpx.Timeout(TIMEOUT_S, connect=min(10.0, TIMEOUT_S))
                _CLIENT = httpx.AsyncClient(timeout=timeout, headers=headers, follow_redirects=True)
    return _CLIENT


async def aclose_eodhd_client() -> None:
    global _CLIENT
    c = _CLIENT
    _CLIENT = None
    if c is not None:
        try:
            await c.aclose()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Env + parsing helpers
# ---------------------------------------------------------------------------
def _token() -> Optional[str]:
    for k in ("EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_TOKEN"):
        v = (os.getenv(k) or "").strip()
        if v:
            return v
    return None


def _looks_like_ksa(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    if not s:
        return False
    if s.endswith(".SR"):
        return True
    if s.isdigit():  # raw Tadawul numeric code
        return True
    return False


def _is_nan(x: Any) -> bool:
    try:
        return x is None or (isinstance(x, float) and math.isnan(x))
    except Exception:
        return False


def _to_float(x: Any) -> Optional[float]:
    try:
        if _is_nan(x) or x is None:
            return None
        return float(x)
    except Exception:
        return None


def _pick(d: Any, *keys: str) -> Any:
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d:
            return d.get(k)
    return None


def _pos_52w(cur: Optional[float], lo: Optional[float], hi: Optional[float]) -> Optional[float]:
    if cur is None or lo is None or hi is None:
        return None
    if hi == lo:
        return None
    try:
        return (cur - lo) / (hi - lo) * 100.0
    except Exception:
        return None


def _clean_patch(patch: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (patch or {}).items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        out[k] = v
    return out


def _base_patch(symbol: str) -> Dict[str, Any]:
    # Keep patch aligned with UnifiedQuote keys + your extra fields used in outputs
    return {
        "symbol": symbol,
        "data_source": PROVIDER_NAME,
        "provider_version": PROVIDER_VERSION,
        # IMPORTANT: keep "error" string but do NOT force it into output unless non-empty
        "error": "",
        # identity
        "name": "",
        "exchange": "",
        "currency": "",
        "sector": "",
        "industry": "",
        "sub_sector": "",
        "listing_date": "",
        # quote
        "current_price": None,
        "previous_close": None,
        "open": None,
        "day_high": None,
        "day_low": None,
        "week_52_high": None,
        "week_52_low": None,
        "position_52w_percent": None,
        "volume": None,
        "avg_volume_30d": None,
        "value_traded": None,
        "price_change": None,
        "percent_change": None,
        # fundamentals
        "market_cap": None,
        "shares_outstanding": None,
        "free_float": None,  # %
        "free_float_market_cap": None,
        "eps_ttm": None,
        "pe_ttm": None,
        "pb": None,
        "ps": None,
        "dividend_yield": None,
        "roe": None,
        "roa": None,
        "beta": None,
        # extra ratios (optional; engine can ignore)
        "net_margin": None,
        "debt_to_equity": None,
        "current_ratio": None,
        "quick_ratio": None,
        "ev_ebitda": None,
        "ma20": None,
        "ma50": None,
    }


async def _get_json(url: str, params: Dict[str, Any]) -> Tuple[Optional[dict], Optional[str]]:
    try:
        client = await _get_client()
        r = await client.get(url, params=params)

        if r.status_code != 200:
            hint = ""
            if r.status_code in (401, 403):
                hint = " (auth failed: check EODHD_API_KEY)"
            msg = ""
            try:
                js = r.json()
                if isinstance(js, dict):
                    msg = str(js.get("message") or js.get("error") or "").strip()
            except Exception:
                msg = ""
            if msg:
                return None, f"HTTP {r.status_code}{hint}: {msg}"
            return None, f"HTTP {r.status_code}{hint}"

        try:
            js = r.json()
        except Exception:
            return None, "invalid JSON response"

        if not isinstance(js, dict):
            return None, "unexpected JSON type"
        return js, None

    except Exception as e:
        return None, f"{e.__class__.__name__}: {e}"


async def _fetch_realtime(symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    tok = _token()
    if not tok:
        return {}, "not configured (EODHD_API_KEY)"

    url = f"{BASE_URL}/real-time/{symbol}"
    js, err = await _get_json(url, {"api_token": tok, "fmt": "json"})
    if js is None:
        return {}, f"realtime failed: {err}"

    close = _pick(js, "close", "Close")
    prev = _pick(js, "previous_close", "previousClose", "PreviousClose", "previousClosePrice")
    opn = _pick(js, "open", "Open")
    high = _pick(js, "high", "High")
    low = _pick(js, "low", "Low")
    vol = _pick(js, "volume", "Volume")

    chg = _pick(js, "change", "Change")
    chg_p = _pick(js, "change_p", "ChangePercent", "changePercent", "changePercentages")

    patch: Dict[str, Any] = {
        "current_price": _to_float(close),
        "previous_close": _to_float(prev),
        "open": _to_float(opn),
        "day_high": _to_float(high),
        "day_low": _to_float(low),
        "volume": _to_float(vol),
        "price_change": _to_float(chg),
        "percent_change": _to_float(chg_p),
    }
    return _clean_patch(patch), None


async def _fetch_fundamentals(symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    tok = _token()
    if not tok:
        return {}, "not configured (EODHD_API_KEY)"

    url = f"{BASE_URL}/fundamentals/{symbol}"
    js, err = await _get_json(url, {"api_token": tok, "fmt": "json"})
    if js is None:
        return {}, f"fundamentals failed: {err}"

    general = js.get("General") or {}
    highlights = js.get("Highlights") or {}
    valuation = js.get("Valuation") or {}
    technicals = js.get("Technicals") or {}
    shares = js.get("SharesStats") or {}

    patch: Dict[str, Any] = {}

    patch["name"] = (general.get("Name") or general.get("LongName") or "") or ""
    patch["sector"] = (general.get("Sector") or "") or ""
    patch["industry"] = (general.get("Industry") or "") or ""
    patch["sub_sector"] = (general.get("GicSector") or general.get("GicIndustry") or "") or ""
    patch["currency"] = (general.get("CurrencyCode") or "") or ""
    patch["listing_date"] = (general.get("IPODate") or "") or ""
    patch["exchange"] = (general.get("Exchange") or general.get("ExchangeName") or "") or ""

    mc = _to_float(highlights.get("MarketCapitalization"))
    mc_mln = _to_float(highlights.get("MarketCapitalizationMln"))
    if mc is None and mc_mln is not None:
        mc = mc_mln * 1_000_000.0
    patch["market_cap"] = mc

    patch["eps_ttm"] = _to_float(highlights.get("EarningsShare"))
    patch["pe_ttm"] = _to_float(highlights.get("PERatio"))
    patch["pb"] = _to_float(valuation.get("PriceBookMRQ") or highlights.get("PriceBook"))
    patch["ps"] = _to_float(valuation.get("PriceSalesTTM") or highlights.get("PriceSalesTTM"))
    patch["ev_ebitda"] = _to_float(valuation.get("EnterpriseValueEbitda") or highlights.get("EVToEBITDA"))

    patch["dividend_yield"] = _to_float(highlights.get("DividendYield"))
    patch["roe"] = _to_float(highlights.get("ReturnOnEquityTTM"))
    patch["roa"] = _to_float(highlights.get("ReturnOnAssetsTTM"))
    patch["net_margin"] = _to_float(highlights.get("ProfitMargin"))
    patch["beta"] = _to_float(technicals.get("Beta"))

    patch["week_52_high"] = _to_float(technicals.get("52WeekHigh"))
    patch["week_52_low"] = _to_float(technicals.get("52WeekLow"))

    patch["ma50"] = _to_float(technicals.get("50DayMA"))
    patch["ma20"] = _to_float(technicals.get("20DayMA") or technicals.get("10DayMA"))
    patch["avg_volume_30d"] = _to_float(technicals.get("AverageVolume"))

    so = _to_float(
        _pick(shares, "SharesOutstanding", "SharesOutstandingFloat", "SharesOutstandingEOD")
        or general.get("SharesOutstanding")
    )
    patch["shares_outstanding"] = so

    float_shares = _to_float(_pick(shares, "SharesFloat", "FloatShares", "SharesOutstandingFloat"))
    if so and float_shares and so > 0:
        patch["free_float"] = (float_shares / so) * 100.0

    patch["debt_to_equity"] = _to_float(highlights.get("DebtToEquity"))
    patch["current_ratio"] = _to_float(highlights.get("CurrentRatio"))
    patch["quick_ratio"] = _to_float(highlights.get("QuickRatio"))

    return _clean_patch(patch), None


def _fill_derived(patch: Dict[str, Any]) -> None:
    cur = _to_float(patch.get("current_price"))
    prev = _to_float(patch.get("previous_close"))
    vol = _to_float(patch.get("volume"))

    if patch.get("price_change") is None and cur is not None and prev is not None:
        patch["price_change"] = cur - prev

    if patch.get("percent_change") is None and cur is not None and prev not in (None, 0.0):
        patch["percent_change"] = (cur - prev) / prev * 100.0

    if patch.get("value_traded") is None and cur is not None and vol is not None:
        patch["value_traded"] = cur * vol

    if patch.get("position_52w_percent") is None:
        patch["position_52w_percent"] = _pos_52w(
            cur,
            _to_float(patch.get("week_52_low")),
            _to_float(patch.get("week_52_high")),
        )

    mc = _to_float(patch.get("market_cap"))
    ff = _to_float(patch.get("free_float"))
    if patch.get("free_float_market_cap") is None and mc is not None and ff is not None:
        patch["free_float_market_cap"] = mc * (ff / 100.0)


def _merge_into(dst: Dict[str, Any], src: Dict[str, Any], *, force_keys: Tuple[str, ...] = ()) -> None:
    """
    Merge src into dst:
    - Fill blanks/missing in dst
    - Force overwrite for keys in force_keys
    """
    for k, v in (src or {}).items():
        if v is None:
            continue
        if k in force_keys:
            dst[k] = v
            continue
        if dst.get(k) in (None, "", 0):
            dst[k] = v


async def _fetch(symbol: str, want_fundamentals: bool = True) -> Dict[str, Any]:
    sym = (symbol or "").strip()
    patch = _base_patch(sym)

    # Controlled KSA enablement
    if _looks_like_ksa(sym) and not ALLOW_EODHD_KSA:
        patch["error"] = "warning: KSA blocked for EODHD (set ALLOW_EODHD_KSA=true to enable)"
        return _clean_patch(patch)

    rt_patch, rt_err = await _fetch_realtime(sym)
    if rt_err:
        patch["error"] = f"{PROVIDER_NAME}: {rt_err}"
        return _clean_patch(patch)

    _merge_into(patch, rt_patch)

    # Fundamentals (best-effort)
    if want_fundamentals and ENABLE_FUNDAMENTALS:
        f_patch, f_err = await _fetch_fundamentals(sym)
        if not f_err:
            _merge_into(
                patch,
                f_patch,
                force_keys=(
                    "market_cap",
                    "pe_ttm",
                    "eps_ttm",
                    "week_52_high",
                    "week_52_low",
                    "shares_outstanding",
                    "free_float",
                ),
            )
        else:
            # Do not kill price if fundamentals fail
            patch["error"] = "warning: " + f"{PROVIDER_NAME}: {f_err}"

    _fill_derived(patch)
    return _clean_patch(patch)


# ---------------------------------------------------------------------------
# Engine-compatible exported callables (name-based discovery)
# ---------------------------------------------------------------------------
async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _fetch(symbol, want_fundamentals=False)


async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _fetch(symbol, want_fundamentals=True)


async def fetch_quote_and_enrichment_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _fetch(symbol, want_fundamentals=True)


# Compatibility aliases (some engines try these names)
async def fetch_quote_and_fundamentals_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _fetch(symbol, want_fundamentals=True)


async def fetch_enriched_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _fetch(symbol, want_fundamentals=True)


__all__ = [
    "fetch_quote_patch",
    "fetch_enriched_quote_patch",
    "fetch_quote_and_enrichment_patch",
    "fetch_quote_and_fundamentals_patch",
    "fetch_enriched_patch",
    "aclose_eodhd_client",
    "PROVIDER_VERSION",
    "PROVIDER_NAME",
]
