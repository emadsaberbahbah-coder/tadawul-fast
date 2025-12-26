"""
core/providers/eodhd_provider.py
------------------------------------------------------------
EODHD Provider (GLOBAL primary) — v1.6.0 (PROD SAFE)

Key goals
- Provide reliable GLOBAL quote + fundamentals enrichment from EODHD.
- Expose engine-compatible callables:
    fetch_quote_patch
    fetch_enriched_quote_patch
    fetch_quote_and_enrichment_patch
- Never crash startup; no network at import-time.
- Async httpx; defensive parsing.

Env vars
- EODHD_API_TOKEN (or EODHD_TOKEN / EODHD_API_KEY)
- EODHD_BASE_URL (default: https://eodhd.com/api)
- EODHD_ENABLE_FUNDAMENTALS (default: true)
- EODHD_TIMEOUT_S (default: 8.5)
"""

from __future__ import annotations

import os
import math
from typing import Any, Dict, Optional, Tuple

import httpx

PROVIDER_VERSION = "1.6.0"
PROVIDER_NAME = "eodhd"

BASE_URL = (os.getenv("EODHD_BASE_URL") or "https://eodhd.com/api").rstrip("/")
TIMEOUT_S = float(os.getenv("EODHD_TIMEOUT_S") or "8.5")
ENABLE_FUNDAMENTALS = (os.getenv("EODHD_ENABLE_FUNDAMENTALS") or "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
}

UA = os.getenv(
    "EODHD_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
)


def _token() -> Optional[str]:
    for k in ("EODHD_API_TOKEN", "EODHD_TOKEN", "EODHD_API_KEY"):
        v = (os.getenv(k) or "").strip()
        if v:
            return v
    return None


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


def _to_int(x: Any) -> Optional[int]:
    try:
        if _is_nan(x) or x is None:
            return None
        return int(float(x))
    except Exception:
        return None


def _safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b in (None, 0.0):
        return None
    try:
        return a / b
    except Exception:
        return None


def _pos_52w(cur: Optional[float], lo: Optional[float], hi: Optional[float]) -> Optional[float]:
    if cur is None or lo is None or hi is None:
        return None
    if hi == lo:
        return None
    return (cur - lo) / (hi - lo) * 100.0


def _base(symbol: str) -> Dict[str, Any]:
    return {
        "status": "success",
        "symbol": symbol,
        "market": "GLOBAL",
        "currency": "",
        "data_source": PROVIDER_NAME,
        "data_quality": "OK",
        "error": "",
        "provider_version": PROVIDER_VERSION,
        # identity / fundamentals
        "name": "",
        "sector": "",
        "industry": "",
        "sub_sector": "",
        "listing_date": "",
        "shares_outstanding": None,
        "free_float": None,
        "market_cap": None,
        "free_float_market_cap": None,
        # price / liquidity
        "current_price": None,
        "previous_close": None,
        "open": None,
        "day_high": None,
        "day_low": None,
        "high_52w": None,
        "low_52w": None,
        "position_52w_percent": None,
        "volume": None,
        "avg_volume_30d": None,
        "value_traded": None,
        # valuation / ratios (best-effort)
        "eps_ttm": None,
        "pe_ttm": None,
        "pb": None,
        "ps": None,
        "ev_ebitda": None,
        "dividend_yield": None,
        "dividend_rate": None,
        "payout_ratio": None,
        "roe": None,
        "roa": None,
        "net_margin": None,
        "beta": None,
        "debt_to_equity": None,
        "current_ratio": None,
        "quick_ratio": None,
        # technicals (best-effort)
        "ma20": None,
        "ma50": None,
        # derived
        "price_change": None,
        "percent_change": None,
    }


async def _get_json(url: str, params: Dict[str, Any]) -> Tuple[Optional[dict], Optional[str]]:
    try:
        headers = {"User-Agent": UA, "Accept": "application/json,text/plain,*/*"}
        async with httpx.AsyncClient(timeout=TIMEOUT_S, headers=headers) as client:
            r = await client.get(url, params=params)
            if r.status_code != 200:
                return None, f"HTTP {r.status_code}"
            return r.json(), None
    except Exception as e:
        return None, f"{e.__class__.__name__}: {e}"


async def _fetch_realtime(symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    tok = _token()
    if not tok:
        return {}, "not configured (EODHD_API_TOKEN)"

    # EODHD real-time endpoint
    url = f"{BASE_URL}/real-time/{symbol}"
    js, err = await _get_json(url, {"api_token": tok, "fmt": "json"})
    if js is None:
        return {}, f"realtime failed: {err}"

    # typical fields: close, previous_close, open, high, low, volume, change, change_p
    patch: Dict[str, Any] = {}
    patch["current_price"] = _to_float(js.get("close"))
    patch["previous_close"] = _to_float(js.get("previous_close"))
    patch["open"] = _to_float(js.get("open"))
    patch["day_high"] = _to_float(js.get("high"))
    patch["day_low"] = _to_float(js.get("low"))
    patch["volume"] = _to_float(js.get("volume"))

    # sometimes change/change_p present
    patch["price_change"] = _to_float(js.get("change"))
    patch["percent_change"] = _to_float(js.get("change_p"))

    return patch, None


def _pick(d: dict, *keys: str) -> Any:
    for k in keys:
        if isinstance(d, dict) and k in d:
            return d.get(k)
    return None


async def _fetch_fundamentals(symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    tok = _token()
    if not tok:
        return {}, "not configured (EODHD_API_TOKEN)"

    url = f"{BASE_URL}/fundamentals/{symbol}"
    js, err = await _get_json(url, {"api_token": tok, "fmt": "json"})
    if js is None:
        return {}, f"fundamentals failed: {err}"

    patch: Dict[str, Any] = {}

    general = js.get("General") or {}
    highlights = js.get("Highlights") or {}
    valuation = js.get("Valuation") or {}
    technicals = js.get("Technicals") or {}
    shares = js.get("SharesStats") or {}

    patch["name"] = (general.get("Name") or general.get("LongName") or "") or ""
    patch["sector"] = (general.get("Sector") or "") or ""
    patch["industry"] = (general.get("Industry") or "") or ""
    patch["sub_sector"] = (general.get("GicSector") or general.get("GicIndustry") or "") or ""
    patch["currency"] = (general.get("CurrencyCode") or "") or ""

    patch["listing_date"] = (general.get("IPODate") or "") or ""

    patch["market_cap"] = _to_float(highlights.get("MarketCapitalization") or highlights.get("MarketCapitalizationMln"))
    patch["eps_ttm"] = _to_float(highlights.get("EarningsShare"))
    patch["pe_ttm"] = _to_float(highlights.get("PERatio"))
    patch["pb"] = _to_float(valuation.get("PriceBookMRQ") or highlights.get("PriceBook"))
    patch["ps"] = _to_float(valuation.get("PriceSalesTTM") or highlights.get("PriceSalesTTM"))
    patch["ev_ebitda"] = _to_float(valuation.get("EnterpriseValueEbitda") or highlights.get("EVToEBITDA"))

    patch["dividend_yield"] = _to_float(highlights.get("DividendYield"))  # often already %
    patch["dividend_rate"] = _to_float(highlights.get("DividendShare"))
    patch["payout_ratio"] = _to_float(highlights.get("PayoutRatio"))

    patch["roe"] = _to_float(highlights.get("ReturnOnEquityTTM"))
    patch["roa"] = _to_float(highlights.get("ReturnOnAssetsTTM"))
    patch["net_margin"] = _to_float(highlights.get("ProfitMargin"))

    patch["beta"] = _to_float(technicals.get("Beta"))

    patch["high_52w"] = _to_float(technicals.get("52WeekHigh"))
    patch["low_52w"] = _to_float(technicals.get("52WeekLow"))

    patch["ma50"] = _to_float(technicals.get("50DayMA"))
    patch["ma20"] = _to_float(technicals.get("20DayMA") or technicals.get("10DayMA"))

    patch["shares_outstanding"] = _to_float(
        _pick(shares, "SharesOutstanding", "SharesOutstandingFloat", "SharesOutstandingEOD")
        or general.get("SharesOutstanding")
    )

    patch["avg_volume_30d"] = _to_float(technicals.get("AverageVolume"))

    # debt/liquidity if present
    patch["debt_to_equity"] = _to_float(highlights.get("DebtToEquity"))
    patch["current_ratio"] = _to_float(highlights.get("CurrentRatio"))
    patch["quick_ratio"] = _to_float(highlights.get("QuickRatio"))

    return patch, None


def _fill_derived(out: Dict[str, Any]) -> None:
    cur = _to_float(out.get("current_price"))
    prev = _to_float(out.get("previous_close"))
    vol = _to_float(out.get("volume"))

    if out.get("price_change") is None and cur is not None and prev is not None:
        out["price_change"] = cur - prev

    if out.get("percent_change") is None and cur is not None and prev not in (None, 0.0):
        out["percent_change"] = (cur - prev) / prev * 100.0

    if out.get("value_traded") is None and cur is not None and vol is not None:
        out["value_traded"] = cur * vol

    if out.get("position_52w_percent") is None:
        out["position_52w_percent"] = _pos_52w(cur, _to_float(out.get("low_52w")), _to_float(out.get("high_52w")))


def _quality(out: Dict[str, Any]) -> None:
    # FULL when key quote fields exist + at least a name or market_cap
    keys = ["current_price", "previous_close", "day_high", "day_low", "volume"]
    ok = all(_to_float(out.get(k)) is not None for k in keys)
    rich = bool((out.get("name") or "").strip()) or (_to_float(out.get("market_cap")) is not None)
    out["data_quality"] = "FULL" if (ok and rich) else ("OK" if ok else "BAD")
    if out.get("error"):
        out["status"] = "error"


async def _fetch(symbol: str, want_fundamentals: bool = True) -> Dict[str, Any]:
    out = _base(symbol)

    rt_patch, rt_err = await _fetch_realtime(symbol)
    if rt_err:
        out["error"] = f"{PROVIDER_NAME}: {rt_err}"
        _quality(out)
        return out

    out.update({k: v for k, v in rt_patch.items() if v is not None})

    if want_fundamentals and ENABLE_FUNDAMENTALS:
        f_patch, f_err = await _fetch_fundamentals(symbol)
        if not f_err:
            # only fill blanks / None
            for k, v in f_patch.items():
                if v is None:
                    continue
                if out.get(k) in (None, "", 0) or k in ("market_cap", "high_52w", "low_52w"):
                    out[k] = v
        else:
            # do NOT fail quote if fundamentals fail
            out.setdefault("_warn", f_err)

    _fill_derived(out)
    _quality(out)
    return out


# ----------------------------------------------------------------------
# Engine-compatible exported callables (name-based discovery)
# ----------------------------------------------------------------------
async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _fetch(symbol, want_fundamentals=False)


async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _fetch(symbol, want_fundamentals=True)


async def fetch_quote_and_enrichment_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _fetch(symbol, want_fundamentals=True)
