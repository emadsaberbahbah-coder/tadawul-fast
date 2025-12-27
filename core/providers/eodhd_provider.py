# core/providers/eodhd_provider.py  (FULL REPLACEMENT)
"""
core/providers/eodhd_provider.py
------------------------------------------------------------
EODHD Provider (GLOBAL primary) — v1.7.2 (ENV-ALIAS + KSA-GUARD HARDENED)

Aligned with your Render env variable names
- ✅ Uses EODHD_API_KEY (preferred in your Render)
- ✅ Also accepts EODHD_API_TOKEN / EODHD_TOKEN (legacy)
- ✅ Correct default base URL:
      https://eodhistoricaldata.com/api
- ✅ Never does network calls at import-time
- ✅ Defensive JSON parsing + non-misleading errors

KSA safety
- ✅ If a KSA-style symbol is passed (e.g., 1120 or 1120.SR), provider returns
  a clear "not supported" message (prevents accidental routing confusion).

Exports (engine discovery)
- fetch_quote_patch
- fetch_enriched_quote_patch
- fetch_quote_and_enrichment_patch

Env vars (supported)
- EODHD_API_KEY (preferred)
- EODHD_API_TOKEN / EODHD_TOKEN (legacy)
- EODHD_BASE_URL (default: https://eodhistoricaldata.com/api)
- EODHD_ENABLE_FUNDAMENTALS (default: true)
- EODHD_TIMEOUT_S (default: 8.5)  (fallbacks to HTTP_TIMEOUT if present)
- EODHD_UA (optional)
"""

from __future__ import annotations

import math
import os
from typing import Any, Dict, Optional, Tuple

import httpx

PROVIDER_VERSION = "1.7.2"
PROVIDER_NAME = "eodhd"

# ✅ Correct default base URL for EODHD
BASE_URL = (os.getenv("EODHD_BASE_URL") or "https://eodhistoricaldata.com/api").rstrip("/")

# Prefer provider-specific timeout; fall back to global HTTP_TIMEOUT if provided; then default.
def _timeout_default() -> float:
    v = (os.getenv("EODHD_TIMEOUT_S") or "").strip()
    if v:
        try:
            return float(v)
        except Exception:
            pass
    v2 = (os.getenv("HTTP_TIMEOUT") or "").strip()
    if v2:
        try:
            return float(v2)
        except Exception:
            pass
    return 8.5


TIMEOUT_S = _timeout_default()

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


# -----------------------------------------------------------------------------
# Env + parsing helpers
# -----------------------------------------------------------------------------
def _token() -> Optional[str]:
    # ✅ Align with your Render name first, then legacy aliases
    for k in ("EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_TOKEN"):
        v = (os.getenv(k) or "").strip()
        if v:
            return v
    return None


def _looks_like_ksa(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    if not s:
        return False
    # common KSA patterns in this project
    if s.endswith(".SR"):
        return True
    # raw numeric Tadawul codes (e.g., 1120)
    if s.isdigit():
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


def _pos_52w(cur: Optional[float], lo: Optional[float], hi: Optional[float]) -> Optional[float]:
    if cur is None or lo is None or hi is None:
        return None
    if hi == lo:
        return None
    try:
        return (cur - lo) / (hi - lo) * 100.0
    except Exception:
        return None


def _pick(d: Any, *keys: str) -> Any:
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d:
            return d.get(k)
    return None


def _base(symbol: str) -> Dict[str, Any]:
    # Shape stays compatible with your merge logic
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
        "free_float": None,               # percent
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
    headers = {"User-Agent": UA, "Accept": "application/json,text/plain,*/*"}
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_S, headers=headers) as client:
            r = await client.get(url, params=params)

            if r.status_code != 200:
                hint = ""
                if r.status_code in (401, 403):
                    hint = " (auth failed: check EODHD_API_KEY)"
                # EODHD often returns helpful JSON even for errors; try to extract a message (without leaking params)
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

    # field variants (defensive)
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

    return {k: v for k, v in patch.items() if v is not None}, None


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

    # market cap
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
    patch["avg_volume_30d"] = _to_float(technicals.get("AverageVolume"))

    # shares outstanding
    so = _to_float(
        _pick(shares, "SharesOutstanding", "SharesOutstandingFloat", "SharesOutstandingEOD")
        or general.get("SharesOutstanding")
    )
    patch["shares_outstanding"] = so

    # free-float % if float shares present
    float_shares = _to_float(_pick(shares, "SharesFloat", "FloatShares", "SharesOutstandingFloat"))
    if so and float_shares and so > 0:
        patch["free_float"] = (float_shares / so) * 100.0

    patch["debt_to_equity"] = _to_float(highlights.get("DebtToEquity"))
    patch["current_ratio"] = _to_float(highlights.get("CurrentRatio"))
    patch["quick_ratio"] = _to_float(highlights.get("QuickRatio"))

    # remove empty strings / None
    clean: Dict[str, Any] = {}
    for k, v in patch.items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        clean[k] = v

    return clean, None


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

    # free-float market cap
    mc = _to_float(out.get("market_cap"))
    ff = _to_float(out.get("free_float"))
    if out.get("free_float_market_cap") is None and mc is not None and ff is not None:
        out["free_float_market_cap"] = mc * (ff / 100.0)


def _quality(out: Dict[str, Any]) -> None:
    # Quote OK if these exist
    keys = ["current_price", "day_high", "day_low", "volume"]
    ok = all(_to_float(out.get(k)) is not None for k in keys)

    # Rich if we have some ID/fundamental
    rich = bool((out.get("name") or "").strip()) or (_to_float(out.get("market_cap")) is not None)

    out["data_quality"] = "FULL" if (ok and rich) else ("OK" if ok else "BAD")

    if out.get("error"):
        out["status"] = "error"


async def _fetch(symbol: str, want_fundamentals: bool = True) -> Dict[str, Any]:
    out = _base(symbol)

    # Prevent misleading calls for KSA symbols
    if _looks_like_ksa(symbol):
        out["status"] = "error"
        out["data_quality"] = "BAD"
        out["error"] = f"{PROVIDER_NAME}: KSA symbols not supported by EODHD (route via KSA providers)"
        return out

    rt_patch, rt_err = await _fetch_realtime(symbol)
    if rt_err:
        out["error"] = f"{PROVIDER_NAME}: {rt_err}"
        _quality(out)
        return out

    out.update(rt_patch)

    if want_fundamentals and ENABLE_FUNDAMENTALS:
        f_patch, f_err = await _fetch_fundamentals(symbol)
        if not f_err:
            # Fill blanks; do not overwrite quote fields unless important
            for k, v in f_patch.items():
                if v is None:
                    continue
                if out.get(k) in (None, "", 0):
                    out[k] = v
                if k in ("market_cap", "high_52w", "low_52w", "shares_outstanding", "free_float"):
                    out[k] = v
        else:
            # Do NOT fail quote if fundamentals fail (but keep a warning)
            out["_warn"] = f_err

    _fill_derived(out)
    _quality(out)
    return out


# -----------------------------------------------------------------------------
# Engine-compatible exported callables (name-based discovery)
# -----------------------------------------------------------------------------
async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _fetch(symbol, want_fundamentals=False)


async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _fetch(symbol, want_fundamentals=True)


async def fetch_quote_and_enrichment_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _fetch(symbol, want_fundamentals=True)
