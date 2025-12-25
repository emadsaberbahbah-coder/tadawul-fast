```python
# core/yahoo_chart_provider.py  (FULL REPLACEMENT)
from __future__ import annotations

"""
core/yahoo_chart_provider.py
===========================================================
Yahoo Chart Provider (NO yfinance) — v1.4.0 (PROD SAFE)

Purpose
- Robust fallback for Yahoo price data (especially .SR) without yfinance.
- Best-effort parse for: price, prev_close, open, high, low, volume, currency, exchange
- Defensive JSON parsing (handles Yahoo "chart.error" responses)
- Retry on 429 / 5xx with jittered backoff
- Import-safe: no engine imports, no side effects.

Returns a dict compatible with a UnifiedQuote-like shape (best effort):
  symbol, market, currency,
  current_price, previous_close, open, day_high, day_low, volume,
  price_change, percent_change,
  exchange,
  data_source, data_quality, last_updated_utc, last_updated_riyadh, error
"""

import asyncio
import json
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Union

import httpx

PROVIDER_VERSION = "1.4.0"

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Riyadh is UTC+3 (no DST)
RIYADH_TZ = timezone(timedelta(hours=3))


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_riyadh_iso() -> str:
    return datetime.now(RIYADH_TZ).isoformat()


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, bool):
            return None
        if isinstance(x, (int, float)):
            v = float(x)
            if v != v:  # NaN
                return None
            return v
        s = str(x).strip()
        if not s or s in {"-", "—", "N/A", "NA", "null", "None"}:
            return None
        s = s.replace(",", "")
        v = float(s)
        if v != v:
            return None
        return v
    except Exception:
        return None


def _safe_int(x: Any) -> Optional[int]:
    v = _safe_float(x)
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


def _last_non_none(series: Any) -> Optional[float]:
    try:
        if not isinstance(series, list):
            return None
        for i in range(len(series) - 1, -1, -1):
            v = _safe_float(series[i])
            if v is not None:
                return v
    except Exception:
        return None
    return None


def _prev_from_series(series: Any) -> Optional[float]:
    """
    Previous value from a series: find last valid, then the one before it.
    Useful when meta.previousClose is missing.
    """
    try:
        if not isinstance(series, list) or len(series) < 2:
            return None
        last = None
        for i in range(len(series) - 1, -1, -1):
            v = _safe_float(series[i])
            if v is None:
                continue
            if last is None:
                last = v
                continue
            return v
    except Exception:
        return None
    return None


def _yahoo_symbol(symbol: str) -> str:
    """
    Yahoo chart endpoint prefers:
      - AAPL (not AAPL.US)
      - Keep .SR
      - Keep special symbols: ^GSPC, EURUSD=X, GC=F
    """
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    if any(ch in s for ch in ("=", "^")):
        return s
    if s.endswith(".US"):
        return s[:-3]
    return s


def _market_for(sym: str) -> str:
    s = (sym or "").upper()
    return "KSA" if s.endswith(".SR") else "GLOBAL"


def _default_currency_for(sym: str) -> Optional[str]:
    return "SAR" if (sym or "").upper().endswith(".SR") else None


def _dq_from(price: Optional[float], prev: Optional[float], high: Optional[float], low: Optional[float]) -> str:
    if price is None:
        return "MISSING"
    # FULL means we have core OHLC-ish + prev close
    if prev is not None and high is not None and low is not None:
        return "FULL"
    return "PARTIAL"


def _base_result(sym: str) -> Dict[str, Any]:
    return {
        "symbol": sym,
        "market": _market_for(sym),
        "currency": _default_currency_for(sym),
        "data_source": "yahoo_chart",
        "provider_version": PROVIDER_VERSION,
        "data_quality": "MISSING",
        "last_updated_utc": _now_utc_iso(),
        "last_updated_riyadh": _now_riyadh_iso(),
        "error": None,
    }


def _extract_chart_error(data: Dict[str, Any]) -> Optional[str]:
    try:
        chart = data.get("chart") or {}
        err = chart.get("error")
        if not err:
            return None
        # Yahoo returns {code, description}
        code = (err.get("code") or "").strip()
        desc = (err.get("description") or "").strip()
        if code and desc:
            return f"{code}: {desc}"
        return code or desc or "yahoo_chart error"
    except Exception:
        return None


def _get0(lst: Any) -> Dict[str, Any]:
    if isinstance(lst, list) and lst:
        x = lst[0]
        return x if isinstance(x, dict) else {}
    return {}


async def _sleep_backoff(attempt: int) -> None:
    # jittered exponential backoff, capped
    base = 0.25 * (2**attempt)
    await asyncio.sleep(min(2.5, base + random.random() * 0.35))


async def yahoo_chart_quote(
    symbol: str,
    *,
    timeout: float = 12.0,
    client: Optional[httpx.AsyncClient] = None,
    interval: str = "1d",
    range_: str = "5d",
    retry_attempts: int = 3,
) -> Dict[str, Any]:
    """
    Minimal, robust Yahoo chart fallback (no yfinance).

    Keys returned (best effort):
      symbol, market, currency,
      current_price, previous_close, open, day_high, day_low, volume,
      price_change, percent_change,
      exchange,
      data_source, data_quality, last_updated_utc, last_updated_riyadh, error
    """
    raw = (symbol or "").strip()
    if not raw:
        out = _base_result("")
        out.update({"data_quality": "MISSING", "error": "Empty symbol"})
        return out

    sym = raw.strip().upper()
    ysym = _yahoo_symbol(sym)

    url = YAHOO_CHART_URL.format(symbol=ysym)
    params = {
        "interval": interval,
        "range": range_,
        "includePrePost": "false",
    }

    close_client = False
    if client is None:
        client = httpx.AsyncClient(
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/json,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.8,ar;q=0.6",
                "Referer": "https://finance.yahoo.com/",
                "Connection": "keep-alive",
            },
            follow_redirects=True,
            timeout=httpx.Timeout(timeout),
            limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
        )
        close_client = True

    try:
        attempts = max(1, int(retry_attempts))
        last_exc: Optional[Exception] = None

        for attempt in range(attempts):
            try:
                r = await client.get(url, params=params)

                # Retry on 429/5xx
                if r.status_code == 429 or (500 <= r.status_code < 600):
                    if attempt < attempts - 1:
                        ra = r.headers.get("Retry-After")
                        if ra and ra.strip().isdigit():
                            await asyncio.sleep(min(2.5, float(ra.strip())))
                        else:
                            await _sleep_backoff(attempt)
                        continue
                    r.raise_for_status()

                r.raise_for_status()

                # Robust JSON parse (handle non-json bodies)
                data: Union[Dict[str, Any], List[Any]]
                try:
                    data = r.json() or {}
                except Exception:
                    txt = (r.text or "").strip()
                    if txt.startswith("{") or txt.startswith("["):
                        data = json.loads(txt)
                    else:
                        data = {}

                if not isinstance(data, dict) or not data:
                    out = _base_result(sym)
                    out.update({"error": "yahoo_chart empty response", "data_quality": "MISSING"})
                    return out

                # Handle Yahoo explicit error
                cerr = _extract_chart_error(data)
                if cerr:
                    out = _base_result(sym)
                    out.update({"error": f"yahoo_chart: {cerr}", "data_quality": "MISSING"})
                    return out

                chart = data.get("chart") or {}
                result0 = _get0(chart.get("result"))

                if not result0:
                    out = _base_result(sym)
                    out.update({"error": "yahoo_chart missing result", "data_quality": "MISSING"})
                    return out

                meta = result0.get("meta") or {}

                indicators = (result0.get("indicators") or {}).get("quote")
                quote0 = _get0(indicators) if indicators is not None else {}

                close_series = quote0.get("close") or []
                open_series = quote0.get("open") or []
                high_series = quote0.get("high") or []
                low_series = quote0.get("low") or []
                vol_series = quote0.get("volume") or []

                # Prefer meta fields when available (more accurate intraday)
                price = _safe_float(meta.get("regularMarketPrice")) or _last_non_none(close_series)

                prev_close = (
                    _safe_float(meta.get("previousClose"))
                    or _safe_float(meta.get("regularMarketPreviousClose"))
                    or _safe_float(meta.get("chartPreviousClose"))
                    or _prev_from_series(close_series)
                )

                opn = _safe_float(meta.get("regularMarketOpen")) or _last_non_none(open_series)
                hi = _safe_float(meta.get("regularMarketDayHigh")) or _last_non_none(high_series)
                lo = _safe_float(meta.get("regularMarketDayLow")) or _last_non_none(low_series)

                vol = _safe_float(meta.get("regularMarketVolume")) or _last_non_none(vol_series)

                currency = meta.get("currency") or _default_currency_for(sym)
                exchange = meta.get("exchangeName") or meta.get("fullExchangeName") or meta.get("exchange")

                chg = None
                pct = None
                if price is not None and prev_close not in (None, 0):
                    chg = price - float(prev_close)
                    pct = (chg / float(prev_close)) * 100.0

                dq = _dq_from(price, prev_close, hi, lo)

                out = _base_result(sym)
                out.update(
                    {
                        "market": _market_for(sym),
                        "currency": currency,
                        "current_price": price,
                        "previous_close": prev_close,
                        "open": opn,
                        "day_high": hi,
                        "day_low": lo,
                        "volume": _safe_int(vol) if vol is not None else None,
                        "price_change": chg,
                        "percent_change": pct,
                        "exchange": exchange,
                        "data_quality": dq,
                        "error": None if dq != "MISSING" else "Yahoo chart returned no price",
                        # helpful debug fields (harmless if ignored)
                        "symbol_yahoo": ysym,
                    }
                )
                return out

            except Exception as exc:
                last_exc = exc
                if attempt < attempts - 1:
                    await _sleep_backoff(attempt)
                    continue

        out = _base_result(sym)
        out.update({"data_quality": "MISSING", "error": f"yahoo_chart error: {last_exc}"})
        return out

    finally:
        if close_client and client is not None:
            try:
                await client.aclose()
            except Exception:
                pass


__all__ = ["yahoo_chart_quote", "YAHOO_CHART_URL", "PROVIDER_VERSION"]
```
