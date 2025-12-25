```python
# core/yahoo_chart_provider.py
from __future__ import annotations

import asyncio
import json
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Union

import httpx

# =============================================================================
# Yahoo Chart Provider (NO yfinance)
# - Robust fallback for .SR (fixes yfinance KeyError('currentTradingPeriod') cases)
# - Best-effort parse for: price, prev_close, open, high, low, volume, currency, exchange
# =============================================================================

PROVIDER_VERSION = "1.3.0"

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
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if not s or s in {"-", "—", "N/A", "NA", "null", "None"}:
            return None
        s = s.replace(",", "")
        return float(s)
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
    Returns a dict compatible with your UnifiedQuote-like shape.

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
            },
            follow_redirects=True,
            timeout=httpx.Timeout(timeout),
        )
        close_client = True

    try:
        # Light retry for transient failures
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
                            await asyncio.sleep(min(2.0, float(ra.strip())))
                        else:
                            await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.35)
                        continue
                    r.raise_for_status()

                r.raise_for_status()

                # Robust JSON parse
                data: Union[Dict[str, Any], List[Any]]
                try:
                    data = r.json() or {}
                except Exception:
                    txt = (r.text or "").strip()
                    data = json.loads(txt) if (txt.startswith("{") or txt.startswith("[")) else {}

                if not isinstance(data, dict) or not data:
                    out = _base_result(sym)
                    out.update({"error": "yahoo_chart empty response", "data_quality": "MISSING"})
                    return out

                result = (((data.get("chart") or {}).get("result")) or [None])[0] or {}
                meta = result.get("meta") or {}

                indicators = (result.get("indicators") or {}).get("quote") or [{}]
                quote0 = indicators[0] or {}

                close_series = quote0.get("close") or []
                open_series = quote0.get("open") or []
                high_series = quote0.get("high") or []
                low_series = quote0.get("low") or []
                vol_series = quote0.get("volume") or []

                # Price: meta first, else last close
                price = _safe_float(meta.get("regularMarketPrice")) or _last_non_none(close_series)

                # Previous close: meta first, else derived from close series
                prev_close = (
                    _safe_float(meta.get("previousClose"))
                    or _safe_float(meta.get("chartPreviousClose"))
                    or _prev_from_series(close_series)
                )

                opn = _last_non_none(open_series)
                hi = _last_non_none(high_series)
                lo = _last_non_none(low_series)

                vol = _safe_float(meta.get("regularMarketVolume")) or _last_non_none(vol_series)

                currency = meta.get("currency") or _default_currency_for(sym)
                exchange = meta.get("exchangeName") or meta.get("fullExchangeName")

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
                        "volume": vol,
                        "price_change": chg,
                        "percent_change": pct,
                        "exchange": exchange,
                        "data_quality": dq,
                        "error": None if dq != "MISSING" else "Yahoo chart returned no price",
                    }
                )
                return out

            except Exception as exc:
                last_exc = exc
                if attempt < attempts - 1:
                    await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.35)
                    continue

        out = _base_result(sym)
        out.update({"data_quality": "MISSING", "error": f"yahoo_chart error: {last_exc}"})
        return out

    finally:
        if close_client and client is not None:
            await client.aclose()


__all__ = ["yahoo_chart_quote", "YAHOO_CHART_URL", "PROVIDER_VERSION"]
```
