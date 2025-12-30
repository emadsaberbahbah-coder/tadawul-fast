# core/yahoo_chart_provider.py  (FULL REPLACEMENT)
from __future__ import annotations

"""
core/yahoo_chart_provider.py
===========================================================
Yahoo Chart Provider (NO yfinance) — v1.5.0 (PROD SAFE)

Phase-0 hygiene + hardening
- ✅ Valid Python file (NO markdown fences)
- ✅ Import-safe (no side effects, no engine imports)
- ✅ Defensive parsing for Yahoo chart errors / partial payloads
- ✅ Retries on 429 / 5xx with jittered exponential backoff
- ✅ Adds timestamp fields (best effort) from Yahoo meta/timestamps
- ✅ Optional batch helper with concurrency cap (non-breaking)

Returns a dict compatible with a UnifiedQuote-like shape (best effort):
  symbol, market, currency,
  current_price, previous_close, open, day_high, day_low, volume,
  price_change, percent_change,
  exchange,
  data_source, provider_version, data_quality,
  last_trade_time_utc,
  last_updated_utc, last_updated_riyadh,
  http_status, error
"""

import asyncio
import json
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import httpx

PROVIDER_VERSION = "1.5.0"

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Riyadh is UTC+3 (no DST)
RIYADH_TZ = timezone(timedelta(hours=3))


# -----------------------------------------------------------------------------
# Time helpers
# -----------------------------------------------------------------------------
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _now_utc_iso() -> str:
    return _now_utc().isoformat()


def _now_riyadh_iso() -> str:
    return datetime.now(RIYADH_TZ).isoformat()


def _dt_from_epoch_utc(epoch_seconds: Any) -> Optional[datetime]:
    try:
        v = _safe_float(epoch_seconds)
        if v is None:
            return None
        return datetime.fromtimestamp(float(v), tz=timezone.utc)
    except Exception:
        return None


# -----------------------------------------------------------------------------
# Safe parsers
# -----------------------------------------------------------------------------
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


def _get0(lst: Any) -> Dict[str, Any]:
    if isinstance(lst, list) and lst:
        x = lst[0]
        return x if isinstance(x, dict) else {}
    return {}


def _last_valid_from_series(series: Any, timestamps: Any = None) -> Tuple[Optional[float], Optional[datetime]]:
    """
    Returns (last_value, last_timestamp_utc) scanning from end.
    timestamps is optional list aligned with series.
    """
    try:
        if not isinstance(series, list):
            return None, None
        ts = timestamps if isinstance(timestamps, list) else None
        for i in range(len(series) - 1, -1, -1):
            v = _safe_float(series[i])
            if v is None:
                continue
            dt = None
            if ts is not None and i < len(ts):
                dt = _dt_from_epoch_utc(ts[i])
            return v, dt
    except Exception:
        return None, None
    return None, None


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


# -----------------------------------------------------------------------------
# Symbol normalization
# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
# Output helpers
# -----------------------------------------------------------------------------
def _dq_from(price: Optional[float], prev: Optional[float], high: Optional[float], low: Optional[float]) -> str:
    if price is None:
        return "MISSING"
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
        "last_trade_time_utc": None,
        "last_updated_utc": _now_utc_iso(),
        "last_updated_riyadh": _now_riyadh_iso(),
        "http_status": None,
        "error": None,
    }


def _extract_chart_error(data: Dict[str, Any]) -> Optional[str]:
    try:
        chart = data.get("chart") or {}
        err = chart.get("error")
        if not err:
            return None
        code = (err.get("code") or "").strip()
        desc = (err.get("description") or "").strip()
        if code and desc:
            return f"{code}: {desc}"
        return code or desc or "yahoo_chart error"
    except Exception:
        return None


async def _sleep_backoff(attempt: int) -> None:
    # jittered exponential backoff, capped
    base = 0.25 * (2**attempt)
    await asyncio.sleep(min(2.5, base + random.random() * 0.35))


# -----------------------------------------------------------------------------
# Public: single quote
# -----------------------------------------------------------------------------
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
      data_source, provider_version, data_quality,
      last_trade_time_utc,
      last_updated_utc, last_updated_riyadh,
      http_status, error
    """
    raw = (symbol or "").strip()
    if not raw:
        out = _base_result("")
        out.update({"data_quality": "MISSING", "error": "Empty symbol"})
        return out

    sym = raw.upper()
    ysym = _yahoo_symbol(sym)

    url = YAHOO_CHART_URL.format(symbol=ysym)
    params = {
        "interval": interval,
        "range": range_,
        "includePrePost": "false",
        "events": "div,splits",
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
                http_status = r.status_code

                # Retry on 429/5xx
                if http_status == 429 or (500 <= http_status < 600):
                    if attempt < attempts - 1:
                        ra = r.headers.get("Retry-After")
                        if ra and ra.strip().isdigit():
                            await asyncio.sleep(min(2.5, float(ra.strip())))
                        else:
                            await _sleep_backoff(attempt)
                        continue
                    r.raise_for_status()

                r.raise_for_status()

                # Robust JSON parse
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
                    out.update({"http_status": http_status, "error": "yahoo_chart empty response", "data_quality": "MISSING"})
                    return out

                cerr = _extract_chart_error(data)
                if cerr:
                    out = _base_result(sym)
                    out.update({"http_status": http_status, "error": f"yahoo_chart: {cerr}", "data_quality": "MISSING"})
                    return out

                chart = data.get("chart") or {}
                result0 = _get0(chart.get("result"))
                if not result0:
                    out = _base_result(sym)
                    out.update({"http_status": http_status, "error": "yahoo_chart missing result", "data_quality": "MISSING"})
                    return out

                meta = result0.get("meta") or {}
                timestamps = result0.get("timestamp") or []

                indicators = (result0.get("indicators") or {}).get("quote")
                quote0 = _get0(indicators) if indicators is not None else {}

                close_series = quote0.get("close") or []
                open_series = quote0.get("open") or []
                high_series = quote0.get("high") or []
                low_series = quote0.get("low") or []
                vol_series = quote0.get("volume") or []

                # Prefer meta fields when available (more accurate intraday)
                price = _safe_float(meta.get("regularMarketPrice"))
                if price is None:
                    price, dt_price = _last_valid_from_series(close_series, timestamps)
                else:
                    dt_price = _dt_from_epoch_utc(meta.get("regularMarketTime"))

                prev_close = (
                    _safe_float(meta.get("previousClose"))
                    or _safe_float(meta.get("regularMarketPreviousClose"))
                    or _safe_float(meta.get("chartPreviousClose"))
                    or _prev_from_series(close_series)
                )

                opn = _safe_float(meta.get("regularMarketOpen"))
                if opn is None:
                    opn, _ = _last_valid_from_series(open_series, timestamps)

                hi = _safe_float(meta.get("regularMarketDayHigh"))
                if hi is None:
                    hi, _ = _last_valid_from_series(high_series, timestamps)

                lo = _safe_float(meta.get("regularMarketDayLow"))
                if lo is None:
                    lo, _ = _last_valid_from_series(low_series, timestamps)

                vol = _safe_float(meta.get("regularMarketVolume"))
                if vol is None:
                    vol_val, _ = _last_valid_from_series(vol_series, timestamps)
                    vol = vol_val

                currency = meta.get("currency") or _default_currency_for(sym)
                exchange = meta.get("exchangeName") or meta.get("fullExchangeName") or meta.get("exchange")

                # Change & % change
                chg = _safe_float(meta.get("regularMarketChange"))
                pct = _safe_float(meta.get("regularMarketChangePercent"))

                # If pct from meta looks like fraction (0.01), convert to percent; if already percent (>1.5), keep.
                if pct is not None and abs(pct) <= 1.5:
                    pct = pct * 100.0

                # Compute if missing
                if chg is None or pct is None:
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
                        "http_status": http_status,
                        "error": None if dq != "MISSING" else "Yahoo chart returned no price",
                        "symbol_yahoo": ysym,
                        "last_trade_time_utc": dt_price.isoformat() if isinstance(dt_price, datetime) else None,
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


# -----------------------------------------------------------------------------
# Optional: batch helper (non-breaking)
# -----------------------------------------------------------------------------
async def yahoo_chart_quotes(
    symbols: List[str],
    *,
    timeout: float = 12.0,
    interval: str = "1d",
    range_: str = "5d",
    retry_attempts: int = 3,
    concurrency: int = 6,
) -> List[Dict[str, Any]]:
    """
    Fetch multiple symbols with a shared AsyncClient + concurrency cap.
    Useful for batch endpoints or internal scans.
    """
    sem = asyncio.Semaphore(max(1, int(concurrency)))

    async with httpx.AsyncClient(
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
    ) as client:

        async def _one(sym: str) -> Dict[str, Any]:
            async with sem:
                return await yahoo_chart_quote(
                    sym,
                    timeout=timeout,
                    client=client,
                    interval=interval,
                    range_=range_,
                    retry_attempts=retry_attempts,
                )

        tasks = [asyncio.create_task(_one(s)) for s in (symbols or [])]
        if not tasks:
            return []
        return await asyncio.gather(*tasks)


__all__ = [
    "yahoo_chart_quote",
    "yahoo_chart_quotes",
    "YAHOO_CHART_URL",
    "PROVIDER_VERSION",
]
