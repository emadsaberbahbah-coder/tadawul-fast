# core/yahoo_chart_provider.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

import httpx


YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def yahoo_chart_quote(
    symbol: str,
    *,
    timeout: float = 12.0,
    client: Optional[httpx.AsyncClient] = None,
) -> Dict[str, Any]:
    """
    Minimal, robust Yahoo fallback (no yfinance).
    Returns a dict compatible with your UnifiedQuote-like shape.

    Works well for .SR tickers where yfinance may throw KeyError('currentTradingPeriod').
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        return {"symbol": "", "data_quality": "MISSING", "data_source": "yahoo_chart", "error": "Empty symbol"}

    url = YAHOO_CHART_URL.format(symbol=sym)
    params = {
        "interval": "1d",
        "range": "5d",
        "includePrePost": "false",
    }

    close_client = False
    if client is None:
        client = httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"})
        close_client = True

    try:
        r = await client.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        data = r.json() or {}

        result = (((data.get("chart") or {}).get("result")) or [None])[0] or {}
        meta = result.get("meta") or {}
        indicators = (result.get("indicators") or {}).get("quote") or [{}]
        quote0 = indicators[0] or {}

        price = _to_float(meta.get("regularMarketPrice"))
        prev_close = _to_float(meta.get("previousClose")) or _to_float(meta.get("chartPreviousClose"))

        # Try volume: meta first, then last item from series
        vol = _to_float(meta.get("regularMarketVolume"))
        if vol is None:
            vols = quote0.get("volume") or []
            if isinstance(vols, list) and vols:
                vol = _to_float(vols[-1])

        currency = meta.get("currency")
        exchange = meta.get("exchangeName") or meta.get("fullExchangeName")

        pct = None
        chg = None
        if price is not None and prev_close not in (None, 0):
            chg = price - float(prev_close)
            pct = (chg / float(prev_close)) * 100.0

        dq = "MISSING"
        if price is not None and prev_close is not None:
            dq = "FULL"
        elif price is not None:
            dq = "PARTIAL"

        return {
            "symbol": sym,
            "market": "KSA" if sym.endswith(".SR") else None,
            "currency": currency or ("SAR" if sym.endswith(".SR") else None),
            "current_price": price,
            "previous_close": prev_close,
            "price_change": chg,
            "percent_change": pct,
            "volume": vol,
            "exchange": exchange,
            "data_source": "yahoo_chart",
            "data_quality": dq,
            "last_updated_utc": _now_utc_iso(),
            "error": None if dq != "MISSING" else "Yahoo chart returned no price",
        }

    except Exception as exc:
        return {
            "symbol": sym,
            "market": "KSA" if sym.endswith(".SR") else None,
            "currency": "SAR" if sym.endswith(".SR") else None,
            "data_source": "yahoo_chart",
            "data_quality": "MISSING",
            "last_updated_utc": _now_utc_iso(),
            "error": f"yahoo_chart error: {exc}",
        }

    finally:
        if close_client:
            await client.aclose()
