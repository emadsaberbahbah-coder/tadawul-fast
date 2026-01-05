# core/providers/yahoo_chart_provider.py
"""
Hardened Yahoo Quote/Chart Provider (v1.8.5)
------------------------------------------------------------
PROD SAFE + SESSION-HARDENED + KSA-SAFE + ENGINE-ALIGNED

What's New in v1.8.5:
- ✅ Utilizes central YahooSessionManager from core.providers.yahoo_provider.
- ✅ Resolves 401 Unauthorized errors via Crumb/Cookie persistence.
- ✅ Robust Fallback: Attempts v7/quote first (rich data); falls back to v8/chart.
- ✅ History Support: fetch_price_history for Technical Analysis (MAs, RSI).
- ✅ 52W Guards: Specifically handles Yahoo's "0.0" low-price bug for KSA stocks.
- ✅ Derived Fields: Automatically computes price_change, percent_change, and value_traded.

Exports:
- fetch_quote_patch (Primary engine entry point)
- fetch_price_history (For technical analytics)
- fetch_enriched_quote_patch / fetch_quote_and_enrichment_patch (Aliases)
"""

import asyncio
import logging
import httpx
import math
from typing import Any, Dict, Optional, List, Tuple
from core.providers.yahoo_provider import session_manager, fetch_yahoo_quote_hardened

logger = logging.getLogger("core.providers.yahoo_chart")

PROVIDER_NAME = "yahoo_chart_hardened"
PROVIDER_VERSION = "1.8.5"

# --- Internal Helpers ---

def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None or isinstance(x, bool): return None
        v = float(x)
        return v if not (math.isnan(v) or math.isinf(v)) else None
    except: return None

def _last_non_null(vals: List[Any]) -> Optional[float]:
    if not vals: return None
    for v in reversed(vals):
        f = _to_float(v)
        if f is not None: return f
    return None

def _normalize_symbol(symbol: str) -> str:
    s = symbol.strip().upper()
    if s.isdigit() and len(s) == 4:
        return f"{s}.SR"
    return s

def _fill_derived(p: Dict[str, Any]) -> None:
    cur = p.get("current_price")
    prev = p.get("previous_close")
    vol = p.get("volume")

    if p.get("price_change") is None and cur is not None and prev is not None:
        p["price_change"] = cur - prev

    if p.get("percent_change") is None and cur is not None and prev:
        p["percent_change"] = (cur - prev) / prev * 100.0

    if p.get("value_traded") is None and cur is not None and vol is not None:
        p["value_traded"] = cur * vol

def _guard_52w(p: Dict[str, Any]) -> None:
    """Yahoo often returns 0.0 for 52W low on KSA stocks during market gaps."""
    cur = p.get("current_price")
    lo = p.get("week_52_low")
    if lo is not None and lo == 0.0 and cur is not None and cur > 1.0:
        p["week_52_low"] = None

# --- Core Logic ---

async def fetch_quote_patch(symbol: str, client: Optional[httpx.AsyncClient] = None) -> Dict[str, Any]:
    """
    Unified fetcher: v7/quote (hardened) -> Fallback to v8/chart.
    Returns a 'patch' dictionary for DataEngineV2.
    """
    norm_symbol = _normalize_symbol(symbol)
    local_client = client or httpx.AsyncClient(headers={"User-Agent": session_manager.user_agent}, timeout=12)
    
    # 1. Attempt Hardened v7/quote (Best data)
    patch, err = await fetch_yahoo_quote_hardened(norm_symbol, local_client)
    
    # 2. If v7 fails (401 or empty), attempt v8/chart (More resilient)
    if not patch or err:
        logger.info("v7/quote failed for %s (%s), trying v8/chart fallback", norm_symbol, err)
        chart_patch = await _fetch_v8_chart_data(norm_symbol, local_client)
        if chart_patch:
            if not patch: patch = {}
            patch.update(chart_patch)
            patch["data_source"] = f"{PROVIDER_NAME}_fallback"

    if not patch:
        return {"error": err or "No data available", "status": "error", "symbol": norm_symbol}

    # Finalize enrichment
    patch["symbol"] = norm_symbol
    patch.setdefault("data_source", PROVIDER_NAME)
    _guard_52w(patch)
    _fill_derived(patch)
    
    if not client:
        await local_client.aclose()
        
    return {k: v for k, v in patch.items() if v is not None}

async def _fetch_v8_chart_data(symbol: str, client: httpx.AsyncClient) -> Dict[str, Any]:
    """Internal fallback for v8 chart endpoint."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {"range": "1d", "interval": "1m"}
    
    try:
        await session_manager.ensure_session(client)
        if session_manager.crumb:
            params["crumb"] = session_manager.crumb
            
        resp = await client.get(url, params=params)
        if resp.status_code != 200: return {}
        
        res = resp.json().get("chart", {}).get("result", [None])[0]
        if not res: return {}
        
        meta = res.get("meta", {})
        return {
            "current_price": _to_float(meta.get("regularMarketPrice")),
            "previous_close": _to_float(meta.get("previousClose")) or _to_float(meta.get("chartPreviousClose")),
            "currency": meta.get("currency"),
            "week_52_high": _to_float(meta.get("fiftyTwoWeekHigh")),
            "week_52_low": _to_float(meta.get("fiftyTwoWeekLow")),
        }
    except:
        return {}

async def fetch_price_history(
    symbol: str, 
    period: str = "1mo", 
    interval: str = "1d", 
    client: Optional[httpx.AsyncClient] = None
) -> List[Dict[str, Any]]:
    """
    Historical OHLCV fetcher for technical indicators (MAs, RSI).
    """
    norm_symbol = _normalize_symbol(symbol)
    local_client = client or httpx.AsyncClient(headers={"User-Agent": session_manager.user_agent}, timeout=15)

    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{norm_symbol}"
    params = {"range": period, "interval": interval}

    try:
        await session_manager.ensure_session(local_client)
        if session_manager.crumb:
            params["crumb"] = session_manager.crumb

        resp = await local_client.get(url, params=params)
        if resp.status_code != 200: return []

        data = resp.json().get("chart", {}).get("result", [None])[0]
        if not data: return []

        timestamps = data.get("timestamp", [])
        indicators = data.get("indicators", {}).get("quote", [{}])[0]
        adj_close = data.get("indicators", {}).get("adjclose", [{}])[0].get("adjclose", [])
        
        history = []
        for i in range(len(timestamps)):
            close_val = _to_float(indicators.get("close", [])[i])
            if close_val is None: continue # Skip missing data points
            
            history.append({
                "timestamp": timestamps[i],
                "close": close_val,
                "adj_close": _to_float(adj_close[i]) if i < len(adj_close) else close_val,
                "open": _to_float(indicators.get("open", [])[i]),
                "high": _to_float(indicators.get("high", [])[i]),
                "low": _to_float(indicators.get("low", [])[i]),
                "volume": _to_float(indicators.get("volume", [])[i]),
            })
        return history
    except Exception as e:
        logger.error("History fetch error for %s: %s", symbol, e)
        return []
    finally:
        if not client:
            await local_client.aclose()

# --- Compatibility Aliases ---
async def fetch_enriched_quote_patch(symbol: str, **kwargs):
    return await fetch_quote_patch(symbol, **kwargs)

async def fetch_quote_and_enrichment_patch(symbol: str, **kwargs):
    return await fetch_quote_patch(symbol, **kwargs)

async def aclose_yahoo_client():
    pass

__all__ = [
    "fetch_quote_patch",
    "fetch_price_history",
    "fetch_enriched_quote_patch",
    "fetch_quote_and_enrichment_patch",
    "aclose_yahoo_client"
]
