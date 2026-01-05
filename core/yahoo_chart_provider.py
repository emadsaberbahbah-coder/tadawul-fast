# core/providers/yahoo_chart_provider.py
"""
Yahoo Chart Provider (v1.8.1)
Revised to utilize the Hardened Yahoo Session Manager to avoid 401 blocks.
Includes history fetching and client management.
"""

import logging
import httpx
from typing import Any, Dict, Optional, List
from core.providers.yahoo_provider import session_manager

logger = logging.getLogger("core.providers.yahoo_chart")

async def fetch_quote_patch(symbol: str, client: Optional[httpx.AsyncClient] = None) -> Dict[str, Any]:
    """
    Fetch price data via the Yahoo v8 chart endpoint.
    Often more resilient to blocks than the v7 quote endpoint.
    """
    # Reuse or create client
    local_client = client or httpx.AsyncClient(headers={"User-Agent": session_manager.user_agent}, timeout=10)
    
    # KSA Normalization
    if symbol.isdigit() and len(symbol) == 4:
        symbol = f"{symbol}.SR"
    elif not symbol.endswith(".SR") and not symbol.endswith(".SA") and symbol.isdigit():
        symbol = f"{symbol}.SR"
        
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {"range": "1d", "interval": "1m", "includePrePost": "false"}

    try:
        # Ensure session if managed by yahoo_provider
        await session_manager.ensure_session(local_client)
        if session_manager.crumb:
            params["crumb"] = session_manager.crumb

        resp = await local_client.get(url, params=params)
        if resp.status_code != 200:
            return {"error": f"yahoo_chart: {resp.status_code}", "status": "error", "symbol": symbol}
            
        res = resp.json().get("chart", {}).get("result", [None])[0]
        if not res:
            return {"error": "yahoo_chart: empty_response", "status": "error", "symbol": symbol}
            
        meta = res.get("meta", {})
        return {
            "price": meta.get("regularMarketPrice"),
            "prev_close": meta.get("previousClose"),
            "change": meta.get("regularMarketPrice", 0) - meta.get("previousClose", 0) if meta.get("previousClose") else None,
            "currency": meta.get("currency"),
            "data_source": "yahoo_chart_hardened",
            "status": "ok",
            "symbol": symbol
        }
    except Exception as e:
        logger.error("Yahoo Chart Fetch Error for %s: %s", symbol, e)
        return {"error": str(e), "status": "error", "symbol": symbol}
    finally:
        if not client:
            await local_client.aclose()

async def fetch_price_history(
    symbol: str, 
    period: str = "1mo", 
    interval: str = "1d", 
    client: Optional[httpx.AsyncClient] = None
) -> List[Dict[str, Any]]:
    """
    Fetch historical price data for technical analysis (MAs, RSI, etc).
    """
    local_client = client or httpx.AsyncClient(headers={"User-Agent": session_manager.user_agent}, timeout=15)
    
    if symbol.isdigit() and len(symbol) == 4:
        symbol = f"{symbol}.SR"

    # Yahoo ranges: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {"range": period, "interval": interval}

    try:
        await session_manager.ensure_session(local_client)
        if session_manager.crumb:
            params["crumb"] = session_manager.crumb

        resp = await local_client.get(url, params=params)
        if resp.status_code != 200:
            return []

        data = resp.json().get("chart", {}).get("result", [None])[0]
        if not data:
            return []

        timestamps = data.get("timestamp", [])
        indicators = data.get("indicators", {}).get("quote", [{}])[0]
        
        history = []
        for i in range(len(timestamps)):
            history.append({
                "timestamp": timestamps[i],
                "close": indicators.get("close", [])[i],
                "open": indicators.get("open", [])[i],
                "high": indicators.get("high", [])[i],
                "low": indicators.get("low", [])[i],
                "volume": indicators.get("volume", [])[i],
            })
        return history
    except Exception as e:
        logger.error("History fetch error for %s: %s", symbol, e)
        return []
    finally:
        if not client:
            await local_client.aclose()

async def aclose_yahoo_chart_client():
    """Best-effort cleanup of global state if any."""
    pass

# Compatibility Aliases
async def fetch_enriched_quote_patch(symbol: str, **kwargs):
    return await fetch_quote_patch(symbol, **kwargs)

async def fetch_quote_and_enrichment_patch(symbol: str, **kwargs):
    return await fetch_quote_patch(symbol, **kwargs)
