# core/providers/yahoo_chart_provider.py
"""
Yahoo Chart Provider (v1.8.0)
Revised to utilize the Hardened Yahoo Session Manager to avoid 401 blocks.
"""

import logging
import httpx
from typing import Any, Dict, Optional
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
        
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {"range": "1d", "interval": "1m", "includePrePost": "false"}

    try:
        # Ensure session if managed by yahoo_provider
        await session_manager.ensure_session(local_client)
        if session_manager.crumb:
            params["crumb"] = session_manager.crumb

        resp = await local_client.get(url, params=params)
        if resp.status_code != 200:
            return {"error": f"yahoo_chart: {resp.status_code}", "status": "error"}
            
        res = resp.json().get("chart", {}).get("result", [None])[0]
        if not res:
            return {"error": "yahoo_chart: empty_response", "status": "error"}
            
        meta = res.get("meta", {})
        return {
            "price": meta.get("regularMarketPrice"),
            "prev_close": meta.get("previousClose"),
            "change": meta.get("regularMarketPrice", 0) - meta.get("previousClose", 0) if meta.get("previousClose") else None,
            "currency": meta.get("currency"),
            "data_source": "yahoo_chart_hardened",
            "status": "ok"
        }
    except Exception as e:
        logger.error("Yahoo Chart Fetch Error for %s: %s", symbol, e)
        return {"error": str(e), "status": "error"}
    finally:
        if not client:
            await local_client.aclose()

# Compatibility Aliases
async def fetch_enriched_quote_patch(symbol: str, **kwargs):
    return await fetch_quote_patch(symbol, **kwargs)
