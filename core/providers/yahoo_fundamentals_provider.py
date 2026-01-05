# core/providers/yahoo_fundamentals_provider.py
"""
Hardened Yahoo Fundamentals Provider (v1.5.0)
NO yfinance. PROD SAFE.
Utilizes central YahooSessionManager for crumb/cookie persistence.
"""

import asyncio
import logging
import httpx
import math
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, List
from core.providers.yahoo_provider import session_manager

logger = logging.getLogger("core.providers.yahoo_fundamentals")

# --- Configuration & Constants ---
MODULES = "price,summaryDetail,defaultKeyStatistics,financialData"
PROVIDER_NAME = "yahoo_fundamentals_hardened"

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None or isinstance(x, bool): return None
        if isinstance(x, (int, float)):
            v = float(x)
            return v if not (math.isnan(v) or math.isinf(v)) else None
        s = str(x).strip().replace(",", "")
        if not s or s in {"-", "N/A", "null"}: return None
        v = float(s)
        return v if not (math.isnan(v) or math.isinf(v)) else None
    except: return None

def _get_raw(obj: Any) -> Optional[float]:
    """Extract raw value from Yahoo's {raw, fmt} structure."""
    if isinstance(obj, dict): return _safe_float(obj.get("raw"))
    return _safe_float(obj)

def _to_percent(v: Optional[float]) -> Optional[float]:
    """Heuristic: converts fractions (0.12) to percent (12.0)."""
    if v is None: return None
    # If it's a small decimal, treat as fraction
    return v * 100.0 if abs(v) <= 1.5 else v

async def fetch_fundamentals_patch(symbol: str, client: Optional[httpx.AsyncClient] = None) -> Dict[str, Any]:
    """
    Fetch fundamental data and return a 'patch' dictionary for DataEngineV2.
    """
    # Use provided client or create temporary
    local_client = client or httpx.AsyncClient(headers={"User-Agent": session_manager.user_agent}, timeout=15)
    
    # Normalize KSA Symbols
    norm_symbol = symbol.strip().upper()
    if norm_symbol.isdigit() and len(norm_symbol) == 4:
        norm_symbol = f"{norm_symbol}.SR"

    # 1. Ensure Session (Crumb/Cookie)
    await session_manager.ensure_session(local_client)
    if not session_manager.crumb:
        return {"error": "yahoo_fund: session_failed", "status": "error"}

    # 2. Build Request
    url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{norm_symbol}"
    params = {
        "modules": MODULES,
        "crumb": session_manager.crumb,
        "lang": "en-US",
        "region": "SA" if ".SR" in norm_symbol else "US"
    }

    try:
        resp = await local_client.get(url, params=params)
        if resp.status_code != 200:
            return {"error": f"yahoo_fund: HTTP_{resp.status_code}", "status": "error"}
        
        raw_data = resp.json().get("quoteSummary", {}).get("result", [{}])[0]
        if not raw_data:
            return {"error": "yahoo_fund: empty_result", "status": "error"}

        # Extraction layers
        price = raw_data.get("price", {})
        summ = raw_data.get("summaryDetail", {})
        stats = raw_data.get("defaultKeyStatistics", {})
        fin = raw_data.get("financialData", {})

        # Build clean patch
        patch = {
            "symbol": norm_symbol,
            "currency": price.get("currency"),
            "current_price": _get_raw(price.get("regularMarketPrice")),
            "market_cap": _get_raw(price.get("marketCap")) or _get_raw(summ.get("marketCap")),
            "shares_outstanding": _get_raw(stats.get("sharesOutstanding")),
            
            # Valuation
            "eps_ttm": _get_raw(stats.get("trailingEps")),
            "pe_ttm": _get_raw(summ.get("trailingPE")) or _get_raw(stats.get("trailingPE")),
            "forward_eps": _get_raw(stats.get("forwardEps")),
            "forward_pe": _get_raw(summ.get("forwardPE")),
            "pb": _get_raw(stats.get("priceToBook")),
            "ps": _get_raw(summ.get("priceToSalesTrailing12Months")),
            "ev_ebitda": _get_raw(stats.get("enterpriseToEbitda")),
            
            # Dividends & Returns
            "dividend_yield": _to_percent(_get_raw(summ.get("dividendYield"))),
            "dividend_rate": _get_raw(summ.get("dividendRate")),
            "payout_ratio": _to_percent(_get_raw(summ.get("payoutRatio"))),
            "roe": _to_percent(_get_raw(fin.get("returnOnEquity"))),
            "roa": _to_percent(_get_raw(fin.get("returnOnAssets"))),
            "beta": _get_raw(stats.get("beta")),
            
            "data_source": PROVIDER_NAME,
            "last_updated_utc": _now_utc_iso(),
            "status": "ok"
        }
        
        return {k: v for k, v in patch.items() if v is not None}

    except Exception as e:
        logger.error("Yahoo Fundamentals Error for %s: %s", symbol, e)
        return {"error": str(e), "status": "error"}
    finally:
        if not client:
            await local_client.aclose()

# --- Compatibility Aliases ---
async def fetch_enriched_quote_patch(symbol: str, **kwargs):
    return await fetch_fundamentals_patch(symbol, **kwargs)

async def fetch_quote_and_enrichment_patch(symbol: str, **kwargs):
    return await fetch_fundamentals_patch(symbol, **kwargs)

async def aclose_yahoo_fundamentals_client():
    """Reserved for global cleanup if necessary."""
    pass
