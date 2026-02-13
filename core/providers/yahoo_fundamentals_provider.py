#!/usr/bin/env python3
# core/providers/yahoo_fundamentals_provider.py
"""
core/providers/yahoo_fundamentals_provider.py
============================================================
Yahoo Fundamentals Provider (NO yfinance) — v1.6.3
PROD SAFE + SHARED NORMALIZATION v1.0.0 + ALIGNED ROI KEYS

What’s improved in v1.6.3
- ✅ Normalization Alignment: Fully integrated with core.symbols.normalize v1.0.0.
- ✅ ROI Key Alignment: Uses 'expected_roi_12m' as primary; aliases 'expected_return_12m'.
- ✅ Riyadh Localization: Adds 'forecast_updated_riyadh' (UTC+3) for KSA dashboards.
- ✅ Resilience: Retains the robust cookie warmup/retry logic to bypass 401/403 blocks.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import re
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import httpx

# ---------------------------------------------------------------------------
# Shared Normalizer Import (Aligned with core/symbols/normalize.py v1.0.0)
# ---------------------------------------------------------------------------
try:
    from core.symbols.normalize import normalize_symbol, is_ksa
except Exception:
    # Minimal local fallback if shared module is not reachable
    def is_ksa(s: str) -> bool:
        u = s.upper()
        return ".SR" in u or u.isdigit()
    def normalize_symbol(s: str) -> str:
        s = s.strip().upper()
        return f"{s}.SR" if s.isdigit() else s

logger = logging.getLogger("core.providers.yahoo_fundamentals_provider")

PROVIDER_NAME = "yahoo_fundamentals"
PROVIDER_VERSION = "1.6.3"

# ---------------------------
# Safe env parsing
# ---------------------------
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

def _env_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw: return default
    return raw not in _FALSY

def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return str(v).strip() if v else default

def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _riyadh_iso() -> str:
    # Asia/Riyadh is UTC+3
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).isoformat()

# ---------------------------
# Client reuse (keep-alive)
# ---------------------------
_CLIENT: Optional[httpx.AsyncClient] = None
_CLIENT_LOCK = asyncio.Lock()
_WARMED: Set[str] = set()
_WARM_LOCK = asyncio.Lock()

async def _get_client() -> httpx.AsyncClient:
    global _CLIENT
    async with _CLIENT_LOCK:
        if _CLIENT is None:
            _CLIENT = httpx.AsyncClient(timeout=12.0, follow_redirects=True)
    return _CLIENT

async def _warmup_cookie(symbol: str) -> None:
    """Hits the main Yahoo quote page to establish valid session cookies."""
    sym = symbol.strip().upper()
    async with _WARM_LOCK:
        if sym in _WARMED: return
        _WARMED.add(sym)
    try:
        client = await _get_client()
        url = f"https://finance.yahoo.com/quote/{sym}"
        await client.get(url, headers={"User-Agent": _env_str("YAHOO_UA", "Mozilla/5.0")})
    except:
        pass

# ---------------------------
# Numeric Value Helpers
# ---------------------------
def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None or isinstance(x, bool): return None
        f = float(str(x).replace(",", "").strip())
        return f if not math.isnan(f) and not math.isinf(f) else None
    except:
        return None

def _get_raw(obj: Any) -> Optional[float]:
    """Yahoo JSON often nests values as {'raw': 1.23, 'fmt': '1.23'}."""
    if isinstance(obj, dict): return _to_float(obj.get("raw"))
    return _to_float(obj)

# ---------------------------
# Forecast & Target Logic
# ---------------------------
def _apply_targets_forecast(out: Dict[str, Any]) -> None:
    """Enriches patch with analyst target-based forecast fields."""
    px = _to_float(out.get("current_price"))
    t_mean = _to_float(out.get("target_mean_price"))
    if not px or not t_mean: return

    roi_12m = ((t_mean / px) - 1.0) * 100.0
    
    out["fair_value"] = t_mean
    out["expected_roi_12m"] = roi_12m
    out["expected_return_12m"] = roi_12m  # Legacy Alias
    out["upside_percent"] = roi_12m
    out["expected_price_12m"] = t_mean
    out["forecast_method"] = "yahoo_analyst_targets_v1"
    
    # Confidence score based on analyst coverage depth
    n = _to_float(out.get("analyst_opinions")) or 0
    base_conf = min(85, 20 + (10 * math.log2(n + 1)))
    out["confidence_score"] = float(base_conf)
    out["forecast_confidence"] = float(base_conf / 100.0)

# ---------------------------
# Core Fetcher
# ---------------------------
async def fetch_fundamentals_patch(symbol: str, debug: bool = False) -> Dict[str, Any]:
    """
    Main entry point for fundamental data extraction.
    Returns a dictionary intended to patch a UnifiedQuote object.
    """
    # 1. Standardize the symbol (v1.0.0 logic)
    u_sym = normalize_symbol(symbol)
    if not u_sym:
        return {}
    
    # 2. Check market-specific permission
    if is_ksa(u_sym) and not _env_bool("ENABLE_YAHOO_FUNDAMENTALS_KSA", True):
        return {}

    client = await _get_client()
    url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{u_sym}"
    params = {
        "modules": "price,summaryDetail,defaultKeyStatistics,financialData",
        "lang": "en-US",
        "region": "SA" if u_sym.endswith(".SR") else "US"
    }

    try:
        r = await client.get(url, params=params)
        
        # 3. Handle 401/403 via Cookie Warmup flow
        if r.status_code in {401, 403}:
            await _warmup_cookie(u_sym)
            r = await client.get(url, params=params)

        if r.status_code != 200:
            return {"error": f"warning: {PROVIDER_NAME}: HTTP {r.status_code}"}

        js = r.json()
        res = js.get("quoteSummary", {}).get("result", [])
        if not res:
            return {"error": f"warning: {PROVIDER_NAME}: empty result set"}
        
        data = res[0]
        price_mod = data.get("price", {})
        summ = data.get("summaryDetail", {})
        stats = data.get("defaultKeyStatistics", {})
        fin = data.get("financialData", {})

        # 4. Map Yahoo's specific modules to our standard schema
        out = {
            "symbol": u_sym,
            "current_price": _get_raw(price_mod.get("regularMarketPrice")),
            "market_cap": _get_raw(summ.get("marketCap")),
            "shares_outstanding": _get_raw(stats.get("sharesOutstanding")),
            "pe_ttm": _get_raw(summ.get("trailingPE")),
            "eps_ttm": _get_raw(stats.get("trailingEps")),
            "forward_pe": _get_raw(summ.get("forwardPE")),
            "forward_eps": _get_raw(stats.get("forwardEps")),
            "pb": _get_raw(summ.get("priceToBook")),
            "dividend_yield": _get_raw(summ.get("dividendYield")) * 100.0 if _get_raw(summ.get("dividendYield")) else None,
            "roe": _get_raw(fin.get("returnOnEquity")) * 100.0 if _get_raw(fin.get("returnOnEquity")) else None,
            "roa": _get_raw(fin.get("returnOnAssets")) * 100.0 if _get_raw(fin.get("returnOnAssets")) else None,
            "beta": _get_raw(stats.get("beta")),
            "target_mean_price": _get_raw(fin.get("targetMeanPrice")),
            "analyst_opinions": _get_raw(fin.get("numberOfAnalystOpinions")),
            "data_source": PROVIDER_NAME,
            "provider_version": PROVIDER_VERSION,
            "last_updated_utc": _utc_iso(),
            "forecast_updated_riyadh": _riyadh_iso()
        }

        # 5. Apply Analyst-driven Forecast Enrichment
        _apply_targets_forecast(out)

        # 6. Final cleanup of empty results
        return {k: v for k, v in out.items() if v is not None}

    except Exception as e:
        logger.error("Yahoo fundamentals failure for %s: %s", u_sym, e)
        return {"error": f"warning: {PROVIDER_NAME} internal error"}

async def aclose_yahoo_fundamentals_client():
    global _CLIENT
    if _CLIENT:
        await _CLIENT.aclose()
        _CLIENT = None

# Compatibility Aliases for Engine Auto-Discovery
async def fetch_enriched_quote_patch(symbol: str, **kwargs) -> Dict[str, Any]:
    return await fetch_fundamentals_patch(symbol)

async def fetch_quote_and_fundamentals_patch(symbol: str, **kwargs) -> Dict[str, Any]:
    return await fetch_fundamentals_patch(symbol)

__all__ = [
    "fetch_fundamentals_patch", 
    "fetch_enriched_quote_patch", 
    "fetch_quote_and_fundamentals_patch",
    "aclose_yahoo_fundamentals_client", 
    "PROVIDER_NAME"
]
