#!/usr/bin/env python3
# core/providers/yahoo_chart_provider.py
"""
core/providers/yahoo_chart_provider.py
============================================================
Yahoo Quote/Chart Provider (KSA-safe) — v2.1.1
(PATCH-ALIGNED + ALIGNED ROI KEYS + RIYADH TIME)

What’s improved in v2.1.1
- ✅ ROI Key Alignment: Uses 'expected_roi_1m/3m/12m' to match EODHD/Finnhub.
- ✅ Riyadh Localization: Adds 'forecast_updated_riyadh' (UTC+3).
- ✅ Provenance: Ensures 'data_source="yahoo_chart"' is used in the final patch.
- ✅ Engine alignment: Returns Dict[str, Any] only.
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
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union

import httpx

logger = logging.getLogger("core.providers.yahoo_chart_provider")

PROVIDER_VERSION = "2.1.1"
PROVIDER_NAME = "yahoo_chart"

# ---------------------------
# Safe env parsing
# ---------------------------
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

def _env_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw: return default
    return raw not in _FALSY

def _env_float(name: str, default: float) -> float:
    try: return float(os.getenv(name, str(default)))
    except: return default

def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return str(v).strip() if v else default

def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _riyadh_iso() -> str:
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).isoformat()

# ---------------------------
# Caching (TTLCache Fallback)
# ---------------------------
try:
    from cachetools import TTLCache
    _HAS_CACHETOOLS = True
except Exception:
    _HAS_CACHETOOLS = False
    class TTLCache(dict):
        def __init__(self, maxsize: int, ttl: float):
            super().__init__()
            self._ttl = ttl
            self._exp = {}
        def get(self, key, default=None):
            if key in self._exp and self._exp[key] < time.time():
                self.pop(key, None)
                return default
            return super().get(key, default)
        def __setitem__(self, key, value):
            super().__setitem__(key, value)
            self._exp[key] = time.time() + self._ttl

_Q_CACHE = TTLCache(maxsize=9000, ttl=10.0)
_C_CACHE = TTLCache(maxsize=5000, ttl=20.0)
_H_CACHE = TTLCache(maxsize=2500, ttl=900.0)

# ---------------------------
# Logic & Helpers
# ---------------------------
_KSA_CODE_RE = re.compile(r"^\d{3,6}$")

def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None: return None
        f = float(str(x).replace(",", "").strip())
        return f if not math.isnan(f) and not math.isinf(f) else None
    except: return None

def _history_analytics(closes: List[float]) -> Dict[str, Any]:
    out = {}
    if not closes: return out
    last = closes[-1]
    
    def momentum_roi(days: int) -> Optional[float]:
        if len(closes) < (days + 1): return None
        start = closes[-(days + 1)]
        if start == 0: return None
        return ((last / start) - 1.0) * 100.0

    # Aligned with v2.1.1 Standard
    out["expected_roi_1m"] = momentum_roi(21)
    out["expected_roi_3m"] = momentum_roi(63)
    out["expected_roi_12m"] = momentum_roi(252)

    if out["expected_roi_1m"] is not None:
        out["forecast_price_1m"] = last * (1.0 + (out["expected_roi_1m"] / 100.0))
    if out["expected_roi_3m"] is not None:
        out["forecast_price_3m"] = last * (1.0 + (out["expected_roi_3m"] / 100.0))
    
    out["forecast_confidence"] = 0.80
    out["forecast_method"] = "yahoo_momentum_v1"
    out["forecast_updated_utc"] = _utc_iso()
    out["forecast_updated_riyadh"] = _riyadh_iso()
    return out

# ---------------------------
# Client reuse
# ---------------------------
_CLIENT: Optional[httpx.AsyncClient] = None
_CLIENT_LOCK = asyncio.Lock()

async def _get_client() -> httpx.AsyncClient:
    global _CLIENT
    async with _CLIENT_LOCK:
        if _CLIENT is None:
            _CLIENT = httpx.AsyncClient(timeout=8.5, follow_redirects=True)
    return _CLIENT

async def aclose_yahoo_client() -> None:
    global _CLIENT
    if _CLIENT: await _CLIENT.aclose()
    _CLIENT = None

# ---------------------------
# Main Fetchers
# ---------------------------
async def _fetch_quote(symbol: str) -> Dict[str, Any]:
    u_sym = symbol.strip().upper()
    if not u_sym: return {}
    
    # Normalize: "2222" -> "2222.SR"
    if _KSA_CODE_RE.match(u_sym):
        u_sym = f"{u_sym}.SR"

    client = await _get_client()
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    params = {"symbols": u_sym}

    try:
        r = await client.get(url, params=params)
        js = r.json()
        res = js.get("quoteResponse", {}).get("result", [])
        if not res: return {"error": f"Symbol {u_sym} not found"}
        
        q = res[0]
        out = {
            "symbol": u_sym,
            "current_price": _to_float(q.get("regularMarketPrice")),
            "previous_close": _to_float(q.get("regularMarketPreviousClose")),
            "percent_change": _to_float(q.get("regularMarketChangePercent")),
            "day_high": _to_float(q.get("regularMarketDayHigh")),
            "day_low": _to_float(q.get("regularMarketDayLow")),
            "volume": _to_float(q.get("regularMarketVolume")),
            "name": q.get("longName") or q.get("shortName"),
            "data_source": PROVIDER_NAME,
            "provider_version": PROVIDER_VERSION,
            "last_updated_utc": _utc_iso()
        }

        # Supplemental enrichment for history/forecast
        if _env_bool("YAHOO_ENABLE_HISTORY", True):
            c_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{u_sym}"
            c_res = await client.get(c_url, params={"range": "2y", "interval": "1d"})
            c_js = c_res.json()
            chart_res = c_js.get("chart", {}).get("result", [None])[0]
            if chart_res:
                closes = chart_res.get("indicators", {}).get("quote", [{}])[0].get("close", [])
                closes = [c for c in closes if c is not None]
                if closes:
                    out.update(_history_analytics(closes))

        return out
    except Exception as e:
        return {"error": f"Yahoo fetch failed: {str(e)}"}

async def fetch_enriched_quote_patch(symbol: str, debug: bool = False) -> Dict[str, Any]:
    return await _fetch_quote(symbol)

# Compatibility Class
@dataclass
class YahooChartProvider:
    name: str = PROVIDER_NAME
    async def fetch_quote(self, symbol: str, debug: bool = False) -> Dict[str, Any]:
        return await _fetch_quote(symbol)

__all__ = [
    "fetch_enriched_quote_patch",
    "aclose_yahoo_client",
    "PROVIDER_NAME",
    "YahooChartProvider",
]
