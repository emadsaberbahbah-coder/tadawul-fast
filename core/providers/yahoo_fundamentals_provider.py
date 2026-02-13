#!/usr/bin/env python3
# core/providers/yahoo_fundamentals_provider.py
"""
core/providers/yahoo_fundamentals_provider.py
============================================================
Yahoo Fundamentals Provider (NO yfinance) — v1.6.1
PROD SAFE + ENGINE-ALIGNED + ALIGNED ROI KEYS + RIYADH TIME

What’s improved in v1.6.1
- ✅ ROI Key Alignment: Uses 'expected_roi_12m' to match EODHD/Finnhub/Tadawul.
- ✅ Riyadh Localization: Adds 'forecast_updated_riyadh' (UTC+3).
- ✅ Provenance: Explicitly sets 'data_source="yahoo_fundamentals"'.
- ✅ Cookie Warmup: Retains robust 401/403 bypass logic for production stability.
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

logger = logging.getLogger("core.providers.yahoo_fundamentals_provider")

PROVIDER_NAME = "yahoo_fundamentals"
PROVIDER_VERSION = "1.6.1"

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
# Symbol Normalization
# ---------------------------
_KSA_CODE_RE = re.compile(r"^\d{3,6}$")

def _normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s: return ""
    if s.startswith("TADAWUL:"): s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"): s = s.replace(".TADAWUL", "").strip()
    if _KSA_CODE_RE.match(s): return f"{s}.SR"
    return s

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
    sym = symbol.strip().upper()
    async with _WARM_LOCK:
        if sym in _WARMED: return
        _WARMED.add(sym)
    try:
        client = await _get_client()
        url = f"https://finance.yahoo.com/quote/{sym}"
        await client.get(url, headers={"User-Agent": _env_str("YAHOO_UA", "Mozilla/5.0")})
    except: pass

# ---------------------------
# Value helpers
# ---------------------------
def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None: return None
        f = float(str(x).replace(",", "").strip())
        return f if not math.isnan(f) and not math.isinf(f) else None
    except: return None

def _get_raw(obj: Any) -> Optional[float]:
    if isinstance(obj, dict): return _to_float(obj.get("raw"))
    return _to_float(obj)

# ---------------------------
# Forecast Logic
# ---------------------------
def _apply_targets_forecast(out: Dict[str, Any]) -> None:
    px = _to_float(out.get("current_price"))
    t_mean = _to_float(out.get("target_mean_price"))
    if not px or not t_mean: return

    out["fair_value"] = t_mean
    out["expected_roi_12m"] = ((t_mean / px) - 1.0) * 100.0
    out["upside_percent"] = out["expected_roi_12m"]
    out["expected_price_12m"] = t_mean
    out["forecast_method"] = "yahoo_analyst_targets_v1"
    
    # Confidence based on analyst count
    n = _to_float(out.get("analyst_opinions")) or 0
    base_conf = min(85, 20 + (10 * math.log2(n + 1)))
    out["confidence_score"] = float(base_conf)
    out["forecast_confidence"] = float(base_conf / 100.0)

# ---------------------------
# Core Fetcher
# ---------------------------
async def fetch_fundamentals_patch(symbol: str, debug: bool = False) -> Dict[str, Any]:
    u_sym = _normalize_symbol(symbol)
    if not u_sym: return {}
    
    if u_sym.endswith(".SR") and not _env_bool("ENABLE_YAHOO_FUNDAMENTALS_KSA", True):
        return {}

    client = await _get_client()
    url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{u_sym}"
    params = {
        "modules": "summaryDetail,defaultKeyStatistics,financialData",
        "lang": "en-US",
        "region": "SA" if u_sym.endswith(".SR") else "US"
    }

    try:
        r = await client.get(url, params=params)
        if r.status_code in {401, 403}:
            await _warmup_cookie(u_sym)
            r = await client.get(url, params=params)

        js = r.json()
        res = js.get("quoteSummary", {}).get("result", [])
        if not res: return {"error": "No fundamental data found"}
        
        data = res[0]
        summ = data.get("summaryDetail", {})
        stats = data.get("defaultKeyStatistics", {})
        fin = data.get("financialData", {})

        out = {
            "symbol": u_sym,
            "market_cap": _get_raw(summ.get("marketCap")),
            "shares_outstanding": _get_raw(stats.get("sharesOutstanding")),
            "pe_ttm": _get_raw(summ.get("trailingPE")),
            "eps_ttm": _get_raw(stats.get("trailingEps")),
            "dividend_yield": _get_raw(summ.get("dividendYield")) * 100.0 if _get_raw(summ.get("dividendYield")) else None,
            "roe": _get_raw(fin.get("returnOnEquity")) * 100.0 if _get_raw(fin.get("returnOnEquity")) else None,
            "target_mean_price": _get_raw(fin.get("targetMeanPrice")),
            "analyst_opinions": _get_raw(fin.get("numberOfAnalystOpinions")),
            "data_source": PROVIDER_NAME,
            "provider_version": PROVIDER_VERSION,
            "last_updated_utc": _utc_iso(),
            "forecast_updated_riyadh": _riyadh_iso()
        }

        # Apply standardized forecast keys
        _apply_targets_forecast(out)

        # Cleanup Nones
        return {k: v for k, v in out.items() if v is not None}

    except Exception as e:
        return {"error": f"Yahoo fundamentals failed: {str(e)}"}

async def aclose_yahoo_fundamentals_client():
    global _CLIENT
    if _CLIENT: await _CLIENT.aclose()
    _CLIENT = None

__all__ = ["fetch_fundamentals_patch", "aclose_yahoo_fundamentals_client", "PROVIDER_NAME"]
