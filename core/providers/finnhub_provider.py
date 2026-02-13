#!/usr/bin/env python3
# core/providers/finnhub_provider.py
"""
core/providers/finnhub_provider.py
============================================================
Finnhub Provider (GLOBAL enrichment fallback) — v2.1.1
(PROD SAFE + ENGINE PATCH + ALIGNED ROI KEYS + RIYADH TIME)

What’s improved in v2.1.1
- ✅ ROI Key Alignment: Uses 'expected_roi_1m/3m/12m' to match EODHD standard.
- ✅ Riyadh Localization: Adds 'forecast_updated_riyadh' (UTC+3).
- ✅ Provenance: Explicitly sets 'data_source="finnhub"'.
- ✅ Routing Guard: Rejects KSA and Yahoo-special symbols (^GSPC, GC=F).
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

logger = logging.getLogger("core.providers.finnhub_provider")

PROVIDER_VERSION = "2.1.1"
PROVIDER_NAME = "finnhub"

DEFAULT_BASE_URL = "https://finnhub.io/api/v1"
DEFAULT_TIMEOUT_SEC = 8.0
DEFAULT_RETRY_ATTEMPTS = 2

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

USER_AGENT_DEFAULT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_CLIENT: Optional[httpx.AsyncClient] = None
_LOCK = asyncio.Lock()

# ---------------------------------------------------------------------------
# Shared Normalizer Import
# ---------------------------------------------------------------------------
def _try_import_shared_normalizer() -> Tuple[Optional[Any], Optional[Any]]:
    try:
        from core.symbols.normalize import normalize_symbol as _ns
        from core.symbols.normalize import looks_like_ksa as _lk
        return _ns, _lk
    except Exception:
        return None, None

_SHARED_NORMALIZE, _SHARED_LOOKS_KSA = _try_import_shared_normalizer()

# ---------------------------------------------------------------------------
# Env & Helpers
# ---------------------------------------------------------------------------
def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return str(v).strip() if v else default

def _env_int(name: str, default: int) -> int:
    try: return int(os.getenv(name, str(default)))
    except: return default

def _env_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw: return default
    return raw not in _FALSY

def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _riyadh_iso() -> str:
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).isoformat()

def _token() -> Optional[str]:
    for k in ("FINNHUB_API_KEY", "FINNHUB_API_TOKEN", "FINNHUB_TOKEN"):
        v = (os.getenv(k) or "").strip()
        if v: return v
    return None

# ---------------------------------------------------------------------------
# Caching (TTLCache Fallback)
# ---------------------------------------------------------------------------
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

_Q_CACHE = TTLCache(maxsize=8000, ttl=10.0)
_P_CACHE = TTLCache(maxsize=4000, ttl=21600.0)
_H_CACHE = TTLCache(maxsize=2500, ttl=1200.0)

# ---------------------------------------------------------------------------
# Logic
# ---------------------------------------------------------------------------
def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None: return None
        f = float(str(x).replace(",", "").strip())
        return f if not math.isnan(f) and not math.isinf(f) else None
    except: return None

def _compute_history_analytics(closes: List[float]) -> Dict[str, Any]:
    out = {}
    if not closes: return out
    last = closes[-1]
    
    # Simple Momentum Forecast (1M/3M/12M)
    def momentum_roi(days: int) -> Optional[float]:
        if len(closes) < (days + 1): return None
        start = closes[-(days + 1)]
        if start == 0: return None
        return ((last / start) - 1.0) * 100.0

    out["expected_roi_1m"] = momentum_roi(21)
    out["expected_roi_3m"] = momentum_roi(63)
    out["expected_roi_12m"] = momentum_roi(252)

    if out["expected_roi_1m"] is not None:
        out["forecast_price_1m"] = last * (1.0 + (out["expected_roi_1m"] / 100.0))
    if out["expected_roi_3m"] is not None:
        out["forecast_price_3m"] = last * (1.0 + (out["expected_roi_3m"] / 100.0))
    
    out["forecast_confidence"] = 0.70 # Static base for fallback
    out["forecast_method"] = "finnhub_momentum_v1"
    out["forecast_updated_utc"] = _utc_iso()
    out["forecast_updated_riyadh"] = _riyadh_iso()
    return out

async def _get_client() -> httpx.AsyncClient:
    global _CLIENT
    async with _LOCK:
        if _CLIENT is None:
            _CLIENT = httpx.AsyncClient(timeout=DEFAULT_TIMEOUT_SEC, follow_redirects=True)
    return _CLIENT

async def _fetch(symbol_raw: str, want_profile: bool, want_history: bool) -> Dict[str, Any]:
    sym = symbol_raw.strip().upper()
    if not sym or _SHARED_LOOKS_KSA and _SHARED_LOOKS_KSA(sym):
        return {"error": "KSA not supported in Finnhub"}
    
    # Routing: Reject indices/commodities
    if "^" in sym or "=" in sym:
        return {"error": "Index/Commodity routing blocked for Finnhub"}

    # Finnhub Symbol Format (AAPL.US -> AAPL)
    fh_sym = sym.replace(".US", "")
    token = _token()
    if not token: return {"error": "FINNHUB_API_KEY missing"}

    client = await _get_client()
    params = {"symbol": fh_sym, "token": token}
    
    try:
        # Quote Call
        r = await client.get(f"{DEFAULT_BASE_URL}/quote", params=params)
        js = r.json()
        if not js or js.get("c") == 0: return {"error": "Empty data from Finnhub"}

        out = {
            "symbol": sym,
            "current_price": _to_float(js.get("c")),
            "previous_close": _to_float(js.get("pc")),
            "percent_change": _to_float(js.get("dp")),
            "data_source": PROVIDER_NAME,
            "provider_version": PROVIDER_VERSION,
            "last_updated_utc": _utc_iso()
        }

        # History Call (Enrichment)
        if want_history:
            end = int(time.time())
            start = end - (400 * 86400)
            h_res = await client.get(f"{DEFAULT_BASE_URL}/stock/candle", params={**params, "resolution": "D", "from": start, "to": end})
            h_js = h_res.json()
            if h_js.get("s") == "ok":
                out.update(_compute_history_analytics(h_js.get("c", [])))

        return out
    except Exception as e:
        return {"error": f"Finnhub failed: {str(e)}"}

async def fetch_enriched_quote_patch(symbol: str, *args, **kwargs) -> Dict[str, Any]:
    return await _fetch(symbol, want_profile=True, want_history=True)

async def aclose_finnhub_client():
    global _CLIENT
    if _CLIENT: await _CLIENT.aclose()
    _CLIENT = None

__all__ = ["fetch_enriched_quote_patch", "aclose_finnhub_client", "PROVIDER_NAME"]
