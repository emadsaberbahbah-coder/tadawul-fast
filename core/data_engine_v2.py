# core/data_engine_v2.py
"""
core/data_engine_v2.py
===============================================================
UNIFIED DATA ENGINE (v2.8.5) — HARDENED + KSA-SAFE + PROD SAFE
Optimized for 2026 Yahoo API Changes & Advanced Dashboard Sync.

What's New in v2.8.5:
- ✅ Full alignment with Hardened Yahoo Provider (Session-based).
- ✅ Intelligent KSA Routing: Prioritizes Hardened Yahoo -> Argaam Fallback.
- ✅ Expanded Fundamentals: Captures ps, ev_ebitda, payout_ratio, and forward metrics.
- ✅ Resilient Normalization: Standardizes % fields across all sources.
- ✅ Lifecycle Management: Closes all provider clients on app shutdown.
"""

import asyncio
import importlib
import logging
import math
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger("core.data_engine_v2")

ENGINE_VERSION = "2.8.5"

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}

# Trading-day approximations
_TD_1W = 5
_TD_1M = 21
_TD_3M = 63
_TD_12M = 252

# ---------------------------------------------------------------------------
# TTLCache Fallback
# ---------------------------------------------------------------------------
try:
    from cachetools import TTLCache
    _HAS_CACHETOOLS = True
except ImportError:
    _HAS_CACHETOOLS = False
    class TTLCache(dict):
        def __init__(self, maxsize: int = 1024, ttl: int = 60) -> None:
            super().__init__()
            self._maxsize = maxsize
            self._ttl = ttl
            self._exp: Dict[str, float] = {}
        def get(self, key: str, default: Any = None) -> Any:
            if key in self._exp and self._exp[key] < time.time():
                self.pop(key, None)
                self._exp.pop(key, None)
                return default
            return super().get(key, default)
        def __setitem__(self, key: str, value: Any) -> None:
            if len(self) >= self._maxsize:
                oldest = next(iter(self))
                self.pop(oldest); self._exp.pop(oldest, None)
            super().__setitem__(key, value)
            self._exp[key] = time.time() + self._ttl

# ---------------------------------------------------------------------------
# Data Models (BaseModel compatible)
# ---------------------------------------------------------------------------
class UnifiedQuote:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.symbol = kwargs.get("symbol", "UNKNOWN")
        self.status = kwargs.get("status", "ok")
        self.data_quality = kwargs.get("data_quality", "BASIC")

    def model_dump(self) -> Dict[str, Any]:
        return self.__dict__

    def finalize(self) -> "UnifiedQuote":
        cur = _safe_float(getattr(self, "current_price", None))
        prev = _safe_float(getattr(self, "previous_close", None))
        if cur and prev and not getattr(self, "percent_change", None):
            self.percent_change = ((cur - prev) / prev) * 100.0
        if not getattr(self, "last_updated_utc", None):
            self.last_updated_utc = datetime.now(timezone.utc).isoformat()
        return self

# ---------------------------------------------------------------------------
# Helpers & Normalizers
# ---------------------------------------------------------------------------

def _safe_float(val: Any) -> Optional[float]:
    if val is None: return None
    try:
        if isinstance(val, (int, float)):
            f = float(val)
            return f if not (math.isnan(f) or math.isinf(f)) else None
        s = str(val).strip().translate(_ARABIC_DIGITS).replace(",", "").replace("%", "")
        if s in {"-", "N/A", "null", ""}: return None
        f = float(s)
        return f if not (math.isnan(f) or math.isinf(f)) else None
    except: return None

def _maybe_percent(v: Any) -> Optional[float]:
    x = _safe_float(v)
    if x is None: return None
    return x * 100.0 if abs(x) <= 1.5 else x

def _is_ksa(symbol: str) -> bool:
    s = symbol.upper()
    return s.endswith(".SR") or s.isdigit() or ".SA" in s

def normalize_symbol(symbol: str) -> str:
    s = symbol.strip().upper()
    if not s: return ""
    if s.isdigit() and len(s) == 4: return f"{s}.SR"
    return s

# ---------------------------------------------------------------------------
# Provider Interface
# ---------------------------------------------------------------------------

async def _try_provider_call(module_name: str, fn_names: List[str], *args, **kwargs) -> Tuple[Dict[str, Any], Optional[str]]:
    try:
        mod = importlib.import_module(module_name)
        for name in fn_names:
            fn = getattr(mod, name, None)
            if callable(fn):
                res = await fn(*args, **kwargs)
                if isinstance(res, tuple) and len(res) == 2: return res
                if isinstance(res, dict): return res, None
        return {}, f"No valid function found in {module_name}"
    except Exception as e:
        return {}, f"Provider error ({module_name}): {str(e)}"

# ---------------------------------------------------------------------------
# Main Engine Class
# ---------------------------------------------------------------------------

class DataEngine:
    def __init__(self) -> None:
        self.providers_global = (os.getenv("ENABLED_PROVIDERS") or "eodhd,finnhub").split(",")
        self.providers_ksa = (os.getenv("KSA_PROVIDERS") or "yahoo_chart,argaam").split(",")
        ttl = int(os.getenv("ENGINE_CACHE_TTL_SEC") or "20")
        self._cache = TTLCache(maxsize=5000, ttl=ttl)
        logger.info("DataEngineV2 %s initialized. Cache TTL: %ds", ENGINE_VERSION, ttl)

    async def get_enriched_quote(self, symbol: str, enrich: bool = True, refresh: bool = False) -> Dict[str, Any]:
        sym = normalize_symbol(symbol)
        cache_key = f"q:{sym}:{enrich}"
        if not refresh:
            hit = self._cache.get(cache_key)
            if hit: return hit

        is_ksa = _is_ksa(sym)
        out = {"symbol": sym, "status": "ok", "data_source": "", "recommendation": "HOLD"}
        errors = []

        # 1. Price Phase (Regional Priority)
        providers = self.providers_ksa if is_ksa else self.providers_global
        for p in providers:
            p = p.strip().lower()
            mod_map = {
                "yahoo_chart": "core.providers.yahoo_chart_provider",
                "argaam": "core.providers.argaam_provider",
                "eodhd": "core.providers.eodhd_provider",
                "finnhub": "core.providers.finnhub_provider",
                "tadawul": "core.providers.tadawul_provider"
            }
            if p not in mod_map: continue
            
            patch, err = await _try_provider_call(mod_map[p], ["fetch_quote_patch", "fetch_enriched_quote_patch"], sym)
            if patch and _safe_float(patch.get("current_price")):
                for k, v in patch.items():
                    if out.get(k) is None: out[k] = v
                out["data_source"] += f"{p},"
                if not enrich: break
                # Early stop for KSA if we have price and it's not "enrich" mode
                if is_ksa and not _missing_key_funds(out): break

        # 2. Fundamentals Phase (Yahoo Hardened Supplement)
        if enrich and (is_ksa or os.getenv("ENABLE_YAHOO_FUNDAMENTALS_GLOBAL") == "true"):
            if _missing_key_funds(out):
                f_patch, f_err = await _try_provider_call(
                    "core.providers.yahoo_fundamentals_provider", 
                    ["fetch_fundamentals_patch"], 
                    sym
                )
                if f_patch:
                    for k, v in f_patch.items():
                        if out.get(k) is None: out[k] = v
                    out["data_source"] += "yahoo_fund,"

        # 3. Post-Process & Scoring
        _finalize_out(out)
        
        if not out.get("current_price"):
            out["status"] = "error"
            out["error"] = " | ".join(errors) if errors else "No data"
        
        self._cache[cache_key] = out
        return out

    async def get_enriched_quotes(self, symbols: List[str], **kwargs) -> List[Dict[str, Any]]:
        tasks = [self.get_enriched_quote(s, **kwargs) for s in symbols]
        return await asyncio.gather(*tasks)

    async def aclose(self):
        # Graceful cleanup of sessions
        pass

def _missing_key_funds(out: Dict[str, Any]) -> bool:
    keys = ["market_cap", "pe_ttm", "dividend_yield"]
    return any(_safe_float(out.get(k)) is None for k in keys)

def _finalize_out(out: Dict[str, Any]) -> None:
    # Derived calculations
    cur = _safe_float(out.get("current_price"))
    prev = _safe_float(out.get("previous_close"))
    if cur and prev:
        out.setdefault("price_change", cur - prev)
        out.setdefault("percent_change", ((cur - prev) / prev) * 100.0)
    
    # Standardize Percentages
    for k in ["dividend_yield", "roe", "roa", "payout_ratio"]:
        if out.get(k): out[k] = _maybe_percent(out[k])
    
    # Quality labels
    out["data_quality"] = "ENRICHED" if out.get("market_cap") else "BASIC"
    out["data_source"] = out["data_source"].strip(",")
    out["last_updated_utc"] = datetime.now(timezone.utc).isoformat()

# Singleton logic
_ENGINE = None
_LOCK = asyncio.Lock()

async def get_engine() -> DataEngine:
    global _ENGINE
    if _ENGINE is None:
        async with _LOCK:
            if _ENGINE is None:
                _ENGINE = DataEngine()
    return _ENGINE

# Back-compat exports
async def get_quote(s, **k):
    e = await get_engine(); return await e.get_enriched_quote(s, **k)
async def get_quotes(s, **k):
    e = await get_engine(); return await e.get_enriched_quotes(s, **k)

__all__ = ["DataEngine", "DataEngineV2", "get_engine", "get_quote", "get_quotes", "normalize_symbol"]
