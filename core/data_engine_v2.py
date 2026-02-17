#!/usr/bin/env python3
"""
core/data_engine_v2.py
============================================================
Data Engine V2 (THE MASTER ORCHESTRATOR) — v2.11.0
(PROD SAFE + DEEP FETCH GUARANTEE + ROBUST MERGE)

What this module does:
1.  Loads providers dynamically from core.providers.*
2.  Routes symbols (KSA -> Tadawul/Argaam, US/Global -> Yahoo/Finnhub/EODHD)
3.  **Crucial:** Aggressively merges data from multiple providers to ensure
    Fundamentals (EPS, PE) and History (RSI, Volatility) are present.
4.  Calculates final scores (0-100) and recommendation.

✅ v2.11.0 Enhancements:
- ✅ **Deep Fetch Strategy:** If a symbol lacks fundamentals/history after the primary fetch,
     it explicitly triggers secondary providers (Yahoo/Finnhub) to fill the gaps.
- ✅ **Smart Merging:** Keeps the *best* data. Won't overwrite a valid P/E with None.
- ✅ **History Integration:** Ensures price history is fetched so technicals (RSI, MA200) can be computed.
- ✅ **Forecast enablement:** With history now guaranteed, forecasts will generate.

Environment variables:
- ENABLED_PROVIDERS (default: tadawul,argaam,yahoo_chart,yahoo_fundamentals,finnhub,eodhd)
- KSA_PROVIDERS (default: tadawul,argaam)
- US_PROVIDERS (default: yahoo_chart,yahoo_fundamentals,finnhub,eodhd)
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

# --- Local Imports (Safe) ---
try:
    from core.schemas import UnifiedQuote  # The master Pydantic model
except ImportError:
    # Fallback if schemas not found
    from pydantic import BaseModel, Field

    class UnifiedQuote(BaseModel):  # type: ignore
        symbol: str
        current_price: Optional[float] = None
        class Config:
            extra = "ignore"

from core.symbols.normalize import normalize_symbol

# --- Provider Imports (Lazy/Safe) ---
from core.providers import (
    argaam_provider,
    tadawul_provider,
    yahoo_chart_provider,
    yahoo_fundamentals_provider,
    finnhub_provider,
    eodhd_provider,
)

logger = logging.getLogger("core.data_engine_v2")

ENGINE_VERSION = "2.11.0"

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
def _get_env_list(key: str, default: str) -> List[str]:
    raw = os.getenv(key, default)
    return [s.strip().lower() for s in raw.split(",") if s.strip()]

DEFAULT_PROVIDERS = "tadawul,argaam,yahoo_chart,yahoo_fundamentals,finnhub,eodhd"
DEFAULT_KSA = "tadawul,argaam"
DEFAULT_US = "yahoo_chart,yahoo_fundamentals,finnhub,eodhd"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _now_riyadh_iso() -> str:
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).isoformat()

def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None: return None
        return float(x)
    except (ValueError, TypeError):
        return None

# ---------------------------------------------------------------------------
# Score Calculation Logic (The "Brain")
# ---------------------------------------------------------------------------
def _calculate_scores(q: UnifiedQuote) -> None:
    """
    Mutates the quote object to add calculated scores.
    Uses available data; defaults to neutral (50) if missing.
    """
    # 1. Quality Score
    roe = _safe_float(q.roe) or 0
    margin = _safe_float(q.net_margin) or 0
    debt_eq = _safe_float(q.debt_to_equity) or 100
    
    qual = 50.0
    qual += min(20, roe * 0.5)
    qual += min(20, margin * 0.5)
    qual -= min(20, max(0, debt_eq - 50) * 0.2)
    q.quality_score = max(0.0, min(100.0, qual))

    # 2. Value Score
    pe = _safe_float(q.pe_ttm)
    upside = _safe_float(q.upside_percent) or 0
    
    val = 50.0
    if pe and pe > 0:
        if pe < 15: val += 15
        elif pe > 35: val -= 15
    val += min(25, upside * 0.5)
    q.value_score = max(0.0, min(100.0, val))

    # 3. Momentum Score
    rsi = _safe_float(q.rsi_14) or 50
    price = _safe_float(q.current_price)
    ma50 = _safe_float(q.ma50)
    ma200 = _safe_float(q.ma200)

    mom = 50.0
    if rsi > 70: mom += 10
    elif rsi < 30: mom -= 10
    else: mom += (rsi - 50) * 0.5

    if price and ma50 and price > ma50: mom += 10
    if price and ma200 and price > ma200: mom += 10
    
    q.momentum_score = max(0.0, min(100.0, mom))

    # 4. Risk Score
    beta = _safe_float(q.beta) or 1.0
    vol = _safe_float(q.volatility_30d) or 20.0
    
    risk_inv = 100.0
    if beta > 1.0: risk_inv -= (beta - 1.0) * 20
    risk_inv -= min(30, max(0, vol - 15) * 1.0)
    
    q.risk_score = max(0.0, min(100.0, risk_inv))

    # 5. Overall
    overall = (
        (q.quality_score * 0.25) +
        (q.value_score * 0.25) +
        (q.momentum_score * 0.25) +
        (q.risk_score * 0.25)
    )
    q.overall_score = max(0.0, min(100.0, overall))

    # 6. Recommendation
    if overall >= 75: q.recommendation = "BUY"
    elif overall >= 65: q.recommendation = "BUY"
    elif overall <= 35: q.recommendation = "SELL"
    elif overall <= 45: q.recommendation = "REDUCE"
    else: q.recommendation = "HOLD"


# ---------------------------------------------------------------------------
# Singleflight / Concurrency Helper
# ---------------------------------------------------------------------------
class _SingleFlight:
    def __init__(self):
        self._calls: Dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()

    async def do(self, key: str, coro_fn) -> Any:
        async with self._lock:
            if key in self._calls:
                return await self._calls[key]
            fut = asyncio.get_running_loop().create_future()
            self._calls[key] = fut
        
        try:
            res = await coro_fn()
            if not fut.done(): fut.set_result(res)
            return res
        except Exception as e:
            if not fut.done(): fut.set_exception(e)
            raise
        finally:
            async with self._lock:
                self._calls.pop(key, None)


# ---------------------------------------------------------------------------
# Data Engine V2 Class
# ---------------------------------------------------------------------------
@dataclass
class DataEngineV2:
    def __init__(self, settings: Any = None):
        self.settings = settings
        self.enabled_providers = _get_env_list("ENABLED_PROVIDERS", DEFAULT_PROVIDERS)
        self.ksa_providers = _get_env_list("KSA_PROVIDERS", DEFAULT_KSA)
        self.us_providers = _get_env_list("US_PROVIDERS", DEFAULT_US)
        
        self._sf = _SingleFlight()
        self._semaphore = asyncio.Semaphore(int(os.getenv("DATA_ENGINE_MAX_CONCURRENCY", "50")))
        
        logger.info(f"DataEngineV2 v{ENGINE_VERSION} initialized.")

    async def aclose(self):
        pass # Providers handle their own cleanup if needed

    # --- Routing Logic ---
    def _get_providers_for_symbol(self, symbol: str) -> List[str]:
        sym = normalize_symbol(symbol)
        is_ksa = sym.endswith(".SR") or sym.isdigit() or sym.startswith("TADAWUL:")
        
        if is_ksa:
            # For KSA, we need a mix of local (Tadawul/Argaam) and global (Yahoo)
            # Global providers are needed for History/Technicals which local ones often lack
            selection = [p for p in self.ksa_providers if p in self.enabled_providers]
            
            # **FORCE** add fallback providers if not present, to ensure we get *some* data
            if "yahoo_chart" in self.enabled_providers and "yahoo_chart" not in selection:
                selection.append("yahoo_chart")
            if "yahoo_fundamentals" in self.enabled_providers and "yahoo_fundamentals" not in selection:
                selection.append("yahoo_fundamentals")
            
            return selection
        else:
            # US/Global: Yahoo is king, Finnhub/EODHD supplement
            selection = [p for p in self.us_providers if p in self.enabled_providers]
            if "yahoo_chart" not in selection and "yahoo_chart" in self.enabled_providers:
                 selection.insert(0, "yahoo_chart") # Prioritize Yahoo for global
            return selection

    # --- Fetch & Merge Logic ---
    async def _fetch_provider_patch(self, provider_name: str, symbol: str) -> Dict[str, Any]:
        try:
            if provider_name == "tadawul":
                return await tadawul_provider.fetch_enriched_quote_patch(symbol)
            elif provider_name == "argaam":
                return await argaam_provider.fetch_enriched_quote_patch(symbol)
            elif provider_name == "yahoo_chart":
                return await yahoo_chart_provider.fetch_enriched_quote_patch(symbol)
            elif provider_name == "yahoo_fundamentals":
                return await yahoo_fundamentals_provider.fetch_fundamentals_patch(symbol)
            elif provider_name == "finnhub":
                return await finnhub_provider.fetch_enriched_quote_patch(symbol)
            elif provider_name == "eodhd":
                return await eodhd_provider.fetch_enriched_quote_patch(symbol)
            return {}
        except Exception as e:
            logger.warning(f"Provider {provider_name} failed for {symbol}: {e}")
            return {}

    async def get_enriched_quote(self, symbol: str) -> UnifiedQuote:
        return await self._sf.do(symbol, lambda: self._get_enriched_quote_impl(symbol))

    async def _get_enriched_quote_impl(self, symbol: str) -> UnifiedQuote:
        start_ts = time.monotonic()
        norm_sym = normalize_symbol(symbol)
        providers = self._get_providers_for_symbol(norm_sym)
        
        # 1. Fetch patches concurrently
        async with self._semaphore:
            tasks = [self._fetch_provider_patch(p, norm_sym) for p in providers]
            patches = await asyncio.gather(*tasks, return_exceptions=True)

        # 2. Merge Patches (Smart Merge)
        # We start with an empty dict and layer data on top.
        # Valid data overwrites None/Empty.
        merged_data: Dict[str, Any] = {
            "symbol": norm_sym,
            "requested_symbol": symbol,
            "symbol_normalized": norm_sym,
            "last_updated_utc": _now_utc_iso(),
            "last_updated_riyadh": _now_riyadh_iso(),
            "data_sources": [],
            "status": "success"
        }

        # Apply patches in order of provider priority (as returned by _get_providers)
        # BUT, we only overwrite if the new value is truthy/valid.
        # This prevents a "lite" provider from wiping out good data from a "heavy" one.
        for i, res in enumerate(patches):
            if isinstance(res, dict) and "error" not in res:
                prov_name = providers[i]
                merged_data["data_sources"].append(prov_name)
                
                for k, v in res.items():
                    # If we don't have it, take it.
                    if k not in merged_data or merged_data[k] in (None, ""):
                        merged_data[k] = v
                    # If we have it, but new value is "better" (e.g. not None), update it?
                    # For now, let's assume the provider list order IS the priority order.
                    # So later providers (fallbacks) should NOT overwrite early ones (primary)
                    # UNLESS the primary value was None.
                    # Wait, _get_providers returns [Primary, ..., Fallback].
                    # So iterating 0..N means Primary is processed FIRST.
                    # We should keep Primary's value if it exists.
                    # So: only update if current value is None/Empty.
                    elif v is not None and v != "":
                         # Only overwrite if current is "weak" (placeholder)
                         # Actually, let's trust the provider order:
                         # We iterate 0..N. 0 is highest priority.
                         # So
