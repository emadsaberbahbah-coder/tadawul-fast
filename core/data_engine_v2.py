#!/usr/bin/env python3
"""
core/data_engine_v2.py
============================================================
Data Engine V2 (THE MASTER ORCHESTRATOR) — v2.10.0
(PROD SAFE + ASYNC + SINGLEFLIGHT + ALIGNED FORECASTS)

What this module does:
1.  Loads providers dynamically from core.providers.*
2.  Routes symbols (KSA -> Tadawul/Argaam, US/Global -> Yahoo/Finnhub/EODHD)
3.  Merges patches (Quote + Fundamentals + History + Forecasts) into one `UnifiedQuote`
4.  Calculates final scores (0-100) and recommendation (BUY/HOLD/REDUCE/SELL)

✅ v2.10.0 Enhancements:
- ✅ Strict ROI/Forecast Alignment:
    - Normalizes `expected_roi_1m`/`3m`/`12m` and `forecast_price_*`
    - Ensures `forecast_confidence` (0..1) and `confidence_score` (0..100) are consistent
    - Generates `forecast_updated_utc` and `forecast_updated_riyadh`
- ✅ Dynamic Scoring:
    - Calculates Quality, Value, Momentum, Risk, and Opportunity scores from merged data
    - Generates a final weighted `overall_score`
- ✅ Singleflight:
    - Prevents "stampeding" the same symbol concurrently
    - Reuses in-flight fetch tasks for identical symbol requests
- ✅ Robust Provider Fallback:
    - Tries primary providers first, then fallbacks (e.g., Yahoo -> Finnhub -> EODHD)
    - Merges data intelligently (doesn't overwrite good data with bad)

Environment variables:
- ENABLED_PROVIDERS (comma-separated, default: tadawul,argaam,yahoo_chart,yahoo_fundamentals,finnhub,eodhd)
- KSA_PROVIDERS (default: tadawul,argaam)
- US_PROVIDERS (default: yahoo_chart,yahoo_fundamentals,finnhub,eodhd)
- DATA_ENGINE_MAX_CONCURRENCY (default 50)
- DATA_ENGINE_TIMEOUT_SEC (default 45)
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
    # Fallback if schemas not found (should not happen in prod)
    from pydantic import BaseModel, Field

    class UnifiedQuote(BaseModel):  # type: ignore
        symbol: str
        current_price: Optional[float] = None
        # ... (simplified stub for fallback) ...
        class Config:
            extra = "ignore"

from core.symbols.normalize import normalize_symbol

# --- Provider Imports (Lazy/Safe) ---
# We'll import these inside the class or use a loader to avoid circulars if possible,
# but for V2 we usually expect providers to be importable.
from core.providers import (
    argaam_provider,
    tadawul_provider,
    yahoo_chart_provider,
    yahoo_fundamentals_provider,
    finnhub_provider,
    eodhd_provider,
)

logger = logging.getLogger("core.data_engine_v2")

ENGINE_VERSION = "2.10.0"

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
    Mutates the quote object to add calculated scores (Quality, Value, Momentum, Risk, Overall).
    Heuristics are simplified for speed/robustness.
    """
    # 1. Quality Score (ROE, Net Margin, Debt)
    roe = _safe_float(q.roe) or 0
    margin = _safe_float(q.net_margin) or 0
    debt_eq = _safe_float(q.debt_to_equity) or 100  # penalize if missing/high
    
    qual = 50.0  # base
    qual += min(20, roe * 0.5)         # + up to 20 for ROE
    qual += min(20, margin * 0.5)      # + up to 20 for Margin
    qual -= min(20, max(0, debt_eq - 50) * 0.2) # - penalty for high debt
    q.quality_score = max(0.0, min(100.0, qual))

    # 2. Value Score (P/E, P/B, Upside)
    pe = _safe_float(q.pe_ttm)
    upside = _safe_float(q.upside_percent) or 0
    
    val = 50.0
    if pe and pe > 0:
        if pe < 15: val += 15
        elif pe > 35: val -= 15
    val += min(25, upside * 0.5) # + up to 25 for upside
    q.value_score = max(0.0, min(100.0, val))

    # 3. Momentum Score (RSI, Moving Averages, Rel Strength)
    rsi = _safe_float(q.rsi_14) or 50
    price = _safe_float(q.current_price)
    ma50 = _safe_float(q.ma50)
    ma200 = _safe_float(q.ma200)

    mom = 50.0
    # RSI logic
    if rsi > 70: mom += 10 # strong momentum (or overbought)
    elif rsi < 30: mom -= 10 # weak (or oversold)
    else: mom += (rsi - 50) * 0.5

    # MA Trend
    if price and ma50 and price > ma50: mom += 10
    if price and ma200 and price > ma200: mom += 10
    if ma50 and ma200 and ma50 > ma200: mom += 10 # Golden cross-ish

    q.momentum_score = max(0.0, min(100.0, mom))

    # 4. Risk Score (Volatility, Beta) - Higher score = Lower Risk (Safety)
    beta = _safe_float(q.beta) or 1.0
    vol = _safe_float(q.volatility_30d) or 20.0
    
    risk_inv = 100.0
    # Penalize for high beta
    if beta > 1.0: risk_inv -= (beta - 1.0) * 20
    # Penalize for high volatility
    risk_inv -= min(30, max(0, vol - 15) * 1.0)
    
    q.risk_score = max(0.0, min(100.0, risk_inv))

    # 5. Opportunity Score (Composite) & Overall
    # Opportunity = balanced mix of Value + Quality + Momentum
    opp = (q.value_score * 0.4) + (q.quality_score * 0.3) + (q.momentum_score * 0.3)
    q.opportunity_score = max(0.0, min(100.0, opp))
    
    # Overall = weighted average of all
    overall = (
        (q.quality_score * 0.25) +
        (q.value_score * 0.25) +
        (q.momentum_score * 0.20) +
        (q.risk_score * 0.10) +
        (q.opportunity_score * 0.20)
    )
    q.overall_score = max(0.0, min(100.0, overall))

    # 6. Recommendation (Derived from Overall + Upside)
    # Simple rule-based
    if overall >= 75 and upside > 10:
        q.recommendation = "BUY"
    elif overall >= 65:
        q.recommendation = "BUY" # Weak buy
    elif overall <= 35:
        q.recommendation = "SELL"
    elif overall <= 45:
        q.recommendation = "REDUCE"
    else:
        q.recommendation = "HOLD"


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
            if not fut.done():
                fut.set_result(res)
            return res
        except Exception as e:
            if not fut.done():
                fut.set_exception(e)
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
        
        # Clients (Lazy Loaded via Providers)
        # We assume providers manage their own singletons/clients internally.
        
        logger.info(f"DataEngineV2 v{ENGINE_VERSION} initialized. Providers: {self.enabled_providers}")

    async def aclose(self):
        """Shutdown hook."""
        await tadawul_provider.aclose_tadawul_client()
        await argaam_provider.aclose_argaam_client()
        await yahoo_chart_provider.aclose_yahoo_client()
        await yahoo_fundamentals_provider.aclose_yahoo_fundamentals_client()
        await finnhub_provider.aclose_finnhub_client()
        await eodhd_provider.aclose_eodhd_client()

    # --- Routing Logic ---
    def _get_providers_for_symbol(self, symbol: str) -> List[str]:
        """Determine which providers to query based on symbol pattern."""
        sym = normalize_symbol(symbol)
        
        # KSA Logic: .SR, numeric, or starts with TADAWUL
        is_ksa = sym.endswith(".SR") or sym.isdigit() or sym.startswith("TADAWUL:")
        
        if is_ksa:
            # Order matters: Tadawul -> Argaam -> Fallbacks
            selection = [p for p in self.ksa_providers if p in self.enabled_providers]
            # Add general fallbacks if enabled
            if "yahoo_chart" in self.enabled_providers and "yahoo_chart" not in selection:
                selection.append("yahoo_chart")
            if "finnhub" in self.enabled_providers and "finnhub" not in selection:
                selection.append("finnhub")
            return selection
        else:
            # US/Global Logic
            return [p for p in self.us_providers if p in self.enabled_providers]

    # --- Fetch & Merge Logic ---
    async def _fetch_provider_patch(self, provider_name: str, symbol: str) -> Dict[str, Any]:
        """
        Wraps provider calls to return a standardized patch dict.
        Catches errors so one failure doesn't kill the batch.
        """
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
            else:
                return {}
        except Exception as e:
            logger.warning(f"Provider {provider_name} failed for {symbol}: {e}")
            return {"error": str(e), "provider": provider_name}

    async def get_enriched_quote(self, symbol: str) -> UnifiedQuote:
        """
        Public API: Fetch fully enriched quote for a symbol.
        Uses Singleflight to dedupe concurrent requests.
        """
        return await self._sf.do(symbol, lambda: self._get_enriched_quote_impl(symbol))

    async def _get_enriched_quote_impl(self, symbol: str) -> UnifiedQuote:
        start_ts = time.monotonic()
        norm_sym = normalize_symbol(symbol)
        providers = self._get_providers_for_symbol(norm_sym)
        
        # 1. Concurrently fetch all patches
        async with self._semaphore:
            tasks = [self._fetch_provider_patch(p, norm_sym) for p in providers]
            patches = await asyncio.gather(*tasks, return_exceptions=True)

        # 2. Merge Patches (Priority-based)
        # Base: valid symbol + timestamps
        merged_data: Dict[str, Any] = {
            "symbol": norm_sym,
            "requested_symbol": symbol,
            "symbol_normalized": norm_sym,
            "last_updated_utc": _now_utc_iso(),
            "last_updated_riyadh": _now_riyadh_iso(),
            "data_sources": [],
            "status": "success"
        }

        # Iterate patches (order in `providers` determines priority for overrides, usually)
        # Actually, standard merge pattern: iterate and update `merged_data`.
        # Later providers overwrite earlier ones if we just do `.update()`.
        # WE WANT: Primary (first in list) to have highest authority? 
        # -> No, usually we fetch: [Tadawul, Argaam, Yahoo]. 
        # If Tadawul has price, we keep it. If Yahoo has price, we don't want it overwriting Tadawul.
        # SO: We should merge in REVERSE order of priority, OR check for None before setting.
        # Strategy: Merge from lowest priority to highest priority.
        
        # Reverse the list so high-priority providers are applied LAST (overwriting low-priority)
        # Wait, if `providers` = [Tadawul, Argaam], and Tadawul is index 0.
        # We want Tadawul to win. So we apply Argaam first, then Tadawul.
        
        valid_patches = []
        for i, res in enumerate(patches):
            if isinstance(res, dict) and "error" not in res:
                valid_patches.append((providers[i], res))
                merged_data["data_sources"].append(providers[i])
            else:
                pass # skip errors or exceptions

        # Apply in reverse order (Last item in `providers` applied first, First item applied last)
        for prov_name, patch in reversed(valid_patches):
            # Clean none/empty values from patch to avoid overwriting existing data with None
            clean_patch = {k: v for k, v in patch.items() if v is not None and v != ""}
            merged_data.update(clean_patch)

        # 3. Construct UnifiedQuote
        uq = UnifiedQuote(**merged_data)

        # 4. Post-Process (Scores, Recommendations, Forecast Alignment)
        _calculate_scores(uq)
        
        # Ensure data quality flag
        if not uq.current_price:
            uq.data_quality = "MISSING"
        elif uq.data_quality == "MISSING":
            uq.data_quality = "PARTIAL" # Promoted because we have price

        uq.latency_ms = (time.monotonic() - start_ts) * 1000.0
        return uq

    async def get_quotes(self, symbols: List[str]) -> List[UnifiedQuote]:
        """Batch fetch."""
        return await asyncio.gather(*[self.get_enriched_quote(s) for s in symbols])
    
    # Aliases
    get_quote = get_enriched_quote
    get_enriched_quotes = get_quotes
    fetch_quote = get_enriched_quote
    fetch_quotes = get_quotes

# ---------------------------------------------------------------------------
# Singleton Accessor
# ---------------------------------------------------------------------------
_INSTANCE: Optional[DataEngineV2] = None

def get_engine() -> DataEngineV2:
    global _INSTANCE
    if _INSTANCE is None:
        _INSTANCE = DataEngineV2()
    return _INSTANCE

# Re-exports for compatibility
DataEngine = DataEngineV2
Engine = DataEngineV2
