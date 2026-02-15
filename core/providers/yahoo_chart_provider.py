#!/usr/bin/env python3
# core/providers/yahoo_chart_provider.py
"""
core/providers/yahoo_chart_provider.py
============================================================
Yahoo Quote/Chart Provider (via yfinance) — v2.2.0
(RELIABLE PRICE/HISTORY FETCH + MOMENTUM FORECASTS)

Updates in v2.2.0:
- ✅ Engine Switch: Uses yfinance for reliable price & history data.
- ✅ Momentum Forecasts: Calculates ROI based on historical closes.
- ✅ Resilience: Bypasses direct HTTP blocks by using yfinance's internal handling.
"""

from __future__ import annotations

import asyncio
import logging
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import yfinance as yf

logger = logging.getLogger("core.providers.yahoo_chart_provider")

PROVIDER_VERSION = "2.2.0"
PROVIDER_NAME = "yahoo_chart"

_KSA_CODE_RE = re.compile(r"^\d{3,6}$")

# ---------------------------
# Helpers
# ---------------------------
def _normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s: return ""
    if s.startswith("TADAWUL:"): s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"): s = s.replace(".TADAWUL", "").strip()
    if _KSA_CODE_RE.match(s): return f"{s}.SR"
    return s

def _to_float(x: Any) -> Optional[float]:
    if x is None: return None
    try:
        if isinstance(x, str): x = x.replace(",", "").replace("%", "")
        f = float(x)
        return f if not math.isnan(f) and not math.isinf(f) else None
    except: return None

def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _riyadh_iso() -> str:
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).isoformat()

def _calculate_momentum_forecasts(closes: List[float]) -> Dict[str, Any]:
    """Calculates simple momentum-based forecasts from historical closes."""
    out = {}
    if not closes: return out
    
    last = closes[-1]
    n = len(closes)

    # Helper to get ROI over 'days' ago
    def get_roi(days_ago: int) -> Optional[float]:
        if n > days_ago and closes[-(days_ago+1)] > 0:
            start_price = closes[-(days_ago+1)]
            return ((last / start_price) - 1.0) * 100.0
        return None

    # Calculate ROIs (1M~21d, 3M~63d, 12M~252d)
    roi_1m = get_roi(21)
    roi_3m = get_roi(63)
    roi_12m = get_roi(252)

    # Project future price based on past momentum (Simple Projection)
    # Note: This is a technical projection, distinct from Analyst Targets in Fundamentals
    if roi_1m is not None:
        out["expected_roi_1m"] = roi_1m
        out["forecast_price_1m"] = last * (1 + (roi_1m / 100.0))
    
    if roi_3m is not None:
        out["expected_roi_3m"] = roi_3m
        out["forecast_price_3m"] = last * (1 + (roi_3m / 100.0))

    # For 12M, we often prefer analyst targets, but if missing, use momentum
    if roi_12m is not None:
        out["momentum_roi_12m"] = roi_12m # Distinct key to avoid overwriting analyst target

    out["forecast_method"] = "technical_momentum"
    out["forecast_confidence"] = 0.60 # Lower confidence than analysts
    out["forecast_updated_utc"] = _utc_iso()
    out["forecast_updated_riyadh"] = _riyadh_iso()
    
    return out

# ---------------------------
# Main Provider Class
# ---------------------------
@dataclass
class YahooChartProvider:
    name: str = PROVIDER_NAME

    async def fetch_quote(self, symbol: str, debug: bool = False) -> Dict[str, Any]:
        """
        Fetches current price and historical context via yfinance.
        """
        u_sym = _normalize_symbol(symbol)
        if not u_sym: return {}

        try:
            # Blocking call to yfinance in thread
            def _get_data():
                ticker = yf.Ticker(u_sym)
                # Fast price check
                # Note: .fast_info is often faster/more reliable for current price than .info
                price = ticker.fast_info.last_price
                prev_close = ticker.fast_info.previous_close
                
                # Fetch history for charts/momentum
                hist = ticker.history(period="1y") # 1 year for 12m momentum
                return price, prev_close, hist

            current_price, prev_close, history = await asyncio.to_thread(_get_data)

            if current_price is None:
                return {"error": "Price not found"}

            # Basic Quote Data
            out = {
                "symbol": u_sym,
                "current_price": float(current_price),
                "previous_close": float(prev_close) if prev_close else None,
                "data_source": PROVIDER_NAME,
                "provider_version": PROVIDER_VERSION,
                "last_updated_utc": _utc_iso()
            }

            # Calc basic change if possible
            if out["previous_close"]:
                diff = out["current_price"] - out["previous_close"]
                out["change"] = diff
                out["percent_change"] = (diff / out["previous_close"]) * 100.0

            # Add History/Forecasts
            if not history.empty and "Close" in history:
                closes = history["Close"].tolist()
                # Clean NaNs
                closes = [c for c in closes if c is not None and not math.isnan(c)]
                
                if closes:
                    # Enrich with Day/Year High/Low from history
                    out["day_high"] = max(closes[-5:]) # Approx recent high
                    out["day_low"] = min(closes[-5:])
                    out["high_52w"] = max(closes)
                    out["low_52w"] = min(closes)
                    
                    # 52W Position
                    if out["high_52w"] > out["low_52w"]:
                        out["position_52w"] = ((out["current_price"] - out["low_52w"]) / 
                                              (out["high_52w"] - out["low_52w"])) * 100.0

                    # Momentum Forecasts
                    forecasts = _calculate_momentum_forecasts(closes)
                    out.update(forecasts)

            if debug:
                logger.info(f"[{u_sym}] Price: {out['current_price']}")

            return out

        except Exception as e:
            logger.error(f"[{u_sym}] Chart Error: {e}")
            return {"error": str(e)}

# ---------------------------
# Compatibility Aliases
# ---------------------------
async def fetch_enriched_quote_patch(symbol: str, debug: bool = False) -> Dict[str, Any]:
    p = YahooChartProvider()
    return await p.fetch_quote(symbol, debug)

# Cleanup (no-op for yfinance)
async def aclose_yahoo_client():
    pass

__all__ = [
    "YahooChartProvider",
    "fetch_enriched_quote_patch",
    "aclose_yahoo_client",
    "PROVIDER_NAME"
]
