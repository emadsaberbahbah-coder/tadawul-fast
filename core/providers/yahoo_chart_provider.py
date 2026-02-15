#!/usr/bin/env python3
# core/providers/yahoo_chart_provider.py
"""
core/providers/yahoo_chart_provider.py
============================================================
Yahoo Quote/Chart Provider (via yfinance) — v2.3.0
(RELIABLE PRICE/HISTORY FETCH + ALIGNED MOMENTUM FORECASTS)

Updates in v2.3.0:
- ✅ Key Alignment: Strictly uses 'expected_roi_1m/3m/12m' and 'forecast_price_*'.
- ✅ Riyadh Localization: Adds 'forecast_updated_riyadh' (UTC+3).
- ✅ Resilience: Bypasses direct HTTP blocks by using yfinance's internal handling.
- ✅ History Analytics: Calculates Momentum ROI for 1M/3M/12M.
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

PROVIDER_VERSION = "2.3.0"
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
    # Note: These keys must match the Sheet Controller's expectations
    if roi_1m is not None:
        out["expected_roi_1m"] = roi_1m
        out["forecast_price_1m"] = last * (1 + (roi_1m / 100.0))
    
    if roi_3m is not None:
        out["expected_roi_3m"] = roi_3m
        out["forecast_price_3m"] = last * (1 + (roi_3m / 100.0))

    # For 12M, we align with the key 'expected_roi_12m' used by other providers
    if roi_12m is not None:
        out["expected_roi_12m"] = roi_12m 
        out["forecast_price_12m"] = last * (1 + (roi_12m / 100.0))

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
            # Blocking call to yfinance in thread to avoid event loop blocking
            def _get_data():
                ticker = yf.Ticker(u_sym)
                # Fast price check
                # .fast_info is generally faster for realtime/delayed price
                price = ticker.fast_info.last_price
                prev_close = ticker.fast_info.previous_close
                
                # Fetch history for charts/momentum
                # 1y history is sufficient for 12m momentum calculation
                hist = ticker.history(period="1y") 
                return price, prev_close, hist

            current_price, prev_close, history = await asyncio.to_thread(_get_data)

            if current_price is None:
                return {"error": "Price not found"}

            # Basic Quote Data - keys aligned with data engine
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
                # Clean NaNs and non-numeric values
                closes = [c for c in closes if c is not None and not math.isnan(c)]
                
                if closes:
                    # Enrich with Day/Year High/Low from history
                    # Using history for high/low is a good fallback if realtime data is sparse
                    out["day_high"] = max(closes[-5:]) # Approx recent high (last 5 days)
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

# Cleanup (no-op for yfinance as it manages its own sessions)
async def aclose_yahoo_client():
    pass

__all__ = [
    "YahooChartProvider",
    "fetch_enriched_quote_patch",
    "aclose_yahoo_client",
    "PROVIDER_NAME"
]
