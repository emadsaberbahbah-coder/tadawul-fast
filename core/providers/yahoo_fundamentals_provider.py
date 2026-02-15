#!/usr/bin/env python3
# core/providers/yahoo_fundamentals_provider.py
"""
core/providers/yahoo_fundamentals_provider.py
============================================================
Yahoo Fundamentals Provider (via yfinance) — v1.9.0
(ROBUST PROFILE FETCH + ALIGNED FORECAST KEYS + SAFETY)

Updates in v1.9.0:
- ✅ Engine Switch: Uses yfinance for reliable access to Sector/Industry/Name.
- ✅ Forecasts: Extracts targetMeanPrice for 12M Forecasts & ROI.
- ✅ Key Alignment: Matches Sheet Controller v5.8.0 (e.g., 'sub_sector', 'name').
- ✅ Safety: robust float conversion and None checks.
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional

import yfinance as yf

logger = logging.getLogger("core.providers.yahoo_fundamentals_provider")

PROVIDER_NAME = "yahoo_fundamentals"
PROVIDER_VERSION = "1.9.0"

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
        if isinstance(x, str): 
            # Clean common trash from string numbers
            x = x.replace(",", "").replace("%", "").replace("$", "").strip()
            if x.lower() in ("n/a", "null", "none", "-"): return None
        f = float(x)
        return f if not math.isnan(f) and not math.isinf(f) else None
    except: return None

def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _riyadh_iso() -> str:
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).isoformat()

# ---------------------------
# Main Provider Class
# ---------------------------
@dataclass
class YahooFundamentalsProvider:
    name: str = PROVIDER_NAME

    async def fetch_profile(self, symbol: str, debug: bool = False) -> Dict[str, Any]:
        """
        Fetches asset profile, financials, and analyst targets.
        """
        u_sym = _normalize_symbol(symbol)
        if not u_sym: return {}

        if debug: logger.info(f"[{u_sym}] Fetching fundamentals via yfinance...")

        try:
            # Blocking call to yfinance in thread to avoid blocking event loop
            def _get_info():
                try:
                    return yf.Ticker(u_sym).info
                except Exception as e:
                    logger.error(f"[{u_sym}] yfinance internal error: {e}")
                    return {}

            info = await asyncio.to_thread(_get_info)

            # Check if info is effectively empty (common failure mode)
            if not info or ("symbol" not in info and "shortName" not in info and "regularMarketPrice" not in info):
                if debug: logger.warning(f"[{u_sym}] Empty info returned.")
                return {"error": "No data found"}

            # --- 1. Basic Identity (Matched to Sheet Controller Dictionary) ---
            out = {
                "symbol": u_sym,
                "name": info.get("longName") or info.get("shortName"),
                "sector": info.get("sector"),
                "sub_sector": info.get("industry"), # Critical mapping: industry -> sub_sector
                "industry": info.get("industry"),
                "market": info.get("market"),
                "currency": info.get("currency") or info.get("financialCurrency"),
                "exchange": info.get("exchange"),
                "country": info.get("country"),
                "website": info.get("website"),
                "employees": info.get("fullTimeEmployees"),
                "summary": info.get("longBusinessSummary") or info.get("description"),
                "listing_date": info.get("firstTradeDateEpochUtc"), # Timestamp
            }

            # --- 2. Financials (Raw -> Float) ---
            out.update({
                "market_cap": _to_float(info.get("marketCap")),
                "pe_ttm": _to_float(info.get("trailingPE")),
                "forward_pe": _to_float(info.get("forwardPE")),
                "eps_ttm": _to_float(info.get("trailingEps")),
                "forward_eps": _to_float(info.get("forwardEps")),
                "pb": _to_float(info.get("priceToBook")),
                "ps": _to_float(info.get("priceToSalesTrailing12Months")),
                "beta": _to_float(info.get("beta")),
                "book_value": _to_float(info.get("bookValue")),
                "ebitda": _to_float(info.get("ebitda")),
                "total_debt": _to_float(info.get("totalDebt")),
                "total_revenue": _to_float(info.get("totalRevenue")),
                "free_cashflow": _to_float(info.get("freeCashflow")),
                "shares_outstanding": _to_float(info.get("sharesOutstanding")),
            })

            # --- 3. Percentage Fields (Convert Decimal 0.05 -> 5.00 for Sheet) ---
            # Helper to convert decimal to percentage safely
            def _get_pct(k):
                val = _to_float(info.get(k))
                return val * 100.0 if val is not None else None

            out["dividend_yield"] = _get_pct("dividendYield")
            out["dividend_rate"] = _to_float(info.get("dividendRate"))
            out["roe"] = _get_pct("returnOnEquity")
            out["roa"] = _get_pct("returnOnAssets")
            out["profit_margin"] = _get_pct("profitMargins")
            out["net_margin"] = _get_pct("profitMargins") # Alias
            out["revenue_growth"] = _get_pct("revenueGrowth")
            out["payout_ratio"] = _get_pct("payoutRatio")

            # --- 4. Analyst Forecasts & Targets ---
            # Yahoo provides 'targetMeanPrice' which is the 12-month analyst target
            current_price = _to_float(info.get("currentPrice")) or _to_float(info.get("regularMarketPrice"))
            target_mean = _to_float(info.get("targetMeanPrice"))
            recommendation = info.get("recommendationKey") # e.g. 'buy', 'hold'
            num_analysts = _to_float(info.get("numberOfAnalystOpinions"))

            out["recommendation"] = recommendation.upper() if recommendation else "HOLD"
            out["target_mean_price"] = target_mean
            out["analyst_count"] = num_analysts
            
            # Calculate ROI 12M based on Analyst Target
            if current_price and target_mean:
                roi = ((target_mean / current_price) - 1.0) * 100.0
                out["forecast_price_12m"] = target_mean
                out["target_price_12m"] = target_mean # Alias
                out["expected_roi_12m"] = roi
                out["upside_percent"] = roi
                
                # Dynamic confidence based on number of analysts
                base_conf = 0.60
                if num_analysts:
                    base_conf = min(0.95, 0.60 + (math.log(num_analysts) * 0.05))
                
                out["forecast_confidence"] = base_conf
                out["forecast_method"] = "analyst_consensus"
                out["forecast_updated_utc"] = _utc_iso()
            
            # --- 5. Metadata ---
            out["data_source"] = PROVIDER_NAME
            out["provider_version"] = PROVIDER_VERSION
            out["last_updated_utc"] = _utc_iso()
            out["last_updated_riyadh"] = _riyadh_iso()
            # If forecast wasn't set by analysts, set a localized timestamp anyway so sheet doesn't look broken
            if "forecast_updated_utc" not in out:
                 out["forecast_updated_utc"] = _utc_iso()
            out["forecast_updated_riyadh"] = _riyadh_iso()

            if debug:
                logger.info(f"[{u_sym}] Success. Name: {out.get('name')}, Sector: {out.get('sector')}")

            # Filter None values to keep JSON clean
            return {k: v for k, v in out.items() if v is not None}

        except Exception as e:
            logger.error(f"[{u_sym}] Fundamentals Error: {e}")
            return {"error": str(e)}

# ---------------------------
# Compatibility Aliases
# ---------------------------
async def fetch_fundamentals_patch(symbol: str, debug: bool = False) -> Dict[str, Any]:
    p = YahooFundamentalsProvider()
    return await p.fetch_profile(symbol, debug)

async def fetch_enriched_quote_patch(symbol: str, **kwargs) -> Dict[str, Any]:
    return await fetch_fundamentals_patch(symbol)

# Cleanup (no-op for yfinance)
async def aclose_yahoo_fundamentals_client():
    pass

__all__ = [
    "YahooFundamentalsProvider",
    "fetch_fundamentals_patch",
    "fetch_enriched_quote_patch",
    "aclose_yahoo_fundamentals_client",
    "PROVIDER_NAME"
]
