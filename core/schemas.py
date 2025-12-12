"""
core/schemas.py
===============================================
Schema helpers & sheet header registry - v1.3

Author: Emad Bahbah (with GPT-5.2 Thinking)

Why this file exists
--------------------
Some routes (routes.enriched_quote / ai_analysis / advanced_analysis) prefer to ask
core.schemas for the exact header list of a sheet, so the backend output is ALWAYS
aligned with Google Sheets columns.

This file provides:
- UNIVERSAL_MARKET_HEADERS (59 columns)
- get_headers_for_sheet(sheet_name)
- sheet name aliases (KSA/Global/Mutual Funds/FX variations)

If a sheet name is unknown, we return the universal headers by default.
"""

from __future__ import annotations

import re
from typing import Dict, List, Sequence

# ============================================================================
# Universal Market Template (59 columns)
# IMPORTANT: Keep this aligned with Apps Script header row (Row 5)
# ============================================================================

UNIVERSAL_MARKET_HEADERS: List[str] = [
    # Identity
    "Symbol",
    "Company Name",
    "Sector",
    "Sub-Sector",
    "Market",
    "Currency",
    "Listing Date",

    # Price snapshot
    "Last Price",
    "Previous Close",
    "Price Change",
    "Percent Change",
    "Day High",
    "Day Low",

    # 52W stats
    "52W High",
    "52W Low",
    "52W Position %",

    # Liquidity
    "Volume",
    "Avg Volume (30D)",
    "Value Traded",
    "Turnover %",
    "Shares Outstanding",
    "Free Float %",
    "Market Cap",
    "Free Float Market Cap",
    "Liquidity Score",

    # Fundamentals / valuation
    "EPS (TTM)",
    "Forward EPS",
    "P/E (TTM)",
    "Forward P/E",
    "P/B",
    "P/S",
    "EV/EBITDA",

    # Dividends
    "Dividend Yield %",
    "Dividend Rate",
    "Payout Ratio %",

    # Profitability
    "ROE %",
    "ROA %",
    "Net Margin %",
    "EBITDA Margin %",

    # Growth
    "Revenue Growth %",
    "Net Income Growth %",

    # Risk / technical
    "Beta",
    "Volatility (30D)",
    "RSI (14)",
    "MACD",

    # AI valuation & scoring
    "Fair Value",
    "Upside %",
    "Valuation Label",
    "Value Score",
    "Quality Score",
    "Momentum Score",
    "Opportunity Score",
    "Rank",
    "Recommendation",

    # Provenance / timestamps
    "Data Source",
    "Last Updated (Riyadh)",
    "Data Quality",
    "Last Updated (UTC)",
    "Error",
]

# Backward compatible aliases (some older files may import these names)
UNIVERSAL_HEADERS = UNIVERSAL_MARKET_HEADERS
UNIVERSAL_MARKET_TEMPLATE_HEADERS = UNIVERSAL_MARKET_HEADERS


# ============================================================================
# Sheet header registry (use universal headers for core market pages)
# ============================================================================

_SHEET_HEADERS: Dict[str, List[str]] = {
    # Main market pages (use same 59-col template)
    "ksa_tadawul_market": UNIVERSAL_MARKET_HEADERS,
    "ksa_tadawul": UNIVERSAL_MARKET_HEADERS,
    "ksa_market": UNIVERSAL_MARKET_HEADERS,

    "global_markets": UNIVERSAL_MARKET_HEADERS,
    "global_market": UNIVERSAL_MARKET_HEADERS,

    "mutual_funds": UNIVERSAL_MARKET_HEADERS,
    "mutual_fund": UNIVERSAL_MARKET_HEADERS,
    "funds": UNIVERSAL_MARKET_HEADERS,

    "commodities_fx": UNIVERSAL_MARKET_HEADERS,
    "commodities__fx": UNIVERSAL_MARKET_HEADERS,
    "fx": UNIVERSAL_MARKET_HEADERS,
    "commodities": UNIVERSAL_MARKET_HEADERS,

    "my_portfolio": UNIVERSAL_MARKET_HEADERS,
    "portfolio": UNIVERSAL_MARKET_HEADERS,
}

# Friendly aliases (sheet tab names may contain spaces or different casing)
_SHEET_ALIASES: Dict[str, str] = {
    "ksa tadawul market": "ksa_tadawul_market",
    "ksa_tadawul_market": "ksa_tadawul_market",
    "ksa_tadawul": "ksa_tadawul",
    "ksa market": "ksa_market",
    "ksa_market": "ksa_market",

    "global markets": "global_markets",
    "global_markets": "global_markets",

    "mutual funds": "mutual_funds",
    "mutual_funds": "mutual_funds",

    "commodities & fx": "commodities_fx",
    "commodities_fx": "commodities_fx",
    "commodities fx": "commodities_fx",

    "my portfolio": "my_portfolio",
    "my_portfolio": "my_portfolio",
}


def _norm_sheet_name(name: str) -> str:
    s = (name or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def get_headers_for_sheet(sheet_name: str) -> List[str]:
    """
    Return headers for a given sheet name.

    - Accepts many variants ("Global_Markets", "global markets", etc.)
    - If unknown sheet name: returns UNIVERSAL_MARKET_HEADERS (safe default)
    """
    raw = _norm_sheet_name(sheet_name)
    if not raw:
        return list(UNIVERSAL_MARKET_HEADERS)

    key = _SHEET_ALIASES.get(raw, raw)
    key = key.replace(" ", "_")

    headers = _SHEET_HEADERS.get(key)
    if headers:
        return list(headers)

    # Safe fallback (prevents crashes and stops the routes warning)
    return list(UNIVERSAL_MARKET_HEADERS)


def list_supported_sheets() -> List[str]:
    """Return canonical sheet keys supported by get_headers_for_sheet()."""
    return sorted(set(_SHEET_HEADERS.keys()))


__all__ = [
    "UNIVERSAL_MARKET_HEADERS",
    "UNIVERSAL_HEADERS",
    "UNIVERSAL_MARKET_TEMPLATE_HEADERS",
    "get_headers_for_sheet",
    "list_supported_sheets",
]
