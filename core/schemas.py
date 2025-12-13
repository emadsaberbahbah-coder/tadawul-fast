"""
core/schemas.py
====================================================
Single source of truth for:
- Canonical Google Sheet names (9-page dashboard)
- Sheet name aliases (tolerant to rename / spacing / case)
- Universal Market Template headers (59 columns)
- Public helper: get_headers_for_sheet(sheet_name)

Author: Emad Bahbah (with GPT-5)
Version: 1.4.0
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

# =============================================================================
# 1) CANONICAL SHEET NAMES (SORTED ORDER)
# =============================================================================

# The "sorted order" you want the system to treat as the official dashboard order.
CANONICAL_SHEET_ORDER: List[str] = [
    "Market_Leaders",
    "KSA_Tadawul_Market",
    "Global_Markets",
    "Mutual_Funds",
    "Commodities_FX",
    "My_Portfolio",
    "Insights_Analysis",
    "Advanced_Analysis",
    "Investment_Advisor",
]

# =============================================================================
# 2) UNIVERSAL MARKET TEMPLATE (59 COLUMNS)
# =============================================================================
# NOTE:
# - Keep this stable across KSA / Global / Mutual Funds / Commodities-FX.
# - Apps Script and backend should both align to this order.
# - These headers MAP PRECISELY to core.enriched_quote._HEADER_FIELD_MAP

UNIVERSAL_MARKET_HEADERS_V59: List[str] = [
    # Identity
    "Symbol",
    "Company Name",
    "Sector",
    "Industry",
    "Market",
    "Currency",

    # Price snapshot
    "Current Price",
    "Previous Close",
    "Price Change",
    "Percent Change",
    "Day High",
    "Day Low",
    "52W High",
    "52W Low",

    # Liquidity
    "Volume",
    "Avg Volume (30D)",
    "Value Traded",

    # Size
    "Shares Outstanding",
    "Free Float %",
    "Market Cap",

    # Valuation + earnings
    "EPS (TTM)",
    "Forward EPS",
    "P/E (TTM)",
    "Forward P/E",
    "P/B",
    "P/S",
    "EV/EBITDA",

    # Dividends + profitability
    "Dividend Yield %",
    "Payout Ratio %",
    "ROE %",
    "ROA %",
    "Net Margin %",
    "EBITDA Margin %",

    # Growth
    "Revenue Growth %",
    "Net Income Growth %",

    # Balance sheet + risk
    "Debt/Equity",
    "Current Ratio",
    "Quick Ratio",
    "Beta",
    "Volatility (30D)",

    # Technical
    "RSI (14)",
    "MACD",
    "MA20",
    "MA50",

    # AI / scoring
    "Fair Value",
    "Upside %",
    "Valuation Label",
    "Value Score",
    "Quality Score",
    "Momentum Score",
    "Opportunity Score",
    "Overall Score",
    "Target Price",
    "Recommendation",

    # Provenance
    "Data Source",
    "Data Quality",
    "Last Updated (UTC)",
    "Last Updated (Riyadh)",
    "Error",
]

# =============================================================================
# 3) TEMPLATES PER SHEET
# =============================================================================

# For now we keep the same template for all market pages.
# If later you want Mutual Funds to have a trimmed template, do it here only.
SHEET_HEADERS: Dict[str, List[str]] = {
    "KSA_Tadawul_Market": UNIVERSAL_MARKET_HEADERS_V59,
    "Global_Markets": UNIVERSAL_MARKET_HEADERS_V59,
    "Mutual_Funds": UNIVERSAL_MARKET_HEADERS_V59,
    "Commodities_FX": UNIVERSAL_MARKET_HEADERS_V59,

    # Non-market pages: default to universal headers if needed (safe fallback).
    "Market_Leaders": UNIVERSAL_MARKET_HEADERS_V59,
    "My_Portfolio": UNIVERSAL_MARKET_HEADERS_V59,
    "Insights_Analysis": UNIVERSAL_MARKET_HEADERS_V59,
    "Advanced_Analysis": UNIVERSAL_MARKET_HEADERS_V59,
    "Investment_Advisor": UNIVERSAL_MARKET_HEADERS_V59,
}

# =============================================================================
# 4) SHEET NAME ALIASES (RENAMES / SPACING / OLD NAMES)
# =============================================================================

SHEET_NAME_ALIASES: Dict[str, List[str]] = {
    # Market pages
    "KSA_Tadawul_Market": [
        "KSA_Tadawul",
        "KSA Tadawul",
        "KSA Tadawul Market",
        "KSA_TadawulMarket",
        "KSA_Market",
        "KSA Market",
        "KSA_Tadawul_Market",
        "05_KSA_Tadawul_Market",
    ],
    "Global_Markets": [
        "Global Markets",
        "Global_Market",
        "Global Market",
        "Global_Markets_Stock",
        "06_Global_Markets",
        "Global_Markets",
    ],
    "Mutual_Funds": [
        "Mutual Funds",
        "Mutual_Fund",
        "MutualFund",
        "Mutual_Funds",
    ],
    "Commodities_FX": [
        "Commodities & FX",
        "Commodities_FX_Market",
        "Commodities FX",
        "Commodities_FX",
        "09_Commodities_FX_Market",
    ],

    # Other pages
    "Market_Leaders": [
        "Market Leaders",
        "Leaders",
        "Market_Leaders",
    ],
    "My_Portfolio": [
        "My Portfolio",
        "Portfolio",
        "My_Portfolio",
        "Holdings",
        "Positions",
    ],
    "Insights_Analysis": [
        "Insights",
        "Insights & Analysis",
        "Insights_Analysis",
        "Insights_And_Analysis",
    ],
    "Advanced_Analysis": [
        "Advanced Analysis",
        "Analysis",
        "Advanced_Analysis",
    ],
    "Investment_Advisor": [
        "Investment Advisor",
        "Advisor",
        "Investment_Advisor",
    ],
}

# =============================================================================
# 5) INTERNAL NORMALIZATION + LOOKUPS
# =============================================================================

def _norm(s: Optional[str]) -> str:
    """Normalize sheet name for alias matching (case/space/underscore tolerant)."""
    if s is None:
        return ""
    return (
        str(s)
        .strip()
        .lower()
        .replace("&", "and")
        .replace("-", "_")
        .replace(" ", "_")
        .replace("__", "_")
    )

# alias -> canonical
_ALIAS_TO_CANONICAL: Dict[str, str] = {}

def _build_alias_index() -> None:
    _ALIAS_TO_CANONICAL.clear()

    # canonical names map to themselves
    for canon in CANONICAL_SHEET_ORDER:
        _ALIAS_TO_CANONICAL[_norm(canon)] = canon

    # add configured aliases
    for canon, aliases in SHEET_NAME_ALIASES.items():
        _ALIAS_TO_CANONICAL[_norm(canon)] = canon
        for a in aliases:
            _ALIAS_TO_CANONICAL[_norm(a)] = canon

_build_alias_index()

# =============================================================================
# 6) PUBLIC API
# =============================================================================

def normalize_sheet_name(sheet_name: str) -> str:
    """
    Return canonical sheet name if recognized, otherwise return the original input.
    """
    key = _norm(sheet_name)
    return _ALIAS_TO_CANONICAL.get(key, sheet_name)

def get_headers_for_sheet(sheet_name: str, *, fallback: Optional[List[str]] = None) -> List[str]:
    """
    Main function expected by routes (enriched_quote / others).

    - Accepts many aliases.
    - Returns a COPY of the header list to avoid accidental mutation.
    - If sheet is unknown, returns fallback (or universal template).
    """
    canon = normalize_sheet_name(sheet_name)

    headers = SHEET_HEADERS.get(canon)
    if headers:
        return list(headers)

    if fallback is not None:
        return list(fallback)

    return list(UNIVERSAL_MARKET_HEADERS_V59)

def get_canonical_sheets_sorted() -> List[str]:
    """Return the official sorted sheet list (your requested ordering)."""
    return list(CANONICAL_SHEET_ORDER)

def get_alias_map() -> Dict[str, str]:
    """Return a normalized alias->canonical map (for debugging)."""
    return dict(_ALIAS_TO_CANONICAL)

def validate_templates(raise_on_error: bool = False) -> Tuple[bool, List[str]]:
    """
    Validate that:
    - All canonical sheets have a template entry
    - Header templates have no duplicates
    """
    issues: List[str] = []

    for canon in CANONICAL_SHEET_ORDER:
        if canon not in SHEET_HEADERS:
            issues.append(f"Missing SHEET_HEADERS entry for canonical sheet: {canon}")

    for name, headers in SHEET_HEADERS.items():
        seen = set()
        dups = []
        for h in headers:
            if h in seen:
                dups.append(h)
            seen.add(h)
        if dups:
            issues.append(f"Duplicate headers in template '{name}': {sorted(set(dups))}")

    ok = len(issues) == 0
    if (not ok) and raise_on_error:
        raise ValueError("Schema template validation failed:\n- " + "\n- ".join(issues))
    return ok, issues

# Validate at import-time (safe, non-fatal)
validate_templates(raise_on_error=False)

__all__ = [
    "CANONICAL_SHEET_ORDER",
    "UNIVERSAL_MARKET_HEADERS_V59",
    "SHEET_HEADERS",
    "SHEET_NAME_ALIASES",
    "normalize_sheet_name",
    "get_headers_for_sheet",
    "get_canonical_sheets_sorted",
    "get_alias_map",
    "validate_templates",
]
