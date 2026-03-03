#!/usr/bin/env python3
# core/sheets/page_catalog.py
"""
================================================================================
Page Catalog — v2.0.0 (CANONICAL PAGES + ALIASES + TOP10 FEEDS)
================================================================================
Tadawul Fast Bridge (TFB)

Purpose:
- Define the ONLY valid sheet/page names the backend will accept.
- Provide safe alias normalization (e.g., "Market Leaders" -> "Market_Leaders").
- Explicitly remove/forbid legacy pages like KSA_Tadawul.
- Define which pages can feed Top_10_Investments selection.

Hard rules:
- No network calls. Import-safe.
- If a page is unknown, raise a clear error with allowed values.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

from core.sheets.schema_registry import SCHEMA_REGISTRY

__all__ = [
    "PAGE_CATALOG_VERSION",
    "PageInfo",
    "CANONICAL_PAGES",
    "ALIAS_TO_CANONICAL",
    "TOP10_FEED_PAGES_DEFAULT",
    "is_canonical_page",
    "is_forbidden_page",
    "normalize_page_name",
    "get_top10_feed_pages",
    "allowed_pages",
    "validate_page_catalog",
]

PAGE_CATALOG_VERSION = "2.0.0"


@dataclass(frozen=True)
class PageInfo:
    canonical: str
    description: str = ""
    is_data_page: bool = True          # used for row-per-instrument pages
    eligible_for_top10: bool = False   # if included in Top 10 selection universe


# -----------------------------
# Canonical pages (single truth)
# -----------------------------

# SCHEMA_REGISTRY is authoritative for what exists as sheets.
# We keep the catalog aligned with schema_registry by referencing its keys.
_CANONICAL_FROM_SCHEMA: Set[str] = set(SCHEMA_REGISTRY.keys())

# Explicitly forbid any legacy or removed pages (even if user passes them).
FORBIDDEN_PAGES: Set[str] = {"KSA_Tadawul", "Advisor_Criteria"}  # Advisor_Criteria moved into Insights_Analysis


# Define PageInfo (adds meaning / eligibility)
PAGE_INFO: Dict[str, PageInfo] = {
    "Market_Leaders": PageInfo(
        canonical="Market_Leaders",
        description="Primary leaders/watchlist universe.",
        is_data_page=True,
        eligible_for_top10=True,
    ),
    "Global_Markets": PageInfo(
        canonical="Global_Markets",
        description="Global indices/shares universe.",
        is_data_page=True,
        eligible_for_top10=True,
    ),
    "Commodities_FX": PageInfo(
        canonical="Commodities_FX",
        description="Commodities and FX tickers.",
        is_data_page=True,
        eligible_for_top10=True,
    ),
    "Mutual_Funds": PageInfo(
        canonical="Mutual_Funds",
        description="Mutual funds / ETFs universe.",
        is_data_page=True,
        eligible_for_top10=True,
    ),
    "My_Portfolio": PageInfo(
        canonical="My_Portfolio",
        description="User portfolio positions.",
        is_data_page=True,
        eligible_for_top10=True,  # can be turned off if you want Top10 to exclude portfolio
    ),
    "Insights_Analysis": PageInfo(
        canonical="Insights_Analysis",
        description="Criteria block + insights table (not an instrument table).",
        is_data_page=False,
        eligible_for_top10=False,
    ),
    "Top_10_Investments": PageInfo(
        canonical="Top_10_Investments",
        description="Output sheet populated by Top10 selector.",
        is_data_page=False,
        eligible_for_top10=False,
    ),
    "Data_Dictionary": PageInfo(
        canonical="Data_Dictionary",
        description="Auto-built from schema_registry; documentation sheet.",
        is_data_page=False,
        eligible_for_top10=False,
    ),
}

# Canonical list (sorted for stable UX)
CANONICAL_PAGES: List[str] = sorted(PAGE_INFO.keys())


# -----------------------------
# Aliases
# -----------------------------

def _normalize_token(s: str) -> str:
    """
    Normalize user input for alias matching:
    - trim
    - collapse whitespace
    - remove non-alphanum (keep underscores)
    - casefold
    """
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    s = s.replace("-", " ")
    s = s.replace("/", " ")
    s = s.casefold()
    s = re.sub(r"[^a-z0-9_ ]+", "", s)
    s = s.replace(" ", "_")
    return s


# map normalized alias -> canonical
ALIAS_TO_CANONICAL: Dict[str, str] = {}

def _add_alias(alias: str, canonical: str) -> None:
    ALIAS_TO_CANONICAL[_normalize_token(alias)] = canonical


# Human-friendly
_add_alias("Market Leaders", "Market_Leaders")
_add_alias("Market_Leaders", "Market_Leaders")
_add_alias("Leaders", "Market_Leaders")

_add_alias("Global Markets", "Global_Markets")
_add_alias("Global_Markets", "Global_Markets")
_add_alias("Global", "Global_Markets")

_add_alias("Commodities & FX", "Commodities_FX")
_add_alias("Commodities_FX", "Commodities_FX")
_add_alias("FX", "Commodities_FX")
_add_alias("Commodities", "Commodities_FX")

_add_alias("Mutual Funds", "Mutual_Funds")
_add_alias("Mutual_Funds", "Mutual_Funds")
_add_alias("Funds", "Mutual_Funds")
_add_alias("ETF", "Mutual_Funds")
_add_alias("ETFs", "Mutual_Funds")

_add_alias("My Portfolio", "My_Portfolio")
_add_alias("My_Portfolio", "My_Portfolio")
_add_alias("Portfolio", "My_Portfolio")

_add_alias("Insights", "Insights_Analysis")
_add_alias("Insights Analysis", "Insights_Analysis")
_add_alias("Insights_Analysis", "Insights_Analysis")

_add_alias("Top 10", "Top_10_Investments")
_add_alias("Top10", "Top_10_Investments")
_add_alias("Top_10_Investments", "Top_10_Investments")

_add_alias("Data Dictionary", "Data_Dictionary")
_add_alias("Data_Dictionary", "Data_Dictionary")
_add_alias("Dictionary", "Data_Dictionary")

# Explicit forbidden legacy aliases (these normalize to forbidden tokens)
FORBIDDEN_ALIASES: Set[str] = {_normalize_token("KSA_Tadawul"), _normalize_token("Advisor_Criteria")}


# -----------------------------
# Top10 feeds (default universe)
# -----------------------------

TOP10_FEED_PAGES_DEFAULT: List[str] = [
    "Market_Leaders",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
    "My_Portfolio",
]


# -----------------------------
# API
# -----------------------------

def allowed_pages() -> List[str]:
    return list(CANONICAL_PAGES)


def is_canonical_page(page: str) -> bool:
    return page in PAGE_INFO


def is_forbidden_page(page: str) -> bool:
    # check raw and normalized
    if page in FORBIDDEN_PAGES:
        return True
    token = _normalize_token(page)
    return token in FORBIDDEN_ALIASES


def normalize_page_name(page: str, *, allow_output_pages: bool = True) -> str:
    """
    Convert any alias into a canonical page name.
    - Raises ValueError if page is forbidden or unknown.
    - If allow_output_pages=False, forbids Top_10_Investments / Data_Dictionary too.
    """
    if not page or not str(page).strip():
        raise ValueError("Page name is empty. Provide a valid page name.")

    if is_forbidden_page(page):
        raise ValueError(
            f"Page '{page}' is forbidden/removed (e.g., KSA_Tadawul). "
            f"Allowed pages: {', '.join(CANONICAL_PAGES)}"
        )

    token = _normalize_token(page)
    canonical = ALIAS_TO_CANONICAL.get(token)

    if not canonical:
        # Also allow exact canonical (case-sensitive) as a fallback
        if page in PAGE_INFO:
            canonical = page
        else:
            raise ValueError(
                f"Unknown page '{page}'. Allowed pages: {', '.join(CANONICAL_PAGES)}"
            )

    if canonical not in PAGE_INFO:
        raise ValueError(
            f"Page '{page}' resolved to '{canonical}', but it is not registered. "
            f"Allowed pages: {', '.join(CANONICAL_PAGES)}"
        )

    if not allow_output_pages and canonical in {"Top_10_Investments", "Data_Dictionary"}:
        raise ValueError(
            f"Page '{canonical}' is an output/meta sheet and not allowed for this operation."
        )

    return canonical


def get_top10_feed_pages(pages_override: Optional[List[str]] = None) -> List[str]:
    """
    Returns canonical pages eligible for Top10 selection.
    If pages_override is provided, it will normalize & filter it.
    """
    pages = pages_override or TOP10_FEED_PAGES_DEFAULT
    normalized: List[str] = []
    for p in pages:
        cp = normalize_page_name(p, allow_output_pages=False)
        info = PAGE_INFO.get(cp)
        if info and info.eligible_for_top10:
            normalized.append(cp)

    # de-duplicate but keep order
    seen = set()
    out = []
    for p in normalized:
        if p not in seen:
            seen.add(p)
            out.append(p)

    return out


# -----------------------------
# Validation
# -----------------------------

def validate_page_catalog() -> None:
    # Forbidden pages must not exist in schema registry
    for fp in FORBIDDEN_PAGES:
        if fp in _CANONICAL_FROM_SCHEMA:
            raise ValueError(f"Forbidden page '{fp}' exists in schema_registry. Remove it.")

    # Catalog pages must exist in schema registry
    for canonical in PAGE_INFO.keys():
        if canonical not in _CANONICAL_FROM_SCHEMA:
            raise ValueError(
                f"Page '{canonical}' exists in page_catalog but not in schema_registry."
            )

    # Schema registry pages must be known in the catalog (strict)
    for sheet in _CANONICAL_FROM_SCHEMA:
        if sheet not in PAGE_INFO:
            raise ValueError(
                f"Sheet '{sheet}' exists in schema_registry but not in page_catalog. Add PageInfo."
            )

    # Aliases must resolve to known pages and must not map to forbidden pages
    for alias_token, canonical in ALIAS_TO_CANONICAL.items():
        if canonical in FORBIDDEN_PAGES:
            raise ValueError(f"Alias token '{alias_token}' maps to forbidden page '{canonical}'.")
        if canonical not in PAGE_INFO:
            raise ValueError(f"Alias token '{alias_token}' maps to unknown page '{canonical}'.")

    # Ensure Top10 default feeds are eligible and are data pages
    for p in TOP10_FEED_PAGES_DEFAULT:
        if p not in PAGE_INFO:
            raise ValueError(f"TOP10_FEED_PAGES_DEFAULT includes unknown page '{p}'.")
        info = PAGE_INFO[p]
        if not info.is_data_page:
            raise ValueError(f"TOP10_FEED_PAGES_DEFAULT includes non-data page '{p}'.")
        if not info.eligible_for_top10:
            raise ValueError(f"TOP10_FEED_PAGES_DEFAULT includes ineligible page '{p}'.")


# Validate immediately (fast, no I/O)
validate_page_catalog()
