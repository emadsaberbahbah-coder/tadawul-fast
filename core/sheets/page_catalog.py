#!/usr/bin/env python3
# core/sheets/page_catalog.py
"""
================================================================================
Page Catalog — v2.1.0 (CANONICAL PAGES + STRONG NORMALIZATION + NO 80-FALLBACK)
================================================================================
Tadawul Fast Bridge (TFB)

Purpose:
- Define the ONLY valid sheet/page names the backend will accept.
- Provide robust alias normalization (case/space/dash/slash/dots) so the request
  never "misses" and falls into a default-schema path.
- Explicitly forbid legacy/removed pages like KSA_Tadawul and Advisor_Criteria.
- Define which pages can feed Top_10_Investments selection.

Key fix for your issue:
- "Top_10_Investments", "Insights_Analysis", "Data_Dictionary" MUST ALWAYS
  normalize correctly even if written as:
    "Top 10 Investments", "top10", "Top10_Investments", "TOP_10_INVESTMENTS",
    "Insights Analysis", "Insights-Analysis", "data dictionary", etc.
- We also expose helper aliases `resolve_page()` and `canonicalize_page()` because
  several routers/services try different function names.

Hard rules:
- No network calls. Import-safe.
- Unknown page raises ValueError with allowed pages.
================================================================================
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
    "FORBIDDEN_PAGES",
    "ALIAS_TO_CANONICAL",
    "TOP10_FEED_PAGES_DEFAULT",
    "is_canonical_page",
    "is_forbidden_page",
    "normalize_page_name",
    "resolve_page",
    "canonicalize_page",
    "allowed_pages",
    "get_top10_feed_pages",
    "validate_page_catalog",
]

PAGE_CATALOG_VERSION = "2.1.0"


@dataclass(frozen=True)
class PageInfo:
    canonical: str
    description: str = ""
    is_data_page: bool = True          # row-per-instrument pages
    eligible_for_top10: bool = False   # included in Top 10 selection universe


# -----------------------------
# Canonical pages (single truth)
# -----------------------------
_CANONICAL_FROM_SCHEMA: Set[str] = set(SCHEMA_REGISTRY.keys())

# Explicitly forbid any legacy or removed pages (even if user passes them).
FORBIDDEN_PAGES: Set[str] = {"KSA_Tadawul", "Advisor_Criteria"}  # Advisor_Criteria moved into Insights_Analysis


# Define PageInfo (meaning / eligibility)
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
        eligible_for_top10=True,
    ),
    "Insights_Analysis": PageInfo(
        canonical="Insights_Analysis",
        description="Criteria fields + insights table (not an instrument table).",
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

# Stable UX order:
# Prefer schema ordering if any upstream uses it; else sorted.
CANONICAL_PAGES: List[str] = sorted(PAGE_INFO.keys())


# -----------------------------
# Normalization helpers
# -----------------------------
_SEP_RE = re.compile(r"[\s\-/\\\.\|]+", re.UNICODE)
_NON_ALNUM_RE = re.compile(r"[^a-z0-9_]+", re.UNICODE)

def _normalize_token(s: str) -> str:
    """
    Normalize user input for alias matching.

    Goals:
    - Accept "Top 10", "Top-10", "top10", "TOP10", "Top_10", "Top.10"
    - Accept "Insights Analysis", "Insights-Analysis", "insights_analysis"
    - Accept "Data Dictionary", "data-dictionary", "data_dictionary"
    """
    s = (s or "").strip()
    if not s:
        return ""
    s = s.casefold()
    s = _SEP_RE.sub("_", s)              # unify separators to underscore
    s = _NON_ALNUM_RE.sub("", s)         # drop any remaining symbols
    s = re.sub(r"_+", "_", s)            # collapse repeated underscores
    s = s.strip("_")
    return s


def _canonical_token_for_page(canonical: str) -> str:
    return _normalize_token(canonical)


# -----------------------------
# Aliases
# -----------------------------
ALIAS_TO_CANONICAL: Dict[str, str] = {}

def _add_alias(alias: str, canonical: str) -> None:
    tok = _normalize_token(alias)
    if not tok:
        return
    ALIAS_TO_CANONICAL[tok] = canonical


# Build tokens for canonicals themselves (so exact canonical always matches, any case)
for c in PAGE_INFO.keys():
    _add_alias(c, c)
    _add_alias(c.lower(), c)
    _add_alias(c.replace("_", " "), c)
    _add_alias(c.replace("_", "-"), c)

# Human-friendly / short forms
_add_alias("Market Leaders", "Market_Leaders")
_add_alias("Leaders", "Market_Leaders")
_add_alias("Market", "Market_Leaders")

_add_alias("Global Markets", "Global_Markets")
_add_alias("Global", "Global_Markets")

_add_alias("Commodities & FX", "Commodities_FX")
_add_alias("Commodities and FX", "Commodities_FX")
_add_alias("Commodities", "Commodities_FX")
_add_alias("FX", "Commodities_FX")

_add_alias("Mutual Funds", "Mutual_Funds")
_add_alias("Funds", "Mutual_Funds")
_add_alias("ETF", "Mutual_Funds")
_add_alias("ETFs", "Mutual_Funds")

_add_alias("My Portfolio", "My_Portfolio")
_add_alias("Portfolio", "My_Portfolio")

_add_alias("Insights", "Insights_Analysis")
_add_alias("Insights Analysis", "Insights_Analysis")
_add_alias("Insights-Analysis", "Insights_Analysis")

# Top10: make this *very* tolerant
_add_alias("Top 10", "Top_10_Investments")
_add_alias("Top10", "Top_10_Investments")
_add_alias("Top 10 Investments", "Top_10_Investments")
_add_alias("Top10 Investments", "Top_10_Investments")
_add_alias("Top_10", "Top_10_Investments")
_add_alias("Top10_Investments", "Top_10_Investments")
_add_alias("Top_10_Investment", "Top_10_Investments")  # common typo
_add_alias("Top10Investment", "Top_10_Investments")    # common typo

_add_alias("Data Dictionary", "Data_Dictionary")
_add_alias("Dictionary", "Data_Dictionary")
_add_alias("Data-Dictionary", "Data_Dictionary")

# Forbidden aliases (normalize to these tokens)
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
    if page in FORBIDDEN_PAGES:
        return True
    tok = _normalize_token(page)
    return tok in FORBIDDEN_ALIASES


def normalize_page_name(page: str, *, allow_output_pages: bool = True) -> str:
    """
    Convert any alias into a canonical page name.

    - Raises ValueError if page is forbidden or unknown.
    - If allow_output_pages=False, forbids Top_10_Investments / Data_Dictionary as inputs.
    """
    raw = (page or "").strip()
    if not raw:
        raise ValueError("Page name is empty. Provide a valid page name.")

    if is_forbidden_page(raw):
        raise ValueError(
            f"Page '{page}' is forbidden/removed. "
            f"Allowed pages: {', '.join(CANONICAL_PAGES)}"
        )

    tok = _normalize_token(raw)

    canonical = ALIAS_TO_CANONICAL.get(tok)
    if not canonical:
        # Extra safety: if token equals a canonical token (even if ALIAS map missed)
        for c in PAGE_INFO.keys():
            if tok == _canonical_token_for_page(c):
                canonical = c
                break

    if not canonical:
        raise ValueError(f"Unknown page '{page}'. Allowed pages: {', '.join(CANONICAL_PAGES)}")

    if canonical not in PAGE_INFO:
        raise ValueError(
            f"Page '{page}' resolved to '{canonical}', but it is not registered. "
            f"Allowed pages: {', '.join(CANONICAL_PAGES)}"
        )

    if not allow_output_pages and canonical in {"Top_10_Investments", "Data_Dictionary"}:
        raise ValueError(f"Page '{canonical}' is an output/meta sheet and not allowed for this operation.")

    return canonical


# Common alternative names used across the repo (so other modules don't "miss")
def resolve_page(page: str) -> str:
    return normalize_page_name(page, allow_output_pages=True)


def canonicalize_page(page: str) -> str:
    return normalize_page_name(page, allow_output_pages=True)


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
        if info and info.eligible_for_top10 and info.is_data_page:
            normalized.append(cp)

    # de-duplicate while preserving order
    seen: Set[str] = set()
    out: List[str] = []
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
            raise ValueError(f"Page '{canonical}' exists in page_catalog but not in schema_registry.")

    # Schema registry pages must be known in the catalog (strict)
    for sheet in _CANONICAL_FROM_SCHEMA:
        if sheet not in PAGE_INFO:
            raise ValueError(f"Sheet '{sheet}' exists in schema_registry but not in page_catalog. Add PageInfo.")

    # Critical pages must normalize correctly (prevents your 80-column fallback)
    must_resolve = [
        "Top_10_Investments",
        "Top10",
        "Top 10 Investments",
        "Insights_Analysis",
        "Insights Analysis",
        "Data_Dictionary",
        "Data Dictionary",
    ]
    for s in must_resolve:
        r = normalize_page_name(s, allow_output_pages=True)
        if s.lower().startswith("top") and r != "Top_10_Investments":
            raise ValueError(f"Normalization failed for '{s}' -> '{r}' (expected Top_10_Investments)")
        if s.lower().startswith("ins") and r != "Insights_Analysis":
            raise ValueError(f"Normalization failed for '{s}' -> '{r}' (expected Insights_Analysis)")
        if s.lower().startswith("data") and r != "Data_Dictionary":
            raise ValueError(f"Normalization failed for '{s}' -> '{r}' (expected Data_Dictionary)")

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
