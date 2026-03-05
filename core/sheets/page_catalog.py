#!/usr/bin/env python3
# core/sheets/page_catalog.py
"""
================================================================================
Page Catalog — v2.2.1 (SCHEMA-DRIVEN + MAX-TOLERANCE NORMALIZATION)
================================================================================
Priority 3 alignment:
- normalize names/aliases so “Top 10 Investments” ALWAYS maps to Top_10_Investments
- prevent routing mismatches that cause fallback to the default/80-col builder

Hard rules:
- No network calls. Import-safe.
- Unknown page raises ValueError with allowed pages.
- Canonical pages derived from schema_registry (single source of truth).
================================================================================
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# Schema Registry (authoritative)
# ---------------------------------------------------------------------------
try:
    from core.sheets.schema_registry import (  # type: ignore
        SCHEMA_REGISTRY,
        CANONICAL_SHEETS,
        list_sheets,
        get_sheet_spec,
    )
except Exception as e:  # pragma: no cover
    raise ImportError(f"page_catalog failed to import schema_registry: {e!r}") from e


__all__ = [
    "PAGE_CATALOG_VERSION",
    "PageInfo",
    "CANONICAL_PAGES",
    "FORBIDDEN_PAGES",
    "ALIAS_TO_CANONICAL",
    "TOP10_FEED_PAGES_DEFAULT",
    "OUTPUT_PAGES",
    "SPECIAL_PAGES",
    "is_canonical_page",
    "is_forbidden_page",
    "normalize_page_name",
    "resolve_page",
    "canonicalize_page",
    "allowed_pages",
    "get_top10_feed_pages",
    "page_info",
    "validate_page_catalog",
]

PAGE_CATALOG_VERSION = "2.2.1"


@dataclass(frozen=True)
class PageInfo:
    canonical: str
    description: str = ""
    is_data_page: bool = True
    eligible_for_top10: bool = False
    kind: str = ""  # mirrors schema_registry SheetSpec.kind (if available)


# -----------------------------
# Forbidden / Removed pages
# -----------------------------
FORBIDDEN_PAGES: Set[str] = {"KSA_Tadawul", "Advisor_Criteria"}

# Output/meta pages: readable, but not valid as *input universes* for some operations
OUTPUT_PAGES: Set[str] = {"Top_10_Investments", "Data_Dictionary"}

# Special pages that must never hit instrument builder fallback
SPECIAL_PAGES: Set[str] = {"Insights_Analysis", "Top_10_Investments", "Data_Dictionary"}


# -----------------------------
# Canonical pages (single truth)
# -----------------------------
_CANONICAL_FROM_SCHEMA: Set[str] = set(SCHEMA_REGISTRY.keys())

def _canonical_pages_from_schema() -> List[str]:
    try:
        if CANONICAL_SHEETS and all(s in _CANONICAL_FROM_SCHEMA for s in CANONICAL_SHEETS):
            return list(CANONICAL_SHEETS)
    except Exception:
        pass
    try:
        ss = list_sheets()
        if ss:
            return [s for s in ss if s in _CANONICAL_FROM_SCHEMA]
    except Exception:
        pass
    return sorted(_CANONICAL_FROM_SCHEMA)

CANONICAL_PAGES: List[str] = _canonical_pages_from_schema()


# -----------------------------
# PageInfo (derived from schema_registry to stop drift)
# -----------------------------
_DESC_OVERRIDES: Dict[str, str] = {
    "Market_Leaders": "Primary leaders/watchlist universe.",
    "Global_Markets": "Global indices/shares universe.",
    "Commodities_FX": "Commodities and FX tickers.",
    "Mutual_Funds": "Mutual funds / ETFs universe.",
    "My_Portfolio": "User portfolio positions.",
    "Insights_Analysis": "Criteria fields + insights table (not an instrument table).",
    "Top_10_Investments": "Output sheet populated by Top10 selector.",
    "Data_Dictionary": "Auto-built from schema_registry; documentation sheet.",
}

def _derive_page_info(canonical: str) -> PageInfo:
    try:
        spec = get_sheet_spec(canonical)
        kind = str(getattr(spec, "kind", "") or "")
    except Exception:
        kind = ""
    is_data = (kind == "instrument_table")
    eligible = bool(is_data and canonical not in OUTPUT_PAGES)
    return PageInfo(
        canonical=canonical,
        description=_DESC_OVERRIDES.get(canonical, ""),
        is_data_page=is_data,
        eligible_for_top10=eligible,
        kind=kind,
    )

PAGE_INFO: Dict[str, PageInfo] = {c: _derive_page_info(c) for c in CANONICAL_PAGES}

def page_info(page: str) -> PageInfo:
    p = normalize_page_name(page, allow_output_pages=True)
    return PAGE_INFO[p]


# -----------------------------
# Normalization helpers
# -----------------------------
_SEP_RE = re.compile(r"[\s\-/\\\.\|]+", re.UNICODE)
_NON_ALNUM_RE = re.compile(r"[^a-z0-9_]+", re.UNICODE)

def _normalize_token(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    s = s.casefold()
    s = _SEP_RE.sub("_", s)
    s = _NON_ALNUM_RE.sub("", s)
    s = re.sub(r"_+", "_", s).strip("_")

    # common normalization patterns
    s = s.replace("topten", "top10")   # TopTen -> Top10
    s = s.replace("top_10", "top10")   # unify token style
    s = s.replace("top10_", "top10")   # collapse weird separators
    return s


def _canonical_token_for_page(canonical: str) -> str:
    return _normalize_token(canonical)


# -----------------------------
# Aliases
# -----------------------------
ALIAS_TO_CANONICAL: Dict[str, str] = {}

def _add_alias(alias: str, canonical: str) -> None:
    tok = _normalize_token(alias)
    if tok:
        ALIAS_TO_CANONICAL[tok] = canonical

# Canonical tokens for themselves
for c in CANONICAL_PAGES:
    _add_alias(c, c)
    _add_alias(c.lower(), c)
    _add_alias(c.replace("_", " "), c)
    _add_alias(c.replace("_", "-"), c)
    _add_alias(c.replace("_", "/"), c)

# Friendly / short forms
_add_alias("Market Leaders", "Market_Leaders")
_add_alias("Leaders", "Market_Leaders")
_add_alias("Market", "Market_Leaders")

_add_alias("Global Markets", "Global_Markets")
_add_alias("Global", "Global_Markets")

_add_alias("Commodities & FX", "Commodities_FX")
_add_alias("Commodities and FX", "Commodities_FX")
_add_alias("Commodities", "Commodities_FX")
_add_alias("FX", "Commodities_FX")
_add_alias("Forex", "Commodities_FX")

_add_alias("Mutual Funds", "Mutual_Funds")
_add_alias("Funds", "Mutual_Funds")
_add_alias("ETF", "Mutual_Funds")
_add_alias("ETFs", "Mutual_Funds")

_add_alias("My Portfolio", "My_Portfolio")
_add_alias("Portfolio", "My_Portfolio")
_add_alias("Holdings", "My_Portfolio")

_add_alias("Insights", "Insights_Analysis")
_add_alias("Insights Analysis", "Insights_Analysis")
_add_alias("Insights-Analysis", "Insights_Analysis")
_add_alias("Analysis", "Insights_Analysis")

# Top10: maximum tolerance + common typos
for a in (
    "Top 10",
    "Top10",
    "Top Ten",
    "Top 10 Investments",
    "Top10 Investments",
    "Top Ten Investments",
    "Top_10",
    "Top10_Investments",
    "Top_10_Investment",
    "Top10Investment",
    "Top10Investments",
    "Top 10 Investements",   # common misspelling
    "Top10 Investements",
    "Top 10 Investmant",     # common typo
    "Top10 Investmant",
):
    _add_alias(a, "Top_10_Investments")

# Data Dictionary tolerance
for a in ("Data Dictionary", "Dictionary", "Data-Dictionary", "DataDict", "Data Dic"):
    _add_alias(a, "Data_Dictionary")

# Forbidden aliases
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
    raw = (page or "").strip()
    if not raw:
        return False
    if raw in FORBIDDEN_PAGES:
        return True
    return _normalize_token(raw) in FORBIDDEN_ALIASES


def normalize_page_name(page: str, *, allow_output_pages: bool = True) -> str:
    raw = (page or "").strip()
    if not raw:
        raise ValueError("Page name is empty. Provide a valid page name.")

    if is_forbidden_page(raw):
        raise ValueError(
            f"Page '{page}' is forbidden/removed. Allowed pages: {', '.join(CANONICAL_PAGES)}"
        )

    tok = _normalize_token(raw)

    canonical = ALIAS_TO_CANONICAL.get(tok)

    # Extra safety: if token equals a canonical token (even if alias map missed)
    if not canonical:
        for c in CANONICAL_PAGES:
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

    if not allow_output_pages and canonical in OUTPUT_PAGES:
        raise ValueError(f"Page '{canonical}' is an output/meta sheet and not allowed for this operation.")

    return canonical


def resolve_page(page: str) -> str:
    return normalize_page_name(page, allow_output_pages=True)


def canonicalize_page(page: str) -> str:
    return normalize_page_name(page, allow_output_pages=True)


def get_top10_feed_pages(pages_override: Optional[List[str]] = None) -> List[str]:
    pages = pages_override or TOP10_FEED_PAGES_DEFAULT
    normalized: List[str] = []

    for p in pages:
        cp = normalize_page_name(p, allow_output_pages=False)
        info = PAGE_INFO.get(cp)
        if info and info.eligible_for_top10 and info.is_data_page:
            normalized.append(cp)

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

    # Catalog pages must exist in schema registry (derived, but assert)
    for canonical in CANONICAL_PAGES:
        if canonical not in _CANONICAL_FROM_SCHEMA:
            raise ValueError(f"Page '{canonical}' exists in page_catalog but not in schema_registry.")

    # Critical pages must normalize correctly (prevents 80-column fallback)
    must_resolve: List[Tuple[str, str]] = [
        ("Top 10 Investments", "Top_10_Investments"),
        ("Top10", "Top_10_Investments"),
        ("Top Ten Investments", "Top_10_Investments"),
        ("Insights Analysis", "Insights_Analysis"),
        ("Insights-Analysis", "Insights_Analysis"),
        ("Data Dictionary", "Data_Dictionary"),
        ("Data-Dictionary", "Data_Dictionary"),
    ]
    for inp, expected in must_resolve:
        r = normalize_page_name(inp, allow_output_pages=True)
        if r != expected:
            raise ValueError(f"Normalization failed for '{inp}' -> '{r}' (expected {expected})")

    # Aliases must map only to valid pages (never forbidden)
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

    # Special pages sanity
    for sp in SPECIAL_PAGES:
        if sp not in PAGE_INFO:
            raise ValueError(f"Special page '{sp}' is not registered in PAGE_INFO.")


# Validate immediately (fast, no I/O)
validate_page_catalog()
