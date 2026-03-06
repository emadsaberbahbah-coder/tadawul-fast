#!/usr/bin/env python3
# core/sheets/page_catalog.py
"""
================================================================================
Page Catalog — v3.0.0 (SCHEMA-FIRST / CANONICAL / ROUTE-DISPATCH SAFE)
================================================================================

Purpose
-------
Single normalization and page-resolution layer for all sheet/page operations.

Why this revision
-----------------
- ✅ FIX: Tightens canonical page normalization across all route families
- ✅ FIX: Prevents special pages from drifting into default instrument fallback logic
- ✅ FIX: Makes aliases deterministic and case/spacing tolerant
- ✅ FIX: Adds explicit page classification helpers for routing decisions
- ✅ FIX: Exposes stable helpers for sheet-rows, schema, top10, and meta pages
- ✅ FIX: Keeps schema_registry as the single source of truth

Hard rules
----------
- No network calls
- Import-safe
- Unknown page raises ValueError with allowed pages
- Forbidden pages are rejected explicitly
- Canonical pages come from schema_registry only

Canonical pages expected
------------------------
- Market_Leaders
- Global_Markets
- Commodities_FX
- Mutual_Funds
- My_Portfolio
- Insights_Analysis
- Top_10_Investments
- Data_Dictionary
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

# --------------------------------------------------------------------------------------
# Schema Registry (authoritative)
# --------------------------------------------------------------------------------------
try:
    from core.sheets.schema_registry import (  # type: ignore
        SCHEMA_REGISTRY,
        CANONICAL_SHEETS,
        list_sheets,
        get_sheet_spec,
    )
except Exception as e:  # pragma: no cover
    raise ImportError(f"page_catalog failed to import schema_registry: {e!r}") from e


PAGE_CATALOG_VERSION = "3.0.0"

__all__ = [
    "PAGE_CATALOG_VERSION",
    "PageInfo",
    "CANONICAL_PAGES",
    "FORBIDDEN_PAGES",
    "FORBIDDEN_ALIASES",
    "ALIAS_TO_CANONICAL",
    "OUTPUT_PAGES",
    "SPECIAL_PAGES",
    "INSTRUMENT_PAGES",
    "TOP10_FEED_PAGES_DEFAULT",
    "page_info",
    "allowed_pages",
    "allowed_input_pages",
    "is_canonical_page",
    "is_forbidden_page",
    "is_output_page",
    "is_special_page",
    "is_instrument_page",
    "normalize_page_name",
    "resolve_page",
    "canonicalize_page",
    "normalize_page_names",
    "get_top10_feed_pages",
    "get_page_aliases",
    "get_route_family",
    "validate_page_catalog",
]


# --------------------------------------------------------------------------------------
# Models
# --------------------------------------------------------------------------------------
@dataclass(frozen=True)
class PageInfo:
    canonical: str
    description: str = ""
    kind: str = ""
    is_data_page: bool = True
    is_output_page: bool = False
    is_special_page: bool = False
    eligible_for_top10: bool = False


# --------------------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------------------
FORBIDDEN_PAGES: Set[str] = {"KSA_Tadawul", "Advisor_Criteria"}

# Output/meta pages: readable, but generally not input universes for scanners/selectors
OUTPUT_PAGES: Set[str] = {"Top_10_Investments", "Data_Dictionary"}

# Pages that must never be routed to the generic 80-col instrument fallback by mistake
SPECIAL_PAGES: Set[str] = {"Insights_Analysis", "Top_10_Investments", "Data_Dictionary"}

_DESC_OVERRIDES: Dict[str, str] = {
    "Market_Leaders": "Primary leaders/watchlist universe.",
    "Global_Markets": "Global markets universe.",
    "Commodities_FX": "Commodities and FX universe.",
    "Mutual_Funds": "Mutual funds / ETFs universe.",
    "My_Portfolio": "User portfolio positions and analytics.",
    "Insights_Analysis": "Insights / analysis output page.",
    "Top_10_Investments": "Top 10 selected investment output page.",
    "Data_Dictionary": "Schema-derived data dictionary page.",
}

_SEP_RE = re.compile(r"[\s\-/\\\.\|&]+", re.UNICODE)
_NON_ALNUM_RE = re.compile(r"[^a-z0-9_]+", re.UNICODE)


# --------------------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------------------
def _obj_get(obj: Any, name: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, Mapping):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, set):
        return list(value)
    if isinstance(value, str):
        return [value]
    if isinstance(value, Iterable):
        try:
            return list(value)
        except Exception:
            return [value]
    return [value]


def _normalize_token(value: str) -> str:
    s = (value or "").strip()
    if not s:
        return ""
    s = s.casefold()
    s = _SEP_RE.sub("_", s)
    s = _NON_ALNUM_RE.sub("", s)
    s = re.sub(r"_+", "_", s).strip("_")

    # Common typo / style harmonization
    s = s.replace("topten", "top10")
    s = s.replace("top_10", "top10")
    s = s.replace("topen", "top10") if s == "topen" else s
    s = s.replace("investements", "investments")
    s = s.replace("investmant", "investments")
    s = s.replace("portfolios", "portfolio")
    return s


def _canonical_token_for_page(canonical: str) -> str:
    return _normalize_token(canonical)


# --------------------------------------------------------------------------------------
# Canonical pages derived from schema_registry
# --------------------------------------------------------------------------------------
_CANONICAL_FROM_SCHEMA: Set[str] = set(str(k) for k in SCHEMA_REGISTRY.keys())


def _canonical_pages_from_schema() -> List[str]:
    # Prefer explicit order from schema_registry if valid
    try:
        if CANONICAL_SHEETS:
            ordered = [str(s) for s in CANONICAL_SHEETS if str(s) in _CANONICAL_FROM_SCHEMA]
            if ordered:
                # append any schema pages not present in CANONICAL_SHEETS
                extras = [s for s in sorted(_CANONICAL_FROM_SCHEMA) if s not in ordered]
                return ordered + extras
    except Exception:
        pass

    # Prefer list_sheets() if available
    try:
        sheets = [str(s) for s in list_sheets() if str(s) in _CANONICAL_FROM_SCHEMA]
        if sheets:
            extras = [s for s in sorted(_CANONICAL_FROM_SCHEMA) if s not in sheets]
            return sheets + extras
    except Exception:
        pass

    return sorted(_CANONICAL_FROM_SCHEMA)


CANONICAL_PAGES: List[str] = _canonical_pages_from_schema()


# --------------------------------------------------------------------------------------
# PageInfo derived from schema_registry
# --------------------------------------------------------------------------------------
def _derive_page_info(canonical: str) -> PageInfo:
    kind = ""
    try:
        spec = get_sheet_spec(canonical)
        kind = str(_obj_get(spec, "kind", "") or "")
    except Exception:
        kind = ""

    # Treat instrument_table as the only true input universe type by default
    is_data_page = kind == "instrument_table"
    is_output_page = canonical in OUTPUT_PAGES
    is_special_page = canonical in SPECIAL_PAGES
    eligible_for_top10 = bool(is_data_page and not is_output_page and not is_special_page)

    return PageInfo(
        canonical=canonical,
        description=_DESC_OVERRIDES.get(canonical, ""),
        kind=kind,
        is_data_page=is_data_page,
        is_output_page=is_output_page,
        is_special_page=is_special_page,
        eligible_for_top10=eligible_for_top10,
    )


PAGE_INFO: Dict[str, PageInfo] = {c: _derive_page_info(c) for c in CANONICAL_PAGES}

INSTRUMENT_PAGES: Set[str] = {p for p, info in PAGE_INFO.items() if info.is_data_page and not info.is_special_page}

TOP10_FEED_PAGES_DEFAULT: List[str] = [
    p for p in ["Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds", "My_Portfolio"]
    if p in PAGE_INFO and PAGE_INFO[p].eligible_for_top10
]


# --------------------------------------------------------------------------------------
# Aliases
# --------------------------------------------------------------------------------------
ALIAS_TO_CANONICAL: Dict[str, str] = {}


def _add_alias(alias: str, canonical: str) -> None:
    tok = _normalize_token(alias)
    if tok:
        ALIAS_TO_CANONICAL[tok] = canonical


# Canonical self aliases
for c in CANONICAL_PAGES:
    _add_alias(c, c)
    _add_alias(c.lower(), c)
    _add_alias(c.replace("_", " "), c)
    _add_alias(c.replace("_", "-"), c)
    _add_alias(c.replace("_", "/"), c)


# Friendly aliases
_add_alias("Market Leaders", "Market_Leaders")
_add_alias("Leaders", "Market_Leaders")
_add_alias("Leader", "Market_Leaders")
_add_alias("Market", "Market_Leaders")

_add_alias("Global Markets", "Global_Markets")
_add_alias("Global Market", "Global_Markets")
_add_alias("Global", "Global_Markets")

_add_alias("Commodities FX", "Commodities_FX")
_add_alias("Commodities & FX", "Commodities_FX")
_add_alias("Commodities and FX", "Commodities_FX")
_add_alias("Commodity FX", "Commodities_FX")
_add_alias("Commodities", "Commodities_FX")
_add_alias("FX", "Commodities_FX")
_add_alias("Forex", "Commodities_FX")

_add_alias("Mutual Funds", "Mutual_Funds")
_add_alias("Mutual Fund", "Mutual_Funds")
_add_alias("Funds", "Mutual_Funds")
_add_alias("Fund", "Mutual_Funds")
_add_alias("ETF", "Mutual_Funds")
_add_alias("ETFs", "Mutual_Funds")

_add_alias("My Portfolio", "My_Portfolio")
_add_alias("Portfolio", "My_Portfolio")
_add_alias("My Holdings", "My_Portfolio")
_add_alias("Holdings", "My_Portfolio")

_add_alias("Insights", "Insights_Analysis")
_add_alias("Insight", "Insights_Analysis")
_add_alias("Insights Analysis", "Insights_Analysis")
_add_alias("Insights-Analysis", "Insights_Analysis")
_add_alias("Insights & Analysis", "Insights_Analysis")

for alias in (
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
    "Top 10 Investements",
    "Top10 Investements",
    "Top 10 Investmant",
    "Top10 Investmant",
):
    _add_alias(alias, "Top_10_Investments")

for alias in (
    "Data Dictionary",
    "Dictionary",
    "Data-Dictionary",
    "DataDict",
    "Data Dic",
    "Data Dictionary Page",
):
    _add_alias(alias, "Data_Dictionary")


FORBIDDEN_ALIASES: Set[str] = {
    _normalize_token("KSA_Tadawul"),
    _normalize_token("Advisor_Criteria"),
    _normalize_token("KSA Tadawul"),
    _normalize_token("Advisor Criteria"),
}


# --------------------------------------------------------------------------------------
# Public helpers
# --------------------------------------------------------------------------------------
def page_info(page: str) -> PageInfo:
    canonical = normalize_page_name(page, allow_output_pages=True)
    return PAGE_INFO[canonical]


def allowed_pages() -> List[str]:
    return list(CANONICAL_PAGES)


def allowed_input_pages() -> List[str]:
    return [p for p in CANONICAL_PAGES if p in PAGE_INFO and not PAGE_INFO[p].is_output_page]


def get_page_aliases() -> Dict[str, str]:
    return dict(ALIAS_TO_CANONICAL)


def is_canonical_page(page: str) -> bool:
    return str(page) in PAGE_INFO


def is_forbidden_page(page: str) -> bool:
    raw = (page or "").strip()
    if not raw:
        return False
    if raw in FORBIDDEN_PAGES:
        return True
    return _normalize_token(raw) in FORBIDDEN_ALIASES


def is_output_page(page: str) -> bool:
    try:
        canonical = normalize_page_name(page, allow_output_pages=True)
    except Exception:
        return False
    return canonical in OUTPUT_PAGES


def is_special_page(page: str) -> bool:
    try:
        canonical = normalize_page_name(page, allow_output_pages=True)
    except Exception:
        return False
    return canonical in SPECIAL_PAGES


def is_instrument_page(page: str) -> bool:
    try:
        canonical = normalize_page_name(page, allow_output_pages=True)
    except Exception:
        return False
    return canonical in INSTRUMENT_PAGES


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

    # Fallback: compare against canonical tokens
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

    if not allow_output_pages and PAGE_INFO[canonical].is_output_page:
        raise ValueError(f"Page '{canonical}' is an output/meta sheet and not allowed for this operation.")

    return canonical


def resolve_page(page: str) -> str:
    return normalize_page_name(page, allow_output_pages=True)


def canonicalize_page(page: str) -> str:
    return normalize_page_name(page, allow_output_pages=True)


def normalize_page_names(pages: Sequence[str], *, allow_output_pages: bool = True, dedupe: bool = True) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()

    for page in pages:
        canonical = normalize_page_name(page, allow_output_pages=allow_output_pages)
        if dedupe:
            if canonical in seen:
                continue
            seen.add(canonical)
        out.append(canonical)

    return out


def get_top10_feed_pages(pages_override: Optional[Sequence[str]] = None) -> List[str]:
    pages = list(pages_override) if pages_override is not None else list(TOP10_FEED_PAGES_DEFAULT)
    normalized: List[str] = []

    for p in pages:
        canonical = normalize_page_name(p, allow_output_pages=False)
        info = PAGE_INFO.get(canonical)
        if info and info.eligible_for_top10 and info.is_data_page and not info.is_special_page:
            normalized.append(canonical)

    # de-duplicate preserve order
    out: List[str] = []
    seen: Set[str] = set()
    for p in normalized:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def get_route_family(page: str) -> str:
    """
    Helper for route dispatch decisions.

    Returns one of:
    - "instrument"
    - "insights"
    - "top10"
    - "dictionary"
    """
    canonical = normalize_page_name(page, allow_output_pages=True)

    if canonical == "Insights_Analysis":
        return "insights"
    if canonical == "Top_10_Investments":
        return "top10"
    if canonical == "Data_Dictionary":
        return "dictionary"
    return "instrument"


# --------------------------------------------------------------------------------------
# Validation
# --------------------------------------------------------------------------------------
def validate_page_catalog() -> None:
    # Forbidden pages must not exist in schema registry
    for fp in FORBIDDEN_PAGES:
        if fp in _CANONICAL_FROM_SCHEMA:
            raise ValueError(f"Forbidden page '{fp}' exists in schema_registry. Remove it.")

    # Page catalog pages must exist in schema registry
    for canonical in CANONICAL_PAGES:
        if canonical not in _CANONICAL_FROM_SCHEMA:
            raise ValueError(f"Page '{canonical}' exists in page_catalog but not in schema_registry.")

    # Critical normalization checks
    must_resolve: List[Tuple[str, str]] = [
        ("Market Leaders", "Market_Leaders"),
        ("Global Markets", "Global_Markets"),
        ("Commodities & FX", "Commodities_FX"),
        ("Mutual Funds", "Mutual_Funds"),
        ("My Portfolio", "My_Portfolio"),
        ("Insights Analysis", "Insights_Analysis"),
        ("Insights-Analysis", "Insights_Analysis"),
        ("Top 10 Investments", "Top_10_Investments"),
        ("Top10", "Top_10_Investments"),
        ("Top Ten Investments", "Top_10_Investments"),
        ("Data Dictionary", "Data_Dictionary"),
        ("Data-Dictionary", "Data_Dictionary"),
    ]
    for raw, expected in must_resolve:
        resolved = normalize_page_name(raw, allow_output_pages=True)
        if resolved != expected:
            raise ValueError(f"Normalization failed for '{raw}' -> '{resolved}' (expected '{expected}')")

    # Aliases must map only to valid non-forbidden pages
    for alias_token, canonical in ALIAS_TO_CANONICAL.items():
        if canonical in FORBIDDEN_PAGES:
            raise ValueError(f"Alias token '{alias_token}' maps to forbidden page '{canonical}'.")
        if canonical not in PAGE_INFO:
            raise ValueError(f"Alias token '{alias_token}' maps to unknown page '{canonical}'.")

    # Special pages must exist and must not be treated as instrument pages
    for sp in SPECIAL_PAGES:
        if sp not in PAGE_INFO:
            raise ValueError(f"Special page '{sp}' is not registered in PAGE_INFO.")
        if PAGE_INFO[sp].is_data_page and sp in {"Insights_Analysis", "Data_Dictionary"}:
            raise ValueError(f"Special page '{sp}' must not be treated as instrument_table.")

    # Top10 default feeds must be valid input universes
    for p in TOP10_FEED_PAGES_DEFAULT:
        if p not in PAGE_INFO:
            raise ValueError(f"TOP10_FEED_PAGES_DEFAULT includes unknown page '{p}'.")
        info = PAGE_INFO[p]
        if not info.is_data_page:
            raise ValueError(f"TOP10_FEED_PAGES_DEFAULT includes non-data page '{p}'.")
        if not info.eligible_for_top10:
            raise ValueError(f"TOP10_FEED_PAGES_DEFAULT includes ineligible page '{p}'.")

    # Canonical pages should include the expected sheet family
    expected_core = {
        "Market_Leaders",
        "Global_Markets",
        "Commodities_FX",
        "Mutual_Funds",
        "My_Portfolio",
        "Insights_Analysis",
        "Top_10_Investments",
        "Data_Dictionary",
    }
    missing_expected = expected_core.difference(set(CANONICAL_PAGES))
    if missing_expected:
        raise ValueError(f"Missing expected canonical pages: {sorted(missing_expected)}")


# Validate immediately (fast, no I/O)
validate_page_catalog()
