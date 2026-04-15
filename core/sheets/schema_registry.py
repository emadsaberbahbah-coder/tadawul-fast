#!/usr/bin/env python3
# core/sheets/page_catalog.py
"""
================================================================================
Page Catalog -- v3.3.0 (SCHEMA-FIRST / CANONICAL / ROUTE-DISPATCH SAFE)
================================================================================

Purpose
-------
Single normalization and page-resolution layer for all sheet/page operations.

v3.3.0 changes vs v3.2.0
--------------------------
FIX: Import path hardened to resolve schema_registry across all deployment
  layouts. v3.2.0 assumed `from core.sheets.schema_registry import ...`,
  which fails when the module is at a different depth. v3.3.0 tries all four
  canonical locations in order:
    1) core.sheets.schema_registry   (core/sheets/schema_registry.py)
    2) core.schema_registry          (core/schema_registry.py)
    3) schema_registry               (repo-root schema_registry.py)
    4) sys.path walk for schema_registry.py as last resort

FIX: _spec_kind() column-count fallback for insights corrected from 7 to 10.
  schema_registry v3.0.0 defines Insights_Analysis with 10 columns, not 7.
  Note: this fallback is dead code in practice (all specs have kind set), but
  the incorrect value (7) would have caused wrong classification if kind were
  ever missing from the spec.

FIX: _derive_page_info() now explicitly handles My_Portfolio (portfolio_table)
  as a non-instrument page. kind='portfolio_table' != 'instrument_table', so
  is_data_page=False. My_Portfolio is therefore correctly excluded from
  INPUT_PAGES, INSTRUMENT_PAGES, and TOP10_FEED_PAGES_DEFAULT.
  get_route_family() still returns "instrument" for My_Portfolio (it is not a
  special/output page), which is the correct routing behavior.

ENH: PAGE_CATALOG_VERSION bumped to 3.3.0.

Preserved from v3.2.0:
  All normalization logic, alias definitions, and validation.
  All public exports and __all__ contents.
  All route-dispatch helpers (get_route_family, resolve_page_candidate, etc.).

Hard rules (unchanged)
-----------------------
- No network calls
- Import-safe
- Unknown page raises ValueError with allowed pages listed
- Forbidden pages are rejected explicitly
- Canonical pages come from schema_registry only
================================================================================
"""

from __future__ import annotations

import re
import sys
import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

# =============================================================================
# Schema Registry (authoritative)
# FIX v3.3.0: Try all deployment layouts before failing.
# =============================================================================
_schema_registry = None
_sr_import_errors: List[str] = []

for _mod_path in (
    "core.sheets.schema_registry",
    "core.schema_registry",
    "schema_registry",
):
    try:
        import importlib as _il
        _schema_registry = _il.import_module(_mod_path)
        break
    except ImportError as _e:
        _sr_import_errors.append(f"{_mod_path}: {_e}")

if _schema_registry is None:
    for _sp in sys.path:
        _candidate = os.path.join(_sp, "schema_registry.py")
        if os.path.isfile(_candidate):
            try:
                import importlib.util as _ilu
                _spec_obj = _ilu.spec_from_file_location("schema_registry", _candidate)
                if _spec_obj and _spec_obj.loader:
                    _schema_registry = _ilu.module_from_spec(_spec_obj)
                    _spec_obj.loader.exec_module(_schema_registry)  # type: ignore[union-attr]
                    break
            except Exception as _e:
                _sr_import_errors.append(f"file:{_candidate}: {_e}")

if _schema_registry is None:
    raise ImportError(
        "page_catalog failed to import schema_registry. Tried: " +
        "; ".join(_sr_import_errors)
    )

# Extract required attributes
try:
    SCHEMA_REGISTRY = getattr(_schema_registry, "SCHEMA_REGISTRY")
except AttributeError as _e:
    raise ImportError(f"page_catalog: schema_registry missing SCHEMA_REGISTRY: {_e}") from _e

CANONICAL_SHEETS = getattr(_schema_registry, "CANONICAL_SHEETS", [])
list_sheets      = getattr(_schema_registry, "list_sheets", None)

try:
    get_sheet_spec = getattr(_schema_registry, "get_sheet_spec")
except AttributeError as _e:
    raise ImportError(f"page_catalog: schema_registry missing get_sheet_spec: {_e}") from _e


PAGE_CATALOG_VERSION = "3.3.0"

__all__ = [
    "PAGE_CATALOG_VERSION",
    "PageInfo",
    "CANONICAL_PAGES",
    "SUPPORTED_PAGES",
    "PAGES",
    "INPUT_PAGES",
    "FORBIDDEN_PAGES",
    "FORBIDDEN_ALIASES",
    "PAGE_PARAM_NAMES",
    "ALIAS_TO_CANONICAL",
    "PAGE_ALIASES",
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
    "is_input_page",
    "normalize_page_name",
    "normalize_input_page_name",
    "resolve_page",
    "canonicalize_page",
    "extract_page_candidate",
    "resolve_page_candidate",
    "normalize_page_names",
    "get_top10_feed_pages",
    "get_page_aliases",
    "get_route_family",
    "validate_page_catalog",
]


# =============================================================================
# Models
# =============================================================================

@dataclass(frozen=True)
class PageInfo:
    canonical:         str
    description:       str  = ""
    kind:              str  = ""
    is_data_page:      bool = True
    is_output_page:    bool = False
    is_special_page:   bool = False
    eligible_for_top10: bool = False


# =============================================================================
# Constants
# =============================================================================

FORBIDDEN_PAGES: Set[str] = {"KSA_Tadawul", "Advisor_Criteria"}

# Output/meta pages: readable, but not input universes for scanners/selectors
OUTPUT_PAGES: Set[str] = {"Top_10_Investments", "Data_Dictionary"}

# Pages that must never be routed to the generic instrument fallback by mistake
SPECIAL_PAGES: Set[str] = {"Insights_Analysis", "Top_10_Investments", "Data_Dictionary"}

PAGE_PARAM_NAMES: Tuple[str, ...] = (
    "sheet",
    "sheet_name",
    "page",
    "page_name",
    "name",
    "tab",
    "worksheet",
)

_DESC_OVERRIDES: Dict[str, str] = {
    "Market_Leaders":    "Primary leaders/watchlist universe.",
    "Global_Markets":    "Global markets universe.",
    "Commodities_FX":    "Commodities and FX universe.",
    "Mutual_Funds":      "Mutual funds / ETFs universe.",
    "My_Portfolio":      "User portfolio positions and analytics.",
    "Insights_Analysis": "Insights / analysis output page.",
    "Top_10_Investments":"Top 10 selected investment output page.",
    "Data_Dictionary":   "Schema-derived data dictionary page.",
}

_SEP_RE         = re.compile(r"[\s\-/\\\.\|&]+", re.UNICODE)
_NON_ALNUM_RE   = re.compile(r"[^a-z0-9_]+",     re.UNICODE)


# =============================================================================
# Small helpers
# =============================================================================

def _obj_get(obj: Any, name: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, Mapping):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):   return value
    if isinstance(value, tuple):  return list(value)
    if isinstance(value, set):    return list(value)
    if isinstance(value, str):    return [value]
    if isinstance(value, Iterable):
        try:    return list(value)
        except: return [value]
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
    replacements = {
        "topten":               "top10",
        "top_10":               "top10",
        "top__10":              "top10",
        "investements":         "investments",
        "investement":          "investment",
        "investmant":           "investments",
        "portfolios":           "portfolio",
        "commoditiesandfx":     "commodities_fx",
        "commodities_fx_fx":    "commodities_fx",
        "mutualfund":           "mutual_funds",
        "mutualfunds":          "mutual_funds",
        "globalmarket":         "global_markets",
        "globalmarkets":        "global_markets",
        "marketleaders":        "market_leaders",
        "myportfolio":          "my_portfolio",
        "insightsanalysis":     "insights_analysis",
        "datadictionary":       "data_dictionary",
        "top10investments":     "top10_investments",
        "top10investment":      "top10_investments",
    }
    return replacements.get(s, s)


def _canonical_token_for_page(canonical: str) -> str:
    return _normalize_token(canonical)


def _spec_kind(spec: Any) -> str:
    """
    Extract the 'kind' string from a ColumnSpec or dict spec.
    Returns the kind attribute directly if present (all v3.0.0 specs have it).
    Column-count fallbacks are retained for older schema versions where kind
    may be absent. FIX v3.3.0: insights count corrected from 7 to 10.
    """
    kind = str(_obj_get(spec, "kind", "") or "").strip()
    if kind:
        return kind

    # Fallback: infer from column count (dead code for schema_registry v3.0.0)
    columns = _obj_get(spec, "columns")
    if columns is None and isinstance(spec, Mapping):
        columns = spec.get("columns")
    cols = _as_list(columns)
    col_count = len(cols)

    if col_count == 0:   return ""
    if col_count == 10:  return "insights_output"   # FIX v3.3.0: was 7, Insights_Analysis has 10 cols
    if col_count == 9:   return "dictionary_output"
    if col_count == 83:  return "top10_output"
    if col_count >= 20:  return "instrument_table"

    return ""


def _extract_nested_page_candidate(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    for field in PAGE_PARAM_NAMES:
        got = _obj_get(value, field)
        if isinstance(got, str) and got.strip():
            return got.strip()
    for container_name in ("payload", "params", "query", "body", "data", "result", "request"):
        nested = _obj_get(value, container_name)
        if nested is value or nested is None:
            continue
        got = _extract_nested_page_candidate(nested)
        if isinstance(got, str) and got.strip():
            return got.strip()
    return None


# =============================================================================
# Canonical pages derived from schema_registry
# =============================================================================

_CANONICAL_FROM_SCHEMA: Set[str] = set(str(k) for k in SCHEMA_REGISTRY.keys())


def _canonical_pages_from_schema() -> List[str]:
    # Prefer explicit order from CANONICAL_SHEETS (schema_registry v3.0.0)
    try:
        if CANONICAL_SHEETS:
            ordered = [str(s) for s in CANONICAL_SHEETS if str(s) in _CANONICAL_FROM_SCHEMA]
            if ordered:
                extras = [s for s in sorted(_CANONICAL_FROM_SCHEMA) if s not in ordered]
                return ordered + extras
    except Exception:
        pass

    # Prefer list_sheets() if available
    try:
        if callable(list_sheets):
            sheets = [str(s) for s in list_sheets() if str(s) in _CANONICAL_FROM_SCHEMA]
            if sheets:
                extras = [s for s in sorted(_CANONICAL_FROM_SCHEMA) if s not in sheets]
                return sheets + extras
    except Exception:
        pass

    return sorted(_CANONICAL_FROM_SCHEMA)


CANONICAL_PAGES: List[str] = _canonical_pages_from_schema()
SUPPORTED_PAGES: List[str] = list(CANONICAL_PAGES)
PAGES:           List[str] = list(CANONICAL_PAGES)


# =============================================================================
# PageInfo derived from schema_registry
# FIX v3.3.0: My_Portfolio (portfolio_table) correctly excluded from
# instrument/input pages. Top_10_Investments explicitly overridden even though
# its kind is 'instrument_table' (83 cols) -- it is an output/special page.
# =============================================================================

def _derive_page_info(canonical: str) -> PageInfo:
    kind = ""
    try:
        spec = get_sheet_spec(canonical)
        kind = _spec_kind(spec)
    except Exception:
        kind = ""

    # Explicit overrides for special/output pages (must take priority over kind)
    if canonical == "Insights_Analysis":
        return PageInfo(
            canonical        = canonical,
            description      = _DESC_OVERRIDES.get(canonical, ""),
            kind             = kind or "insights_analysis",
            is_data_page     = False,
            is_output_page   = False,
            is_special_page  = True,
            eligible_for_top10 = False,
        )

    if canonical == "Top_10_Investments":
        # kind == 'instrument_table' (83 cols) but it is an OUTPUT page -- override
        return PageInfo(
            canonical        = canonical,
            description      = _DESC_OVERRIDES.get(canonical, ""),
            kind             = kind or "top10_output",
            is_data_page     = False,
            is_output_page   = True,
            is_special_page  = True,
            eligible_for_top10 = False,
        )

    if canonical == "Data_Dictionary":
        return PageInfo(
            canonical        = canonical,
            description      = _DESC_OVERRIDES.get(canonical, ""),
            kind             = kind or "data_dictionary",
            is_data_page     = False,
            is_output_page   = True,
            is_special_page  = True,
            eligible_for_top10 = False,
        )

    # FIX v3.3.0: only instrument_table pages are true input data universes.
    # My_Portfolio has kind='portfolio_table' -> is_data_page=False, correctly
    # excluded from INPUT_PAGES, INSTRUMENT_PAGES, and TOP10_FEED_PAGES_DEFAULT.
    is_data_page    = (kind == "instrument_table")
    is_output_page  = (canonical in OUTPUT_PAGES)
    is_special_page = (canonical in SPECIAL_PAGES)
    eligible_for_top10 = bool(is_data_page and not is_output_page and not is_special_page)

    return PageInfo(
        canonical        = canonical,
        description      = _DESC_OVERRIDES.get(canonical, ""),
        kind             = kind,
        is_data_page     = is_data_page,
        is_output_page   = is_output_page,
        is_special_page  = is_special_page,
        eligible_for_top10 = eligible_for_top10,
    )


PAGE_INFO: Dict[str, PageInfo] = {c: _derive_page_info(c) for c in CANONICAL_PAGES}

INSTRUMENT_PAGES: Set[str] = {
    p for p, info in PAGE_INFO.items()
    if info.is_data_page and not info.is_special_page
}

INPUT_PAGES: List[str] = [
    p for p in CANONICAL_PAGES
    if p in PAGE_INFO and PAGE_INFO[p].is_data_page and not PAGE_INFO[p].is_special_page
]

TOP10_FEED_PAGES_DEFAULT: List[str] = [
    p for p in ["Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds", "My_Portfolio"]
    if p in PAGE_INFO and PAGE_INFO[p].eligible_for_top10
]


# =============================================================================
# Aliases
# =============================================================================

ALIAS_TO_CANONICAL: Dict[str, str] = {}


def _add_alias(alias: str, canonical: str) -> None:
    tok = _normalize_token(alias)
    if not tok:
        return
    prior = ALIAS_TO_CANONICAL.get(tok)
    if prior and prior != canonical:
        raise ValueError(
            f"Alias collision for token '{tok}': '{prior}' vs '{canonical}'. "
            f"Rename one of the conflicting aliases."
        )
    ALIAS_TO_CANONICAL[tok] = canonical


# Canonical self-aliases
for c in CANONICAL_PAGES:
    _add_alias(c, c)
    _add_alias(c.lower(), c)
    _add_alias(c.replace("_", " "), c)
    _add_alias(c.replace("_", "-"), c)
    _add_alias(c.replace("_", "/"), c)
    _add_alias(c.replace("_", ""), c)

# Friendly aliases
_add_alias("Market Leaders",          "Market_Leaders")
_add_alias("Leaders",                 "Market_Leaders")
_add_alias("Leader",                  "Market_Leaders")
_add_alias("Market Leaders Page",     "Market_Leaders")
_add_alias("MarketLeader",            "Market_Leaders")

_add_alias("Global Markets",          "Global_Markets")
_add_alias("Global Market",           "Global_Markets")
_add_alias("Global",                  "Global_Markets")
_add_alias("GlobalMarkets",           "Global_Markets")

_add_alias("Commodities FX",          "Commodities_FX")
_add_alias("Commodities & FX",        "Commodities_FX")
_add_alias("Commodities and FX",      "Commodities_FX")
_add_alias("Commodity FX",            "Commodities_FX")
_add_alias("Commodities",             "Commodities_FX")
_add_alias("Commodity",               "Commodities_FX")
_add_alias("FX",                      "Commodities_FX")
_add_alias("Forex",                   "Commodities_FX")
_add_alias("CommoditiesFX",           "Commodities_FX")

_add_alias("Mutual Funds",            "Mutual_Funds")
_add_alias("Mutual Fund",             "Mutual_Funds")
_add_alias("Funds",                   "Mutual_Funds")
_add_alias("Fund",                    "Mutual_Funds")
_add_alias("ETF",                     "Mutual_Funds")
_add_alias("ETFs",                    "Mutual_Funds")
_add_alias("MutualFunds",             "Mutual_Funds")

_add_alias("My Portfolio",            "My_Portfolio")
_add_alias("Portfolio",               "My_Portfolio")
_add_alias("My Holdings",             "My_Portfolio")
_add_alias("Holdings",                "My_Portfolio")
_add_alias("MyPortfolio",             "My_Portfolio")

_add_alias("Insights",                "Insights_Analysis")
_add_alias("Insight",                 "Insights_Analysis")
_add_alias("Insights Analysis",       "Insights_Analysis")
_add_alias("Insights-Analysis",       "Insights_Analysis")
_add_alias("Insights & Analysis",     "Insights_Analysis")
_add_alias("InsightsAnalysis",        "Insights_Analysis")

for _alias in (
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
    _add_alias(_alias, "Top_10_Investments")

for _alias in (
    "Data Dictionary",
    "Dictionary",
    "Data-Dictionary",
    "DataDict",
    "Data Dic",
    "Data Dictionary Page",
):
    _add_alias(_alias, "Data_Dictionary")

PAGE_ALIASES: Dict[str, str] = dict(ALIAS_TO_CANONICAL)

FORBIDDEN_ALIASES: Set[str] = {
    _normalize_token("KSA_Tadawul"),
    _normalize_token("Advisor_Criteria"),
    _normalize_token("KSA Tadawul"),
    _normalize_token("Advisor Criteria"),
}


# =============================================================================
# Public helpers
# =============================================================================

def page_info(page: str) -> PageInfo:
    canonical = normalize_page_name(page, allow_output_pages=True, allow_special_pages=True)
    return PAGE_INFO[canonical]


def allowed_pages() -> List[str]:
    return list(CANONICAL_PAGES)


def allowed_input_pages() -> List[str]:
    return list(INPUT_PAGES)


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
        canonical = normalize_page_name(page, allow_output_pages=True, allow_special_pages=True)
    except Exception:
        return False
    return canonical in OUTPUT_PAGES


def is_special_page(page: str) -> bool:
    try:
        canonical = normalize_page_name(page, allow_output_pages=True, allow_special_pages=True)
    except Exception:
        return False
    return canonical in SPECIAL_PAGES


def is_instrument_page(page: str) -> bool:
    try:
        canonical = normalize_page_name(page, allow_output_pages=True, allow_special_pages=True)
    except Exception:
        return False
    return canonical in INSTRUMENT_PAGES


def is_input_page(page: str) -> bool:
    try:
        canonical = normalize_input_page_name(page)
    except Exception:
        return False
    return canonical in INSTRUMENT_PAGES


def normalize_page_name(
    page: str,
    *,
    allow_output_pages: bool = True,
    allow_special_pages: bool = True,
) -> str:
    raw = (page or "").strip()
    if not raw:
        raise ValueError("Page name is empty. Provide a valid page name.")

    if is_forbidden_page(raw):
        raise ValueError(
            f"Page '{page}' is forbidden/removed. Allowed pages: {', '.join(CANONICAL_PAGES)}"
        )

    tok = _normalize_token(raw)
    canonical = ALIAS_TO_CANONICAL.get(tok)

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

    info = PAGE_INFO[canonical]

    if not allow_output_pages and info.is_output_page:
        raise ValueError(f"Page '{canonical}' is an output/meta sheet and not allowed for this operation.")

    if not allow_special_pages and info.is_special_page:
        raise ValueError(f"Page '{canonical}' is a special/output page and not allowed for this operation.")

    return canonical


def normalize_input_page_name(page: str) -> str:
    canonical = normalize_page_name(page, allow_output_pages=False, allow_special_pages=False)
    info = PAGE_INFO.get(canonical)
    if not info or not info.is_data_page:
        raise ValueError(f"Page '{canonical}' is not a valid instrument/input page.")
    return canonical


def resolve_page(page: str) -> str:
    return normalize_page_name(page, allow_output_pages=True, allow_special_pages=True)


def canonicalize_page(page: str) -> str:
    return normalize_page_name(page, allow_output_pages=True, allow_special_pages=True)


def extract_page_candidate(value: Any) -> Optional[str]:
    candidate = _extract_nested_page_candidate(value)
    if isinstance(candidate, str) and candidate.strip():
        return candidate.strip()
    return None


def resolve_page_candidate(
    value: Any,
    *,
    allow_output_pages: bool = True,
    allow_special_pages: bool = True,
    required: bool = False,
) -> Optional[str]:
    candidate = extract_page_candidate(value)
    if not candidate:
        if required:
            raise ValueError(
                f"Could not resolve page candidate from fields: {', '.join(PAGE_PARAM_NAMES)}"
            )
        return None
    return normalize_page_name(
        candidate,
        allow_output_pages=allow_output_pages,
        allow_special_pages=allow_special_pages,
    )


def normalize_page_names(
    pages: Sequence[str],
    *,
    allow_output_pages: bool = True,
    allow_special_pages: bool = True,
    dedupe: bool = True,
) -> List[str]:
    out:  List[str] = []
    seen: Set[str]  = set()
    for page in pages:
        canonical = normalize_page_name(
            page,
            allow_output_pages=allow_output_pages,
            allow_special_pages=allow_special_pages,
        )
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
        canonical = normalize_input_page_name(p)
        info = PAGE_INFO.get(canonical)
        if info and info.eligible_for_top10 and info.is_data_page and not info.is_special_page:
            normalized.append(canonical)
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
    - "instrument"   (all instrument/portfolio pages incl. My_Portfolio)
    - "insights"     (Insights_Analysis)
    - "top10"        (Top_10_Investments)
    - "dictionary"   (Data_Dictionary)
    """
    canonical = normalize_page_name(page, allow_output_pages=True, allow_special_pages=True)
    if canonical == "Insights_Analysis":  return "insights"
    if canonical == "Top_10_Investments": return "top10"
    if canonical == "Data_Dictionary":    return "dictionary"
    return "instrument"


# =============================================================================
# Validation
# =============================================================================

def validate_page_catalog() -> None:
    # Forbidden pages must not exist in schema registry
    for fp in FORBIDDEN_PAGES:
        if fp in _CANONICAL_FROM_SCHEMA:
            raise ValueError(f"Forbidden page '{fp}' exists in schema_registry. Remove it.")

    # Page catalog pages must exist in schema registry
    for canonical in CANONICAL_PAGES:
        if canonical not in _CANONICAL_FROM_SCHEMA:
            raise ValueError(f"Page '{canonical}' exists in page_catalog but not in schema_registry.")

    # Canonical tokens must be unique after normalization
    normalized_canonical_tokens: Dict[str, str] = {}
    for canonical in CANONICAL_PAGES:
        tok = _canonical_token_for_page(canonical)
        prior = normalized_canonical_tokens.get(tok)
        if prior and prior != canonical:
            raise ValueError(
                f"Canonical token collision after normalization: '{prior}' vs '{canonical}' -> '{tok}'"
            )
        normalized_canonical_tokens[tok] = canonical

    # Critical normalization checks
    must_resolve: List[Tuple[str, str]] = [
        ("Market Leaders",        "Market_Leaders"),
        ("MarketLeaders",         "Market_Leaders"),
        ("Global Markets",        "Global_Markets"),
        ("GlobalMarkets",         "Global_Markets"),
        ("Commodities & FX",      "Commodities_FX"),
        ("CommoditiesFX",         "Commodities_FX"),
        ("Mutual Funds",          "Mutual_Funds"),
        ("MutualFunds",           "Mutual_Funds"),
        ("My Portfolio",          "My_Portfolio"),
        ("MyPortfolio",           "My_Portfolio"),
        ("Insights Analysis",     "Insights_Analysis"),
        ("Insights-Analysis",     "Insights_Analysis"),
        ("InsightsAnalysis",      "Insights_Analysis"),
        ("Top 10 Investments",    "Top_10_Investments"),
        ("Top10",                 "Top_10_Investments"),
        ("Top10Investments",      "Top_10_Investments"),
        ("Top Ten Investments",   "Top_10_Investments"),
        ("Data Dictionary",       "Data_Dictionary"),
        ("Data-Dictionary",       "Data_Dictionary"),
        ("DataDict",              "Data_Dictionary"),
    ]
    for raw, expected in must_resolve:
        resolved = normalize_page_name(raw, allow_output_pages=True, allow_special_pages=True)
        if resolved != expected:
            raise ValueError(f"Normalization failed for '{raw}' -> '{resolved}' (expected '{expected}')")

    # Payload-style resolution checks
    payload_examples: List[Tuple[Any, str]] = [
        ({"sheet":      "Market Leaders"},  "Market_Leaders"),
        ({"sheet_name": "GlobalMarkets"},   "Global_Markets"),
        ({"page":       "Commodities & FX"},"Commodities_FX"),
        ({"page_name":  "Mutual Funds"},    "Mutual_Funds"),
        ({"name":       "My Portfolio"},    "My_Portfolio"),
        ({"tab":        "Insights Analysis"},"Insights_Analysis"),
        ({"worksheet":  "Top10"},           "Top_10_Investments"),
        ({"payload": {"sheet": "Data Dictionary"}}, "Data_Dictionary"),
    ]
    for value, expected in payload_examples:
        resolved = resolve_page_candidate(value, required=True)
        if resolved != expected:
            raise ValueError(f"Payload resolution failed for {value!r} -> '{resolved}' (expected '{expected}')")

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
        if PAGE_INFO[sp].is_data_page:
            raise ValueError(f"Special page '{sp}' must not be treated as instrument_table.")

    # Output pages must also be special pages
    for op in OUTPUT_PAGES:
        if op not in PAGE_INFO:
            raise ValueError(f"Output page '{op}' is not registered in PAGE_INFO.")
        if not PAGE_INFO[op].is_output_page:
            raise ValueError(f"Output page '{op}' must be marked is_output_page=True.")
        if not PAGE_INFO[op].is_special_page:
            raise ValueError(f"Output page '{op}' must be marked is_special_page=True.")

    # Input pages must be the non-special data pages only (instrument_table kind)
    expected_input_pages = [
        p for p in CANONICAL_PAGES
        if p in PAGE_INFO and PAGE_INFO[p].is_data_page and not PAGE_INFO[p].is_special_page
    ]
    if INPUT_PAGES != expected_input_pages:
        raise ValueError(f"INPUT_PAGES drift detected: got {INPUT_PAGES!r}, expected {expected_input_pages!r}")

    # Top10 default feeds must be valid input universes
    for p in TOP10_FEED_PAGES_DEFAULT:
        if p not in PAGE_INFO:
            raise ValueError(f"TOP10_FEED_PAGES_DEFAULT includes unknown page '{p}'.")
        info = PAGE_INFO[p]
        if not info.is_data_page:
            raise ValueError(f"TOP10_FEED_PAGES_DEFAULT includes non-data page '{p}'.")
        if not info.eligible_for_top10:
            raise ValueError(f"TOP10_FEED_PAGES_DEFAULT includes ineligible page '{p}'.")

    # Canonical pages must include the expected 8 sheets
    expected_core = {
        "Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds",
        "My_Portfolio", "Insights_Analysis", "Top_10_Investments", "Data_Dictionary",
    }
    missing_expected = expected_core.difference(set(CANONICAL_PAGES))
    if missing_expected:
        raise ValueError(f"Missing expected canonical pages: {sorted(missing_expected)}")

    # Route family checks
    expected_families = {
        "Market_Leaders":    "instrument",
        "Global_Markets":    "instrument",
        "Commodities_FX":    "instrument",
        "Mutual_Funds":      "instrument",
        "My_Portfolio":      "instrument",
        "Insights_Analysis": "insights",
        "Top_10_Investments":"top10",
        "Data_Dictionary":   "dictionary",
    }
    for page, family in expected_families.items():
        got = get_route_family(page)
        if got != family:
            raise ValueError(f"Route family mismatch for '{page}': got '{got}', expected '{family}'")


# Validate immediately at module load (fast, no I/O)
validate_page_catalog()
