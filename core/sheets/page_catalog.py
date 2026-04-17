#!/usr/bin/env python3
# core/sheets/page_catalog.py
"""
================================================================================
Page Catalog -- v3.4.0 (PER-PAGE SCHEMA / MULTI-KIND / ROUTE-DISPATCH SAFE)
================================================================================

Purpose
-------
Single normalization and page-resolution layer for all sheet/page operations.

v3.4.0 changes vs v3.3.0
--------------------------
ENH: Per-page schema support — each page now has its own dedicated kind string
     matching the new schema_registry.py per-page column definitions:

     instrument_table       → Market_Leaders (standard 80-col equity schema)
     global_markets_table   → Global_Markets (EODHD-enhanced + sector analysis)
     commodities_table      → Commodities_FX (commodity-specific, no equity fields)
     mutual_funds_table     → Mutual_Funds (AUM, expense ratio, NAV, returns)
     portfolio_table        → My_Portfolio (stop/target, weight, rebalancing)
     insights_analysis      → Insights_Analysis (signal + priority columns)
     top10_output           → Top_10_Investments (entry/stop/target trade setup)
     data_dictionary        → Data_Dictionary (schema reference, unchanged)

ENH: INSTRUMENT_TABLE_KINDS set — all kinds that represent live instrument/fund
     data pages (is_data_page=True, eligible_for_top10 where appropriate).
     Previously only "instrument_table" qualified. Now all five page-specific
     instrument-family kinds qualify.

ENH: _spec_kind() fallback column counts updated for new per-page schemas.
     Fallbacks are dead code when schema_registry returns kind directly (all
     v3.4.0 specs), but kept accurate for older schema compatibility.

FIX: _derive_page_info() now checks kind against INSTRUMENT_TABLE_KINDS instead
     of == 'instrument_table'. Global_Markets, Commodities_FX, Mutual_Funds
     retain is_data_page=True with their new kind strings.

FIX: INSTRUMENT_PAGES and INPUT_PAGES correctly include all five instrument
     pages regardless of their specific kind string.

FIX: eligible_for_top10 correctly set for Global_Markets, Commodities_FX,
     Mutual_Funds under their new kind strings.

Preserved from v3.3.0
----------------------
- All normalization logic, alias definitions, and validation.
- All public exports and __all__ contents.
- All route-dispatch helpers (get_route_family returns same values).
- My_Portfolio excluded from TOP10_FEED_PAGES_DEFAULT (portfolio_table kind,
  not eligible_for_top10 — positions data, not a scored universe).
- Validation suite fully retained and extended.

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
# Try all deployment layouts before failing.
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


PAGE_CATALOG_VERSION = "3.4.0"

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
    "INSTRUMENT_TABLE_KINDS",
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
    canonical:          str
    description:        str  = ""
    kind:               str  = ""
    is_data_page:       bool = True
    is_output_page:     bool = False
    is_special_page:    bool = False
    eligible_for_top10: bool = False


# =============================================================================
# Kind classification
# =============================================================================

# ENH v3.4.0: All kind strings that represent live instrument/fund data pages.
# These are the pages that return real market data rows (not derived/output pages).
# Routing treats all of these as "instrument" pages — same route family, same
# handler logic, different column schemas.
INSTRUMENT_TABLE_KINDS: Set[str] = {
    "instrument_table",       # Market_Leaders — standard 80-col+ equity schema
    "global_markets_table",   # Global_Markets — EODHD-enhanced + sector analysis + MSCI comparative
    "commodities_table",      # Commodities_FX — commodity/FX-specific, no equity-only fundamentals
    "mutual_funds_table",     # Mutual_Funds   — AUM, expense ratio, NAV, multi-period returns
    "portfolio_table",        # My_Portfolio   — stop/target, weight, rebalancing cols
}

# Kind strings for output/special pages (not live data universes)
OUTPUT_KIND_STRINGS: Set[str] = {
    "insights_analysis",   # Insights_Analysis
    "top10_output",        # Top_10_Investments
    "data_dictionary",     # Data_Dictionary
}

# Canonical kind → page mapping (for _spec_kind fallback validation)
_KIND_TO_PAGE: Dict[str, str] = {
    "instrument_table":     "Market_Leaders",
    "global_markets_table": "Global_Markets",
    "commodities_table":    "Commodities_FX",
    "mutual_funds_table":   "Mutual_Funds",
    "portfolio_table":      "My_Portfolio",
    "insights_analysis":    "Insights_Analysis",
    "top10_output":         "Top_10_Investments",
    "data_dictionary":      "Data_Dictionary",
}


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
    "Market_Leaders":    "Primary Saudi & global equity leaders — full AI scoring + short-term signals.",
    "Global_Markets":    "International equity universe — EODHD-enhanced + sector analysis + comparative performance.",
    "Commodities_FX":    "Commodities & FX pairs — commodity-specific signals, carry rate, USD correlation.",
    "Mutual_Funds":      "ETFs & mutual funds — AUM, expense ratio, NAV premium, multi-period returns.",
    "My_Portfolio":      "User portfolio — P&L, stop/target, weight allocation, rebalancing signals.",
    "Insights_Analysis": "AI market intelligence — top picks, risk alerts, sector signals, portfolio KPIs.",
    "Top_10_Investments":"AI-selected top 10 — ranked with entry price, stop loss, take profit, R/R ratio.",
    "Data_Dictionary":   "Schema reference — all column definitions, sources, formats.",
}

_SEP_RE       = re.compile(r"[\s\-/\\\.\|&]+", re.UNICODE)
_NON_ALNUM_RE = re.compile(r"[^a-z0-9_]+",     re.UNICODE)


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
    Extract the 'kind' string from a SheetSpec.

    v3.4.0: schema_registry assigns per-page kind strings directly.
    All specs have kind set — fallbacks below are kept for older schema
    compatibility only and should not trigger in production.

    Fallback column-count heuristics (updated for new per-page schemas):
      9  cols  → data_dictionary or insights_analysis (ambiguous — prefer kind attr)
      80-95    → instrument_table (Market_Leaders range)
      87-93    → commodities_table (Commodities_FX range)
      88-94    → mutual_funds_table (Mutual_Funds range)
      89-95    → global_markets_table (Global_Markets range)
      90-95    → portfolio_table (My_Portfolio range)
      88-95    → top10_output (Top_10_Investments base+extras range)
      >=20     → instrument_table (generic fallback)
    """
    kind = str(_obj_get(spec, "kind", "") or "").strip()
    if kind:
        return kind

    # Fallback: infer from column count (should not trigger with schema_registry v3.4.0)
    columns = _obj_get(spec, "columns")
    if columns is None and isinstance(spec, Mapping):
        columns = spec.get("columns")
    cols = _as_list(columns)
    col_count = len(cols)

    if col_count == 0:   return ""
    if col_count == 9:   return "data_dictionary"  # most likely — Insights has 9 too
    if col_count >= 88:  return "instrument_table"  # top10, global, commodities, funds, portfolio all >=88
    if col_count >= 20:  return "instrument_table"  # generic instrument fallback

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
    try:
        if CANONICAL_SHEETS:
            ordered = [str(s) for s in CANONICAL_SHEETS if str(s) in _CANONICAL_FROM_SCHEMA]
            if ordered:
                extras = [s for s in sorted(_CANONICAL_FROM_SCHEMA) if s not in ordered]
                return ordered + extras
    except Exception:
        pass

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
#
# v3.4.0: is_data_page determined by kind in INSTRUMENT_TABLE_KINDS.
# Global_Markets (global_markets_table), Commodities_FX (commodities_table),
# and Mutual_Funds (mutual_funds_table) now have their own kind strings but
# remain is_data_page=True and eligible_for_top10=True.
#
# My_Portfolio (portfolio_table) retains is_data_page=True for data routing
# but eligible_for_top10=False (portfolio positions ≠ scored universe).
#
# Explicit overrides for special/output pages take priority over kind dispatch.
# =============================================================================

def _derive_page_info(canonical: str) -> PageInfo:
    kind = ""
    try:
        spec = get_sheet_spec(canonical)
        kind = _spec_kind(spec)
    except Exception:
        kind = ""

    # ── Explicit overrides for special / output pages ──────────────────────
    # These must take priority over kind strings.

    if canonical == "Insights_Analysis":
        return PageInfo(
            canonical          = canonical,
            description        = _DESC_OVERRIDES.get(canonical, ""),
            kind               = kind or "insights_analysis",
            is_data_page       = False,
            is_output_page     = False,
            is_special_page    = True,
            eligible_for_top10 = False,
        )

    if canonical == "Top_10_Investments":
        # kind may be 'instrument_table' (base cols) — override to output
        return PageInfo(
            canonical          = canonical,
            description        = _DESC_OVERRIDES.get(canonical, ""),
            kind               = kind or "top10_output",
            is_data_page       = False,
            is_output_page     = True,
            is_special_page    = True,
            eligible_for_top10 = False,
        )

    if canonical == "Data_Dictionary":
        return PageInfo(
            canonical          = canonical,
            description        = _DESC_OVERRIDES.get(canonical, ""),
            kind               = kind or "data_dictionary",
            is_data_page       = False,
            is_output_page     = True,
            is_special_page    = True,
            eligible_for_top10 = False,
        )

    # ── ENH v3.4.0: per-page kind dispatch ────────────────────────────────
    # All five instrument-family kinds are live data pages.
    # My_Portfolio (portfolio_table) is a data page for routing but NOT
    # eligible_for_top10 — portfolio positions are not a scored universe.

    is_data_page    = kind in INSTRUMENT_TABLE_KINDS
    is_output_page  = canonical in OUTPUT_PAGES
    is_special_page = canonical in SPECIAL_PAGES

    # portfolio_table: data page for routing, excluded from top10 feeds
    if kind == "portfolio_table":
        eligible_for_top10 = False
    else:
        eligible_for_top10 = bool(
            is_data_page and not is_output_page and not is_special_page
        )

    return PageInfo(
        canonical          = canonical,
        description        = _DESC_OVERRIDES.get(canonical, ""),
        kind               = kind,
        is_data_page       = is_data_page,
        is_output_page     = is_output_page,
        is_special_page    = is_special_page,
        eligible_for_top10 = eligible_for_top10,
    )


PAGE_INFO: Dict[str, PageInfo] = {c: _derive_page_info(c) for c in CANONICAL_PAGES}

# ENH v3.4.0: INSTRUMENT_PAGES includes all pages whose kind is in INSTRUMENT_TABLE_KINDS
INSTRUMENT_PAGES: Set[str] = {
    p for p, info in PAGE_INFO.items()
    if info.is_data_page and not info.is_special_page
}

INPUT_PAGES: List[str] = [
    p for p in CANONICAL_PAGES
    if p in PAGE_INFO and PAGE_INFO[p].is_data_page and not PAGE_INFO[p].is_special_page
]

# Top10 feed pages: all eligible instrument pages EXCLUDING My_Portfolio
# (portfolio_table kind is not a scored universe of instruments)
TOP10_FEED_PAGES_DEFAULT: List[str] = [
    p for p in ["Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds"]
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

# Friendly aliases — Market_Leaders
_add_alias("Market Leaders",        "Market_Leaders")
_add_alias("Leaders",               "Market_Leaders")
_add_alias("Leader",                "Market_Leaders")
_add_alias("Market Leaders Page",   "Market_Leaders")
_add_alias("MarketLeader",          "Market_Leaders")
_add_alias("TASI Leaders",          "Market_Leaders")
_add_alias("Saudi Leaders",         "Market_Leaders")

# Friendly aliases — Global_Markets
_add_alias("Global Markets",        "Global_Markets")
_add_alias("Global Market",         "Global_Markets")
_add_alias("Global",                "Global_Markets")
_add_alias("GlobalMarkets",         "Global_Markets")
_add_alias("International Markets", "Global_Markets")
_add_alias("World Markets",         "Global_Markets")
_add_alias("International",         "Global_Markets")

# Friendly aliases — Commodities_FX
_add_alias("Commodities FX",        "Commodities_FX")
_add_alias("Commodities & FX",      "Commodities_FX")
_add_alias("Commodities and FX",    "Commodities_FX")
_add_alias("Commodity FX",          "Commodities_FX")
_add_alias("Commodities",           "Commodities_FX")
_add_alias("Commodity",             "Commodities_FX")
_add_alias("FX",                    "Commodities_FX")
_add_alias("Forex",                 "Commodities_FX")
_add_alias("CommoditiesFX",         "Commodities_FX")
_add_alias("Metals",                "Commodities_FX")
_add_alias("Gold",                  "Commodities_FX")
_add_alias("Oil",                   "Commodities_FX")

# Friendly aliases — Mutual_Funds
_add_alias("Mutual Funds",          "Mutual_Funds")
_add_alias("Mutual Fund",           "Mutual_Funds")
_add_alias("Funds",                 "Mutual_Funds")
_add_alias("Fund",                  "Mutual_Funds")
_add_alias("ETF",                   "Mutual_Funds")
_add_alias("ETFs",                  "Mutual_Funds")
_add_alias("MutualFunds",           "Mutual_Funds")
_add_alias("Index Funds",           "Mutual_Funds")
_add_alias("Investment Funds",      "Mutual_Funds")

# Friendly aliases — My_Portfolio
_add_alias("My Portfolio",          "My_Portfolio")
_add_alias("Portfolio",             "My_Portfolio")
_add_alias("My Holdings",           "My_Portfolio")
_add_alias("Holdings",              "My_Portfolio")
_add_alias("MyPortfolio",           "My_Portfolio")
_add_alias("My Investments",        "My_Portfolio")
_add_alias("Positions",             "My_Portfolio")

# Friendly aliases — Insights_Analysis
_add_alias("Insights",              "Insights_Analysis")
_add_alias("Insight",               "Insights_Analysis")
_add_alias("Insights Analysis",     "Insights_Analysis")
_add_alias("Insights-Analysis",     "Insights_Analysis")
_add_alias("Insights & Analysis",   "Insights_Analysis")
_add_alias("InsightsAnalysis",      "Insights_Analysis")
_add_alias("AI Insights",           "Insights_Analysis")
_add_alias("Market Insights",       "Insights_Analysis")

# Friendly aliases — Top_10_Investments
for _alias in (
    "Top 10", "Top10", "Top Ten",
    "Top 10 Investments", "Top10 Investments", "Top Ten Investments",
    "Top_10", "Top10_Investments", "Top_10_Investment", "Top10Investment",
    "Top10Investments", "Top 10 Investements", "Top10 Investements",
    "Top 10 Investmant", "Top10 Investmant",
    "Best Picks", "Top Picks", "Best Investments",
):
    _add_alias(_alias, "Top_10_Investments")

# Friendly aliases — Data_Dictionary
for _alias in (
    "Data Dictionary", "Dictionary", "Data-Dictionary",
    "DataDict", "Data Dic", "Data Dictionary Page",
    "Schema Reference", "Column Reference",
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
    """Returns True for any of the five live data pages (all INSTRUMENT_TABLE_KINDS)."""
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


def get_page_kind(page: str) -> str:
    """Return the kind string for a page. Returns '' if unknown."""
    try:
        canonical = normalize_page_name(page, allow_output_pages=True, allow_special_pages=True)
        return PAGE_INFO[canonical].kind
    except Exception:
        return ""


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
    """
    Returns ordered list of pages eligible as Top10 feed universes.

    Default feed pages (v3.4.0):
      Market_Leaders, Global_Markets, Commodities_FX, Mutual_Funds
      (My_Portfolio excluded — portfolio positions are not a scored universe)

    pages_override allows callers to specify a custom subset/order.
    """
    pages = list(pages_override) if pages_override is not None else list(TOP10_FEED_PAGES_DEFAULT)
    normalized: List[str] = []
    for p in pages:
        try:
            canonical = normalize_input_page_name(p)
            info = PAGE_INFO.get(canonical)
            if info and info.eligible_for_top10 and info.is_data_page and not info.is_special_page:
                normalized.append(canonical)
        except Exception:
            pass
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
      "instrument"   — all five live data pages (all INSTRUMENT_TABLE_KINDS)
      "insights"     — Insights_Analysis
      "top10"        — Top_10_Investments
      "dictionary"   — Data_Dictionary

    Note: routing family is independent of kind string. Global_Markets,
    Commodities_FX, Mutual_Funds, and My_Portfolio all return "instrument"
    even with their new per-page kind strings in v3.4.0.
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

    # All page catalog pages must exist in schema registry
    for canonical in CANONICAL_PAGES:
        if canonical not
