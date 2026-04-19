#!/usr/bin/env python3
# core/sheets/page_catalog.py
"""
================================================================================
Page Catalog -- v5.0.0 (PER-PAGE SCHEMA / MULTI-KIND / ROUTE-DISPATCH SAFE)
================================================================================

Purpose
-------
Single normalization and page-resolution layer for all sheet/page operations.

v5.0.0 Changes (from v4.0.0)
----------------------------
Bug fixes:
  - Module import is now actually startup-safe. v4.0.0 claimed to be
    "STARTUP-SAFE" in the header but ran
        CANONICAL_PAGES = get_catalog().get_canonical_pages()
        INPUT_PAGES     = get_catalog().get_input_pages()
        ... etc (7 module-level lines)
    at the BOTTOM of the module. This forced PageCatalog() ->
    SchemaRegistryLoader() -> _load() at IMPORT TIME. If schema_registry
    wasn't importable (dev, partial install, Docker staging), `import
    core.sheets.page_catalog` crashed, cascading through everything that
    imports it. v5.0.0 exposes these seven constants via PEP 562
    `__getattr__`, computed lazily on first access and cached in
    module globals so subsequent reads hit the fast path directly.
  - `SchemaRegistryLoader.__new__` cached `cls._instance` BEFORE calling
    `_load()`, so a failed load left a broken singleton that later
    `SchemaRegistryLoader()` calls would silently reuse. v5.0.0 caches
    only AFTER successful load (same fix applied in data_dictionary v5).
  - `extract_page_candidate` used `field` as a loop variable. Harmless
    today (no `dataclasses.field()` calls in the module) but a landmine.
    Renamed to `param_name`. The unused `field` import from dataclasses
    is also removed.

Dead code removed:
  - `PageInfoBuilder._extract_kind` had a `>= 88` branch shadowed by
    `>= 20` (both returned INSTRUMENT_TABLE). Dropped `>= 88`.
  - `normalize_page_name`'s fallback loop iterating canonical tokens was
    dead -- `build_default_aliases` registers `canonical -> canonical`
    for every page, so the alias lookup covers that case already.
  - Unused imports: `cast`, `Union`, `field` from dataclasses.

Cleanup:
  - Three hardcoded `if canonical == "Insights_Analysis":` / Top_10 /
    Data_Dictionary branches in `PageInfoBuilder.build` consolidated
    into a `_SPECIAL_PAGE_CONFIG` table.
  - Added `PageCatalog`, `PageInfoBuilder`, `AliasManager`,
    `TokenNormalizer`, `SchemaRegistryLoader`, `RouteFamily`, `PageKind`
    and the four exception classes to `__all__`.

Preserved for backward compatibility:
  - All public function surfaces (normalize_page_name, resolve_page,
    canonicalize_page, extract_page_candidate, resolve_page_candidate,
    normalize_page_names, get_top10_feed_pages, get_route_family, etc.).
  - Module-level constants CANONICAL_PAGES / SUPPORTED_PAGES / PAGES /
    INPUT_PAGES / INSTRUMENT_PAGES / ALIAS_TO_CANONICAL / PAGE_ALIASES
    (now lazy but behaviorally identical).
  - Static constants FORBIDDEN_PAGES, OUTPUT_PAGES, SPECIAL_PAGES,
    INSTRUMENT_TABLE_KINDS, TOP10_FEED_PAGES_DEFAULT, PAGE_PARAM_NAMES,
    PAGE_CATALOG_VERSION unchanged.
  - Every alias registered in v4 (Market Leaders, Top 10, etc.).
  - Route family strings and dispatch behavior.
================================================================================
"""

from __future__ import annotations

import importlib
import importlib.util
import logging
import os
import re
import sys
from dataclasses import dataclass
from enum import Enum
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
)

# =============================================================================
# Logging
# =============================================================================

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# =============================================================================
# Constants (static -- safe at import time)
# =============================================================================

PAGE_CATALOG_VERSION = "5.0.0"

PAGE_PARAM_NAMES: Tuple[str, ...] = (
    "sheet",
    "sheet_name",
    "page",
    "page_name",
    "name",
    "tab",
    "worksheet",
)

FORBIDDEN_PAGES: Set[str] = {"KSA_Tadawul", "Advisor_Criteria"}
OUTPUT_PAGES: Set[str] = {"Top_10_Investments", "Data_Dictionary"}
SPECIAL_PAGES: Set[str] = {"Insights_Analysis", "Top_10_Investments", "Data_Dictionary"}

_DESCRIPTIONS: Dict[str, str] = {
    "Market_Leaders": "Primary Saudi & global equity leaders — full AI scoring + short-term signals.",
    "Global_Markets": "International equity universe — EODHD-enhanced + sector analysis + comparative performance.",
    "Commodities_FX": "Commodities & FX pairs — commodity-specific signals, carry rate, USD correlation.",
    "Mutual_Funds": "ETFs & mutual funds — AUM, expense ratio, NAV premium, multi-period returns.",
    "My_Portfolio": "User portfolio — P&L, stop/target, weight allocation, rebalancing signals.",
    "Insights_Analysis": "AI market intelligence — top picks, risk alerts, sector signals, portfolio KPIs.",
    "Top_10_Investments": "AI-selected top 10 — ranked with entry price, stop loss, take profit, R/R ratio.",
    "Data_Dictionary": "Schema reference — all column definitions, sources, formats.",
}

INSTRUMENT_TABLE_KINDS: Set[str] = {
    "instrument_table",
    "global_markets_table",
    "commodities_table",
    "mutual_funds_table",
    "portfolio_table",
}

OUTPUT_KIND_STRINGS: Set[str] = {
    "insights_analysis",
    "top10_output",
    "data_dictionary",
}

_KIND_TO_PAGE: Dict[str, str] = {
    "instrument_table": "Market_Leaders",
    "global_markets_table": "Global_Markets",
    "commodities_table": "Commodities_FX",
    "mutual_funds_table": "Mutual_Funds",
    "portfolio_table": "My_Portfolio",
    "insights_analysis": "Insights_Analysis",
    "top10_output": "Top_10_Investments",
    "data_dictionary": "Data_Dictionary",
}

TOP10_FEED_PAGES_DEFAULT: List[str] = [
    "Market_Leaders",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
]


# =============================================================================
# Custom Exceptions
# =============================================================================

class PageCatalogError(Exception):
    """Base exception for Page Catalog errors."""


class PageNotFoundError(PageCatalogError):
    """Raised when a page is not found."""


class ForbiddenPageError(PageCatalogError):
    """Raised when a forbidden page is requested."""


class InvalidPageError(PageCatalogError):
    """Raised when a page is invalid for an operation."""


# =============================================================================
# Enums
# =============================================================================

class RouteFamily(str, Enum):
    """Route family classification for dispatch."""
    INSTRUMENT = "instrument"
    INSIGHTS = "insights"
    TOP10 = "top10"
    DICTIONARY = "dictionary"


class PageKind(str, Enum):
    """Page kind classification."""
    INSTRUMENT_TABLE = "instrument_table"
    GLOBAL_MARKETS_TABLE = "global_markets_table"
    COMMODITIES_TABLE = "commodities_table"
    MUTUAL_FUNDS_TABLE = "mutual_funds_table"
    PORTFOLIO_TABLE = "portfolio_table"
    INSIGHTS_ANALYSIS = "insights_analysis"
    TOP10_OUTPUT = "top10_output"
    DATA_DICTIONARY = "data_dictionary"


# v5.0.0: consolidates three hardcoded special-page branches from v4's
# PageInfoBuilder.build into a single lookup table.
_SPECIAL_PAGE_CONFIG: Dict[str, Dict[str, Any]] = {
    "Insights_Analysis": {
        "kind": PageKind.INSIGHTS_ANALYSIS.value,
        "is_data_page": False,
        "is_output_page": False,
        "is_special_page": True,
    },
    "Top_10_Investments": {
        "kind": PageKind.TOP10_OUTPUT.value,
        "is_data_page": False,
        "is_output_page": True,
        "is_special_page": True,
    },
    "Data_Dictionary": {
        "kind": PageKind.DATA_DICTIONARY.value,
        "is_data_page": False,
        "is_output_page": True,
        "is_special_page": True,
    },
}


# =============================================================================
# Data Models
# =============================================================================

@dataclass(frozen=True)
class PageInfo:
    """Information about a page."""
    canonical: str
    description: str = ""
    kind: str = ""
    is_data_page: bool = True
    is_output_page: bool = False
    is_special_page: bool = False
    eligible_for_top10: bool = False


# =============================================================================
# Schema Registry Loader (robust import, singleton)
# =============================================================================

class SchemaRegistryLoader:
    """Robust loader for schema_registry module."""

    _instance: Optional["SchemaRegistryLoader"] = None
    _module: Optional[Any] = None

    _IMPORT_PATHS = [
        "core.sheets.schema_registry",
        "core.schema_registry",
        "schema_registry",
    ]

    def __new__(cls) -> "SchemaRegistryLoader":
        # v5.0.0 fix: cache cls._instance only AFTER successful _load().
        # v4.0.0 set it before, leaving a broken singleton on import failure.
        if cls._instance is None:
            instance = super().__new__(cls)
            instance._load()  # may raise PageCatalogError
            cls._instance = instance
        return cls._instance

    def _load(self) -> None:
        """Load schema_registry module, trying known import paths then sys.path walk."""
        errors: List[str] = []

        for module_path in self._IMPORT_PATHS:
            try:
                self._module = importlib.import_module(module_path)
                logger.debug("Loaded schema_registry from: %s", module_path)
                return
            except ImportError as exc:
                errors.append(f"{module_path}: {exc}")

        for sys_path in sys.path:
            candidate = os.path.join(sys_path, "schema_registry.py")
            if os.path.isfile(candidate):
                try:
                    spec = importlib.util.spec_from_file_location("schema_registry", candidate)
                    if spec and spec.loader:
                        self._module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(self._module)
                        logger.debug("Loaded schema_registry from file: %s", candidate)
                        return
                except Exception as exc:
                    errors.append(f"file:{candidate}: {exc}")

        raise PageCatalogError(
            f"page_catalog failed to import schema_registry. Tried: {'; '.join(errors)}"
        )

    @property
    def module(self) -> Any:
        if self._module is None:
            self._load()
        return self._module

    def get_schema_registry(self) -> Dict[str, Any]:
        """Return the registry's sheet-spec map."""
        for attr_name in ("SCHEMA_REGISTRY", "SHEET_SPECS", "REGISTRY", "SHEETS"):
            value = getattr(self.module, attr_name, None)
            if isinstance(value, Mapping):
                return dict(value)
        return {}

    def get_canonical_sheets(self) -> List[str]:
        """Return the registry's canonical-sheet list."""
        for attr_name in ("CANONICAL_SHEETS", "CANONICAL_PAGES"):
            value = getattr(self.module, attr_name, None)
            if value is not None:
                return [str(x) for x in self._as_list(value) if str(x)]
        return []

    def get_list_sheets(self) -> List[str]:
        """Return the registry's list of all sheets."""
        fn = getattr(self.module, "list_sheets", None)
        if callable(fn):
            try:
                return [str(x) for x in self._as_list(fn()) if str(x)]
            except Exception:
                pass
        return list(self.get_schema_registry().keys())

    def get_sheet_spec(self, sheet_name: str) -> Any:
        """Return the registry's spec for a given sheet."""
        fn = getattr(self.module, "get_sheet_spec", None)
        if callable(fn):
            try:
                return fn(sheet_name)
            except Exception:
                pass

        registry = self.get_schema_registry()
        if sheet_name in registry:
            return registry[sheet_name]

        raise PageNotFoundError(f"Sheet not found: {sheet_name}")

    @staticmethod
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


# =============================================================================
# Token Normalization
# =============================================================================

class TokenNormalizer:
    """Normalizes page names to canonical tokens for matching."""

    _SEP_RE = re.compile(r"[\s\-/\\\.\|&]+", re.UNICODE)
    _NON_ALNUM_RE = re.compile(r"[^a-z0-9_]+", re.UNICODE)

    _REPLACEMENTS: Dict[str, str] = {
        "topten": "top10",
        "top_10": "top10",
        "top__10": "top10",
        "investements": "investments",
        "investement": "investment",
        "investmant": "investments",
        "portfolios": "portfolio",
        "commoditiesandfx": "commodities_fx",
        "commodities_fx_fx": "commodities_fx",
        "mutualfund": "mutual_funds",
        "mutualfunds": "mutual_funds",
        "globalmarket": "global_markets",
        "globalmarkets": "global_markets",
        "marketleaders": "market_leaders",
        "myportfolio": "my_portfolio",
        "insightsanalysis": "insights_analysis",
        "datadictionary": "data_dictionary",
        "top10investments": "top10_investments",
        "top10investment": "top10_investments",
    }

    @classmethod
    def normalize(cls, value: str) -> str:
        """Lower-case, strip separators/punctuation, then apply fuzzy replacements."""
        s = (value or "").strip()
        if not s:
            return ""

        s = s.casefold()
        s = cls._SEP_RE.sub("_", s)
        s = cls._NON_ALNUM_RE.sub("", s)
        s = re.sub(r"_+", "_", s).strip("_")

        return cls._REPLACEMENTS.get(s, s)

    @classmethod
    def canonical_token(cls, canonical: str) -> str:
        """Canonical token for a canonical sheet name."""
        return cls.normalize(canonical)


# =============================================================================
# Page Info Builder
# =============================================================================

class PageInfoBuilder:
    """Builds PageInfo objects from the schema registry."""

    def __init__(self, loader: SchemaRegistryLoader):
        self._loader = loader

    def build(self, canonical: str) -> PageInfo:
        """Build PageInfo for a canonical page name."""
        kind = self._extract_kind(canonical)

        # v5.0.0: special-page classification via config table (was three
        # nearly-identical `if canonical == "...":` branches in v4)
        special_config = _SPECIAL_PAGE_CONFIG.get(canonical)
        if special_config is not None:
            return PageInfo(
                canonical=canonical,
                description=_DESCRIPTIONS.get(canonical, ""),
                kind=kind or special_config["kind"],
                is_data_page=special_config["is_data_page"],
                is_output_page=special_config["is_output_page"],
                is_special_page=special_config["is_special_page"],
                eligible_for_top10=False,
            )

        # Instrument / data pages
        is_data_page = kind in INSTRUMENT_TABLE_KINDS
        is_output_page = canonical in OUTPUT_PAGES
        is_special_page = canonical in SPECIAL_PAGES

        # Portfolio is a data page but not eligible for Top10 scoring
        if kind == PageKind.PORTFOLIO_TABLE.value:
            eligible_for_top10 = False
        else:
            eligible_for_top10 = bool(
                is_data_page and not is_output_page and not is_special_page
            )

        return PageInfo(
            canonical=canonical,
            description=_DESCRIPTIONS.get(canonical, ""),
            kind=kind,
            is_data_page=is_data_page,
            is_output_page=is_output_page,
            is_special_page=is_special_page,
            eligible_for_top10=eligible_for_top10,
        )

    def _extract_kind(self, canonical: str) -> str:
        """Extract kind from the sheet spec (explicit, then heuristic)."""
        try:
            spec = self._loader.get_sheet_spec(canonical)
            kind = self._get_kind_from_spec(spec)
            if kind:
                return kind
        except Exception:
            pass
        return ""

    def _get_kind_from_spec(self, spec: Any) -> str:
        """Get kind from spec, with column-count heuristic fallback."""
        kind = self._get_attr(spec, "kind", "")
        if kind:
            return str(kind).strip()

        columns = self._get_attr(spec, "columns", None)
        if columns is None and isinstance(spec, Mapping):
            columns = spec.get("columns")
        col_count = len(self._as_list(columns))

        # v5.0.0: removed dead `>= 88` branch that was shadowed by `>= 20`.
        if col_count == 0:
            return ""
        if col_count == 9:
            return PageKind.DATA_DICTIONARY.value
        if col_count >= 20:
            return PageKind.INSTRUMENT_TABLE.value

        return ""

    @staticmethod
    def _get_attr(obj: Any, name: str, default: Any = None) -> Any:
        if obj is None:
            return default
        if isinstance(obj, Mapping):
            return obj.get(name, default)
        return getattr(obj, name, default)

    @staticmethod
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
        return [value]


# =============================================================================
# Alias Manager
# =============================================================================

class AliasManager:
    """Manages page-name aliases and forbidden tokens."""

    def __init__(self, normalizer: TokenNormalizer):
        self._normalizer = normalizer
        self._aliases: Dict[str, str] = {}
        self._forbidden_tokens: Set[str] = set()

    def add_alias(self, alias: str, canonical: str) -> None:
        """Add alias → canonical mapping; raises on collision with a different canonical."""
        token = self._normalizer.normalize(alias)
        if not token:
            return

        prior = self._aliases.get(token)
        if prior and prior != canonical:
            raise PageCatalogError(
                f"Alias collision for token '{token}': '{prior}' vs '{canonical}'. "
                f"Rename one of the conflicting aliases."
            )

        self._aliases[token] = canonical

    def add_forbidden(self, alias: str) -> None:
        """Add a forbidden alias token."""
        token = self._normalizer.normalize(alias)
        if token:
            self._forbidden_tokens.add(token)

    def resolve(self, token: str) -> Optional[str]:
        """Resolve a normalized token to a canonical page name."""
        return self._aliases.get(token)

    def is_forbidden(self, token: str) -> bool:
        """Check if token is forbidden."""
        return token in self._forbidden_tokens

    def get_all(self) -> Dict[str, str]:
        """Return a shallow copy of all alias mappings."""
        return dict(self._aliases)

    def build_default_aliases(self, canonical_pages: List[str]) -> None:
        """Populate default aliases for all known canonical pages."""
        for canonical in canonical_pages:
            self.add_alias(canonical, canonical)
            self.add_alias(canonical.lower(), canonical)
            self.add_alias(canonical.replace("_", " "), canonical)
            self.add_alias(canonical.replace("_", "-"), canonical)
            self.add_alias(canonical.replace("_", "/"), canonical)
            self.add_alias(canonical.replace("_", ""), canonical)

        for alias in ("Market Leaders", "Leaders", "Leader", "Market Leaders Page",
                      "MarketLeader", "TASI Leaders", "Saudi Leaders"):
            self.add_alias(alias, "Market_Leaders")

        for alias in ("Global Markets", "Global Market", "Global", "GlobalMarkets",
                      "International Markets", "World Markets", "International"):
            self.add_alias(alias, "Global_Markets")

        for alias in ("Commodities FX", "Commodities & FX", "Commodities and FX", "Commodity FX",
                      "Commodities", "Commodity", "FX", "Forex", "CommoditiesFX",
                      "Metals", "Gold", "Oil"):
            self.add_alias(alias, "Commodities_FX")

        for alias in ("Mutual Funds", "Mutual Fund", "Funds", "Fund", "ETF", "ETFs",
                      "MutualFunds", "Index Funds", "Investment Funds"):
            self.add_alias(alias, "Mutual_Funds")

        for alias in ("My Portfolio", "Portfolio", "My Holdings", "Holdings",
                      "MyPortfolio", "My Investments", "Positions"):
            self.add_alias(alias, "My_Portfolio")

        for alias in ("Insights", "Insight", "Insights Analysis", "Insights-Analysis",
                      "Insights & Analysis", "InsightsAnalysis", "AI Insights", "Market Insights"):
            self.add_alias(alias, "Insights_Analysis")

        for alias in ("Top 10", "Top10", "Top Ten", "Top 10 Investments", "Top10 Investments",
                      "Top Ten Investments", "Top_10", "Top10_Investments", "Top_10_Investment",
                      "Top10Investment", "Top10Investments", "Best Picks", "Top Picks",
                      "Best Investments"):
            self.add_alias(alias, "Top_10_Investments")

        for alias in ("Data Dictionary", "Dictionary", "Data-Dictionary", "DataDict",
                      "Data Dic", "Data Dictionary Page", "Schema Reference", "Column Reference"):
            self.add_alias(alias, "Data_Dictionary")

        for alias in ("KSA_Tadawul", "Advisor_Criteria", "KSA Tadawul", "Advisor Criteria"):
            self.add_forbidden(alias)


# =============================================================================
# Page Catalog
# =============================================================================

class PageCatalog:
    """Central page catalog for all sheet/page operations."""

    def __init__(self) -> None:
        self._loader = SchemaRegistryLoader()
        self._normalizer = TokenNormalizer()
        self._alias_manager = AliasManager(self._normalizer)
        self._page_info: Dict[str, PageInfo] = {}
        self._canonical_pages: List[str] = []
        self._initialize()

    def _initialize(self) -> None:
        """Build canonical page list, PageInfo records, and aliases."""
        canonical_from_schema = set(self._loader.get_schema_registry().keys())

        ordered: List[str] = []
        for sheet in self._loader.get_canonical_sheets():
            if sheet in canonical_from_schema and sheet not in ordered:
                ordered.append(sheet)
        for sheet in self._loader.get_list_sheets():
            if sheet in canonical_from_schema and sheet not in ordered:
                ordered.append(sheet)
        for sheet in sorted(canonical_from_schema):
            if sheet not in ordered:
                ordered.append(sheet)

        self._canonical_pages = ordered

        builder = PageInfoBuilder(self._loader)
        for canonical in self._canonical_pages:
            self._page_info[canonical] = builder.build(canonical)

        self._alias_manager.build_default_aliases(self._canonical_pages)

        self._validate()

    def _validate(self) -> None:
        """Validate catalog consistency."""
        registry_keys = set(self._loader.get_schema_registry().keys())
        for forbidden in FORBIDDEN_PAGES:
            if forbidden in registry_keys:
                raise PageCatalogError(
                    f"Forbidden page '{forbidden}' exists in schema_registry. Remove it."
                )
        for canonical in self._canonical_pages:
            if canonical not in registry_keys:
                raise PageCatalogError(
                    f"Canonical page '{canonical}' is missing from schema_registry."
                )

    # -- Accessors -----------------------------------------------------

    def get_canonical_pages(self) -> List[str]:
        """Get all canonical pages in preferred order."""
        return list(self._canonical_pages)

    def get_input_pages(self) -> List[str]:
        """Get input pages (data pages that are not special)."""
        return [
            p for p in self._canonical_pages
            if p in self._page_info
            and self._page_info[p].is_data_page
            and not self._page_info[p].is_special_page
        ]

    def get_instrument_pages(self) -> Set[str]:
        """Get instrument pages (all INSTRUMENT_TABLE_KINDS)."""
        return {
            p for p, info in self._page_info.items()
            if info.is_data_page and not info.is_special_page
        }

    def get_top10_feed_pages(
        self,
        pages_override: Optional[Sequence[str]] = None,
    ) -> List[str]:
        """
        Get pages eligible as Top10 feed universes.

        Default: Market_Leaders, Global_Markets, Commodities_FX, Mutual_Funds
        (My_Portfolio excluded — portfolio positions are not a scored universe).
        """
        pages = list(pages_override) if pages_override is not None else list(TOP10_FEED_PAGES_DEFAULT)

        normalized: List[str] = []
        for page in pages:
            try:
                canonical = self.normalize_input_page_name(page)
                info = self._page_info.get(canonical)
                if (info
                        and info.eligible_for_top10
                        and info.is_data_page
                        and not info.is_special_page):
                    normalized.append(canonical)
            except Exception:
                pass

        # Deduplicate, preserve order
        seen: Set[str] = set()
        result: List[str] = []
        for p in normalized:
            if p not in seen:
                seen.add(p)
                result.append(p)
        return result

    def get_page_info(self, page: str) -> PageInfo:
        canonical = self.normalize_page_name(page, allow_output_pages=True, allow_special_pages=True)
        return self._page_info[canonical]

    def get_page_kind(self, page: str) -> str:
        try:
            canonical = self.normalize_page_name(page, allow_output_pages=True, allow_special_pages=True)
            return self._page_info[canonical].kind
        except Exception:
            return ""

    def get_route_family(self, page: str) -> str:
        canonical = self.normalize_page_name(page, allow_output_pages=True, allow_special_pages=True)
        if canonical == "Insights_Analysis":
            return RouteFamily.INSIGHTS.value
        if canonical == "Top_10_Investments":
            return RouteFamily.TOP10.value
        if canonical == "Data_Dictionary":
            return RouteFamily.DICTIONARY.value
        return RouteFamily.INSTRUMENT.value

    def get_page_aliases(self) -> Dict[str, str]:
        return self._alias_manager.get_all()

    # -- Predicates ----------------------------------------------------

    def is_canonical_page(self, page: str) -> bool:
        return page in self._page_info

    def is_forbidden_page(self, page: str) -> bool:
        raw = (page or "").strip()
        if not raw:
            return False
        if raw in FORBIDDEN_PAGES:
            return True
        token = self._normalizer.normalize(raw)
        return self._alias_manager.is_forbidden(token)

    def is_output_page(self, page: str) -> bool:
        try:
            canonical = self.normalize_page_name(page, allow_output_pages=True, allow_special_pages=True)
        except Exception:
            return False
        return canonical in OUTPUT_PAGES

    def is_special_page(self, page: str) -> bool:
        try:
            canonical = self.normalize_page_name(page, allow_output_pages=True, allow_special_pages=True)
        except Exception:
            return False
        return canonical in SPECIAL_PAGES

    def is_instrument_page(self, page: str) -> bool:
        try:
            canonical = self.normalize_page_name(page, allow_output_pages=True, allow_special_pages=True)
        except Exception:
            return False
        return canonical in self.get_instrument_pages()

    def is_input_page(self, page: str) -> bool:
        try:
            canonical = self.normalize_input_page_name(page)
        except Exception:
            return False
        return canonical in self.get_instrument_pages()

    # -- Normalizers ---------------------------------------------------

    def normalize_page_name(
        self,
        page: str,
        allow_output_pages: bool = True,
        allow_special_pages: bool = True,
    ) -> str:
        """
        Normalize a page name to canonical form.

        Raises:
            InvalidPageError: If page is empty, or output/special and not allowed.
            ForbiddenPageError: If page is forbidden.
            PageNotFoundError: If page cannot be resolved.
        """
        raw = (page or "").strip()
        if not raw:
            raise InvalidPageError("Page name is empty. Provide a valid page name.")

        if self.is_forbidden_page(raw):
            raise ForbiddenPageError(
                f"Page '{page}' is forbidden/removed. "
                f"Allowed pages: {', '.join(self._canonical_pages)}"
            )

        # v5.0.0: removed the dead fallback loop that re-scanned canonical
        # tokens -- build_default_aliases already registers every canonical
        # page as its own alias, so alias_manager.resolve() covers that case.
        token = self._normalizer.normalize(raw)
        canonical = self._alias_manager.resolve(token)

        if not canonical:
            raise PageNotFoundError(
                f"Unknown page '{page}'. Allowed pages: {', '.join(self._canonical_pages)}"
            )

        if canonical not in self._page_info:
            raise PageNotFoundError(
                f"Page '{page}' resolved to '{canonical}', but it is not registered. "
                f"Allowed pages: {', '.join(self._canonical_pages)}"
            )

        info = self._page_info[canonical]

        if not allow_output_pages and info.is_output_page:
            raise InvalidPageError(
                f"Page '{canonical}' is an output/meta sheet and not allowed for this operation."
            )

        if not allow_special_pages and info.is_special_page:
            raise InvalidPageError(
                f"Page '{canonical}' is a special/output page and not allowed for this operation."
            )

        return canonical

    def normalize_input_page_name(self, page: str) -> str:
        """Normalize a page name for input operations (rejects output/special)."""
        canonical = self.normalize_page_name(
            page, allow_output_pages=False, allow_special_pages=False,
        )
        info = self._page_info.get(canonical)
        if not info or not info.is_data_page:
            raise InvalidPageError(f"Page '{canonical}' is not a valid instrument/input page.")
        return canonical

    def resolve_page(self, page: str) -> str:
        """Alias for normalize_page_name(allow_output_pages=True, allow_special_pages=True)."""
        return self.normalize_page_name(page, allow_output_pages=True, allow_special_pages=True)

    def canonicalize_page(self, page: str) -> str:
        """Alias for normalize_page_name(allow_output_pages=True, allow_special_pages=True)."""
        return self.normalize_page_name(page, allow_output_pages=True, allow_special_pages=True)

    # -- Extraction ----------------------------------------------------

    def extract_page_candidate(self, value: Any) -> Optional[str]:
        """Walk nested dict/object structure extracting a page string."""
        if value is None:
            return None
        if isinstance(value, str):
            return value

        # v5.0.0: renamed loop variable from `field` to `param_name` to
        # avoid shadowing `dataclasses.field` (even though that import
        # was also removed, the pattern is bad form).
        for param_name in PAGE_PARAM_NAMES:
            if isinstance(value, Mapping) and param_name in value:
                candidate = value.get(param_name)
                if isinstance(candidate, str) and candidate.strip():
                    return candidate.strip()
            elif hasattr(value, param_name):
                candidate = getattr(value, param_name)
                if isinstance(candidate, str) and candidate.strip():
                    return candidate.strip()

        for container in ("payload", "params", "query", "body", "data", "result", "request"):
            nested = None
            if isinstance(value, Mapping):
                nested = value.get(container)
            elif hasattr(value, container):
                nested = getattr(value, container)
            if nested is not None and nested is not value:
                result = self.extract_page_candidate(nested)
                if result:
                    return result

        return None

    def resolve_page_candidate(
        self,
        value: Any,
        allow_output_pages: bool = True,
        allow_special_pages: bool = True,
        required: bool = False,
    ) -> Optional[str]:
        """Resolve page from a candidate value (None-tolerant unless required=True)."""
        candidate = self.extract_page_candidate(value)
        if not candidate:
            if required:
                raise InvalidPageError(
                    f"Could not resolve page candidate from fields: {', '.join(PAGE_PARAM_NAMES)}"
                )
            return None
        return self.normalize_page_name(
            candidate,
            allow_output_pages=allow_output_pages,
            allow_special_pages=allow_special_pages,
        )

    def normalize_page_names(
        self,
        pages: Sequence[str],
        allow_output_pages: bool = True,
        allow_special_pages: bool = True,
        dedupe: bool = True,
    ) -> List[str]:
        """Normalize multiple page names."""
        result: List[str] = []
        seen: Set[str] = set()
        for page in pages:
            canonical = self.normalize_page_name(
                page,
                allow_output_pages=allow_output_pages,
                allow_special_pages=allow_special_pages,
            )
            if dedupe:
                if canonical in seen:
                    continue
                seen.add(canonical)
            result.append(canonical)
        return result


# =============================================================================
# Singleton Instance (lazy)
# =============================================================================

_CATALOG_INSTANCE: Optional[PageCatalog] = None


def get_catalog() -> PageCatalog:
    """Get the singleton PageCatalog instance (lazy)."""
    global _CATALOG_INSTANCE
    if _CATALOG_INSTANCE is None:
        _CATALOG_INSTANCE = PageCatalog()
    return _CATALOG_INSTANCE


# =============================================================================
# Public API Functions
# =============================================================================

def page_info(page: str) -> PageInfo:
    return get_catalog().get_page_info(page)


def allowed_pages() -> List[str]:
    return get_catalog().get_canonical_pages()


def allowed_input_pages() -> List[str]:
    return get_catalog().get_input_pages()


def get_page_aliases() -> Dict[str, str]:
    return get_catalog().get_page_aliases()


def is_canonical_page(page: str) -> bool:
    return get_catalog().is_canonical_page(page)


def is_forbidden_page(page: str) -> bool:
    return get_catalog().is_forbidden_page(page)


def is_output_page(page: str) -> bool:
    return get_catalog().is_output_page(page)


def is_special_page(page: str) -> bool:
    return get_catalog().is_special_page(page)


def is_instrument_page(page: str) -> bool:
    return get_catalog().is_instrument_page(page)


def is_input_page(page: str) -> bool:
    return get_catalog().is_input_page(page)


def get_page_kind(page: str) -> str:
    return get_catalog().get_page_kind(page)


def normalize_page_name(
    page: str,
    allow_output_pages: bool = True,
    allow_special_pages: bool = True,
) -> str:
    return get_catalog().normalize_page_name(page, allow_output_pages, allow_special_pages)


def normalize_input_page_name(page: str) -> str:
    return get_catalog().normalize_input_page_name(page)


def resolve_page(page: str) -> str:
    return get_catalog().resolve_page(page)


def canonicalize_page(page: str) -> str:
    return get_catalog().canonicalize_page(page)


def extract_page_candidate(value: Any) -> Optional[str]:
    return get_catalog().extract_page_candidate(value)


def resolve_page_candidate(
    value: Any,
    allow_output_pages: bool = True,
    allow_special_pages: bool = True,
    required: bool = False,
) -> Optional[str]:
    return get_catalog().resolve_page_candidate(
        value, allow_output_pages, allow_special_pages, required,
    )


def normalize_page_names(
    pages: Sequence[str],
    allow_output_pages: bool = True,
    allow_special_pages: bool = True,
    dedupe: bool = True,
) -> List[str]:
    return get_catalog().normalize_page_names(
        pages, allow_output_pages, allow_special_pages, dedupe,
    )


def get_top10_feed_pages(pages_override: Optional[Sequence[str]] = None) -> List[str]:
    return get_catalog().get_top10_feed_pages(pages_override)


def get_route_family(page: str) -> str:
    return get_catalog().get_route_family(page)


def validate_page_catalog() -> None:
    """Force catalog initialization (validation happens in PageCatalog._initialize)."""
    get_catalog()


# =============================================================================
# PEP 562 Lazy Module Constants
# =============================================================================
# v5.0.0 fix: v4.0.0 computed these at module import time via
#   CANONICAL_PAGES = get_catalog().get_canonical_pages()
# which forced eager schema_registry import and crashed the whole module
# on schema-registry-unavailable. Now lazy: first access triggers a
# one-shot populator that writes the constants into module globals, so
# subsequent accesses go through the fast path (Python only calls
# __getattr__ when the attribute is missing from __dict__).

_LAZY_CONSTANT_NAMES: Set[str] = {
    "CANONICAL_PAGES",
    "SUPPORTED_PAGES",
    "PAGES",
    "INPUT_PAGES",
    "INSTRUMENT_PAGES",
    "ALIAS_TO_CANONICAL",
    "PAGE_ALIASES",
}


def _ensure_lazy_constants() -> None:
    """Populate the lazy module-level constants into globals() on first access."""
    g = globals()
    if "CANONICAL_PAGES" in g:
        return
    catalog = get_catalog()
    canonical = catalog.get_canonical_pages()
    aliases = catalog.get_page_aliases()
    g["CANONICAL_PAGES"] = canonical
    g["SUPPORTED_PAGES"] = canonical
    g["PAGES"] = canonical
    g["INPUT_PAGES"] = catalog.get_input_pages()
    g["INSTRUMENT_PAGES"] = catalog.get_instrument_pages()
    g["ALIAS_TO_CANONICAL"] = aliases
    g["PAGE_ALIASES"] = aliases


def __getattr__(name: str) -> Any:
    """PEP 562 lazy module attributes."""
    if name in _LAZY_CONSTANT_NAMES:
        _ensure_lazy_constants()
        return globals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> List[str]:
    """Include lazy constants in dir() output."""
    return sorted(set(globals().keys()) | _LAZY_CONSTANT_NAMES)


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    # Version
    "PAGE_CATALOG_VERSION",
    # Data models
    "PageInfo",
    # Static constants
    "FORBIDDEN_PAGES",
    "OUTPUT_PAGES",
    "SPECIAL_PAGES",
    "INSTRUMENT_TABLE_KINDS",
    "TOP10_FEED_PAGES_DEFAULT",
    "PAGE_PARAM_NAMES",
    # Lazy constants (PEP 562)
    "CANONICAL_PAGES",
    "SUPPORTED_PAGES",
    "PAGES",
    "INPUT_PAGES",
    "INSTRUMENT_PAGES",
    "ALIAS_TO_CANONICAL",
    "PAGE_ALIASES",
    # Public functions
    "page_info",
    "allowed_pages",
    "allowed_input_pages",
    "is_canonical_page",
    "is_forbidden_page",
    "is_output_page",
    "is_special_page",
    "is_instrument_page",
    "is_input_page",
    "get_page_kind",
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
    "get_catalog",
    # Enums (v5.0.0: added)
    "RouteFamily",
    "PageKind",
    # Classes (v5.0.0: added)
    "PageCatalog",
    "SchemaRegistryLoader",
    "TokenNormalizer",
    "AliasManager",
    "PageInfoBuilder",
    # Exceptions (v5.0.0: added)
    "PageCatalogError",
    "PageNotFoundError",
    "ForbiddenPageError",
    "InvalidPageError",
]
