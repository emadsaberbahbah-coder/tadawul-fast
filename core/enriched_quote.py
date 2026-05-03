#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/enriched_quote.py
===============================================================================
TFB Enriched Quote Core — v4.2.0 (VIEW-AWARE / SCHEMA-ALIGNED)
===============================================================================
CORE-ONLY • SCHEMA-FIRST • PAGE-CANONICAL • SPECIAL-PAGE SAFE • ENGINE-TOLERANT
JSON-SAFE • SYNC/ASYNC SAFE • ROUTE-FRIENDLY • LIGHTWEIGHT • IMPORT-SAFE

v4.2.0 changes (what moved from v4.1.0)
---------------------------------------
- FIX [HIGH]: Fallback instrument schema is now exactly 85 columns to match
  `core.sheets.schema_registry` v2.5.0. v4.1.0's fallback was 80 columns and
  was MISSING:
    * `upside_pct` (Valuation, added in registry v2.4.0)
    * `fundamental_view`, `technical_view`, `risk_view`, `value_view`
      (Views group, added in registry v2.3.0)
  When the schema registry could not be imported (minimal deploys, tests,
  tooling), `schema_projection()` would silently drop these five fields from
  every row, even though `core.scoring` v5.0.0 emits them and
  `core.reco_normalize` v7.0.0 needs them as inputs to the 5-tier rec
  cascade. With the registry healthy (the normal path) the views came
  through correctly because `page_schema()` reads from the registry; the
  fallback was the only break.
- FIX [HIGH]: Top_10_Investments fallback total bumped from 83 → 88 (85 + 3)
  to stay consistent with registry validation `validate_schema_registry`
  which requires Top10 to be exactly 88 columns.
- FIX [MED]: Fallback header list extended in lockstep — added "Upside %",
  "Fundamental View", "Technical View", "Risk View", "Value View".
  Previously the headers list was 80 entries while the keys list was 80;
  both are now 85.
- NEW: `VIEW_COLUMN_KEYS` constant exported, mirrors the registry export so
  callers can treat enriched_quote and schema_registry as a single source.
- NEW: `_strip_internal_fields()` helper (defence-in-depth). Engine
  v5.47.4 / advisor_engine v4.2.0 already strip internal coordination flags
  (`_skip_recommendation_synthesis`, `unit_normalization_warnings`,
  `intrinsic_value_source`, anything starting with `_internal_` / `_meta_`
  / `_debug_`) at source, but legacy / proxy / cached rows arriving here
  may still carry them. `schema_projection()` would naturally filter them
  too (since they aren't in the schema), but stripping early keeps the
  matrix path clean and the `row` field on single-row responses honest.
- ALIGN: header docstring now references registry v2.5.0 (not v2.2.0) and
  the v5/v7 view-aware family in general.

Public API is preserved verbatim from v4.1.0:
`build_enriched_quote_payload`, `build_enriched_sheet_rows_payload`,
`build_enriched_quote_payload_sync`, `get_enriched_quote_payload`,
`enriched_quote`, `quote`, `get_router`, and `router` are all still
exported under the same names.

v4.1.0 changes (PRESERVED)
--------------------------
- FIX: added missing `dataclass` import (v4.0.0 referenced @dataclass at class
  body time without importing it — module failed to load).
- FIX: renamed the `request_id()` helper to `_resolve_request_id()` to avoid
  being shadowed by the `request_id: Optional[str]` keyword argument inside
  `build_enriched_quote_payload()`.
================================================================================
"""

from __future__ import annotations

import asyncio
import datetime as dt
import importlib
import inspect
import logging
import math
import os
import time
import uuid
from dataclasses import asdict, dataclass, is_dataclass
from decimal import Decimal
from enum import Enum
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

# =============================================================================
# Logging Setup
# =============================================================================

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# =============================================================================
# Version
# =============================================================================

MODULE_VERSION = "4.2.0"

# =============================================================================
# Constants
# =============================================================================

# Page groups (aligned with core.sheets.page_catalog)
INSTRUMENT_PAGES: set = {
    "Market_Leaders",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
    "My_Portfolio",
}

SPECIAL_PAGES: set = {
    "Top_10_Investments",
    "Insights_Analysis",
    "Data_Dictionary",
}

OUTPUT_PAGES: set = {"Top_10_Investments", "Data_Dictionary"}

DEFAULT_PAGE = "Market_Leaders"
DEFAULT_LIMIT = 50
DEFAULT_TIMEOUT_SEC = 25.0
SPARSE_ROW_THRESHOLD = 8

# Canonical view column keys (mirrors core.sheets.schema_registry.VIEW_COLUMN_KEYS).
# Exposed for downstream code that needs to project/select view fields without
# depending on a hardcoded list.
VIEW_COLUMN_KEYS: Tuple[str, ...] = (
    "fundamental_view",
    "technical_view",
    "risk_view",
    "value_view",
)

# Page aliases (fallback when page_catalog unavailable)
_PAGE_ALIASES: Dict[str, str] = {
    "market_leaders": "Market_Leaders",
    "market-leaders": "Market_Leaders",
    "global_markets": "Global_Markets",
    "global-markets": "Global_Markets",
    "commodities_fx": "Commodities_FX",
    "commodities-fx": "Commodities_FX",
    "commodities_and_fx": "Commodities_FX",
    "mutual_funds": "Mutual_Funds",
    "mutual-funds": "Mutual_Funds",
    "my_portfolio": "My_Portfolio",
    "my-portfolio": "My_Portfolio",
    "my_investments": "My_Portfolio",
    "my-investments": "My_Portfolio",
    "top_10_investments": "Top_10_Investments",
    "top-10-investments": "Top_10_Investments",
    "top10": "Top_10_Investments",
    "insights_analysis": "Insights_Analysis",
    "insights-analysis": "Insights_Analysis",
    "data_dictionary": "Data_Dictionary",
    "data-dictionary": "Data_Dictionary",
}

# Engine method names
_BATCH_QUOTE_METHODS = (
    "get_enriched_quotes_batch",
    "get_analysis_quotes_batch",
    "get_quotes_batch",
    "quotes_batch",
    "get_enriched_quotes",
    "get_quotes",
)

_SINGLE_QUOTE_METHODS = (
    "get_enriched_quote_dict",
    "get_quote_dict",
    "get_analysis_row_dict",
    "get_enriched_quote",
    "get_quote",
    "get_analysis_row",
)

_SHEET_ROW_METHODS = (
    "get_sheet_rows",
    "get_page_rows",
    "sheet_rows",
    "build_sheet_rows",
    "get_sheet",
    "build_rows_for_sheet",
)

_SNAPSHOT_METHODS = (
    "get_cached_sheet_snapshot",
    "get_sheet_snapshot",
    "get_cached_sheet_rows",
)

# Engine resolution
_ENGINE_ATTR_CANDIDATES = (
    "engine",
    "data_engine",
    "data_engine_v2",
    "tfb_engine",
)

_ENGINE_MODULE_CANDIDATES = (
    "core.data_engine_v2",
    "core.data_engine",
)

_ENGINE_FACTORY_NAMES = (
    "get_engine",
    "get_data_engine",
    "build_engine",
    "create_engine",
    "get_or_create_engine",
)

_ENGINE_SINGLETON_NAMES = (
    "engine",
    "data_engine",
    "ENGINE",
    "DATA_ENGINE",
)

# v4.2.0: internal coordination fields that should never leak into the public
# response. Engine v5.47.4 and advisor_engine v4.2.0 already strip these at
# source; this is defence-in-depth for legacy / proxy / cached rows.
_INTERNAL_FIELD_PREFIXES: Tuple[str, ...] = (
    "_skip_", "_internal_", "_meta_", "_debug_", "_trace_",
)
_INTERNAL_FIELDS_TO_STRIP_HARD: frozenset = frozenset({
    "_placeholder",
    "_skip_recommendation_synthesis",
    "unit_normalization_warnings",
    "intrinsic_value_source",
})


# =============================================================================
# Enums
# =============================================================================

class Mode(str, Enum):
    """Execution mode."""
    AUTO = "auto"
    LIVE_QUOTES = "live_quotes"
    LIVE_SHEET = "live_sheet"
    SNAPSHOT = "snapshot"


class RouteFamily(str, Enum):
    """Route family classification."""
    INSTRUMENT = "instrument"
    TOP10 = "top10"
    INSIGHTS = "insights"
    DICTIONARY = "dictionary"


# =============================================================================
# Custom Exceptions
# =============================================================================

class EnrichedQuoteError(Exception):
    """Base exception for enriched quote module."""
    pass


class EngineResolutionError(EnrichedQuoteError):
    """Raised when engine cannot be resolved."""
    pass


class PageNotFoundError(EnrichedQuoteError):
    """Raised when page is not found."""
    pass


# =============================================================================
# Configuration
# =============================================================================

@dataclass(frozen=True)
class EnrichedQuoteConfig:
    """Configuration for enriched quote module."""
    default_limit: int = DEFAULT_LIMIT
    default_timeout_sec: float = DEFAULT_TIMEOUT_SEC
    sparse_row_threshold: int = SPARSE_ROW_THRESHOLD
    max_limit: int = 5000
    min_limit: int = 1

    @classmethod
    def from_env(cls) -> "EnrichedQuoteConfig":
        """Load configuration from environment."""
        def _env_int(name: str, default: int) -> int:
            try:
                return int(os.getenv(name, str(default)))
            except Exception:
                return default

        def _env_float(name: str, default: float) -> float:
            try:
                return float(os.getenv(name, str(default)))
            except Exception:
                return default

        return cls(
            default_limit=_env_int("ENRICHED_QUOTE_DEFAULT_LIMIT", DEFAULT_LIMIT),
            default_timeout_sec=_env_float("ENRICHED_QUOTE_TIMEOUT_SEC", DEFAULT_TIMEOUT_SEC),
            sparse_row_threshold=_env_int("ENRICHED_QUOTE_SPARSE_THRESHOLD", SPARSE_ROW_THRESHOLD),
            max_limit=_env_int("ENRICHED_QUOTE_MAX_LIMIT", 5000),
            min_limit=_env_int("ENRICHED_QUOTE_MIN_LIMIT", 1),
        )


_CONFIG = EnrichedQuoteConfig.from_env()


# =============================================================================
# Pure Utility Functions
# =============================================================================

def _strip(value: Any) -> str:
    """Safely convert to stripped string."""
    if value is None:
        return ""
    try:
        s = str(value).strip()
        return "" if s.lower() in {"none", "null", "nil"} else s
    except Exception:
        return ""


def _truthy(value: Any, default: bool = False) -> bool:
    """Convert to boolean."""
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    s = _strip(value).lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _to_int(value: Any, default: int) -> int:
    """Convert to integer."""
    try:
        if value is None or value == "" or isinstance(value, bool):
            return int(default)
        return int(float(value))
    except Exception:
        return int(default)


def _clamp(value: int, min_val: int, max_val: int) -> int:
    """Clamp value between min and max."""
    return max(min_val, min(value, max_val))


def _json_safe(value: Any) -> Any:
    """Convert value to JSON-safe format."""
    if value is None:
        return None

    if isinstance(value, (bool, int, str)):
        return value

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value

    if isinstance(value, Decimal):
        try:
            f = float(value)
            if math.isnan(f) or math.isinf(f):
                return None
            return f
        except Exception:
            return str(value)

    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        try:
            return value.isoformat()
        except Exception:
            return str(value)

    if is_dataclass(value):
        try:
            return {str(k): _json_safe(v) for k, v in asdict(value).items()}
        except Exception:
            return str(value)

    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]

    try:
        if hasattr(value, "model_dump") and callable(value.model_dump):
            return _json_safe(value.model_dump(mode="python"))
    except Exception:
        pass

    try:
        if hasattr(value, "dict") and callable(value.dict):
            return _json_safe(value.dict())
    except Exception:
        pass

    try:
        if hasattr(value, "__dict__"):
            return _json_safe(vars(value))
    except Exception:
        pass

    try:
        return str(value)
    except Exception:
        return None


def _as_dict(value: Any) -> Dict[str, Any]:
    """Convert to dictionary."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, Mapping):
        try:
            return dict(value)
        except Exception:
            return {}
    if is_dataclass(value):
        try:
            dumped = asdict(value)
            return dumped if isinstance(dumped, dict) else {}
        except Exception:
            return {}
    try:
        if hasattr(value, "model_dump") and callable(value.model_dump):
            dumped = value.model_dump(mode="python")
            return dumped if isinstance(dumped, dict) else {}
    except Exception:
        pass
    try:
        if hasattr(value, "dict") and callable(value.dict):
            dumped = value.dict()
            return dumped if isinstance(dumped, dict) else {}
    except Exception:
        pass
    try:
        d = vars(value)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _as_list(value: Any) -> List[Any]:
    """Convert to list."""
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
    try:
        if isinstance(value, Iterable):
            return list(value)
    except Exception:
        pass
    return [value]


def _compact_dict(d: Mapping[str, Any]) -> Dict[str, Any]:
    """Remove None and empty values from dict."""
    result: Dict[str, Any] = {}
    for k, v in d.items():
        if v is None:
            continue
        if isinstance(v, str) and not _strip(v):
            continue
        result[str(k)] = v
    return result


def _merge_dicts(*parts: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """Merge multiple dictionaries."""
    result: Dict[str, Any] = {}
    for part in parts:
        if isinstance(part, Mapping):
            result.update(dict(part))
    return result


def _rows_to_matrix(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    """Convert rows to matrix."""
    key_list = [str(k) for k in keys if _strip(k)]
    if not key_list:
        return []
    return [[_json_safe(dict(row or {}).get(k)) for k in key_list] for row in rows]


def _title_header(key: str) -> str:
    """Convert key to title case header (used only when a registry header is unavailable)."""
    text = str(key or "").replace("_", " ").strip()
    if not text:
        return ""

    words = []
    for part in text.split():
        up = part.upper()
        if up in {"TTM", "ROI", "RSI14", "EPS", "EV", "P", "W", "D", "Y", "FX", "UTC"}:
            words.append(up)
        elif part.lower() in {"m", "d", "w", "y"}:
            words.append(part.upper())
        else:
            words.append(part.capitalize())
    return " ".join(words)


def _strip_internal_fields(row: Any) -> Any:
    """
    Remove engine internal coordination flags from a row dict (v4.2.0).

    Engine v5.47.4 / advisor_engine v4.2.0 already strip these at source;
    this is defence-in-depth for legacy / proxy / cached rows that may
    still carry them. `schema_projection()` would naturally filter them
    too (since they aren't in the schema list), but stripping early keeps
    the matrix path clean and the `row` field on single-row responses
    honest.
    """
    if not isinstance(row, dict):
        return row
    for k in list(row.keys()):
        ks = str(k)
        if ks in _INTERNAL_FIELDS_TO_STRIP_HARD:
            try:
                del row[k]
            except Exception:
                pass
            continue
        if any(ks.startswith(p) for p in _INTERNAL_FIELD_PREFIXES):
            try:
                del row[k]
            except Exception:
                pass
    return row


# =============================================================================
# Symbol Normalization
# =============================================================================

def normalize_symbol(symbol: Any) -> str:
    """Normalize a symbol to canonical format."""
    s = _strip(symbol).upper().replace(" ", "")
    if not s:
        return ""

    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1]

    if s.endswith(".SA"):
        s = s[:-3] + ".SR"

    if s.isdigit() and 3 <= len(s) <= 6:
        return f"{s}.SR"

    return s


def normalize_symbols(value: Any) -> List[str]:
    """Normalize multiple symbols."""
    if value is None:
        return []

    result: List[str] = []
    seen: set = set()

    def _add(v: Any) -> None:
        sym = normalize_symbol(v)
        if sym and sym not in seen:
            seen.add(sym)
            result.append(sym)

    if isinstance(value, str):
        raw = value.replace(";", ",").replace("\n", ",").replace("\t", ",")
        for part in raw.split(","):
            _add(part)
        return result

    if isinstance(value, (list, tuple, set)):
        for item in value:
            _add(item)
        return result

    _add(value)
    return result


def symbol_candidates(symbol: Any) -> List[str]:
    """Generate symbol variants for matching."""
    norm = normalize_symbol(symbol)
    base = _strip(symbol)

    items = [base, base.upper(), norm, norm.upper()]
    if norm.endswith(".SR"):
        code = norm[:-3]
        items.extend([code, code.upper(), f"TADAWUL:{code}"])

    result: List[str] = []
    seen: set = set()
    for item in items:
        s = _strip(item)
        if s and s not in seen:
            seen.add(s)
            result.append(s)
    return result


def extract_symbols(
    *sources: Mapping[str, Any],
    explicit_symbol: Any = None,
    explicit_symbols: Any = None,
) -> List[str]:
    """Extract symbols from various sources."""
    result: List[str] = []
    seen: set = set()

    def _add_many(vals: List[str]) -> None:
        for v in vals:
            if v and v not in seen:
                seen.add(v)
                result.append(v)

    _add_many(normalize_symbols(explicit_symbols))
    _add_many(normalize_symbols(explicit_symbol))

    for source in sources:
        if not isinstance(source, Mapping):
            continue
        for key in ("symbols", "tickers", "tickers_list", "symbol", "ticker", "code", "codes"):
            if key in source:
                _add_many(normalize_symbols(source.get(key)))

    return result


# =============================================================================
# Page Schema Resolution
# =============================================================================

# Lazy-loaded catalog functions
_catalog_normalize_page_name: Optional[Callable] = None
_catalog_route_family: Optional[Callable] = None
_catalog_is_instrument_page: Optional[Callable] = None

for _pcat_path in ("core.sheets.page_catalog", "core.page_catalog", "page_catalog"):
    try:
        mod = importlib.import_module(_pcat_path)
        if callable(getattr(mod, "normalize_page_name", None)):
            _catalog_normalize_page_name = getattr(mod, "normalize_page_name")
        if callable(getattr(mod, "get_route_family", None)):
            _catalog_route_family = getattr(mod, "get_route_family")
        if callable(getattr(mod, "is_instrument_page", None)):
            _catalog_is_instrument_page = getattr(mod, "is_instrument_page")
        if _catalog_normalize_page_name:
            break
    except ImportError:
        continue
    except Exception:  # pragma: no cover
        continue

# Fallback implementations
if _catalog_normalize_page_name is None:
    def _normalize_page_fallback(name: str, allow_output_pages: bool = True) -> str:
        return _strip(name) or ""

    _catalog_normalize_page_name = _normalize_page_fallback

if _catalog_route_family is None:
    def _route_family_fallback(name: str) -> str:
        if name == "Top_10_Investments":
            return "top10"
        if name == "Insights_Analysis":
            return "insights"
        if name == "Data_Dictionary":
            return "dictionary"
        return "instrument"

    _catalog_route_family = _route_family_fallback

if _catalog_is_instrument_page is None:
    def _is_instrument_page_fallback(name: str) -> bool:
        return _catalog_route_family(name) == "instrument"

    _catalog_is_instrument_page = _is_instrument_page_fallback

# Schema registry
_get_sheet_spec: Optional[Callable] = None
for _sreg_path in ("core.sheets.schema_registry", "core.schema_registry", "schema_registry"):
    try:
        mod = importlib.import_module(_sreg_path)
        fn = getattr(mod, "get_sheet_spec", None)
        if callable(fn):
            _get_sheet_spec = fn
            break
    except ImportError:
        continue
    except Exception:  # pragma: no cover
        continue

if _get_sheet_spec is None:
    def _get_sheet_spec_fallback(sheet_name: str) -> Any:
        raise KeyError(f"schema_registry unavailable for {sheet_name}")

    _get_sheet_spec = _get_sheet_spec_fallback


def normalize_page(raw: Any) -> str:
    """Normalize page name to canonical form."""
    text = _strip(raw)
    if not text:
        return DEFAULT_PAGE

    try:
        page = _catalog_normalize_page_name(text, allow_output_pages=True)
        page = _strip(page)
        if page:
            return page
    except TypeError:
        try:
            page = _catalog_normalize_page_name(text)
            page = _strip(page)
            if page:
                return page
        except Exception:
            pass
    except Exception:
        pass

    key = text.lower().replace(" ", "_")
    return _PAGE_ALIASES.get(key, text)


def route_family(page: str) -> str:
    """Get route family for page."""
    try:
        fam = _strip(_catalog_route_family(page))
        if fam:
            return fam
    except Exception:
        pass
    if page == "Top_10_Investments":
        return "top10"
    if page == "Insights_Analysis":
        return "insights"
    if page == "Data_Dictionary":
        return "dictionary"
    return "instrument"


def is_instrument_page(page: str) -> bool:
    """Check if page is an instrument page."""
    try:
        return bool(_catalog_is_instrument_page(page))
    except Exception:
        return page in INSTRUMENT_PAGES


def page_schema(page: str) -> Tuple[List[str], List[str], List[str], List[Dict[str, Any]]]:
    """Get page schema headers, keys, display_headers, and columns."""
    headers: List[str] = []
    keys: List[str] = []
    display_headers: List[str] = []
    columns: List[Dict[str, Any]] = []

    try:
        spec = _get_sheet_spec(page)
    except Exception:
        spec = None

    if spec is None:
        return _canonical_contract_fallback(page)

    cols = getattr(spec, "columns", None)
    if cols is None and isinstance(spec, Mapping):
        cols = spec.get("columns") or []

    for col in _as_list(cols):
        cd = col if isinstance(col, Mapping) else _as_dict(col)
        key = _strip(cd.get("key") or cd.get("field") or cd.get("name"))
        header = _strip(cd.get("header") or cd.get("label") or key)
        if not key:
            continue
        keys.append(key)
        headers.append(header or key)
        display_headers.append(header or key)
        columns.append({
            "group": _strip(cd.get("group")) or None,
            "header": header or key,
            "key": key,
            "dtype": _strip(cd.get("dtype")) or None,
            "fmt": _strip(cd.get("fmt")) or None,
            "required": bool(cd.get("required")),
            "source": _strip(cd.get("source")) or None,
            "notes": _strip(cd.get("notes")) or None,
        })

    if not keys:
        return _canonical_contract_fallback(page)

    return headers, keys, display_headers or headers, columns


def _canonical_contract_fallback(
    page: str,
) -> Tuple[List[str], List[str], List[str], List[Dict[str, Any]]]:
    """Fallback contract when schema registry unavailable."""
    # 1) Try data_engine_v2 contracts first (same source-of-truth fallback)
    for module_name in ("core.data_engine_v2", "core.data_engine"):
        try:
            mod = importlib.import_module(module_name)
        except ImportError:
            continue
        except Exception:  # pragma: no cover
            continue

        contracts = getattr(mod, "STATIC_CANONICAL_SHEET_CONTRACTS", None)
        if isinstance(contracts, Mapping) and page in contracts:
            entry = contracts.get(page) or {}
            headers = [str(x) for x in _as_list(entry.get("headers")) if _strip(x)]
            keys = [str(x) for x in _as_list(entry.get("keys")) if _strip(x)]
            if keys:
                display = headers or [_title_header(k) or k for k in keys]
                columns = [
                    {
                        "group": None,
                        "header": display[idx] if idx < len(display) else (_title_header(key) or key),
                        "key": key,
                        "dtype": None,
                        "fmt": None,
                        "required": False,
                        "source": "static_canonical_contract",
                        "notes": None,
                    }
                    for idx, key in enumerate(keys)
                ]
                return headers or display, keys, display, columns

        if page in INSTRUMENT_PAGES:
            headers = [str(x) for x in _as_list(getattr(mod, "INSTRUMENT_CANONICAL_HEADERS", [])) if _strip(x)]
            keys = [str(x) for x in _as_list(getattr(mod, "INSTRUMENT_CANONICAL_KEYS", [])) if _strip(x)]
            if keys:
                display = headers or [_title_header(k) or k for k in keys]
                columns = [
                    {
                        "group": None,
                        "header": display[idx] if idx < len(display) else (_title_header(key) or key),
                        "key": key,
                        "dtype": None,
                        "fmt": None,
                        "required": False,
                        "source": "instrument_canonical_contract",
                        "notes": None,
                    }
                    for idx, key in enumerate(keys)
                ]
                return headers or display, keys, display, columns

    # 2) Hardcoded fallbacks matching the registry exactly
    if page == "Top_10_Investments":
        keys = list(_FALLBACK_INSTRUMENT_KEYS) + list(_FALLBACK_TOP10_EXTRA_KEYS)
        headers = list(_FALLBACK_INSTRUMENT_HEADERS) + list(_FALLBACK_TOP10_EXTRA_HEADERS)
        return _contract_from_paired(keys, headers, source="static_canonical_top10_contract")

    if page == "Insights_Analysis":
        return _contract_from_paired(
            list(_FALLBACK_INSIGHTS_KEYS),
            list(_FALLBACK_INSIGHTS_HEADERS),
            source="static_canonical_insights_contract",
        )

    if page == "Data_Dictionary":
        return _contract_from_paired(
            list(_FALLBACK_DATA_DICTIONARY_KEYS),
            list(_FALLBACK_DATA_DICTIONARY_HEADERS),
            source="static_canonical_data_dictionary_contract",
        )

    if page in INSTRUMENT_PAGES:
        return _contract_from_paired(
            list(_FALLBACK_INSTRUMENT_KEYS),
            list(_FALLBACK_INSTRUMENT_HEADERS),
            source="static_canonical_instrument_contract",
        )

    # 3) Last resort: minimal status/error contract
    return _contract_from_keys(["status", "error"])


def _contract_from_paired(
    keys: Sequence[str],
    headers: Sequence[str],
    source: Optional[str] = None,
) -> Tuple[List[str], List[str], List[str], List[Dict[str, Any]]]:
    """Create contract from paired keys + headers lists (preferred when headers are known)."""
    keys_list = [str(k) for k in keys if _strip(k)]
    headers_list = [str(h) for h in headers if _strip(h)]

    # Pad headers if paired list is shorter than keys
    if len(headers_list) < len(keys_list):
        for key in keys_list[len(headers_list):]:
            headers_list.append(_title_header(key) or key)

    columns = [
        {
            "group": None,
            "header": headers_list[idx] if idx < len(headers_list) else (_title_header(key) or key),
            "key": key,
            "dtype": None,
            "fmt": None,
            "required": False,
            "source": source,
            "notes": None,
        }
        for idx, key in enumerate(keys_list)
    ]
    return headers_list, keys_list, list(headers_list), columns


def _contract_from_keys(
    keys: Sequence[str],
) -> Tuple[List[str], List[str], List[str], List[Dict[str, Any]]]:
    """Create contract from keys list (headers derived via _title_header)."""
    keys_list = [str(k) for k in keys if _strip(k)]
    headers = [_title_header(k) or k for k in keys_list]
    columns = [
        {
            "group": None,
            "header": headers[idx] if idx < len(headers) else (_title_header(key) or key),
            "key": key,
            "dtype": None,
            "fmt": None,
            "required": False,
            "source": None,
            "notes": None,
        }
        for idx, key in enumerate(keys_list)
    ]
    return headers, keys_list, list(headers), columns


# =============================================================================
# Fallback schema constants — aligned with core.sheets.schema_registry v2.5.0
# =============================================================================
#
# Do NOT add non-canonical fields here. The registry is the single source of
# truth; these lists exist only to keep the module functional when the
# registry can't be imported (e.g. minimal deploys, tests, tooling). Any drift
# between these constants and the registry is a bug.
#
# v4.2.0: extended from 80 → 85 columns to match registry v2.5.0
# (added upside_pct + 4 view columns).

# Canonical 85 instrument columns — matches
# core.sheets.schema_registry._canonical_instrument_columns() verbatim.
_FALLBACK_INSTRUMENT_KEYS: List[str] = [
    # Identity (8)            -> 8
    "symbol", "name", "asset_class", "exchange", "currency", "country", "sector", "industry",
    # Price (10)              -> 18
    "current_price", "previous_close", "open_price", "day_high", "day_low",
    "week_52_high", "week_52_low", "price_change", "percent_change", "week_52_position_pct",
    # Liquidity (6)           -> 24
    "volume", "avg_volume_10d", "avg_volume_30d", "market_cap", "float_shares", "beta_5y",
    # Fundamentals (12)       -> 36
    "pe_ttm", "pe_forward", "eps_ttm", "dividend_yield", "payout_ratio", "revenue_ttm",
    "revenue_growth_yoy", "gross_margin", "operating_margin", "profit_margin",
    "debt_to_equity", "free_cash_flow_ttm",
    # Risk (8)                -> 44
    "rsi_14", "volatility_30d", "volatility_90d", "max_drawdown_1y",
    "var_95_1d", "sharpe_1y", "risk_score", "risk_bucket",
    # Valuation (7)           -> 51   (v4.2.0: upside_pct added in registry v2.4.0)
    "pb_ratio", "ps_ratio", "ev_ebitda", "peg_ratio", "intrinsic_value",
    "upside_pct", "valuation_score",
    # Forecast (9)            -> 60
    "forecast_price_1m", "forecast_price_3m", "forecast_price_12m",
    "expected_roi_1m", "expected_roi_3m", "expected_roi_12m",
    "forecast_confidence", "confidence_score", "confidence_bucket",
    # Scores (7)              -> 67
    "value_score", "quality_score", "momentum_score", "growth_score",
    "overall_score", "opportunity_score", "rank_overall",
    # Views (4)               -> 71   (v4.2.0: added in registry v2.3.0)
    "fundamental_view", "technical_view", "risk_view", "value_view",
    # Recommendation (4)      -> 75
    "recommendation", "recommendation_reason", "horizon_days", "invest_period_label",
    # Portfolio (6)           -> 81
    "position_qty", "avg_cost", "position_cost", "position_value",
    "unrealized_pl", "unrealized_pl_pct",
    # Provenance (4)          -> 85
    "data_provider", "last_updated_utc", "last_updated_riyadh", "warnings",
]

_FALLBACK_INSTRUMENT_HEADERS: List[str] = [
    # Identity
    "Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country", "Sector", "Industry",
    # Price
    "Current Price", "Previous Close", "Open", "Day High", "Day Low",
    "52W High", "52W Low", "Price Change", "Percent Change", "52W Position %",
    # Liquidity
    "Volume", "Avg Volume 10D", "Avg Volume 30D", "Market Cap", "Float Shares", "Beta (5Y)",
    # Fundamentals
    "P/E (TTM)", "P/E (Forward)", "EPS (TTM)", "Dividend Yield", "Payout Ratio",
    "Revenue (TTM)", "Revenue Growth YoY", "Gross Margin", "Operating Margin",
    "Profit Margin", "Debt/Equity", "Free Cash Flow (TTM)",
    # Risk
    "RSI (14)", "Volatility 30D", "Volatility 90D", "Max Drawdown 1Y",
    "VaR 95% (1D)", "Sharpe (1Y)", "Risk Score", "Risk Bucket",
    # Valuation (v4.2.0: + Upside %)
    "P/B", "P/S", "EV/EBITDA", "PEG", "Intrinsic Value", "Upside %", "Valuation Score",
    # Forecast
    "Forecast Price 1M", "Forecast Price 3M", "Forecast Price 12M",
    "Expected ROI 1M", "Expected ROI 3M", "Expected ROI 12M",
    "Forecast Confidence", "Confidence Score", "Confidence Bucket",
    # Scores
    "Value Score", "Quality Score", "Momentum Score", "Growth Score",
    "Overall Score", "Opportunity Score", "Rank (Overall)",
    # Views (v4.2.0: 4 new headers)
    "Fundamental View", "Technical View", "Risk View", "Value View",
    # Recommendation
    "Recommendation", "Recommendation Reason", "Horizon Days", "Invest Period Label",
    # Portfolio
    "Position Qty", "Avg Cost", "Position Cost", "Position Value",
    "Unrealized P/L", "Unrealized P/L %",
    # Provenance
    "Data Provider", "Last Updated (UTC)", "Last Updated (Riyadh)", "Warnings",
]

# Top_10_Investments extras (exactly 3 — registry appends these to the 85 canonical → 88)
_FALLBACK_TOP10_EXTRA_KEYS: List[str] = [
    "top10_rank",
    "selection_reason",
    "criteria_snapshot",
]
_FALLBACK_TOP10_EXTRA_HEADERS: List[str] = [
    "Top10 Rank",
    "Selection Reason",
    "Criteria Snapshot",
]

# Insights_Analysis (exactly 7)
_FALLBACK_INSIGHTS_KEYS: List[str] = [
    "section", "item", "symbol", "metric", "value", "notes", "last_updated_riyadh",
]
_FALLBACK_INSIGHTS_HEADERS: List[str] = [
    "Section", "Item", "Symbol", "Metric", "Value", "Notes", "Last Updated (Riyadh)",
]

# Data_Dictionary (9)
_FALLBACK_DATA_DICTIONARY_KEYS: List[str] = [
    "sheet", "group", "header", "key", "dtype", "fmt", "required", "source", "notes",
]
_FALLBACK_DATA_DICTIONARY_HEADERS: List[str] = [
    "Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes",
]


# =============================================================================
# Row Extraction and Normalization
# =============================================================================

def row_from_any(value: Any) -> Optional[Dict[str, Any]]:
    """Extract row dict from any value."""
    if value is None:
        return None
    if isinstance(value, Mapping):
        return {str(k): v for k, v in value.items()}
    d = _as_dict(value)
    return d if d else None


def extract_rows_like(payload: Any) -> List[Dict[str, Any]]:
    """Extract rows from payload."""
    if payload is None:
        return []

    if isinstance(payload, list):
        rows = []
        for item in payload:
            rd = row_from_any(item)
            if rd:
                rows.append(rd)
        return rows

    if isinstance(payload, Mapping):
        # Check common container keys
        for key in ("row_objects", "records", "items", "rows", "data", "results", "quotes"):
            value = payload.get(key)
            if isinstance(value, list):
                rows = []
                for item in value:
                    rd = row_from_any(item)
                    if rd:
                        rows.append(rd)
                if rows:
                    return rows

        # Single row
        for key in ("row", "quote"):
            value = payload.get(key)
            rd = row_from_any(value)
            if rd:
                return [rd]

        # Payload itself looks like a row
        if _looks_like_row(payload):
            rd = row_from_any(payload)
            return [rd] if rd else []

        # Recursively check nested
        for key in ("payload", "result"):
            nested = payload.get(key)
            nested_rows = extract_rows_like(nested)
            if nested_rows:
                return nested_rows

    return []


def extract_matrix_like(payload: Any) -> Optional[List[List[Any]]]:
    """Extract matrix from payload."""
    if not isinstance(payload, Mapping):
        return None

    for key in ("rows_matrix", "matrix"):
        value = payload.get(key)
        if isinstance(value, list):
            return [list(r) if isinstance(r, (list, tuple)) else [r] for r in value]

    rows_value = payload.get("rows")
    if isinstance(rows_value, list) and rows_value and isinstance(rows_value[0], (list, tuple)):
        return [list(r) if isinstance(r, (list, tuple)) else [r] for r in rows_value]

    for key in ("payload", "result", "data"):
        nested = payload.get(key)
        if isinstance(nested, Mapping):
            mx = extract_matrix_like(nested)
            if mx is not None:
                return mx

    return None


def matrix_to_rows(matrix: Sequence[Sequence[Any]], keys: Sequence[str]) -> List[Dict[str, Any]]:
    """Convert matrix to rows."""
    if not matrix or not keys:
        return []

    key_list = [str(k) for k in keys]
    rows: List[Dict[str, Any]] = []
    for row in matrix:
        vals = list(row) if isinstance(row, (list, tuple)) else [row]
        item: Dict[str, Any] = {}
        for idx, key in enumerate(key_list):
            item[key] = vals[idx] if idx < len(vals) else None
        rows.append(item)
    return rows


def _looks_like_row(value: Any) -> bool:
    """Check if value looks like a row dict."""
    if not isinstance(value, Mapping):
        return False
    keys = {str(k).lower() for k in value.keys()}
    return bool(keys & {
        "symbol", "ticker", "name", "company_name", "current_price",
        "section", "metric", "header", "key"
    })


def row_richness(row: Optional[Dict[str, Any]]) -> int:
    """Calculate row richness score."""
    if not isinstance(row, dict):
        return 0

    important_keys = (
        "symbol", "ticker", "name", "company_name", "current_price",
        "exchange", "currency", "country", "sector", "industry",
        "market_cap", "volume", "overall_score", "opportunity_score",
        "recommendation", "recommendation_reason", "selection_reason",
        "metric", "value", "header", "key",
    )

    score = 0
    for key in important_keys:
        val = row.get(key)
        if val is None:
            continue
        if isinstance(val, str) and not _strip(val):
            continue
        score += 1
    return score


def is_sparse_row(row: Optional[Dict[str, Any]]) -> bool:
    """Check if row is sparse (low richness)."""
    return row_richness(row) < _CONFIG.sparse_row_threshold


def merge_payload_dicts(
    base: Optional[Dict[str, Any]],
    addon: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Merge two payload dicts."""
    result = dict(base or {})
    if not isinstance(addon, dict):
        return result
    for k, v in addon.items():
        if v is None:
            continue
        if isinstance(v, str) and not _strip(v):
            continue
        result[k] = v
    return result


def schema_projection(row: Mapping[str, Any], keys: Sequence[str]) -> Dict[str, Any]:
    """Project row to schema keys."""
    if not keys:
        return {str(k): _json_safe(v) for k, v in row.items()}
    row_dict = dict(row or {})
    return {key: _json_safe(row_dict.get(key)) for key in keys}


def normalize_rows(
    rows: Sequence[Mapping[str, Any]],
    keys: Sequence[str],
    page: str,
) -> List[Dict[str, Any]]:
    """
    Normalize rows to schema.

    v4.2.0: defensively strips engine internal coordination flags
    (`_skip_recommendation_synthesis`, `_internal_*`, `unit_normalization_warnings`,
    `intrinsic_value_source`, etc.) before schema projection. The view fields
    (`fundamental_view`/`technical_view`/`risk_view`/`value_view`) flow through
    schema projection automatically as long as `keys` includes them — which it
    does when schema_registry v2.5.0 is loaded, OR when the fallback list above
    is used.
    """
    result: List[Dict[str, Any]] = []

    for row in rows:
        rd = dict(row or {})

        # v4.2.0: strip internal coordination fields early
        _strip_internal_fields(rd)

        # Normalize symbol/ticker
        if "symbol" not in rd and "ticker" in rd:
            rd["symbol"] = rd.get("ticker")
        if "ticker" not in rd and "symbol" in rd:
            rd["ticker"] = rd.get("symbol")

        # Top_10_Investments-specific normalization (aligned with registry's 3 extras)
        if page == "Top_10_Investments":
            if rd.get("top10_rank") is None and rd.get("rank_overall") is not None:
                rd["top10_rank"] = rd.get("rank_overall")

            if rd.get("selection_reason") in {None, ""}:
                reco = _strip(rd.get("recommendation"))
                roi = rd.get("expected_roi_3m")
                conf = rd.get("forecast_confidence") or rd.get("confidence_score")
                overall = rd.get("overall_score")

                parts = [reco or "Candidate"]
                if roi is not None:
                    parts.append(f"ROI 3M={roi}")
                if conf is not None:
                    parts.append(f"Confidence={conf}")
                if overall is not None:
                    try:
                        parts.append(f"Overall={round(float(overall), 1)}")
                    except Exception:
                        pass

                if reco or roi is not None or conf is not None or overall is not None:
                    rd["selection_reason"] = " | ".join(parts)

            if rd.get("criteria_snapshot") in {None, ""}:
                rd["criteria_snapshot"] = None

        result.append(schema_projection(rd, keys))

    return result


def derive_headers_keys(
    page: str,
    rows: Sequence[Mapping[str, Any]],
    source_meta: Mapping[str, Any],
) -> Tuple[List[str], List[str], List[str], List[Dict[str, Any]]]:
    """Derive headers and keys from schema or rows."""
    headers, keys, display_headers, columns = page_schema(page)

    if keys:
        return headers, keys, display_headers or headers, columns

    # Try from meta
    meta_headers = source_meta.get("headers") if isinstance(source_meta, Mapping) else None
    meta_keys = source_meta.get("keys") if isinstance(source_meta, Mapping) else None
    meta_display = source_meta.get("display_headers") if isinstance(source_meta, Mapping) else None
    if isinstance(meta_headers, list) and isinstance(meta_keys, list) and meta_keys:
        return (
            [str(x) for x in meta_headers],
            [str(x) for x in meta_keys],
            [str(x) for x in (meta_display or meta_headers)],
            [],
        )

    # Derive from rows
    seen: set = set()
    derived_keys: List[str] = []
    for row in rows:
        for key in row.keys():
            ks = str(key)
            if ks not in seen:
                seen.add(ks)
                derived_keys.append(ks)

    if not derived_keys:
        return _canonical_contract_fallback(page)

    derived_headers = [_title_header(k) or k for k in derived_keys]
    return derived_headers, derived_keys, list(derived_headers), []


def safe_status(
    status: Any,
    rows: Sequence[Mapping[str, Any]],
    headers: Sequence[str],
    error: Optional[str],
    warnings: Sequence[str],
) -> str:
    """Determine safe status."""
    st = _strip(status).lower()
    if st in {"success", "ok"}:
        return "success"
    if st in {"error", "failed", "fail"}:
        return "error"
    if rows or headers:
        return "partial" if error or warnings else "success"
    return "error" if error else "partial"


# =============================================================================
# Request Helpers
# =============================================================================

def _resolve_request_id(request: Any, explicit: Optional[str] = None) -> str:
    """
    Resolve a request ID for response/log tracing.

    Renamed from v4.0.0 `request_id()` to avoid being shadowed by the
    `request_id: Optional[str]` keyword argument on
    `build_enriched_quote_payload()`.
    """
    if _strip(explicit):
        return _strip(explicit)

    try:
        if request is not None:
            headers = getattr(request, "headers", {}) or {}
            hdr = _strip(headers.get("X-Request-ID"))
            if hdr:
                return hdr
            state = getattr(request, "state", None)
            rid = _strip(getattr(state, "request_id", ""))
            if rid:
                return rid
    except Exception:
        pass

    try:
        return uuid.uuid4().hex[:12]
    except Exception:
        return "core-enriched"


def extract_request_query(request: Any) -> Dict[str, Any]:
    """Extract query params from request."""
    if request is None:
        return {}
    try:
        qp = getattr(request, "query_params", None)
        if qp is None:
            return {}
        data = dict(qp)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def extract_request_path_params(request: Any) -> Dict[str, Any]:
    """Extract path params from request."""
    if request is None:
        return {}
    try:
        pp = getattr(request, "path_params", None)
        if pp is None:
            return {}
        data = dict(pp)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def extract_settings_dict(settings: Any) -> Dict[str, Any]:
    """Extract settings as dictionary."""
    if settings is None:
        return {}
    if isinstance(settings, Mapping):
        return dict(settings)
    try:
        d = vars(settings)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


# =============================================================================
# Engine Resolution
# =============================================================================

def resolve_app_state_engine(request: Any) -> Any:
    """Resolve engine from app state."""
    try:
        app = getattr(request, "app", None)
        state = getattr(app, "state", None)
        if state is None:
            return None
        for name in _ENGINE_ATTR_CANDIDATES:
            obj = getattr(state, name, None)
            if obj is not None:
                return obj
    except Exception:
        return None
    return None


async def call_maybe_async(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Call function, handling both sync and async."""
    if inspect.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)
    result = await asyncio.to_thread(fn, *args, **kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


async def call_variants(
    fn: Callable[..., Any],
    variants: Sequence[Dict[str, Any]],
    timeout_sec: float,
) -> Any:
    """Try multiple call variants."""
    last_exc: Optional[Exception] = None

    for kwargs in variants:
        try:
            cleaned = _compact_dict(kwargs)
            if timeout_sec > 0:
                return await asyncio.wait_for(call_maybe_async(fn, **cleaned), timeout=timeout_sec)
            return await call_maybe_async(fn, **cleaned)
        except TypeError as e:
            last_exc = e
            continue
        except Exception as e:
            last_exc = e
            break

    if last_exc:
        raise last_exc
    return None


async def resolve_engine(engine: Any, request: Any = None) -> Any:
    """Resolve engine from various sources."""
    if engine is not None:
        return engine

    app_engine = resolve_app_state_engine(request)
    if app_engine is not None:
        return app_engine

    for module_name in _ENGINE_MODULE_CANDIDATES:
        try:
            mod = importlib.import_module(module_name)
        except ImportError:
            continue
        except Exception:  # pragma: no cover
            continue

        # Check singletons
        for name in _ENGINE_SINGLETON_NAMES:
            obj = getattr(mod, name, None)
            if obj is not None:
                return obj

        # Check factories
        for name in _ENGINE_FACTORY_NAMES:
            fn = getattr(mod, name, None)
            if not callable(fn):
                continue
            try:
                result = await call_variants(
                    fn,
                    [{"request": request}, {}],
                    timeout_sec=_CONFIG.default_timeout_sec,
                )
                if result is not None:
                    return result
            except Exception:
                continue

    return None


# =============================================================================
# Special Page Builders
# =============================================================================

def build_data_dictionary_from_schema() -> List[Dict[str, Any]]:
    """Build data dictionary from schema."""
    rows: List[Dict[str, Any]] = []
    page_names = list(INSTRUMENT_PAGES) + ["Top_10_Investments", "Insights_Analysis", "Data_Dictionary"]
    seen: set = set()

    for sheet_name in page_names:
        if sheet_name in seen:
            continue
        seen.add(sheet_name)

        headers, keys, _, columns = page_schema(str(sheet_name))
        if columns:
            for col in columns:
                rows.append({
                    "sheet": str(sheet_name),
                    "group": col.get("group"),
                    "header": col.get("header"),
                    "key": col.get("key"),
                    "dtype": col.get("dtype"),
                    "fmt": col.get("fmt"),
                    "required": col.get("required"),
                    "source": col.get("source"),
                    "notes": col.get("notes"),
                })
            continue

        for idx, key in enumerate(keys):
            header = headers[idx] if idx < len(headers) else (_title_header(key) or key)
            rows.append({
                "sheet": str(sheet_name),
                "group": None,
                "header": header,
                "key": key,
                "dtype": None,
                "fmt": None,
                "required": False,
                "source": "schema_fallback",
                "notes": None,
            })

    return rows


async def call_special_builder(
    page: str,
    engine: Any,
    request: Any,
    settings: Any,
    body: Mapping[str, Any],
    limit: int,
    mode: str,
    timeout_sec: float,
) -> Any:
    """Call special page builder."""
    merged_body = dict(body or {})
    criteria = merged_body.get("criteria") if isinstance(merged_body.get("criteria"), dict) else None

    def _variants() -> List[Dict[str, Any]]:
        base = {
            "engine": engine,
            "request": request,
            "settings": settings,
            "body": merged_body,
            "criteria": criteria,
            "page": page,
            "sheet": page,
            "mode": mode,
            "limit": limit,
        }
        return [
            {**base},
            {**base, "payload": merged_body},
            {
                "request": request,
                "settings": settings,
                "body": merged_body,
                "criteria": criteria,
                "limit": limit,
                "mode": mode,
            },
            {
                "engine": engine,
                "body": merged_body,
                "criteria": criteria,
                "limit": limit,
                "mode": mode,
            },
            {
                "body": merged_body,
                "criteria": criteria,
                "limit": limit,
                "mode": mode,
            },
            {
                "payload": merged_body,
                "limit": limit,
                "mode": mode,
            },
            {},
        ]

    candidates: List[Tuple[str, Sequence[str]]] = []

    if page == "Top_10_Investments":
        candidates = [
            ("core.analysis.top10_selector", (
                "build_top10_rows", "build_top10_output_rows", "build_top10_investments_rows",
                "select_top10", "select_top10_symbols",
            )),
            ("core.investment_advisor_engine", (
                "run_investment_advisor_engine", "run_advisor_engine",
            )),
            ("core.investment_advisor", (
                "run_investment_advisor", "run_advisor",
                "_run_investment_advisor_impl", "_run_advisor_impl",
            )),
        ]
    elif page == "Insights_Analysis":
        candidates = [
            ("core.analysis.insights_builder", (
                "build_insights_analysis_rows", "build_insights_rows",
                "build_insights_output_rows", "build_insights_analysis",
            )),
            ("core.investment_advisor_engine", (
                "run_investment_advisor_engine", "run_advisor_engine",
            )),
        ]
    elif page == "Data_Dictionary":
        candidates = [
            ("core.sheets.data_dictionary", (
                "build_data_dictionary_rows", "get_data_dictionary_rows",
                "generate_data_dictionary",
            )),
        ]

    for module_name, fn_names in candidates:
        try:
            mod = importlib.import_module(module_name)
        except ImportError:
            continue
        except Exception:  # pragma: no cover
            continue

        for fn_name in fn_names:
            fn = getattr(mod, fn_name, None)
            if not callable(fn):
                continue
            try:
                result = await call_variants(fn, _variants(), timeout_sec=timeout_sec)
                if result is not None:
                    return result
            except Exception as exc:
                logger.warning("Special builder %s.%s failed: %s", module_name, fn_name, exc)
                continue

    if page == "Data_Dictionary":
        dd_rows = build_data_dictionary_from_schema()
        if dd_rows:
            return {"status": "success", "rows": dd_rows, "meta": {"dispatch": "schema_fallback"}}

    return None


# =============================================================================
# Engine Method Calls
# =============================================================================

def method_variants(
    page: str,
    body: Mapping[str, Any],
    symbols: Sequence[str],
    limit: int,
    mode: str,
    request: Any,
    settings: Any,
    schema_only: bool,
    headers_only: bool,
    table_mode: bool,
) -> List[Dict[str, Any]]:
    """Generate method call variants."""
    symbol = symbols[0] if symbols else None

    common = {
        "page": page,
        "sheet": page,
        "sheet_name": page,
        "name": page,
        "tab": page,
        "body": dict(body),
        "request": request,
        "settings": settings,
        "limit": limit,
        "mode": mode,
        "schema_only": schema_only,
        "headers_only": headers_only,
        "table_mode": table_mode,
    }

    variants = [
        {**common, "symbols": list(symbols), "tickers": list(symbols), "symbol": symbol, "ticker": symbol},
        {**common, "symbols": list(symbols), "tickers": list(symbols)},
        {**common, "symbol": symbol, "ticker": symbol},
        {**common},
        {"page": page, "symbols": list(symbols), "limit": limit, "body": dict(body), "mode": mode},
        {"sheet": page, "symbols": list(symbols), "limit": limit, "body": dict(body), "mode": mode},
        {"page": page, "symbol": symbol, "limit": limit, "mode": mode},
        {"sheet": page, "symbol": symbol, "limit": limit, "mode": mode},
        {"body": dict(body), "limit": limit},
        {"symbol": symbol},
        {"symbols": list(symbols)},
        {},
    ]

    # Deduplicate
    result: List[Dict[str, Any]] = []
    seen: set = set()
    for item in variants:
        sig = repr(sorted(_compact_dict(item).items(), key=lambda kv: kv[0]))
        if sig not in seen:
            seen.add(sig)
            result.append(item)

    return result


async def engine_sheet_rows(
    engine: Any,
    page: str,
    body: Mapping[str, Any],
    symbols: Sequence[str],
    request: Any,
    settings: Any,
    mode: str,
    limit: int,
    schema_only: bool,
    headers_only: bool,
    table_mode: bool,
    timeout_sec: float,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], str, Optional[str]]:
    """Get sheet rows from engine."""
    if engine is None:
        return [], {}, "partial", "engine_unavailable"

    variants = method_variants(
        page=page,
        body=body,
        symbols=symbols,
        limit=limit,
        mode=mode,
        request=request,
        settings=settings,
        schema_only=schema_only,
        headers_only=headers_only,
        table_mode=table_mode,
    )

    for method_name in _SHEET_ROW_METHODS + _SNAPSHOT_METHODS:
        fn = getattr(engine, method_name, None)
        if not callable(fn):
            continue
        try:
            payload = await call_variants(fn, variants, timeout_sec=timeout_sec)
        except Exception:
            continue

        rows = extract_rows_like(payload)
        if not rows:
            matrix = extract_matrix_like(payload)
            if matrix is not None:
                _, keys, _, _ = page_schema(page)
                rows = matrix_to_rows(matrix, keys)

        meta = _as_dict(payload.get("meta") if isinstance(payload, Mapping) else None)
        meta.setdefault("dispatch", f"engine.{method_name}")
        status = _strip(payload.get("status") if isinstance(payload, Mapping) else "") or "success"
        error = _strip(payload.get("error") if isinstance(payload, Mapping) else None) or None

        if rows or isinstance(payload, Mapping):
            return rows, meta, status, error

    return [], {"dispatch": "engine.sheet_rows_missing"}, "partial", "no_sheet_rows_returned"


async def engine_quotes(
    engine: Any,
    page: str,
    symbols: Sequence[str],
    request: Any,
    settings: Any,
    body: Mapping[str, Any],
    mode: str,
    limit: int,
    timeout_sec: float,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], str, Optional[str]]:
    """Get quotes from engine."""
    if engine is None:
        return [], {"dispatch": "engine.missing"}, "partial", "engine_unavailable"

    if not symbols:
        return [], {"dispatch": "quotes.no_symbols"}, "success", None

    variants = method_variants(
        page=page,
        body=body,
        symbols=symbols,
        limit=limit,
        mode=mode,
        request=request,
        settings=settings,
        schema_only=False,
        headers_only=False,
        table_mode=False,
    )

    quote_map: Dict[str, Dict[str, Any]] = {}
    meta: Dict[str, Any] = {}
    status = "success"
    error: Optional[str] = None

    def _put(sym_key: Any, row: Dict[str, Any]) -> None:
        for cand in symbol_candidates(sym_key):
            if cand not in quote_map:
                quote_map[cand] = dict(row)
            else:
                quote_map[cand] = merge_payload_dicts(quote_map[cand], row)

    def _get(sym: str) -> Dict[str, Any]:
        for cand in symbol_candidates(sym):
            if cand in quote_map:
                return quote_map[cand]
        return {}

    # Batch methods first
    for method_name in _BATCH_QUOTE_METHODS:
        fn = getattr(engine, method_name, None)
        if not callable(fn):
            continue
        try:
            payload = await call_variants(fn, variants, timeout_sec=timeout_sec)
        except Exception:
            continue

        meta = merge_payload_dicts(meta, _as_dict(payload.get("meta") if isinstance(payload, Mapping) else None))
        meta.setdefault("dispatch", f"engine.{method_name}")
        status = _strip(payload.get("status") if isinstance(payload, Mapping) else "") or status
        error = _strip(payload.get("error") if isinstance(payload, Mapping) else None) or error

        if isinstance(payload, Mapping):
            rows = extract_rows_like(payload)
            if rows:
                for idx, row in enumerate(rows):
                    sym = _strip(row.get("symbol") or row.get("ticker"))
                    if not sym and idx < len(symbols):
                        sym = str(symbols[idx])
                    if sym:
                        _put(sym, row)
            else:
                for k, v in payload.items():
                    if isinstance(v, Mapping):
                        rd = dict(v)
                        if _looks_like_row(rd) or _strip(k):
                            _put(k, rd)
        elif isinstance(payload, list):
            for idx, item in enumerate(payload):
                row = row_from_any(item)
                if not row:
                    continue
                sym = _strip(row.get("symbol") or row.get("ticker"))
                if not sym and idx < len(symbols):
                    sym = str(symbols[idx])
                if sym:
                    _put(sym, row)

        # Check if all symbols have non-sparse rows
        if all(_get(sym) and not is_sparse_row(_get(sym)) for sym in symbols):
            break

    # Rehydrate sparse/missing rows with single methods
    sparse = [sym for sym in symbols if not _get(sym) or is_sparse_row(_get(sym))]
    if sparse:
        for sym in sparse:
            best_row = _get(sym)
            for method_name in _SINGLE_QUOTE_METHODS:
                fn = getattr(engine, method_name, None)
                if not callable(fn):
                    continue
                per_variants = [
                    {"symbol": sym, "page": page, "sheet": page, "mode": mode, "request": request, "settings": settings},
                    {"ticker": sym, "page": page, "sheet": page, "mode": mode},
                    {"symbol": sym},
                    {"ticker": sym},
                ]
                try:
                    payload = await call_variants(fn, per_variants, timeout_sec=timeout_sec)
                except Exception:
                    continue
                rows = extract_rows_like(payload)
                row = rows[0] if rows else (row_from_any(payload) if _looks_like_row(payload) else None)
                if row:
                    best_row = merge_payload_dicts(best_row, row)
                    _put(sym, best_row)
                    break

    # Build output rows
    result_rows: List[Dict[str, Any]] = []
    for sym in symbols:
        row = _get(sym)
        if not row:
            row = {"symbol": sym, "ticker": sym, "error": "missing_row"}
        elif "symbol" not in row and "ticker" in row:
            row["symbol"] = row.get("ticker")
        elif "ticker" not in row and "symbol" in row:
            row["ticker"] = row.get("symbol")
        result_rows.append(row)

    # Determine final status
    if any(_strip(r.get("error")) for r in result_rows):
        status = "error" if all(_strip(r.get("error")) for r in result_rows) else "partial"

    return result_rows, meta, status, error


# =============================================================================
# Payload Envelope
# =============================================================================

def envelope(
    page: str,
    route_family: str,
    headers: Sequence[str],
    keys: Sequence[str],
    display_headers: Sequence[str],
    columns: Sequence[Mapping[str, Any]],
    rows: Sequence[Mapping[str, Any]],
    include_matrix: bool,
    request_id: str,
    started_at: float,
    mode: str,
    status: str,
    error: Optional[str],
    warnings: Sequence[str],
    meta_extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Create response envelope."""
    rows_out = [dict(r) for r in rows]

    payload = {
        "status": status,
        "page": page,
        "sheet": page,
        "route_family": route_family,
        "headers": list(headers),
        "keys": list(keys),
        "display_headers": list(display_headers or headers),
        "sheet_headers": list(display_headers or headers),
        "column_headers": list(display_headers or headers),
        "columns": [_json_safe(c) for c in columns],
        "rows": rows_out,
        "rows_matrix": _rows_to_matrix(rows_out, keys) if include_matrix and keys else None,
        "data": rows_out,
        "items": rows_out,
        "quotes": rows_out,
        "count": len(rows_out),
        "error": error,
        "version": MODULE_VERSION,
        "request_id": request_id,
        "warnings": list(warnings),
        "meta": {
            "module": "core.enriched_quote",
            "module_version": MODULE_VERSION,
            "mode": mode,
            "count": len(rows_out),
            "duration_ms": round((time.perf_counter() - started_at) * 1000.0, 3) if started_at is not None else None,
            **(meta_extra or {}),
        },
    }

    if len(rows_out) == 1:
        payload["row"] = rows_out[0]
        payload["quote"] = rows_out[0]

    return _json_safe(payload)


# =============================================================================
# Main Builder
# =============================================================================

async def build_enriched_quote_payload(
    engine: Any = None,
    request: Any = None,
    settings: Any = None,
    body: Optional[Mapping[str, Any]] = None,
    page: Optional[str] = None,
    symbol: Optional[str] = None,
    symbols: Optional[Sequence[str]] = None,
    mode: Optional[str] = None,
    limit: Optional[int] = None,
    schema_only: Optional[bool] = None,
    headers_only: Optional[bool] = None,
    include_matrix: Optional[bool] = None,
    table_mode: Optional[bool] = None,
    request_id: Optional[str] = None,
    **extra: Any,
) -> Dict[str, Any]:
    """
    Build enriched quote payload.

    This is the main entry point for the enriched quote module.

    Args:
        engine: Optional engine instance
        request: Optional request object
        settings: Optional settings object
        body: Request body
        page: Page name
        symbol: Single symbol
        symbols: List of symbols
        mode: Execution mode (auto, live_quotes, live_sheet, snapshot)
        limit: Row limit
        schema_only: Return only schema
        headers_only: Return only headers
        include_matrix: Include rows matrix
        table_mode: Enable table mode
        request_id: Request ID for tracing
        **extra: Extra parameters

    Returns:
        Enriched quote payload
    """
    started_at = time.perf_counter()

    # Extract request data
    request_query = extract_request_query(request)
    request_path = extract_request_path_params(request)
    settings_dict = extract_settings_dict(settings)
    body_dict = dict(body or {})
    extra_dict = dict(extra or {})

    merged = _merge_dicts(request_query, request_path, settings_dict, body_dict, extra_dict)

    # Normalize page
    page_norm = normalize_page(
        page
        or merged.get("page")
        or merged.get("sheet")
        or merged.get("sheet_name")
        or merged.get("tab")
        or merged.get("name")
        or DEFAULT_PAGE
    )
    route_fam = route_family(page_norm)

    # Request ID (uses the renamed helper; v4.0.0 shadowed the helper with the
    # kw-argument, which is why the next line previously crashed)
    req_id = _resolve_request_id(request, request_id)

    # Parameters
    limit_raw = limit if isinstance(limit, int) and limit > 0 else _to_int(
        merged.get("limit"), _CONFIG.default_limit
    )
    limit_out = _clamp(limit_raw, _CONFIG.min_limit, _CONFIG.max_limit)

    schema_only_out = (
        bool(schema_only) if isinstance(schema_only, bool)
        else _truthy(merged.get("schema_only"), False)
    )
    headers_only_out = (
        bool(headers_only) if isinstance(headers_only, bool)
        else _truthy(merged.get("headers_only"), False)
    )
    include_matrix_out = (
        bool(include_matrix) if isinstance(include_matrix, bool)
        else _truthy(merged.get("include_matrix"), True)
    )
    table_mode_out = (
        bool(table_mode) if isinstance(table_mode, bool)
        else _truthy(merged.get("table_mode"), False)
    )

    # Extract symbols
    resolved_symbols = extract_symbols(
        body_dict, request_query, request_path, extra_dict,
        explicit_symbol=symbol, explicit_symbols=symbols,
    )

    # Determine mode
    raw_mode = mode or merged.get("mode")
    if raw_mode and _strip(raw_mode).lower() != "auto":
        mode_out = _strip(raw_mode).lower()
    elif page_norm in SPECIAL_PAGES:
        mode_out = "live_sheet"
    elif resolved_symbols:
        mode_out = "live_quotes"
    else:
        mode_out = "live_sheet"

    # Initialize
    warnings: List[str] = []
    rows: List[Dict[str, Any]] = []
    source_meta: Dict[str, Any] = {}
    status_out = "success"
    error_out: Optional[str] = None

    # Resolve engine
    engine_obj = await resolve_engine(engine, request)

    # Special pages
    if page_norm in SPECIAL_PAGES:
        if schema_only_out or headers_only_out:
            headers, keys, display_headers, columns = page_schema(page_norm)
            return envelope(
                page=page_norm,
                route_family=route_fam,
                headers=headers,
                keys=keys,
                display_headers=display_headers,
                columns=columns,
                rows=[],
                include_matrix=include_matrix_out,
                request_id=req_id,
                started_at=started_at,
                mode=mode_out,
                status="success",
                error=None,
                warnings=["schema_only_special_page"] if schema_only_out or headers_only_out else [],
                meta_extra={
                    "schema_only": True,
                    "headers_only": headers_only_out,
                    "engine_present": engine_obj is not None,
                },
            )

        try:
            special_payload = await call_special_builder(
                page=page_norm,
                engine=engine_obj,
                request=request,
                settings=settings,
                body=merged,
                limit=limit_out,
                mode=mode_out,
                timeout_sec=_CONFIG.default_timeout_sec,
            )
        except Exception as exc:
            special_payload = None
            warnings.append(f"special_builder_error:{type(exc).__name__}")

        rows = extract_rows_like(special_payload)
        source_meta = _as_dict(special_payload.get("meta") if isinstance(special_payload, Mapping) else None)
        status_out = _strip(special_payload.get("status") if isinstance(special_payload, Mapping) else "") or "success"
        error_out = _strip(special_payload.get("error") if isinstance(special_payload, Mapping) else None) or None

        if not rows:
            eng_rows, eng_meta, eng_status, eng_error = await engine_sheet_rows(
                engine=engine_obj,
                page=page_norm,
                body=merged,
                symbols=resolved_symbols,
                request=request,
                settings=settings,
                mode=mode_out,
                limit=limit_out,
                schema_only=False,
                headers_only=False,
                table_mode=True,
                timeout_sec=_CONFIG.default_timeout_sec,
            )
            rows = eng_rows
            source_meta = merge_payload_dicts(source_meta, eng_meta)
            status_out = eng_status or status_out
            error_out = eng_error or error_out
            if not rows:
                warnings.append("special_page_engine_fallback_empty")

    # Instrument quote mode
    elif resolved_symbols and is_instrument_page(page_norm):
        quote_rows, quote_meta, quote_status, quote_error = await engine_quotes(
            engine=engine_obj,
            page=page_norm,
            symbols=resolved_symbols[:limit_out],
            request=request,
            settings=settings,
            body=merged,
            mode=mode_out,
            limit=limit_out,
            timeout_sec=_CONFIG.default_timeout_sec,
        )
        rows = quote_rows
        source_meta = quote_meta
        status_out = quote_status
        error_out = quote_error

        if (not rows) or all(is_sparse_row(r) for r in rows):
            sheet_rows, sheet_meta, sheet_status, sheet_error = await engine_sheet_rows(
                engine=engine_obj,
                page=page_norm,
                body=merged,
                symbols=resolved_symbols,
                request=request,
                settings=settings,
                mode="live_sheet",
                limit=limit_out,
                schema_only=False,
                headers_only=False,
                table_mode=True,
                timeout_sec=_CONFIG.default_timeout_sec,
            )
            if sheet_rows:
                rows = [
                    merge_payload_dicts(rows[idx] if idx < len(rows) else {}, row)
                    for idx, row in enumerate(sheet_rows)
                ]
                source_meta = merge_payload_dicts(source_meta, sheet_meta)
                if sheet_status:
                    status_out = sheet_status
                error_out = sheet_error or error_out
                warnings.append("quotes_rehydrated_from_sheet_rows")

    # Table mode / page mode
    else:
        sheet_rows, sheet_meta, sheet_status, sheet_error = await engine_sheet_rows(
            engine=engine_obj,
            page=page_norm,
            body=merged,
            symbols=resolved_symbols,
            request=request,
            settings=settings,
            mode=mode_out,
            limit=limit_out,
            schema_only=schema_only_out,
            headers_only=headers_only_out,
            table_mode=table_mode_out or (not resolved_symbols),
            timeout_sec=_CONFIG.default_timeout_sec,
        )
        rows = sheet_rows
        source_meta = sheet_meta
        status_out = sheet_status
        error_out = sheet_error

    # Derive headers and keys
    headers, keys, display_headers, columns = derive_headers_keys(
        page=page_norm,
        rows=rows,
        source_meta=source_meta,
    )

    # Apply schema-only/headers-only
    if schema_only_out or headers_only_out:
        rows = []
    else:
        rows = normalize_rows(rows[:limit_out], keys, page_norm)

    # Warnings for empty rows
    if not rows and not (schema_only_out or headers_only_out):
        if error_out:
            warnings.append("no_rows_with_error")
        else:
            warnings.append("no_rows_returned")

    # Final status
    status_final = safe_status(status_out, rows, headers, error_out, warnings)

    return envelope(
        page=page_norm,
        route_family=route_fam,
        headers=headers,
        keys=keys,
        display_headers=display_headers,
        columns=columns,
        rows=rows,
        include_matrix=include_matrix_out,
        request_id=req_id,
        started_at=started_at,
        mode=mode_out,
        status=status_final,
        error=error_out,
        warnings=warnings,
        meta_extra={
            "special_page": page_norm in SPECIAL_PAGES,
            "instrument_page": is_instrument_page(page_norm),
            "requested_symbols": resolved_symbols,
            "schema_only": schema_only_out,
            "headers_only": headers_only_out,
            "table_mode": table_mode_out,
            "engine_present": engine_obj is not None,
            "engine_type": type(engine_obj).__name__ if engine_obj is not None else "none",
            "source_meta_keys": sorted(list(source_meta.keys())) if isinstance(source_meta, Mapping) else [],
        },
    )


# =============================================================================
# Compatibility Functions
# =============================================================================

async def build_enriched_sheet_rows_payload(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Alias for build_enriched_quote_payload."""
    return await build_enriched_quote_payload(*args, **kwargs)


async def get_enriched_quote_payload(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Alias for build_enriched_quote_payload."""
    return await build_enriched_quote_payload(*args, **kwargs)


async def enriched_quote(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Alias for build_enriched_quote_payload."""
    return await build_enriched_quote_payload(*args, **kwargs)


async def quote(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Alias for build_enriched_quote_payload."""
    return await build_enriched_quote_payload(*args, **kwargs)


def build_enriched_quote_payload_sync(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Synchronous version of build_enriched_quote_payload."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(build_enriched_quote_payload(*args, **kwargs))
    raise RuntimeError(
        "build_enriched_quote_payload_sync() cannot run inside an active event loop. "
        "Use `await build_enriched_quote_payload(...)` instead."
    )


# =============================================================================
# Router (Placeholder)
# =============================================================================

router = None


def get_router() -> None:
    """Get router (placeholder)."""
    return None


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    "MODULE_VERSION",
    "VIEW_COLUMN_KEYS",
    "build_enriched_quote_payload",
    "build_enriched_sheet_rows_payload",
    "build_enriched_quote_payload_sync",
    "get_enriched_quote_payload",
    "enriched_quote",
    "quote",
    "get_router",
    "router",
]
