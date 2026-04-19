#!/usr/bin/env python3
# core/sheets/data_dictionary.py
"""
================================================================================
Data Dictionary Generator -- v5.0.0
================================================================================
SCHEMA-DRIVEN • CONTRACT-EXPLICIT • SHEET-SPEC SAFE • DRIFT-PROOF • IMPORT-SAFE

Purpose
-------
Generate Data_Dictionary content ONLY from the canonical schema registry.

Critical design rule
--------------------
This module is ONLY for Data_Dictionary generation.
It must NEVER be treated as the builder for /v1/schema/sheet-spec output.

v5.0.0 Changes (from v4.0.0)
----------------------------
Bug fixes:
  - `SCHEMA_VERSION` was in __all__ but never defined at the module level.
    `from core.sheets.data_dictionary import *` raised AttributeError, and
    `from core.sheets.data_dictionary import SCHEMA_VERSION` likewise.
    v5.0.0 exposes it via PEP 562 `__getattr__`, resolving the value
    lazily from the schema registry on first access.
  - `SchemaRegistryLoader.__new__` cached `cls._instance` BEFORE calling
    `_load()`, so a failed import left a broken singleton that later
    `SchemaRegistryLoader()` calls would silently reuse. v5.0.0 caches
    only AFTER a successful load.
  - `RowBuilder.normalize` used `field` as a loop variable, shadowing
    the `from dataclasses import field` import. Harmless today (no
    `field()` calls in this module), but a landmine for future edits.
    Renamed to `dd_field`.

Dead code removed:
  - `DataDictionaryConfig` + `DataDictionaryConfig.from_env()` were
    defined but never instantiated or consulted anywhere.
  - Unused imports: `cast`, `Union` (never used), `field` from
    `dataclasses` (no `field()` call in the module).
  - Module globals `_contract`, `_row_builder`, `_orderer` were assigned
    inside `_get_payload_builder()` but never read at module level --
    now locals in the factory.

Cleanup:
  - `_build_alias_map` deduplicated: it was an identical copy on both
    `SchemaRegistryLoader` and `SheetOrderer`. Now a single module-level
    helper consumed by both.
  - `PageCatalogHelper.resolve_sheet` tries the `allow_output_pages`
    kwarg first, then falls back to positional-only (v4 let the
    TypeError from an incompatible signature get caught by `except
    Exception` and returned the input unchanged -- masking real errors).
  - `build_data_dictionary_values` now delegates to
    `PayloadBuilder.build_values_payload`, eliminating a copy-paste of
    the values-list construction.
  - Added `DataDictionaryError` + subclasses + `PayloadBuilder` /
    `RowBuilder` / `DataDictionaryContract` / `SheetOrderer` /
    `SchemaRegistryLoader` to __all__ so callers can type-check and
    except: them via a clean import.

Preserved for backward compatibility:
  - Public function surface (data_dictionary_headers, _keys, _contract,
    preferred_sheet_order, resolve_requested_sheets, row_dict_from_column,
    normalize_data_dictionary_row[s], build_data_dictionary_rows /
    _values / _payload, validate_*).
  - Payload shape (kind, sheet, page, sheet_name, format, headers,
    display_headers, keys, rows, row_objects, rows_matrix, count,
    version, schema_version, values).
  - Lazy singleton pattern for schema registry + payload builder.
  - Output format strings ("rows", "values").
  - Kind string ("data_dictionary").
================================================================================
"""

from __future__ import annotations

import importlib
import importlib.util
import logging
import os
import sys
from enum import Enum
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

# =============================================================================
# Logging
# =============================================================================

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# =============================================================================
# Constants
# =============================================================================

DATA_DICTIONARY_VERSION = "5.0.0"
DATA_DICTIONARY_SHEET_NAME = "Data_Dictionary"
DATA_DICTIONARY_OUTPUT_KIND = "data_dictionary"

# Schema registry v3.0.0 expected keys and headers
EXPECTED_KEYS = ["sheet", "group", "header", "key", "dtype", "fmt", "required", "source", "notes"]
EXPECTED_HEADERS = ["Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes"]

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}


# =============================================================================
# Custom Exceptions
# =============================================================================

class DataDictionaryError(Exception):
    """Base exception for Data Dictionary errors."""


class SchemaRegistryNotFoundError(DataDictionaryError):
    """Raised when schema registry cannot be imported."""


class SheetNotFoundError(DataDictionaryError):
    """Raised when a requested sheet is not found."""


class ContractMismatchError(DataDictionaryError):
    """Raised when Data Dictionary contract validation fails."""


# =============================================================================
# Enums
# =============================================================================

class OutputFormat(str, Enum):
    """Output format for data dictionary payload."""
    ROWS = "rows"
    VALUES = "values"


class DataDictionaryField(str, Enum):
    """Canonical field names for Data Dictionary columns."""
    SHEET = "sheet"
    GROUP = "group"
    HEADER = "header"
    KEY = "key"
    DTYPE = "dtype"
    FMT = "fmt"
    REQUIRED = "required"
    SOURCE = "source"
    NOTES = "notes"


_STRING_FIELDS = frozenset({
    DataDictionaryField.SHEET.value,
    DataDictionaryField.GROUP.value,
    DataDictionaryField.HEADER.value,
    DataDictionaryField.KEY.value,
    DataDictionaryField.DTYPE.value,
    DataDictionaryField.FMT.value,
    DataDictionaryField.SOURCE.value,
    DataDictionaryField.NOTES.value,
})


# =============================================================================
# Pure Utility Functions
# =============================================================================

def _to_string(value: Any) -> str:
    """Safely convert to non-empty stripped string; '' for None/error."""
    if value is None:
        return ""
    try:
        return str(value).strip()
    except Exception:
        return ""


def _to_bool(value: Any) -> bool:
    """Safely convert to boolean (truthy strings + numeric coercion)."""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        try:
            return bool(int(value))
        except Exception:
            return False
    if isinstance(value, str):
        return value.strip().lower() in _TRUTHY
    try:
        return bool(value)
    except Exception:
        return False


def _get_attr(obj: Any, *names: str, default: Any = None) -> Any:
    """Get attribute/key by multiple possible names."""
    if obj is None:
        return default
    for name in names:
        try:
            if isinstance(obj, Mapping) and name in obj:
                return obj.get(name, default)
        except Exception:
            pass
        try:
            if hasattr(obj, name):
                return getattr(obj, name)
        except Exception:
            pass
    return default


def _normalize_token(value: Any) -> str:
    """Normalize token for case/separator-insensitive matching."""
    return _to_string(value).lower().replace("-", "_").replace(" ", "_")


def _deduplicate_keep_order(items: Sequence[str]) -> List[str]:
    """Deduplicate items while preserving first-seen order."""
    seen: set = set()
    result: List[str] = []
    for item in items:
        s = _to_string(item)
        if not s or s in seen:
            continue
        seen.add(s)
        result.append(s)
    return result


def _as_list(value: Any) -> List[Any]:
    """Coerce value to list (str stays as single-element list)."""
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
# Schema Registry Loader (robust import with singleton semantics)
# =============================================================================

class SchemaRegistryLoader:
    """
    Robust loader for schema_registry module across different deployment
    layouts. Singleton; constructor triggers lazy load on first instantiation.
    """

    _instance: Optional["SchemaRegistryLoader"] = None
    _module: Optional[Any] = None

    _IMPORT_PATHS = [
        "core.sheets.schema_registry",
        "core.schema_registry",
        "schema_registry",
    ]

    def __new__(cls) -> "SchemaRegistryLoader":
        # v5.0.0 fix: cache `cls._instance` only AFTER successful _load().
        # v4.0.0 set it before, leaving broken singleton cached on import failure.
        if cls._instance is None:
            instance = super().__new__(cls)
            instance._load()  # may raise SchemaRegistryNotFoundError
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

        # Last resort: walk sys.path looking for schema_registry.py
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

        raise SchemaRegistryNotFoundError(
            f"schema_registry could not be imported. Tried: {'; '.join(errors)}"
        )

    @property
    def module(self) -> Any:
        """Return the loaded schema_registry module."""
        if self._module is None:
            self._load()
        return self._module

    def get_schema_version(self) -> str:
        """Get the schema registry's version string (default 'unknown')."""
        return _to_string(getattr(self.module, "SCHEMA_VERSION", "unknown"))

    def get_schema_registry_map(self) -> Dict[str, Any]:
        """Return the registry's sheet-spec mapping, tried by known attribute names."""
        for attr_name in ("SCHEMA_REGISTRY", "SHEET_SPECS", "REGISTRY", "SHEETS"):
            value = getattr(self.module, attr_name, None)
            if isinstance(value, Mapping):
                return dict(value)
        return {}

    def get_canonical_sheets(self) -> List[str]:
        """Return the registry's canonical sheet list (if defined)."""
        for attr_name in ("CANONICAL_SHEETS", "CANONICAL_PAGES"):
            value = getattr(self.module, attr_name, None)
            if value is not None:
                return [_to_string(x) for x in _as_list(value) if _to_string(x)]
        return []

    def get_sheet_spec(self, sheet_name: str) -> Any:
        """Get sheet spec, trying registry function first, then direct lookup, then aliases."""
        fn = getattr(self.module, "get_sheet_spec", None)
        if callable(fn):
            try:
                return fn(sheet_name)
            except Exception:
                pass

        registry = self.get_schema_registry_map()
        if sheet_name in registry:
            return registry[sheet_name]

        alias_map = build_alias_map(self)
        normalized = _normalize_token(sheet_name)
        canonical = alias_map.get(normalized)
        if canonical and canonical in registry:
            return registry[canonical]

        raise SheetNotFoundError(f"Unknown sheet: {sheet_name}")

    def list_sheets(self) -> List[str]:
        """List all sheet names (via registry function or direct map)."""
        fn = getattr(self.module, "list_sheets", None)
        if callable(fn):
            try:
                return [_to_string(x) for x in _as_list(fn()) if _to_string(x)]
            except Exception:
                pass

        registry = self.get_schema_registry_map()
        return [_to_string(x) for x in registry.keys() if _to_string(x)]


def build_alias_map(loader: SchemaRegistryLoader) -> Dict[str, str]:
    """
    Build {normalized_token -> canonical_sheet_name} alias map.

    v5.0.0: extracted from duplicate copies in SchemaRegistryLoader and
    SheetOrderer. Looks up optional `list_sheet_aliases(sheet)` from the
    registry module to pull in page-catalog-defined aliases.
    """
    registry = loader.get_schema_registry_map()
    alias_map: Dict[str, str] = {}
    list_aliases = getattr(loader.module, "list_sheet_aliases", None)

    for sheet_name in registry.keys():
        sheet = _to_string(sheet_name)
        if not sheet:
            continue

        candidates = {
            sheet,
            sheet.replace("_", " "),
            sheet.replace("_", "-"),
            _normalize_token(sheet),
        }

        if callable(list_aliases):
            try:
                for alias in _as_list(list_aliases(sheet) or []):
                    alias_str = _to_string(alias)
                    if alias_str:
                        candidates.add(alias_str)
                        candidates.add(_normalize_token(alias_str))
            except Exception:
                pass

        for candidate in candidates:
            token = _normalize_token(candidate)
            if token and token not in alias_map:
                alias_map[token] = sheet

    return alias_map


# =============================================================================
# Page Catalog Integration (optional)
# =============================================================================

class PageCatalogHelper:
    """Optional helper that resolves sheet names through page_catalog."""

    _has_catalog: Optional[bool] = None
    _normalize_fn: Optional[Any] = None

    @classmethod
    def _ensure_loaded(cls) -> None:
        """Load page_catalog once and cache the best-available normalizer."""
        if cls._has_catalog is not None:
            return

        try:
            from core.sheets import page_catalog  # type: ignore
            cls._normalize_fn = (
                getattr(page_catalog, "resolve_page_candidate", None)
                or getattr(page_catalog, "normalize_page_name", None)
            )
            cls._has_catalog = cls._normalize_fn is not None
        except ImportError:
            cls._has_catalog = False

    @classmethod
    def resolve_sheet(cls, name: str) -> str:
        """
        Resolve sheet name using the page catalog.

        v5.0.0: tries the kwarg form first, then the positional-only form.
        v4.0.0 blindly passed `allow_output_pages=True` which raises
        TypeError for `normalize_page_name`-style functions -- that
        TypeError was caught by a bare `except` and silently returned the
        original name, masking the API mismatch.
        """
        cls._ensure_loaded()
        fn = cls._normalize_fn
        if not cls._has_catalog or fn is None:
            return name

        # Try with kwarg (resolve_page_candidate supports this)
        try:
            result = fn(name, allow_output_pages=True)
            if result:
                return _to_string(result)
        except TypeError:
            # Fallback: function doesn't accept the kwarg
            pass
        except Exception:
            return name

        # Positional-only fallback
        try:
            result = fn(name)
            if result:
                return _to_string(result)
        except Exception:
            pass

        return name


# =============================================================================
# Data Dictionary Contract
# =============================================================================

class DataDictionaryContract:
    """Contract for the Data Dictionary sheet (headers + keys)."""

    def __init__(self, loader: SchemaRegistryLoader):
        self._loader = loader
        self._headers: Optional[List[str]] = None
        self._keys: Optional[List[str]] = None

    def get_headers(self) -> List[str]:
        """Get display headers (cached). Warns on mismatch vs EXPECTED_HEADERS."""
        if self._headers is not None:
            return self._headers

        spec = self._loader.get_sheet_spec(DATA_DICTIONARY_SHEET_NAME)
        cols = self._extract_columns(spec)

        headers = [_to_string(_get_attr(c, "header", default="")) for c in cols]
        self._headers = [h for h in headers if h]

        if self._headers and self._headers != EXPECTED_HEADERS:
            logger.warning(
                "Data_Dictionary headers mismatch. Expected %s, got %s",
                EXPECTED_HEADERS, self._headers,
            )

        return self._headers

    def get_keys(self) -> List[str]:
        """Get column keys (cached). Warns on mismatch vs EXPECTED_KEYS."""
        if self._keys is not None:
            return self._keys

        spec = self._loader.get_sheet_spec(DATA_DICTIONARY_SHEET_NAME)
        cols = self._extract_columns(spec)

        keys = [_to_string(_get_attr(c, "key", default="")) for c in cols]
        self._keys = [k for k in keys if k]

        if self._keys and self._keys != EXPECTED_KEYS:
            logger.warning(
                "Data_Dictionary keys mismatch. Expected %s, got %s",
                EXPECTED_KEYS, self._keys,
            )

        return self._keys

    def get_contract(self) -> Tuple[List[str], List[str]]:
        """Return (headers, keys) or raise ContractMismatchError if invalid."""
        headers = self.get_headers()
        keys = self.get_keys()

        if not headers or not keys or len(headers) != len(keys):
            raise ContractMismatchError(
                f"Data_Dictionary spec invalid: headers={len(headers)}, keys={len(keys)}"
            )

        return headers, keys

    @staticmethod
    def _extract_columns(spec: Any) -> List[Any]:
        """Extract columns list from a sheet spec (dict or object)."""
        cols = _get_attr(spec, "columns", default=None)
        if cols is None and isinstance(spec, Mapping):
            cols = spec.get("cols") or spec.get("headers")
        return _as_list(cols)


# =============================================================================
# Sheet Ordering
# =============================================================================

class SheetOrderer:
    """Manages canonical ordering + resolution of requested sheets."""

    def __init__(self, loader: SchemaRegistryLoader):
        self._loader = loader

    def get_preferred_order(self) -> List[str]:
        """
        Stable sheet ordering for dictionary generation.

        Priority:
          1. CANONICAL_SHEETS from registry
          2. CANONICAL_PAGES from registry (fallback)
          3. list_sheets() from registry
          4. sorted registry keys
        """
        registry_keys = set(self._loader.get_schema_registry_map().keys())
        if not registry_keys:
            return []

        ordered: List[str] = []

        for sheet in self._loader.get_canonical_sheets():
            if sheet in registry_keys and sheet not in ordered:
                ordered.append(sheet)

        for sheet in self._loader.list_sheets():
            if sheet in registry_keys and sheet not in ordered:
                ordered.append(sheet)

        for sheet in sorted(registry_keys):
            if sheet not in ordered:
                ordered.append(sheet)

        return ordered

    def resolve_requested_sheets(
        self,
        sheets: Optional[Sequence[str]],
        include_meta_sheet: bool,
    ) -> List[str]:
        """
        Resolve requested sheets to canonical names.

        Args:
            sheets: Requested sheet names (None → all in preferred order)
            include_meta_sheet: Whether to include the Data_Dictionary sheet itself

        Raises:
            SheetNotFoundError: if any requested sheet cannot be resolved
        """
        registry = self._loader.get_schema_registry_map()
        if not registry:
            return []

        if sheets is None:
            ordered = self.get_preferred_order()
        else:
            canonicalized: List[str] = []
            alias_map: Optional[Dict[str, str]] = None  # built lazily
            for sheet in sheets:
                s = _to_string(sheet)
                if not s:
                    continue
                # Direct lookup
                if s in registry:
                    canonicalized.append(s)
                    continue
                # Page catalog resolution
                resolved = PageCatalogHelper.resolve_sheet(s)
                if resolved in registry:
                    canonicalized.append(resolved)
                    continue
                # Alias mapping (v5.0.0: shared helper, not duplicated)
                if alias_map is None:
                    alias_map = build_alias_map(self._loader)
                normalized = _normalize_token(resolved or s)
                canonical = alias_map.get(normalized)
                if canonical and canonical in registry:
                    canonicalized.append(canonical)
                else:
                    raise SheetNotFoundError(f"Unknown sheet: {s}")

            ordered = _deduplicate_keep_order(canonicalized)

        if not include_meta_sheet:
            ordered = [s for s in ordered if s != DATA_DICTIONARY_SHEET_NAME]

        return ordered


# =============================================================================
# Row Builder
# =============================================================================

class RowBuilder:
    """Builds Data Dictionary rows from schema columns."""

    def __init__(self, contract: DataDictionaryContract):
        self._contract = contract
        self._keys = contract.get_keys()

    def from_column(self, sheet: str, column: Any) -> Dict[str, Any]:
        """Convert a schema column spec to a Data Dictionary row."""
        row = {
            DataDictionaryField.SHEET.value: _to_string(sheet),
            DataDictionaryField.GROUP.value: _to_string(_get_attr(column, "group", default="")),
            DataDictionaryField.HEADER.value: _to_string(_get_attr(column, "header", default="")),
            DataDictionaryField.KEY.value: _to_string(_get_attr(column, "key", default="")),
            DataDictionaryField.DTYPE.value: _to_string(_get_attr(column, "dtype", "type", default="")),
            DataDictionaryField.FMT.value: _to_string(_get_attr(column, "fmt", "format", default="")),
            DataDictionaryField.REQUIRED.value: _to_bool(_get_attr(column, "required", default=False)),
            DataDictionaryField.SOURCE.value: _to_string(_get_attr(column, "source", default="schema_registry")),
            DataDictionaryField.NOTES.value: _to_string(
                _get_attr(column, "notes", "note", "description", default="")
            ),
        }
        return self.normalize(row)

    def normalize(self, row: Mapping[str, Any]) -> Dict[str, Any]:
        """Normalize a row dict to canonical keys with proper type coercion."""
        if not row:
            return {}

        row_dict = dict(row)
        row_ci = {_normalize_token(k): v for k, v in row_dict.items()}

        # v5.0.0 fix: renamed loop variable from `field` to `dd_field` to
        # avoid shadowing the `from dataclasses import field` import.
        canonical: Dict[str, Any] = {}
        for dd_field in DataDictionaryField:
            key = dd_field.value
            if key in row_dict:
                canonical[key] = row_dict[key]
            else:
                norm_key = _normalize_token(key)
                if norm_key in row_ci:
                    canonical[key] = row_ci[norm_key]

        result: Dict[str, Any] = {}
        for key in self._keys:
            value = canonical.get(key)
            if key in _STRING_FIELDS:
                result[key] = _to_string(value)
            elif key == DataDictionaryField.REQUIRED.value:
                result[key] = _to_bool(value)
            else:
                result[key] = value

        return result

    def normalize_rows(self, rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
        """Normalize multiple rows."""
        return [self.normalize(r) for r in rows]

    def build_rows(self, sheets: List[str]) -> List[Dict[str, Any]]:
        """Build Data Dictionary rows for the given sheets in order."""
        registry = SchemaRegistryLoader().get_schema_registry_map()
        rows: List[Dict[str, Any]] = []

        for sheet in sheets:
            spec = registry.get(sheet)
            if spec is None:
                continue
            for col in DataDictionaryContract._extract_columns(spec):
                rows.append(self.from_column(sheet, col))

        return rows


# =============================================================================
# Payload Builder
# =============================================================================

class PayloadBuilder:
    """Builds Data Dictionary payloads in rows or values format."""

    def __init__(
        self,
        contract: DataDictionaryContract,
        row_builder: RowBuilder,
        orderer: SheetOrderer,
    ):
        self._contract = contract
        self._row_builder = row_builder
        self._orderer = orderer

    def _base_payload(self, fmt: str) -> Dict[str, Any]:
        """Fields common to both rows and values payloads."""
        return {
            "kind": DATA_DICTIONARY_OUTPUT_KIND,
            "sheet": DATA_DICTIONARY_SHEET_NAME,
            "page": DATA_DICTIONARY_SHEET_NAME,
            "sheet_name": DATA_DICTIONARY_SHEET_NAME,
            "format": fmt,
            "version": DATA_DICTIONARY_VERSION,
            "schema_version": SchemaRegistryLoader().get_schema_version(),
        }

    def build_rows_payload(
        self,
        sheets: Optional[Sequence[str]] = None,
        include_meta_sheet: bool = True,
    ) -> Dict[str, Any]:
        """Build payload with `rows` + `rows_matrix` format."""
        headers, keys = self._contract.get_contract()
        resolved_sheets = self._orderer.resolve_requested_sheets(sheets, include_meta_sheet)
        rows = self._row_builder.build_rows(resolved_sheets)
        rows_matrix = [[row.get(k) for k in keys] for row in rows]

        payload = self._base_payload(OutputFormat.ROWS.value)
        payload.update({
            "headers": headers,
            "display_headers": headers,
            "keys": keys,
            "rows": rows,
            "row_objects": rows,
            "rows_matrix": rows_matrix,
            "count": len(rows),
        })
        return payload

    def build_values_payload(
        self,
        sheets: Optional[Sequence[str]] = None,
        include_header_row: bool = True,
        include_meta_sheet: bool = True,
    ) -> Dict[str, Any]:
        """Build payload with 2D `values` format."""
        headers, keys = self._contract.get_contract()
        resolved_sheets = self._orderer.resolve_requested_sheets(sheets, include_meta_sheet)
        rows = self._row_builder.build_rows(resolved_sheets)

        values: List[List[Any]] = []
        if include_header_row:
            values.append(list(headers))

        key_count = len(keys)
        for row in rows:
            row_values = [row.get(key) for key in keys]
            if len(row_values) < key_count:
                row_values.extend([None] * (key_count - len(row_values)))
            elif len(row_values) > key_count:
                row_values = row_values[:key_count]
            values.append(row_values)

        payload = self._base_payload(OutputFormat.VALUES.value)
        payload.update({
            "headers": headers,
            "display_headers": headers,
            "keys": keys,
            "values": values,
            "count": max(0, len(values) - (1 if include_header_row else 0)),
        })
        return payload

    def build_payload(
        self,
        format: str = OutputFormat.ROWS.value,
        sheets: Optional[Sequence[str]] = None,
        include_meta_sheet: bool = True,
        include_header_row: bool = True,
    ) -> Dict[str, Any]:
        """Build payload in the specified format ('rows' or 'values')."""
        fmt = _to_string(format).lower()
        if fmt not in {OutputFormat.ROWS.value, OutputFormat.VALUES.value}:
            raise ValueError(f"format must be 'rows' or 'values', got '{fmt}'")

        if fmt == OutputFormat.VALUES.value:
            return self.build_values_payload(sheets, include_header_row, include_meta_sheet)
        return self.build_rows_payload(sheets, include_meta_sheet)


# =============================================================================
# Validation Helpers
# =============================================================================

def is_data_dictionary_payload(payload: Any) -> bool:
    """Return True if payload looks like a Data Dictionary payload (by `kind`)."""
    return isinstance(payload, dict) and payload.get("kind") == DATA_DICTIONARY_OUTPUT_KIND


def validate_data_dictionary_output(payload: Any) -> bool:
    """Validate that payload matches expected structure for its declared format."""
    if not is_data_dictionary_payload(payload):
        return False

    format_type = payload.get("format")
    if format_type == OutputFormat.ROWS.value:
        return "rows" in payload and isinstance(payload.get("rows"), list)
    if format_type == OutputFormat.VALUES.value:
        values = payload.get("values")
        return isinstance(values, list) and (not values or isinstance(values[0], list))

    return False


def validate_data_dictionary_values(values: Any) -> bool:
    """Return True if `values` is a 2D list (or empty list)."""
    return isinstance(values, list) and (not values or isinstance(values[0], list))


# =============================================================================
# Public API Functions (lazy payload-builder singleton)
# =============================================================================

_payload_builder: Optional[PayloadBuilder] = None


def _get_payload_builder() -> PayloadBuilder:
    """Get or create the module-level PayloadBuilder singleton."""
    global _payload_builder
    if _payload_builder is None:
        loader = SchemaRegistryLoader()
        contract = DataDictionaryContract(loader)
        row_builder = RowBuilder(contract)
        orderer = SheetOrderer(loader)
        _payload_builder = PayloadBuilder(contract, row_builder, orderer)
    return _payload_builder


def data_dictionary_headers() -> List[str]:
    """Get Data Dictionary display headers."""
    headers, _ = _get_payload_builder()._contract.get_contract()
    return headers


def data_dictionary_keys() -> List[str]:
    """Get Data Dictionary column keys."""
    _, keys = _get_payload_builder()._contract.get_contract()
    return keys


def data_dictionary_contract() -> Tuple[List[str], List[str]]:
    """Get (headers, keys) tuple."""
    return _get_payload_builder()._contract.get_contract()


def preferred_sheet_order() -> List[str]:
    """Get preferred sheet order for dictionary generation."""
    return _get_payload_builder()._orderer.get_preferred_order()


def resolve_requested_sheets(
    sheets: Optional[Sequence[str]],
    include_meta_sheet: bool = True,
) -> List[str]:
    """Resolve requested sheets to canonical names."""
    return _get_payload_builder()._orderer.resolve_requested_sheets(sheets, include_meta_sheet)


def row_dict_from_column(sheet: str, column: Any) -> Dict[str, Any]:
    """Convert a schema column to a Data Dictionary row."""
    return _get_payload_builder()._row_builder.from_column(sheet, column)


def normalize_data_dictionary_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    """Normalize a row dict to canonical keys."""
    return _get_payload_builder()._row_builder.normalize(row)


def normalize_data_dictionary_rows(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """Normalize multiple rows."""
    return _get_payload_builder()._row_builder.normalize_rows(rows)


def build_data_dictionary_rows(
    sheets: Optional[Sequence[str]] = None,
    include_meta_sheet: bool = True,
) -> List[Dict[str, Any]]:
    """Build Data Dictionary rows for the given sheets."""
    builder = _get_payload_builder()
    resolved_sheets = builder._orderer.resolve_requested_sheets(sheets, include_meta_sheet)
    return builder._row_builder.build_rows(resolved_sheets)


def build_data_dictionary_values(
    sheets: Optional[Sequence[str]] = None,
    include_header_row: bool = True,
    include_meta_sheet: bool = True,
) -> List[List[Any]]:
    """
    Build Data Dictionary as 2D values array.

    v5.0.0: delegates to PayloadBuilder.build_values_payload (previously
    duplicated the values-list construction logic inline).
    """
    payload = _get_payload_builder().build_values_payload(
        sheets=sheets,
        include_header_row=include_header_row,
        include_meta_sheet=include_meta_sheet,
    )
    return payload["values"]


def build_data_dictionary_payload(
    format: str = OutputFormat.ROWS.value,
    sheets: Optional[Sequence[str]] = None,
    include_meta_sheet: bool = True,
    include_header_row: bool = True,
) -> Dict[str, Any]:
    """Build Data Dictionary payload in the given format."""
    return _get_payload_builder().build_payload(format, sheets, include_meta_sheet, include_header_row)


# =============================================================================
# PEP 562 Lazy Module Attributes
# =============================================================================

def __getattr__(name: str) -> Any:
    """
    Lazy module-level attributes (PEP 562).

    v5.0.0 fix: `SCHEMA_VERSION` used to be declared in __all__ but was
    never defined anywhere, causing `from ... import *` and
    `from ... import SCHEMA_VERSION` to raise AttributeError. It's now
    resolved lazily from the schema registry on first access.
    """
    if name == "SCHEMA_VERSION":
        return SchemaRegistryLoader().get_schema_version()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> List[str]:
    """Include lazy attributes in dir() output."""
    return sorted(list(globals().keys()) + ["SCHEMA_VERSION"])


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    # Version constants
    "DATA_DICTIONARY_VERSION",
    "SCHEMA_VERSION",
    "DATA_DICTIONARY_SHEET_NAME",
    "DATA_DICTIONARY_OUTPUT_KIND",
    # Contract helpers
    "data_dictionary_headers",
    "data_dictionary_keys",
    "data_dictionary_contract",
    # Sheet resolution
    "preferred_sheet_order",
    "resolve_requested_sheets",
    # Row building
    "row_dict_from_column",
    "normalize_data_dictionary_row",
    "normalize_data_dictionary_rows",
    "build_data_dictionary_rows",
    "build_data_dictionary_values",
    "build_data_dictionary_payload",
    # Validation
    "validate_data_dictionary_output",
    "validate_data_dictionary_values",
    "is_data_dictionary_payload",
    # Enums
    "OutputFormat",
    "DataDictionaryField",
    # Classes (v5.0.0: added so downstream can type-check)
    "SchemaRegistryLoader",
    "DataDictionaryContract",
    "SheetOrderer",
    "RowBuilder",
    "PayloadBuilder",
    "PageCatalogHelper",
    # Exceptions (v5.0.0: added so callers can except: them cleanly)
    "DataDictionaryError",
    "SchemaRegistryNotFoundError",
    "SheetNotFoundError",
    "ContractMismatchError",
    # Shared helper
    "build_alias_map",
]
