#!/usr/bin/env python3
# core/sheets/data_dictionary.py
"""
================================================================================
Data Dictionary Generator — v2.0.0 (SCHEMA-DRIVEN / STABLE / SHEETS-READY)
================================================================================
Tadawul Fast Bridge (TFB)

Generates Data_Dictionary rows directly from core/sheets/schema_registry.py

Design goals:
- ✅ Single source of truth: schema_registry
- ✅ Stable ordering: by sheet name (catalog order if available, else sorted),
  then by column order as defined in schema_registry
- ✅ Output formats:
   1) list[dict] rows (keys match Data_Dictionary schema keys)
   2) 2D values array for Google Sheets (including header row)
- ✅ Import-safe: no I/O, no network

Data_Dictionary columns (must match schema_registry Data_Dictionary sheet spec):
Sheet, Group, Header, Key, DType, Format, Required, Source, Notes
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from core.sheets.schema_registry import (
    SCHEMA_VERSION,
    ColumnSpec,
    SheetSpec,
    SCHEMA_REGISTRY,
    get_sheet_columns,
    get_sheet_headers,
    list_sheets,
)

__all__ = [
    "DATA_DICTIONARY_VERSION",
    "build_data_dictionary_rows",
    "build_data_dictionary_values",
    "data_dictionary_headers",
    "row_dict_from_column",
    "validate_data_dictionary_output",
]

DATA_DICTIONARY_VERSION = "2.0.0"


# -----------------------------
# Core row mapping
# -----------------------------

def row_dict_from_column(sheet: str, col: ColumnSpec) -> Dict[str, Any]:
    """
    Convert a ColumnSpec into a Data_Dictionary row dict.
    Keys MUST match Data_Dictionary schema keys exactly.
    """
    return {
        "sheet": sheet,
        "group": col.group,
        "header": col.header,
        "key": col.key,
        "dtype": col.dtype,
        "fmt": col.fmt,
        "required": bool(col.required),
        "source": col.source,
        "notes": col.notes,
    }


# -----------------------------
# Ordering
# -----------------------------

def _preferred_sheet_order() -> List[str]:
    """
    Try to respect page_catalog's canonical ordering when available.
    If page_catalog import fails for any reason, fall back to sorted registry keys.
    """
    try:
        # optional import (avoid circular issues / keep generator resilient)
        from core.sheets.page_catalog import CANONICAL_PAGES  # type: ignore

        # Ensure it matches registry; append any missing sheets (defensive)
        reg = set(SCHEMA_REGISTRY.keys())
        ordered = [s for s in CANONICAL_PAGES if s in reg]
        for s in sorted(reg):
            if s not in ordered:
                ordered.append(s)
        return ordered
    except Exception:
        return sorted(SCHEMA_REGISTRY.keys())


# -----------------------------
# Builders
# -----------------------------

def build_data_dictionary_rows(
    *,
    sheets: Optional[Sequence[str]] = None,
    include_meta_sheet: bool = True,
) -> List[Dict[str, Any]]:
    """
    Build Data_Dictionary as list[dict] rows.

    Args:
        sheets: If provided, restrict to these sheets (names must exist in SCHEMA_REGISTRY).
        include_meta_sheet: If False, excludes Data_Dictionary itself (rarely desired).
    """
    if sheets is None:
        ordered_sheets = _preferred_sheet_order()
    else:
        # Keep the order as provided by caller, but validate existence
        ordered_sheets = list(sheets)
        missing = [s for s in ordered_sheets if s not in SCHEMA_REGISTRY]
        if missing:
            raise ValueError(f"Unknown sheets in data_dictionary request: {missing}")

    rows: List[Dict[str, Any]] = []
    for sheet in ordered_sheets:
        if not include_meta_sheet and sheet == "Data_Dictionary":
            continue
        spec: SheetSpec = SCHEMA_REGISTRY[sheet]
        for col in spec.columns:
            rows.append(row_dict_from_column(sheet, col))

    return rows


def data_dictionary_headers() -> List[str]:
    """
    Returns the Data_Dictionary headers EXACTLY as defined in schema_registry.
    """
    return get_sheet_headers("Data_Dictionary")


def build_data_dictionary_values(
    *,
    sheets: Optional[Sequence[str]] = None,
    include_header_row: bool = True,
    include_meta_sheet: bool = True,
) -> List[List[Any]]:
    """
    Build a 2D array ready to write into Google Sheets:
    [ [headers...],
      [row1...],
      [row2...], ... ]

    Column order strictly follows Data_Dictionary schema headers.
    """
    headers = data_dictionary_headers()
    rows = build_data_dictionary_rows(sheets=sheets, include_meta_sheet=include_meta_sheet)

    values: List[List[Any]] = []
    if include_header_row:
        values.append(headers)

    for r in rows:
        values.append([r.get(k) for k in _headers_to_keys(headers)])

    return values


def _headers_to_keys(headers: Sequence[str]) -> List[str]:
    """
    Data_Dictionary headers are user-friendly titles; we map them to schema keys.
    We do NOT store this mapping in schema_registry because the Data_Dictionary sheet
    is already keyed by its own schema columns.
    """
    # Must match schema_registry _data_dictionary_columns() keys order.
    # We keep mapping robust to header text; use exact canonical headers.
    mapping = {
        "Sheet": "sheet",
        "Group": "group",
        "Header": "header",
        "Key": "key",
        "DType": "dtype",
        "Format": "fmt",
        "Required": "required",
        "Source": "source",
        "Notes": "notes",
    }
    keys: List[str] = []
    for h in headers:
        if h not in mapping:
            raise ValueError(
                f"Unexpected Data_Dictionary header '{h}'. "
                f"Expected one of: {sorted(mapping.keys())}"
            )
        keys.append(mapping[h])
    return keys


# -----------------------------
# Validation / Quality Gates
# -----------------------------

def validate_data_dictionary_output(
    rows: Sequence[Dict[str, Any]],
    *,
    enforce_unique_sheet_key: bool = False,
) -> None:
    """
    Sanity checks for generated output (fast, no I/O).

    enforce_unique_sheet_key:
      - if True, enforces uniqueness of (sheet, key)
      - usually True for a strict dictionary; keep optional to allow future extensions
    """
    required_keys = {"sheet", "group", "header", "key", "dtype", "fmt", "required", "source", "notes"}
    for i, r in enumerate(rows):
        missing = required_keys - set(r.keys())
        if missing:
            raise ValueError(f"Row {i} missing keys: {sorted(missing)}")

    if enforce_unique_sheet_key:
        seen = set()
        for r in rows:
            k = (r["sheet"], r["key"])
            if k in seen:
                raise ValueError(f"Duplicate (sheet,key) found in data dictionary: {k}")
            seen.add(k)


# -----------------------------
# Quick self-check at import
# -----------------------------

# Ensure the generator stays aligned with schema_registry's declared Data_Dictionary headers
_expected = ["Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes"]
_actual = data_dictionary_headers()
if _actual != _expected:
    raise ValueError(
        "Data_Dictionary header mismatch vs schema_registry. "
        f"Expected {_expected} but got {_actual}. "
        "Fix schema_registry Data_Dictionary columns to match."
    )

# Build once to ensure no runtime surprises (small + safe)
_validate_rows = build_data_dictionary_rows()
validate_data_dictionary_output(_validate_rows, enforce_unique_sheet_key=True)
