#!/usr/bin/env python3
# core/sheets/data_dictionary.py
"""
================================================================================
Data Dictionary Generator — v2.3.0 (SCHEMA-DRIVEN / DRIFT-PROOF / CI-FRIENDLY)
================================================================================
Tadawul Fast Bridge (TFB)

Generates Data_Dictionary rows directly from core/sheets/schema_registry.py

Why this revision (Priority-2 alignment):
- ✅ STRICTLY derives Data_Dictionary schema (headers + keys) from schema_registry
  (no hard-coded lists used for output layout)
- ✅ Stable ordering:
    - uses schema_registry.CANONICAL_SHEETS when available
    - otherwise uses page_catalog.CANONICAL_PAGES
    - otherwise uses schema_registry.list_sheets()
- ✅ Defensive to ColumnSpec variations (fmt/format, dtype/type, etc.)
- ✅ Output formats:
    1) list[dict] rows (dict keys match Data_Dictionary schema keys)
    2) 2D values array for Google Sheets (optional header row)
- ✅ Import-safe: no I/O, no network
- ✅ Optional strict self-test for CI: TFB_SCHEMA_SELFTEST=1

Data_Dictionary expected columns (by header):
Sheet, Group, Header, Key, DType, Format, Required, Source, Notes
================================================================================
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Sequence, Tuple

# ---------------------------------------------------------------------------
# Schema Registry (authoritative)
# ---------------------------------------------------------------------------
try:
    from core.sheets.schema_registry import (  # type: ignore
        SCHEMA_VERSION,
        SCHEMA_REGISTRY,
        CANONICAL_SHEETS,
        get_sheet_spec,
        get_sheet_headers,
        list_sheets,
    )
except Exception as e:  # pragma: no cover
    raise ImportError(f"schema_registry import failed in data_dictionary.py: {e!r}") from e


__all__ = [
    "DATA_DICTIONARY_VERSION",
    "SCHEMA_VERSION",
    "build_data_dictionary_rows",
    "build_data_dictionary_values",
    "data_dictionary_headers",
    "data_dictionary_keys",
    "row_dict_from_column",
    "validate_data_dictionary_output",
    "preferred_sheet_order",
]

DATA_DICTIONARY_VERSION = "2.3.0"


# ---------------------------------------------------------------------------
# Helpers (safe string / attribute access)
# ---------------------------------------------------------------------------
def _s(v: Any) -> str:
    try:
        return str(v).strip()
    except Exception:
        return ""


def _get_attr(obj: Any, *names: str, default: Any = "") -> Any:
    for n in names:
        try:
            if hasattr(obj, n):
                return getattr(obj, n)
        except Exception:
            continue
    return default


def _bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    if isinstance(v, (int, float)):
        try:
            return bool(int(v))
        except Exception:
            return False
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(v)


def _env_truthy(name: str, default: bool = False) -> bool:
    try:
        raw = (os.getenv(name, str(default)) or "").strip().lower()
        return raw in {"1", "true", "yes", "y", "on", "t"}
    except Exception:
        return default


# ---------------------------------------------------------------------------
# Data_Dictionary spec introspection (authoritative contract)
# ---------------------------------------------------------------------------
def data_dictionary_headers() -> List[str]:
    """Returns Data_Dictionary headers EXACTLY as defined in schema_registry."""
    return list(get_sheet_headers("Data_Dictionary"))


def data_dictionary_keys() -> List[str]:
    """Returns Data_Dictionary keys EXACTLY as defined in schema_registry."""
    spec = get_sheet_spec("Data_Dictionary")
    cols = getattr(spec, "columns", None) or []
    keys: List[str] = []
    for c in cols:
        k = _get_attr(c, "key", default=None)
        if k:
            keys.append(str(k))
    return keys


def _data_dictionary_spec() -> Tuple[List[str], List[str]]:
    hdrs = data_dictionary_headers()
    keys = data_dictionary_keys()
    if not hdrs or not keys or len(hdrs) != len(keys):
        raise ValueError(
            "Data_Dictionary spec invalid: headers/keys missing or mismatched "
            f"(headers={len(hdrs)}, keys={len(keys)})"
        )
    return hdrs, keys


# ---------------------------------------------------------------------------
# Ordering (stable + schema-first)
# ---------------------------------------------------------------------------
def preferred_sheet_order() -> List[str]:
    """
    Stable ordering for dictionary generation.

    Priority:
      1) schema_registry.CANONICAL_SHEETS (best, single canonical view)
      2) page_catalog.CANONICAL_PAGES (if available)
      3) schema_registry.list_sheets()
      4) sorted(schema_registry keys)

    Only returns sheets that exist in SCHEMA_REGISTRY.
    """
    reg = set(SCHEMA_REGISTRY.keys())

    # 1) canonical sheets from schema_registry
    try:
        ordered = [s for s in list(CANONICAL_SHEETS) if s in reg]
        if ordered:
            for s in list_sheets():
                if s in reg and s not in ordered:
                    ordered.append(s)
            for s in sorted(reg):
                if s not in ordered:
                    ordered.append(s)
            return ordered
    except Exception:
        pass

    # 2) page_catalog
    try:
        from core.sheets.page_catalog import CANONICAL_PAGES  # type: ignore

        ordered2 = [s for s in list(CANONICAL_PAGES) if s in reg]
        for s in list_sheets():
            if s in reg and s not in ordered2:
                ordered2.append(s)
        for s in sorted(reg):
            if s not in ordered2:
                ordered2.append(s)
        return ordered2
    except Exception:
        pass

    # 3) list_sheets
    try:
        ordered3 = [s for s in list_sheets() if s in reg]
        if ordered3:
            for s in sorted(reg):
                if s not in ordered3:
                    ordered3.append(s)
            return ordered3
    except Exception:
        pass

    return sorted(reg)


def _canonicalize_sheet_name(name: str) -> str:
    """
    Best-effort canonicalization. Avoids hard dependency on page_catalog.
    Accepts aliases when page_catalog is available.
    """
    s = _s(name)
    if not s:
        return s

    for fn_name in ("resolve_page", "canonicalize_page", "normalize_page_name"):
        try:
            mod = __import__("core.sheets.page_catalog", fromlist=[fn_name])
            fn = getattr(mod, fn_name, None)
            if callable(fn):
                try:
                    out = fn(s, allow_output_pages=True)  # normalize_page_name signature
                except TypeError:
                    out = fn(s)
                if isinstance(out, str) and out.strip():
                    return out.strip()
        except Exception:
            continue

    return s.replace(" ", "_")


# ---------------------------------------------------------------------------
# Core row mapping
# ---------------------------------------------------------------------------
def row_dict_from_column(sheet: str, col: Any) -> Dict[str, Any]:
    """
    Convert a ColumnSpec into a Data_Dictionary row dict.
    Dict keys MUST match Data_Dictionary schema keys exactly (derived from schema_registry).
    """
    _, dd_keys = _data_dictionary_spec()

    # ColumnSpec variations (defensive)
    sheet_name = _s(sheet)
    group = _s(_get_attr(col, "group", default=""))
    header = _s(_get_attr(col, "header", default=""))
    key = _s(_get_attr(col, "key", default=""))
    dtype = _s(_get_attr(col, "dtype", "type", default=""))
    fmt = _s(_get_attr(col, "fmt", "format", default=""))
    required = _bool(_get_attr(col, "required", default=False))
    source = _s(_get_attr(col, "source", default=""))
    notes = _s(_get_attr(col, "notes", "note", "description", default=""))

    # canonical internal map
    base: Dict[str, Any] = {
        "sheet": sheet_name,
        "group": group,
        "header": header,
        "key": key,
        "dtype": dtype,
        "fmt": fmt,
        "required": required,
        "source": source,
        "notes": notes,
    }

    # Project into EXACT dd_keys (no extras)
    out: Dict[str, Any] = {}
    for dk in dd_keys:
        lk = dk.strip().lower().replace(" ", "_")

        if dk in base:
            out[dk] = base[dk]
            continue
        if lk in base:
            out[dk] = base[lk]
            continue

        # tolerate alternate names if schema ever changes (future-proof)
        if lk == "format":
            out[dk] = base["fmt"]
        elif lk in {"dtype", "type"}:
            out[dk] = base["dtype"]
        elif lk in {"sheet", "page"}:
            out[dk] = base["sheet"]
        elif lk in {"column", "header"}:
            out[dk] = base["header"]
        elif lk in {"required", "is_required", "req"}:
            out[dk] = base["required"]
        elif lk in {"notes", "note", "description"}:
            out[dk] = base["notes"]
        else:
            out[dk] = None

    return out


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------
def build_data_dictionary_rows(
    *,
    sheets: Optional[Sequence[str]] = None,
    include_meta_sheet: bool = True,
) -> List[Dict[str, Any]]:
    """
    Build Data_Dictionary as list[dict] rows.

    Args:
        sheets:
          If provided, restrict to these sheets (aliases accepted; canonicalized).
        include_meta_sheet:
          If False, excludes Data_Dictionary itself.
    """
    if sheets is None:
        ordered_sheets = preferred_sheet_order()
    else:
        ordered_sheets = [_canonicalize_sheet_name(s) for s in list(sheets)]
        missing = [s for s in ordered_sheets if s not in SCHEMA_REGISTRY]
        if missing:
            raise ValueError(f"Unknown sheets in data_dictionary request: {missing}")

    rows: List[Dict[str, Any]] = []
    for sheet in ordered_sheets:
        if not include_meta_sheet and sheet == "Data_Dictionary":
            continue

        spec = SCHEMA_REGISTRY.get(sheet)
        if spec is None:
            continue

        cols = getattr(spec, "columns", None) or []
        for col in cols:
            rows.append(row_dict_from_column(sheet, col))

    return rows


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

    Column order strictly follows Data_Dictionary schema (headers/keys from schema_registry).
    """
    headers, dd_keys = _data_dictionary_spec()
    rows = build_data_dictionary_rows(sheets=sheets, include_meta_sheet=include_meta_sheet)

    values: List[List[Any]] = []
    if include_header_row:
        values.append(list(headers))

    for r in rows:
        values.append([r.get(k) for k in dd_keys])

    return values


# ---------------------------------------------------------------------------
# Validation / Quality Gates
# ---------------------------------------------------------------------------
def validate_data_dictionary_output(
    rows: Sequence[Dict[str, Any]],
    *,
    enforce_unique_sheet_key: bool = True,
    forbid_extra_keys: bool = False,
) -> None:
    """
    Sanity checks for generated output (fast, no I/O).

    enforce_unique_sheet_key:
      - True by default to prevent duplicates (sheet,key)

    forbid_extra_keys:
      - If True, rows must contain ONLY the Data_Dictionary spec keys (strict CI).
    """
    _, dd_keys = _data_dictionary_spec()
    required_keys = set(dd_keys)

    for i, r in enumerate(rows):
        missing = required_keys - set(r.keys())
        if missing:
            raise ValueError(f"Row {i} missing keys: {sorted(missing)}")
        if forbid_extra_keys:
            extra = set(r.keys()) - required_keys
            if extra:
                raise ValueError(f"Row {i} has extra keys not in spec: {sorted(extra)}")

    if enforce_unique_sheet_key:
        lk = [k.strip().lower().replace(" ", "_") for k in dd_keys]
        sheet_key = None
        col_key = None
        for k, kl in zip(dd_keys, lk):
            if kl in {"sheet", "page"} and sheet_key is None:
                sheet_key = k
            if kl == "key" and col_key is None:
                col_key = k

        if not sheet_key or not col_key:
            raise ValueError("Cannot validate uniqueness: Data_Dictionary spec missing sheet/key columns.")

        seen = set()
        for r in rows:
            pair = (_s(r.get(sheet_key)), _s(r.get(col_key)))
            if pair in seen:
                raise ValueError(f"Duplicate (sheet,key) found in data dictionary: {pair}")
            seen.add(pair)


# ---------------------------------------------------------------------------
# Optional self-check (fast) — enable in CI or strict deployments
# ---------------------------------------------------------------------------
def _self_check() -> None:
    # DO NOT hardcode for output; only for sanity expectation.
    expected_headers = ["Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes"]
    headers = data_dictionary_headers()
    if headers != expected_headers:
        raise ValueError(
            "Data_Dictionary header mismatch vs schema_registry. "
            f"Expected {expected_headers} but got {headers}."
        )

    rows = build_data_dictionary_rows()
    validate_data_dictionary_output(rows, enforce_unique_sheet_key=True, forbid_extra_keys=True)


# Default: do NOT hard-crash startup unless explicitly enabled.
# Enable in CI: TFB_SCHEMA_SELFTEST=1
if _env_truthy("TFB_SCHEMA_SELFTEST", False):
    _self_check()
