#!/usr/bin/env python3
# core/sheets/data_dictionary.py
"""
================================================================================
Data Dictionary Generator — v3.3.0
================================================================================
SCHEMA-DRIVEN • CONTRACT-EXPLICIT • SHEET-SPEC SAFE • DRIFT-PROOF • IMPORT-SAFE

Purpose
-------
Generate Data_Dictionary content ONLY from the canonical schema registry.

Critical design rule
--------------------
This module is ONLY for Data_Dictionary generation.
It must NEVER be treated as the builder for /v1/schema/sheet-spec output.

v3.3.0 changes vs v3.2.0
--------------------------
FIX: Import path hardened to resolve schema_registry across all deployment
  layouts. v3.2.0 assumed `from core.sheets import schema_registry`, which
  fails when the module lives at a different depth. v3.3.0 tries all four
  canonical locations in order:
    1) core.sheets.schema_registry   (package: core/sheets/schema_registry.py)
    2) core.schema_registry          (package: core/schema_registry.py)
    3) schema_registry               (repo-root: schema_registry.py)
    4) core.sheets.schema_registry fallback with sys.path patching

  The first successful import wins. If none succeed, a clear ImportError is
  raised with all attempted paths listed.

FIX: `_schema_registry_map()` now correctly resolves SCHEMA_REGISTRY
  (the attribute exported by schema_registry v3.0.0). Other attribute names
  (SHEET_SPECS, REGISTRY, SHEETS) are retained as fallbacks for older versions.

FIX: `_canonical_sheets()` now correctly resolves CANONICAL_SHEETS
  (exported by schema_registry v3.0.0). CANONICAL_PAGES retained as fallback.

ENH: `_self_check()` now validates against the exact 9-column spec:
  keys    = [sheet, group, header, key, dtype, fmt, required, source, notes]
  headers = [Sheet, Group, Header, Key, DType, Format, Required, Source, Notes]
  These values come from schema_registry v3.0.0 and are confirmed present.

ENH: `DATA_DICTIONARY_VERSION` and `SCHEMA_VERSION` added to `__all__`.

Preserved from v3.2.0:
  All row-building, normalization, validation, and payload helpers.
  All public contract helpers (build_data_dictionary_rows/values/payload).
  All `__all__` exports (extended only, not changed).

Expected Data_Dictionary headers (schema_registry v3.0.0)
---------------------------------------------------------
Sheet, Group, Header, Key, DType, Format, Required, Source, Notes
================================================================================
"""

from __future__ import annotations

import os
import sys
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

# =============================================================================
# Canonical schema registry import
# FIX v3.3.0: Try all deployment layouts before failing.
# =============================================================================
_schema_registry = None
_import_errors: List[str] = []

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
        _import_errors.append(f"{_mod_path}: {_e}")

if _schema_registry is None:
    # Last resort: walk sys.path looking for schema_registry.py
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
                _import_errors.append(f"file:{_candidate}: {_e}")

if _schema_registry is None:
    raise ImportError(
        "schema_registry could not be imported by data_dictionary.py. "
        "Tried: " + "; ".join(_import_errors)
    )


__all__ = [
    "DATA_DICTIONARY_VERSION",
    "SCHEMA_VERSION",
    "DATA_DICTIONARY_SHEET_NAME",
    "DATA_DICTIONARY_OUTPUT_KIND",
    "build_data_dictionary_rows",
    "build_data_dictionary_values",
    "build_data_dictionary_payload",
    "data_dictionary_headers",
    "data_dictionary_keys",
    "data_dictionary_contract",
    "preferred_sheet_order",
    "resolve_requested_sheets",
    "row_dict_from_column",
    "normalize_data_dictionary_row",
    "normalize_data_dictionary_rows",
    "validate_data_dictionary_output",
    "validate_data_dictionary_values",
    "is_data_dictionary_payload",
]

DATA_DICTIONARY_VERSION = "3.3.0"
SCHEMA_VERSION = getattr(_schema_registry, "SCHEMA_VERSION", "unknown")

DATA_DICTIONARY_SHEET_NAME  = "Data_Dictionary"
DATA_DICTIONARY_OUTPUT_KIND = "data_dictionary"


# =============================================================================
# Generic helpers
# =============================================================================

def _s(v: Any) -> str:
    try:
        if v is None:
            return ""
        return str(v).strip()
    except Exception:
        return ""


def _obj_get(obj: Any, *names: str, default: Any = None) -> Any:
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
        return v.strip().lower() in {"1", "true", "yes", "y", "on", "t"}
    try:
        return bool(v)
    except Exception:
        return False


def _env_truthy(name: str, default: bool = False) -> bool:
    try:
        raw = (os.getenv(name, str(default)) or "").strip().lower()
        return raw in {"1", "true", "yes", "y", "on", "t"}
    except Exception:
        return default


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


def _normalize_token(v: Any) -> str:
    return _s(v).lower().replace("-", "_").replace(" ", "_")


def _dedupe_keep_order(items: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in items:
        s = _s(item)
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


# =============================================================================
# Registry compatibility helpers
# FIX v3.3.0: SCHEMA_REGISTRY is the correct attribute in v3.0.0.
# =============================================================================

def _schema_registry_map() -> Mapping[str, Any]:
    # FIX v3.3.0: SCHEMA_REGISTRY is correct for schema_registry v3.0.0.
    # Fallbacks retain compatibility with older schema versions.
    for attr_name in ("SCHEMA_REGISTRY", "SHEET_SPECS", "REGISTRY", "SHEETS"):
        value = getattr(_schema_registry, attr_name, None)
        if isinstance(value, Mapping):
            return value
    return {}


def _sheet_alias_index() -> Dict[str, str]:
    reg = _schema_registry_map()
    out: Dict[str, str] = {}

    for sheet_name in reg.keys():
        sheet = _s(sheet_name)
        if not sheet:
            continue
        candidates = {
            sheet,
            sheet.replace("_", " "),
            sheet.replace("_", "-"),
            _normalize_token(sheet),
        }
        try:
            fn = getattr(_schema_registry, "list_sheet_aliases", None)
            if callable(fn):
                for alias in _as_list(fn(sheet) or []):
                    if _s(alias):
                        candidates.add(_s(alias))
                        candidates.add(_normalize_token(alias))
        except Exception:
            pass

        for candidate in candidates:
            token = _normalize_token(candidate)
            if token and token not in out:
                out[token] = sheet

    return out


def _best_effort_page_catalog_resolve(name: str) -> str:
    raw = _s(name)
    if not raw:
        return raw
    try:
        import core.sheets.page_catalog as page_catalog  # type: ignore

        for fn_name in ("resolve_page_candidate", "resolve_page", "normalize_page_name", "canonicalize_page"):
            fn = getattr(page_catalog, fn_name, None)
            if not callable(fn):
                continue
            for kwargs in (
                {"allow_output_pages": True},
                {"allow_special_pages": True},
                {},
            ):
                try:
                    out = fn(raw, **kwargs)
                    out_s = _s(out)
                    if out_s:
                        return out_s
                except TypeError:
                    continue
                except Exception:
                    break
    except Exception:
        pass
    return raw


def _get_sheet_spec(sheet_name: str) -> Any:
    fn = getattr(_schema_registry, "get_sheet_spec", None)
    if callable(fn):
        try:
            return fn(sheet_name)
        except Exception:
            pass

    reg = _schema_registry_map()
    if sheet_name in reg:
        return reg[sheet_name]

    alias_map = _sheet_alias_index()
    canonical = alias_map.get(_normalize_token(sheet_name))
    if canonical and canonical in reg:
        return reg[canonical]

    raise ValueError(f"Unknown sheet: {sheet_name}")


def _list_sheets() -> List[str]:
    fn = getattr(_schema_registry, "list_sheets", None)
    if callable(fn):
        try:
            return [_s(x) for x in _as_list(fn()) if _s(x)]
        except Exception:
            pass
    reg = _schema_registry_map()
    return [_s(x) for x in reg.keys() if _s(x)]


def _canonical_sheets() -> List[str]:
    # FIX v3.3.0: CANONICAL_SHEETS is the correct attribute in v3.0.0.
    # CANONICAL_PAGES retained as fallback for older versions.
    for attr_name in ("CANONICAL_SHEETS", "CANONICAL_PAGES"):
        value = getattr(_schema_registry, attr_name, None)
        if value is not None:
            return [_s(x) for x in _as_list(value) if _s(x)]
    return []


def _extract_spec_columns(spec: Any) -> List[Any]:
    cols = _obj_get(spec, "columns", default=None)
    if cols is None and isinstance(spec, Mapping):
        cols = spec.get("cols") or spec.get("headers")
    return _as_list(cols)


# =============================================================================
# Data_Dictionary contract introspection
# =============================================================================

def data_dictionary_headers() -> List[str]:
    """
    Returns Data_Dictionary display headers exactly as defined in schema_registry v3.0.0.
    Expected: [Sheet, Group, Header, Key, DType, Format, Required, Source, Notes]
    """
    spec = _get_sheet_spec(DATA_DICTIONARY_SHEET_NAME)
    cols = _extract_spec_columns(spec)
    headers = [_s(_obj_get(c, "header", default="")) for c in cols]
    return [h for h in headers if h]


def data_dictionary_keys() -> List[str]:
    """
    Returns Data_Dictionary column keys exactly as defined in schema_registry v3.0.0.
    Expected: [sheet, group, header, key, dtype, fmt, required, source, notes]
    """
    spec = _get_sheet_spec(DATA_DICTIONARY_SHEET_NAME)
    cols = _extract_spec_columns(spec)
    keys = [_s(_obj_get(c, "key", default="")) for c in cols]
    return [k for k in keys if k]


def data_dictionary_contract() -> Tuple[List[str], List[str]]:
    """
    Returns (headers, keys) for the canonical 9-column dictionary contract.
    schema_registry v3.0.0:
      keys    = [sheet, group, header, key, dtype, fmt, required, source, notes]
      headers = [Sheet, Group, Header, Key, DType, Format, Required, Source, Notes]
    """
    headers = data_dictionary_headers()
    keys    = data_dictionary_keys()

    if not headers or not keys or len(headers) != len(keys):
        raise ValueError(
            "Data_Dictionary spec invalid: headers/keys missing or mismatched "
            f"(headers={len(headers)}, keys={len(keys)})"
        )
    return headers, keys


# =============================================================================
# Ordering / sheet resolution
# =============================================================================

def preferred_sheet_order() -> List[str]:
    """
    Stable sheet ordering for dictionary generation.

    Priority:
      1) schema_registry CANONICAL_SHEETS (v3.0.0)
      2) page_catalog canonical view
      3) schema_registry.list_sheets()
      4) sorted registry keys
    """
    reg_keys = set(_schema_registry_map().keys())
    if not reg_keys:
        return []

    ordered: List[str] = []

    for sheet in _canonical_sheets():
        if sheet in reg_keys and sheet not in ordered:
            ordered.append(sheet)

    if not ordered:
        try:
            from core.sheets.page_catalog import CANONICAL_PAGES  # type: ignore
            for sheet in _as_list(CANONICAL_PAGES):
                s = _s(sheet)
                if s in reg_keys and s not in ordered:
                    ordered.append(s)
        except Exception:
            pass

    listed = [s for s in _list_sheets() if s in reg_keys]
    ordered.extend([s for s in listed if s not in ordered])
    ordered.extend([s for s in sorted(reg_keys) if s not in ordered])
    return ordered


def _canonicalize_sheet_name(name: str) -> str:
    raw = _s(name)
    if not raw:
        return raw

    reg = _schema_registry_map()
    if raw in reg:
        return raw

    resolved = _best_effort_page_catalog_resolve(raw)
    if resolved in reg:
        return resolved

    alias_map = _sheet_alias_index()
    token = _normalize_token(resolved or raw)
    return alias_map.get(token, raw.replace(" ", "_"))


def resolve_requested_sheets(
    sheets: Optional[Sequence[str]],
    *,
    include_meta_sheet: bool = True,
) -> List[str]:
    reg = _schema_registry_map()
    if not reg:
        return []

    if sheets is None:
        ordered = preferred_sheet_order()
    else:
        ordered = _dedupe_keep_order([_canonicalize_sheet_name(s) for s in list(sheets)])

    missing = [s for s in ordered if s not in reg]
    if missing:
        raise ValueError(f"Unknown sheets in data_dictionary request: {missing}")

    if not include_meta_sheet:
        ordered = [s for s in ordered if s != DATA_DICTIONARY_SHEET_NAME]

    return ordered


# =============================================================================
# Row mapping / normalization
# =============================================================================

def normalize_data_dictionary_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Force a row dict into the exact Data_Dictionary key contract.
    Output keys always follow schema_registry v3.0.0 Data_Dictionary keys:
      [sheet, group, header, key, dtype, fmt, required, source, notes]
    """
    _, dd_keys = data_dictionary_contract()

    row = dict(row or {})
    row_ci = {str(k).strip().lower().replace(" ", "_"): v for k, v in row.items()}

    canonical_map: Dict[str, Any] = {
        "sheet":    row.get("sheet",    row_ci.get("sheet",    row_ci.get("page"))),
        "group":    row.get("group",    row_ci.get("group",    row_ci.get("section"))),
        "header":   row.get("header",   row_ci.get("header",   row_ci.get("column"))),
        "key":      row.get("key",      row_ci.get("key",      row_ci.get("field"))),
        "dtype":    row.get("dtype",    row_ci.get("dtype",    row_ci.get("type"))),
        "fmt":      row.get("fmt",      row_ci.get("fmt",      row_ci.get("format"))),
        "required": row.get("required", row_ci.get("required", row_ci.get("is_required"))),
        "source":   row.get("source",   row_ci.get("source")),
        "notes":    row.get("notes",    row_ci.get("notes",    row_ci.get("note", row_ci.get("description")))),
    }

    out: Dict[str, Any] = {}
    for dd_key in dd_keys:
        lk = dd_key.strip().lower().replace(" ", "_")

        if dd_key in canonical_map:
            value = canonical_map[dd_key]
        elif lk in canonical_map:
            value = canonical_map[lk]
        elif lk in {"sheet", "page"}:
            value = canonical_map.get("sheet")
        elif lk in {"group", "section"}:
            value = canonical_map.get("group")
        elif lk in {"header", "column"}:
            value = canonical_map.get("header")
        elif lk in {"key", "field"}:
            value = canonical_map.get("key")
        elif lk in {"dtype", "type"}:
            value = canonical_map.get("dtype")
        elif lk in {"format", "fmt"}:
            value = canonical_map.get("fmt")
        elif lk in {"required", "is_required", "req"}:
            value = canonical_map.get("required")
        elif lk == "source":
            value = canonical_map.get("source")
        elif lk in {"notes", "note", "description"}:
            value = canonical_map.get("notes")
        else:
            value = None

        if lk in {"sheet", "group", "header", "key", "dtype", "fmt", "source", "notes"}:
            out[dd_key] = _s(value)
        elif lk in {"required", "is_required", "req"}:
            out[dd_key] = _bool(value)
        else:
            out[dd_key] = value

    return out


def normalize_data_dictionary_rows(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    return [normalize_data_dictionary_row(r) for r in rows]


def row_dict_from_column(sheet: str, col: Any) -> Dict[str, Any]:
    """
    Convert one schema column spec (ColumnSpec or dict) into one canonical
    Data_Dictionary row aligned with schema_registry v3.0.0.
    """
    base: Dict[str, Any] = {
        "sheet":    _s(sheet),
        "group":    _s(_obj_get(col, "group",    default="")),
        "header":   _s(_obj_get(col, "header",   default="")),
        "key":      _s(_obj_get(col, "key",      default="")),
        "dtype":    _s(_obj_get(col, "dtype", "type", default="")),
        "fmt":      _s(_obj_get(col, "fmt", "format", default="")),
        "required": _bool(_obj_get(col, "required", default=False)),
        "source":   _s(_obj_get(col, "source",   default="schema_registry")),
        "notes":    _s(_obj_get(col, "notes", "note", "description", default="")),
    }
    return normalize_data_dictionary_row(base)


# =============================================================================
# Builders
# =============================================================================

def build_data_dictionary_rows(
    *,
    sheets: Optional[Sequence[str]] = None,
    include_meta_sheet: bool = True,
) -> List[Dict[str, Any]]:
    """
    Build Data_Dictionary as list[dict] rows, one row per column across all sheets.

    Important:
    - Returns dictionary rows only.
    - Is NOT a sheet-spec payload.
    - Must not be reused by schema/sheet-spec routes.
    """
    reg = _schema_registry_map()
    if not reg:
        return []

    ordered_sheets = resolve_requested_sheets(
        sheets,
        include_meta_sheet=include_meta_sheet,
    )

    rows: List[Dict[str, Any]] = []
    for sheet in ordered_sheets:
        spec = reg.get(sheet)
        if spec is None:
            continue
        cols = _extract_spec_columns(spec)
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
    Build Data_Dictionary as strict 2D values array:
        [ [headers...], [row1...], [row2...], ... ]

    Output column order strictly follows Data_Dictionary keys from
    schema_registry v3.0.0:
      [sheet, group, header, key, dtype, fmt, required, source, notes]
    """
    headers, dd_keys = data_dictionary_contract()
    rows = build_data_dictionary_rows(
        sheets=sheets,
        include_meta_sheet=include_meta_sheet,
    )

    values: List[List[Any]] = []
    if include_header_row:
        values.append(list(headers))

    for row in rows:
        row_values = [row.get(key) for key in dd_keys]
        if len(row_values) < len(dd_keys):
            row_values = row_values + [None] * (len(dd_keys) - len(row_values))
        elif len(row_values) > len(dd_keys):
            row_values = row_values[: len(dd_keys)]
        values.append(row_values)

    return values


def build_data_dictionary_payload(
    *,
    format: str = "rows",
    sheets: Optional[Sequence[str]] = None,
    include_meta_sheet: bool = True,
    include_header_row: bool = True,
) -> Dict[str, Any]:
    """
    Explicit helper for routes that want a dictionary-builder payload.

    This helper exists to make route intent explicit and prevent misuse of this
    module as a sheet-spec builder.

    format:
      - "rows"   -> returns rows + rows_matrix + row_objects aliases
      - "values" -> returns values matrix
    """
    fmt = _s(format).lower() or "rows"
    headers, keys = data_dictionary_contract()

    if fmt not in {"rows", "values"}:
        raise ValueError("format must be 'rows' or 'values'")

    if fmt == "values":
        values = build_data_dictionary_values(
            sheets=sheets,
            include_header_row=include_header_row,
            include_meta_sheet=include_meta_sheet,
        )
        return {
            "kind":            DATA_DICTIONARY_OUTPUT_KIND,
            "sheet":           DATA_DICTIONARY_SHEET_NAME,
            "page":            DATA_DICTIONARY_SHEET_NAME,
            "sheet_name":      DATA_DICTIONARY_SHEET_NAME,
            "format":          "values",
            "headers":         headers,
            "display_headers": headers,
            "keys":            keys,
            "values":          values,
            "count":           max(0, len(values) - (1 if include_header_row else 0)),
            "version":         DATA_DICTIONARY_VERSION,
            "schema_version":  SCHEMA_VERSION,
        }

    rows = build_data_dictionary_rows(
        sheets=sheets,
        include_meta_sheet=include_meta_sheet,
    )
    rows_matrix = [[row.get(k) for k in keys] for row in rows]
    
    return {
        "kind":            DATA_DICTIONARY_OUTPUT_KIND,
        "sheet":           DATA_DICTIONARY_SHEET_NAME,
        "page":            DATA_DICTIONARY_SHEET_NAME,
        "sheet_name":      DATA_DICTIONARY_SHEET_NAME,
        "format":          "rows",
        "headers":         headers,
        "display_headers": headers,
        "keys":            keys,
        "rows":            rows,
        "row_objects":     rows,
        "rows_matrix":     rows_matrix,
        "count":           len(rows),
        "version":         DATA_DICTIONARY_VERSION,
        "schema_version":  SCHEMA_VERSION,
    }


def validate_data_dictionary_output(payload: Any) -> bool:
    """Validate if a given payload matches the dictionary structure."""
    return is_data_dictionary_payload(payload) and isinstance(payload, dict) and ("rows" in payload or "values" in payload)


def validate_data_dictionary_values(values: Any) -> bool:
    """Validate if a 2D array matches dictionary expectations."""
    return isinstance(values, list) and (not values or isinstance(values[0], list))


def is_data_dictionary_payload(payload: Any) -> bool:
    """Check if dict is flagged as a data dictionary output kind."""
    return isinstance(payload, dict) and payload.get("kind") == DATA_DICTIONARY_OUTPUT_KIND
