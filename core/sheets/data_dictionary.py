#!/usr/bin/env python3
# core/sheets/data_dictionary.py
"""
================================================================================
Data Dictionary Generator — v3.0.0
================================================================================
SCHEMA-DRIVEN • CONTRACT-EXPLICIT • SHEET-SPEC SAFE • DRIFT-PROOF • IMPORT-SAFE

Purpose
-------
Generate Data_Dictionary content ONLY from the canonical schema registry.

Critical design rule
--------------------
This module is ONLY for Data_Dictionary generation.
It must NEVER be treated as the builder for /v1/schema/sheet-spec output.

What this revision improves
---------------------------
- ✅ Keeps Data_Dictionary generation fully separate from sheet-spec usage
- ✅ Adds explicit public contract helpers for rows vs values outputs
- ✅ Enforces the canonical 9-column Data_Dictionary schema
- ✅ Supports dict-style and object-style schema columns safely
- ✅ Normalizes every row to exact Data_Dictionary keys
- ✅ Produces strict 2D values output in canonical order
- ✅ Adds explicit validation helpers for row shape / width / duplicates
- ✅ Adds metadata helpers so routes can identify this as dictionary-only output
- ✅ Preserves import-safety: no I/O, no network calls, no engine usage

Expected Data_Dictionary headers
--------------------------------
Sheet, Group, Header, Key, DType, Format, Required, Source, Notes
================================================================================
"""

from __future__ import annotations

import os
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

# -----------------------------------------------------------------------------
# Canonical schema registry
# -----------------------------------------------------------------------------
try:
    from core.sheets import schema_registry as _schema_registry  # type: ignore
except Exception as e:  # pragma: no cover
    raise ImportError(f"schema_registry import failed in data_dictionary.py: {e!r}") from e


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
    "row_dict_from_column",
    "normalize_data_dictionary_row",
    "normalize_data_dictionary_rows",
    "validate_data_dictionary_output",
    "validate_data_dictionary_values",
    "is_data_dictionary_payload",
]

DATA_DICTIONARY_VERSION = "3.0.0"
SCHEMA_VERSION = getattr(_schema_registry, "SCHEMA_VERSION", "unknown")

DATA_DICTIONARY_SHEET_NAME = "Data_Dictionary"
DATA_DICTIONARY_OUTPUT_KIND = "data_dictionary"


# -----------------------------------------------------------------------------
# Generic helpers
# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
# Registry compatibility helpers
# -----------------------------------------------------------------------------
def _schema_registry_map() -> Mapping[str, Any]:
    for attr_name in ("SCHEMA_REGISTRY", "SHEET_SPECS", "REGISTRY", "SHEETS"):
        value = getattr(_schema_registry, attr_name, None)
        if isinstance(value, Mapping):
            return value
    return {}


def _get_sheet_spec(sheet_name: str) -> Any:
    fn = getattr(_schema_registry, "get_sheet_spec", None)
    if callable(fn):
        return fn(sheet_name)

    reg = _schema_registry_map()
    if sheet_name in reg:
        return reg[sheet_name]

    raise ValueError(f"Unknown sheet: {sheet_name}")


def _list_sheets() -> List[str]:
    fn = getattr(_schema_registry, "list_sheets", None)
    if callable(fn):
        return [_s(x) for x in _as_list(fn()) if _s(x)]

    reg = _schema_registry_map()
    return [_s(x) for x in reg.keys() if _s(x)]


def _canonical_sheets() -> List[str]:
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


# -----------------------------------------------------------------------------
# Data_Dictionary contract introspection
# -----------------------------------------------------------------------------
def data_dictionary_headers() -> List[str]:
    """
    Returns Data_Dictionary headers exactly as defined in schema_registry.
    """
    spec = _get_sheet_spec(DATA_DICTIONARY_SHEET_NAME)
    cols = _extract_spec_columns(spec)
    headers = [_s(_obj_get(c, "header", default="")) for c in cols]
    return [h for h in headers if h]


def data_dictionary_keys() -> List[str]:
    """
    Returns Data_Dictionary keys exactly as defined in schema_registry.
    """
    spec = _get_sheet_spec(DATA_DICTIONARY_SHEET_NAME)
    cols = _extract_spec_columns(spec)
    keys = [_s(_obj_get(c, "key", default="")) for c in cols]
    return [k for k in keys if k]


def data_dictionary_contract() -> Tuple[List[str], List[str]]:
    """
    Returns (headers, keys) for the canonical 9-column dictionary contract.
    """
    headers = data_dictionary_headers()
    keys = data_dictionary_keys()

    if not headers or not keys or len(headers) != len(keys):
        raise ValueError(
            "Data_Dictionary spec invalid: headers/keys missing or mismatched "
            f"(headers={len(headers)}, keys={len(keys)})"
        )

    return headers, keys


# -----------------------------------------------------------------------------
# Ordering
# -----------------------------------------------------------------------------
def preferred_sheet_order() -> List[str]:
    """
    Stable sheet ordering for dictionary generation.

    Priority:
      1) schema_registry canonical view
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

    if ordered:
        for sheet in _list_sheets():
            if sheet in reg_keys and sheet not in ordered:
                ordered.append(sheet)
        for sheet in sorted(reg_keys):
            if sheet not in ordered:
                ordered.append(sheet)
        return ordered

    try:
        from core.sheets.page_catalog import CANONICAL_PAGES  # type: ignore

        for sheet in _as_list(CANONICAL_PAGES):
            s = _s(sheet)
            if s in reg_keys and s not in ordered:
                ordered.append(s)
    except Exception:
        pass

    if ordered:
        for sheet in _list_sheets():
            if sheet in reg_keys and sheet not in ordered:
                ordered.append(sheet)
        for sheet in sorted(reg_keys):
            if sheet not in ordered:
                ordered.append(sheet)
        return ordered

    listed = [s for s in _list_sheets() if s in reg_keys]
    if listed:
        for sheet in listed:
            if sheet not in ordered:
                ordered.append(sheet)
        for sheet in sorted(reg_keys):
            if sheet not in ordered:
                ordered.append(sheet)
        return ordered

    return sorted(reg_keys)


def _canonicalize_sheet_name(name: str) -> str:
    """
    Best-effort canonicalization.
    """
    raw = _s(name)
    if not raw:
        return raw

    try:
        import core.sheets.page_catalog as page_catalog  # type: ignore

        for fn_name in ("normalize_page_name", "resolve_page", "canonicalize_page"):
            fn = getattr(page_catalog, fn_name, None)
            if callable(fn):
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
                        break
                    except TypeError:
                        continue
                    except Exception:
                        break
                try:
                    out = fn(raw)
                    out_s = _s(out)
                    if out_s:
                        return out_s
                except Exception:
                    pass
    except Exception:
        pass

    return raw.replace(" ", "_")


# -----------------------------------------------------------------------------
# Row mapping / normalization
# -----------------------------------------------------------------------------
def normalize_data_dictionary_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Force a row dict into the exact Data_Dictionary key contract.

    Output keys always follow schema_registry Data_Dictionary keys.
    """
    _, dd_keys = data_dictionary_contract()

    row = dict(row or {})
    row_ci = {str(k).strip().lower().replace(" ", "_"): v for k, v in row.items()}

    canonical_map: Dict[str, Any] = {
        "sheet": row.get("sheet", row_ci.get("sheet", row_ci.get("page"))),
        "group": row.get("group", row_ci.get("group")),
        "header": row.get("header", row_ci.get("header", row_ci.get("column"))),
        "key": row.get("key", row_ci.get("key")),
        "dtype": row.get("dtype", row_ci.get("dtype", row_ci.get("type"))),
        "fmt": row.get("fmt", row_ci.get("fmt", row_ci.get("format"))),
        "required": row.get("required", row_ci.get("required", row_ci.get("is_required"))),
        "source": row.get("source", row_ci.get("source")),
        "notes": row.get("notes", row_ci.get("notes", row_ci.get("note", row_ci.get("description")))),
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
        elif lk == "group":
            value = canonical_map.get("group")
        elif lk in {"header", "column"}:
            value = canonical_map.get("header")
        elif lk == "key":
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
    Convert one schema column spec into one canonical Data_Dictionary row.
    """
    base: Dict[str, Any] = {
        "sheet": _s(sheet),
        "group": _s(_obj_get(col, "group", default="")),
        "header": _s(_obj_get(col, "header", default="")),
        "key": _s(_obj_get(col, "key", default="")),
        "dtype": _s(_obj_get(col, "dtype", "type", default="")),
        "fmt": _s(_obj_get(col, "fmt", "format", default="")),
        "required": _bool(_obj_get(col, "required", default=False)),
        "source": _s(_obj_get(col, "source", default="")),
        "notes": _s(_obj_get(col, "notes", "note", "description", default="")),
    }
    return normalize_data_dictionary_row(base)


# -----------------------------------------------------------------------------
# Builders
# -----------------------------------------------------------------------------
def build_data_dictionary_rows(
    *,
    sheets: Optional[Sequence[str]] = None,
    include_meta_sheet: bool = True,
) -> List[Dict[str, Any]]:
    """
    Build Data_Dictionary as list[dict] rows.

    Important:
    - This returns dictionary rows only
    - It is NOT a sheet-spec payload
    - It must not be reused by schema/sheet-spec routes
    """
    reg = _schema_registry_map()
    if not reg:
        return []

    if sheets is None:
        ordered_sheets = preferred_sheet_order()
    else:
        ordered_sheets = [_canonicalize_sheet_name(s) for s in list(sheets)]
        missing = [s for s in ordered_sheets if s not in reg]
        if missing:
            raise ValueError(f"Unknown sheets in data_dictionary request: {missing}")

    rows: List[Dict[str, Any]] = []
    for sheet in ordered_sheets:
        if not include_meta_sheet and sheet == DATA_DICTIONARY_SHEET_NAME:
            continue

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

    Output order strictly follows Data_Dictionary keys from schema_registry.
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
      - "rows"   -> returns rows + rows_matrix
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
            "kind": DATA_DICTIONARY_OUTPUT_KIND,
            "sheet": DATA_DICTIONARY_SHEET_NAME,
            "format": "values",
            "headers": headers,
            "keys": keys,
            "values": values,
            "count": max(0, len(values) - (1 if include_header_row else 0)),
            "version": DATA_DICTIONARY_VERSION,
            "schema_version": SCHEMA_VERSION,
        }

    rows = build_data_dictionary_rows(
        sheets=sheets,
        include_meta_sheet=include_meta_sheet,
    )
    return {
        "kind": DATA_DICTIONARY_OUTPUT_KIND,
        "sheet": DATA_DICTIONARY_SHEET_NAME,
        "format": "rows",
        "headers": headers,
        "keys": keys,
        "rows": rows,
        "rows_matrix": [[row.get(k) for k in keys] for row in rows],
        "count": len(rows),
        "version": DATA_DICTIONARY_VERSION,
        "schema_version": SCHEMA_VERSION,
    }


def is_data_dictionary_payload(payload: Any) -> bool:
    if not isinstance(payload, Mapping):
        return False
    return _s(payload.get("kind")).lower() == DATA_DICTIONARY_OUTPUT_KIND


# -----------------------------------------------------------------------------
# Validation / quality gates
# -----------------------------------------------------------------------------
def validate_data_dictionary_output(
    rows: Sequence[Dict[str, Any]],
    *,
    enforce_unique_sheet_key: bool = True,
    forbid_extra_keys: bool = False,
) -> None:
    """
    Validate list[dict] Data_Dictionary rows.
    """
    _, dd_keys = data_dictionary_contract()
    required_keys = set(dd_keys)
    normalized_rows = list(rows)

    for i, row in enumerate(normalized_rows):
        missing = required_keys - set(row.keys())
        if missing:
            raise ValueError(f"Row {i} missing keys: {sorted(missing)}")

        if forbid_extra_keys:
            extra = set(row.keys()) - required_keys
            if extra:
                raise ValueError(f"Row {i} has extra keys not in spec: {sorted(extra)}")

    if enforce_unique_sheet_key:
        sheet_key: Optional[str] = None
        col_key: Optional[str] = None

        for original_key in dd_keys:
            lk = original_key.strip().lower().replace(" ", "_")
            if lk in {"sheet", "page"} and sheet_key is None:
                sheet_key = original_key
            if lk == "key" and col_key is None:
                col_key = original_key

        if not sheet_key or not col_key:
            raise ValueError("Cannot validate uniqueness: Data_Dictionary spec missing sheet/key columns")

        seen = set()
        for row in normalized_rows:
            pair = (_s(row.get(sheet_key)), _s(row.get(col_key)))
            if pair in seen:
                raise ValueError(f"Duplicate (sheet,key) found in data dictionary: {pair}")
            seen.add(pair)


def validate_data_dictionary_values(
    values: Sequence[Sequence[Any]],
    *,
    include_header_row: bool = True,
) -> None:
    """
    Validate strict 2D values output width/order expectations.
    """
    headers, _ = data_dictionary_contract()
    expected_width = len(headers)

    values_list = [list(r) for r in values]
    if not values_list:
        raise ValueError("Data_Dictionary values output is empty")

    start_index = 0
    if include_header_row:
        header_row = list(values_list[0])
        if header_row != headers:
            raise ValueError(f"Header row mismatch. Expected {headers} but got {header_row}")
        start_index = 1

    for idx, row in enumerate(values_list[start_index:], start=start_index):
        if len(row) != expected_width:
            raise ValueError(
                f"Data_Dictionary row width mismatch at index {idx}: "
                f"expected {expected_width} got {len(row)}"
            )


# -----------------------------------------------------------------------------
# Optional self-check
# -----------------------------------------------------------------------------
def _self_check() -> None:
    expected_headers = ["Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes"]

    headers = data_dictionary_headers()
    if headers != expected_headers:
        raise ValueError(
            "Data_Dictionary header mismatch vs schema_registry. "
            f"Expected {expected_headers} but got {headers}."
        )

    rows = build_data_dictionary_rows()
    validate_data_dictionary_output(
        rows,
        enforce_unique_sheet_key=True,
        forbid_extra_keys=True,
    )

    values = build_data_dictionary_values(include_header_row=True)
    validate_data_dictionary_values(values, include_header_row=True)

    payload_rows = build_data_dictionary_payload(format="rows")
    if not is_data_dictionary_payload(payload_rows):
        raise ValueError("Rows payload is not marked as data_dictionary kind")

    payload_values = build_data_dictionary_payload(format="values")
    if not is_data_dictionary_payload(payload_values):
        raise ValueError("Values payload is not marked as data_dictionary kind")


if _env_truthy("TFB_SCHEMA_SELFTEST", False):
    _self_check()
