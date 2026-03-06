#!/usr/bin/env python3
# core/sheets/data_dictionary.py
"""
================================================================================
Data Dictionary Generator — v2.4.0 (SCHEMA-DRIVEN / SHAPE-SAFE / DRIFT-PROOF)
================================================================================
Tadawul Fast Bridge (TFB)

Generates Data_Dictionary rows directly from core/sheets/schema_registry.py

What this revision fixes
- ✅ FIX: Enforces the 9-column Data_Dictionary contract from schema_registry
- ✅ FIX: Supports object-style OR dict-style schema specs / columns
- ✅ FIX: Normalizes row dicts so keys always match canonical Data_Dictionary keys
- ✅ FIX: Normalizes values output to a strict 2D matrix in canonical key order
- ✅ FIX: Better sheet ordering with schema-first fallback chain
- ✅ FIX: Safer alias/canonicalization handling
- ✅ SAFE: no I/O, no network, import-safe
- ✅ SAFE: optional self-test only when TFB_SCHEMA_SELFTEST=1

Expected Data_Dictionary headers:
Sheet, Group, Header, Key, DType, Format, Required, Source, Notes
================================================================================
"""

from __future__ import annotations

import os
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

# ---------------------------------------------------------------------------
# Schema Registry (authoritative)
# ---------------------------------------------------------------------------
try:
    from core.sheets import schema_registry as _schema_registry  # type: ignore
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

DATA_DICTIONARY_VERSION = "2.4.0"
SCHEMA_VERSION = getattr(_schema_registry, "SCHEMA_VERSION", "unknown")


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Registry compatibility helpers
# ---------------------------------------------------------------------------
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


def _get_sheet_headers(sheet_name: str) -> List[str]:
    fn = getattr(_schema_registry, "get_sheet_headers", None)
    if callable(fn):
        headers = fn(sheet_name)
        return [_s(x) for x in _as_list(headers)]

    spec = _get_sheet_spec(sheet_name)
    cols = _extract_spec_columns(spec)
    return [_s(_obj_get(c, "header", default="")) for c in cols]


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


# ---------------------------------------------------------------------------
# Data_Dictionary contract introspection
# ---------------------------------------------------------------------------
def data_dictionary_headers() -> List[str]:
    """Returns Data_Dictionary headers EXACTLY as defined in schema_registry."""
    return list(_get_sheet_headers("Data_Dictionary"))


def data_dictionary_keys() -> List[str]:
    """Returns Data_Dictionary keys EXACTLY as defined in schema_registry."""
    spec = _get_sheet_spec("Data_Dictionary")
    cols = _extract_spec_columns(spec)
    keys: List[str] = []

    for col in cols:
        key = _s(_obj_get(col, "key", default=""))
        if key:
            keys.append(key)

    return keys


def _data_dictionary_spec() -> Tuple[List[str], List[str]]:
    headers = data_dictionary_headers()
    keys = data_dictionary_keys()

    if not headers or not keys or len(headers) != len(keys):
        raise ValueError(
            "Data_Dictionary spec invalid: headers/keys missing or mismatched "
            f"(headers={len(headers)}, keys={len(keys)})"
        )

    return headers, keys


# ---------------------------------------------------------------------------
# Ordering (stable + schema-first)
# ---------------------------------------------------------------------------
def preferred_sheet_order() -> List[str]:
    """
    Stable ordering for dictionary generation.

    Priority:
      1) schema_registry.CANONICAL_SHEETS / CANONICAL_PAGES
      2) core.sheets.page_catalog.CANONICAL_PAGES
      3) schema_registry.list_sheets()
      4) sorted(schema registry keys)

    Only returns sheets present in the active schema registry map.
    """
    reg_keys = set(_schema_registry_map().keys())
    if not reg_keys:
        return []

    ordered: List[str] = []

    # 1) registry canonical view
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

    # 2) page_catalog canonical pages
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

    # 3) list_sheets
    listed = [s for s in _list_sheets() if s in reg_keys]
    if listed:
        for sheet in listed:
            if sheet not in ordered:
                ordered.append(sheet)
        for sheet in sorted(reg_keys):
            if sheet not in ordered:
                ordered.append(sheet)
        return ordered

    # 4) registry keys
    return sorted(reg_keys)


def _canonicalize_sheet_name(name: str) -> str:
    """
    Best-effort canonicalization. Avoids hard dependency on one exact
    page_catalog API shape.
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


# ---------------------------------------------------------------------------
# Core row mapping
# ---------------------------------------------------------------------------
def row_dict_from_column(sheet: str, col: Any) -> Dict[str, Any]:
    """
    Convert a ColumnSpec into a Data_Dictionary row dict.
    Dict keys MUST match Data_Dictionary schema keys exactly.
    """
    _, dd_keys = _data_dictionary_spec()

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

    out: Dict[str, Any] = {}
    for dd_key in dd_keys:
        lk = dd_key.strip().lower().replace(" ", "_")

        if dd_key in base:
            out[dd_key] = base[dd_key]
            continue
        if lk in base:
            out[dd_key] = base[lk]
            continue

        if lk in {"sheet", "page"}:
            out[dd_key] = base["sheet"]
        elif lk == "group":
            out[dd_key] = base["group"]
        elif lk in {"header", "column"}:
            out[dd_key] = base["header"]
        elif lk == "key":
            out[dd_key] = base["key"]
        elif lk in {"dtype", "type"}:
            out[dd_key] = base["dtype"]
        elif lk in {"format", "fmt"}:
            out[dd_key] = base["fmt"]
        elif lk in {"required", "is_required", "req"}:
            out[dd_key] = base["required"]
        elif lk == "source":
            out[dd_key] = base["source"]
        elif lk in {"notes", "note", "description"}:
            out[dd_key] = base["notes"]
        else:
            out[dd_key] = None

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
    Build Data_Dictionary as list[dict] rows with canonical 9-key shape.
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
        if not include_meta_sheet and sheet == "Data_Dictionary":
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
    Build a strict 2D array ready for Google Sheets:
    [ [headers...], [row1...], [row2...], ... ]

    Column order strictly follows Data_Dictionary schema keys.
    """
    headers, dd_keys = _data_dictionary_spec()
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
    Fast sanity checks for generated output.

    enforce_unique_sheet_key:
      - prevent duplicate (sheet,key)

    forbid_extra_keys:
      - rows must contain ONLY Data_Dictionary spec keys
    """
    _, dd_keys = _data_dictionary_spec()
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


# ---------------------------------------------------------------------------
# Optional self-check (fast) — enable only in CI / strict mode
# ---------------------------------------------------------------------------
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
    if not values:
        raise ValueError("Data_Dictionary values output is empty")
    if len(values[0]) != len(expected_headers):
        raise ValueError(
            f"Data_Dictionary values header width mismatch: expected {len(expected_headers)} got {len(values[0])}"
        )
    for idx, row in enumerate(values[1:], start=1):
        if len(row) != len(expected_headers):
            raise ValueError(
                f"Data_Dictionary row width mismatch at index {idx}: expected {len(expected_headers)} got {len(row)}"
            )


if _env_truthy("TFB_SCHEMA_SELFTEST", False):
    _self_check()
