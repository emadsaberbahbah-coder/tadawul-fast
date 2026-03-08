#!/usr/bin/env python3
# routes/data_dictionary.py
"""
================================================================================
Schema Router — v2.1.0 (UNIFIED / REGISTRY-FIRST / DATA-DICTIONARY FALLBACK)
================================================================================

Endpoints
---------
GET /v1/schema/data-dictionary
GET /v1/schema/sheet-spec
GET /v1/schema/pages

Purpose
-------
Single schema router that exposes:
- authoritative Data_Dictionary output
- true sheet-spec output (grouped by sheet, not flattened dictionary rows)
- supported pages list / aliases metadata

Why this revision
-----------------
- ✅ FIX: /v1/schema/sheet-spec is no longer dependent on one fragile import path
- ✅ FIX: Falls back to Data_Dictionary builders if schema_registry helpers are partial
- ✅ FIX: /v1/schema/pages always returns extractable pages/items/results arrays
- ✅ FIX: Handles dict-style, object-style, and list-style schema definitions safely
- ✅ FIX: Keeps Apps-Script friendly response shapes and consistent metadata
- ✅ SAFE: No quote engine / provider / network calls
- ✅ SAFE: Optional auth only, open-mode aware
- ✅ SAFE: Import-hardened for partial repo states
"""

from __future__ import annotations

import importlib
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse

try:
    from fastapi.responses import ORJSONResponse as BestJSONResponse  # type: ignore
except Exception:  # pragma: no cover
    BestJSONResponse = JSONResponse  # type: ignore


SCHEMA_ROUTE_VERSION = "2.1.0"

router = APIRouter(prefix="/v1/schema", tags=["Schema"])


# --------------------------------------------------------------------------------------
# Canonical fallbacks
# --------------------------------------------------------------------------------------

_CANONICAL_PAGES_FALLBACK: List[str] = [
    "Market_Leaders",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
    "My_Portfolio",
    "Insights_Analysis",
    "Top_10_Investments",
    "Data_Dictionary",
]

_DATA_DICTIONARY_HEADERS: List[str] = [
    "sheet",
    "group",
    "header",
    "key",
    "dtype",
    "fmt",
    "required",
    "source",
    "notes",
]

_DATA_DICTIONARY_KEYS: List[str] = list(_DATA_DICTIONARY_HEADERS)


# --------------------------------------------------------------------------------------
# Optional modules (kept defensive)
# --------------------------------------------------------------------------------------

def _safe_import(module_name: str) -> Any:
    try:
        return importlib.import_module(module_name)
    except Exception:  # pragma: no cover
        return None


_schema_registry_mod = _safe_import("core.sheets.schema_registry")
_data_dictionary_mod = _safe_import("core.sheets.data_dictionary")
_page_catalog_mod = _safe_import("core.sheets.page_catalog")
_config_mod = _safe_import("core.config")


def _resolve_attr(module: Any, *names: str, default: Any = None) -> Any:
    if module is None:
        return default
    for name in names:
        try:
            if hasattr(module, name):
                return getattr(module, name)
        except Exception:
            continue
    return default


SCHEMA_VERSION = _resolve_attr(_schema_registry_mod, "SCHEMA_VERSION", default="unknown")

build_data_dictionary_rows = _resolve_attr(_data_dictionary_mod, "build_data_dictionary_rows")
build_data_dictionary_values = _resolve_attr(_data_dictionary_mod, "build_data_dictionary_values")
validate_data_dictionary_output = _resolve_attr(_data_dictionary_mod, "validate_data_dictionary_output")

_get_sheet_spec_callable = _resolve_attr(
    _schema_registry_mod,
    "get_sheet_spec",
    "get_schema_for_sheet",
    "get_sheet_schema",
)

_normalize_page_name = _resolve_attr(_page_catalog_mod, "normalize_page_name")
_PAGE_ALIASES = _resolve_attr(_page_catalog_mod, "PAGE_ALIASES")
_SUPPORTED_PAGES = _resolve_attr(_page_catalog_mod, "SUPPORTED_PAGES")
_PAGES = _resolve_attr(_page_catalog_mod, "PAGES")

auth_ok = _resolve_attr(_config_mod, "auth_ok")
is_open_mode = _resolve_attr(_config_mod, "is_open_mode")


# --------------------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------------------

def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _obj_get(obj: Any, name: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, Mapping):
        return obj.get(name, default)
    return getattr(obj, name, default)


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


def _bool_q(v: Optional[str], default: bool) -> bool:
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _get_request_id(request: Optional[Request]) -> str:
    try:
        if request is not None:
            rid = request.headers.get("X-Request-ID")
            if rid:
                return str(rid).strip()
    except Exception:
        pass
    return "schema"


def _error(status_code: int, message: str, *, request_id: str) -> BestJSONResponse:
    return BestJSONResponse(
        status_code=status_code,
        content={
            "status": "error",
            "error": str(message),
            "request_id": request_id,
            "version": SCHEMA_ROUTE_VERSION,
            "schema_version": SCHEMA_VERSION,
            "generated_at_utc": _now_utc(),
        },
    )


def _auth_passed(
    *,
    token: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
) -> bool:
    try:
        if callable(is_open_mode) and bool(is_open_mode()):
            return True
    except Exception:
        pass

    if auth_ok is None:
        return True

    auth_token = (x_app_token or "").strip()
    authz = (authorization or "").strip()

    if authz.lower().startswith("bearer "):
        auth_token = authz.split(" ", 1)[1].strip()
    elif token and not auth_token:
        auth_token = token.strip()

    try:
        return bool(
            auth_ok(
                token=auth_token or None,
                authorization=authorization,
                headers={
                    "X-APP-TOKEN": x_app_token,
                    "Authorization": authorization,
                },
            )
        )
    except TypeError:
        try:
            return bool(auth_ok(auth_token or None))
        except Exception:
            return False
    except Exception:
        return False


def _dedupe_keep_order(values: Iterable[Any]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        try:
            s = str(value).strip()
        except Exception:
            continue
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


# --------------------------------------------------------------------------------------
# Page catalog helpers
# --------------------------------------------------------------------------------------

def _get_page_aliases() -> Dict[str, str]:
    aliases: Dict[str, str] = {}

    if isinstance(_PAGE_ALIASES, Mapping):
        for k, v in _PAGE_ALIASES.items():
            try:
                aliases[str(k).strip().lower()] = str(v).strip()
            except Exception:
                continue

    for p in _CANONICAL_PAGES_FALLBACK:
        aliases[p.lower()] = p
        aliases[p.replace("_", " ").lower()] = p

    aliases.setdefault("market leaders", "Market_Leaders")
    aliases.setdefault("global markets", "Global_Markets")
    aliases.setdefault("commodities fx", "Commodities_FX")
    aliases.setdefault("commodities & fx", "Commodities_FX")
    aliases.setdefault("mutual funds", "Mutual_Funds")
    aliases.setdefault("my portfolio", "My_Portfolio")
    aliases.setdefault("insights analysis", "Insights_Analysis")
    aliases.setdefault("insights & analysis", "Insights_Analysis")
    aliases.setdefault("top 10 investments", "Top_10_Investments")
    aliases.setdefault("data dictionary", "Data_Dictionary")

    return aliases


def _normalize_page_candidate(name: Optional[str]) -> Optional[str]:
    if not name:
        return None

    raw = str(name).strip()
    if not raw:
        return None

    if callable(_normalize_page_name):
        try:
            normalized = _normalize_page_name(raw)
            if normalized:
                return str(normalized)
        except Exception:
            pass

    for page in _get_all_pages():
        if raw == page:
            return page
        if raw.lower() == page.lower():
            return page

    aliases = _get_page_aliases()
    return aliases.get(raw.lower(), raw)


def _extract_pages_from_object(obj: Any) -> List[str]:
    pages: List[str] = []

    if obj is None:
        return pages

    if isinstance(obj, Mapping):
        for k in obj.keys():
            try:
                pages.append(str(k))
            except Exception:
                continue
        return pages

    if isinstance(obj, (list, tuple, set)):
        for item in obj:
            if isinstance(item, Mapping):
                candidate = (
                    item.get("page")
                    or item.get("sheet")
                    or item.get("name")
                    or item.get("id")
                )
                if candidate:
                    pages.append(str(candidate))
            else:
                try:
                    pages.append(str(item))
                except Exception:
                    continue
        return pages

    return pages


def _raw_data_dictionary_rows() -> List[Any]:
    if not callable(build_data_dictionary_rows):
        return []

    try:
        return _as_list(build_data_dictionary_rows(include_meta_sheet=True))
    except TypeError:
        try:
            return _as_list(build_data_dictionary_rows())
        except Exception:
            return []
    except Exception:
        return []


def _infer_specs_from_data_dictionary() -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}

    for row in _raw_data_dictionary_rows():
        sheet_name = (
            _obj_get(row, "sheet")
            or _obj_get(row, "Sheet")
            or _obj_get(row, "page")
            or _obj_get(row, "Page")
        )
        if not sheet_name:
            continue

        header = _obj_get(row, "header") or _obj_get(row, "Header")
        key = _obj_get(row, "key") or _obj_get(row, "Key")

        col = {
            "group": _obj_get(row, "group") or _obj_get(row, "Group"),
            "header": header,
            "key": key,
            "dtype": _obj_get(row, "dtype") or _obj_get(row, "type") or _obj_get(row, "Type"),
            "fmt": _obj_get(row, "fmt") or _obj_get(row, "format") or _obj_get(row, "Format"),
            "required": _obj_get(row, "required") or _obj_get(row, "is_required"),
            "source": _obj_get(row, "source") or _obj_get(row, "Source"),
            "notes": _obj_get(row, "notes") or _obj_get(row, "description") or _obj_get(row, "Notes"),
        }

        grouped.setdefault(str(sheet_name), []).append(col)

    specs: Dict[str, Dict[str, Any]] = {}
    for sheet_name, columns in grouped.items():
        specs[sheet_name] = _normalize_sheet_spec(sheet_name, columns)
    return specs


def _schema_registry_mapping() -> Dict[str, Any]:
    if _schema_registry_mod is None:
        return {}

    for attr_name in (
        "SCHEMA_REGISTRY",
        "SHEET_SCHEMAS",
        "SCHEMA_BY_SHEET",
        "SHEETS",
        "SHEET_REGISTRY",
        "PAGE_SCHEMAS",
    ):
        value = _resolve_attr(_schema_registry_mod, attr_name)
        if isinstance(value, Mapping):
            return dict(value)

    return {}


def _registry_pages() -> List[str]:
    pages: List[str] = []

    for candidate in (_SUPPORTED_PAGES, _PAGES):
        pages.extend(_extract_pages_from_object(candidate))

    registry_map = _schema_registry_mapping()
    pages.extend(_extract_pages_from_object(registry_map))

    inferred_specs = _infer_specs_from_data_dictionary()
    pages.extend(_extract_pages_from_object(inferred_specs))

    pages.extend(_CANONICAL_PAGES_FALLBACK)

    return _dedupe_keep_order(pages)


def _get_all_pages() -> List[str]:
    return _registry_pages()


# --------------------------------------------------------------------------------------
# Schema normalization helpers
# --------------------------------------------------------------------------------------

def _normalize_column(col: Any) -> Dict[str, Any]:
    group = _obj_get(col, "group")
    header = _obj_get(col, "header")
    key = _obj_get(col, "key")
    dtype = _obj_get(col, "dtype")
    fmt = _obj_get(col, "fmt")
    required = _obj_get(col, "required")
    source = _obj_get(col, "source")
    notes = _obj_get(col, "notes")

    if isinstance(col, Mapping):
        group = col.get("group", group)
        header = col.get("header", header)
        key = col.get("key", key)
        dtype = col.get("dtype", col.get("type", dtype))
        fmt = col.get("fmt", col.get("format", fmt))
        required = col.get("required", col.get("is_required", required))
        source = col.get("source", source)
        notes = col.get("notes", col.get("description", notes))

    return {
        "group": group,
        "header": header,
        "key": key,
        "dtype": dtype,
        "fmt": fmt,
        "required": required,
        "source": source,
        "notes": notes,
    }


def _headers_keys_to_columns(headers: Sequence[Any], keys: Sequence[Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    max_len = max(len(headers), len(keys))
    for i in range(max_len):
        header = headers[i] if i < len(headers) else None
        key = keys[i] if i < len(keys) else None
        out.append(
            {
                "group": None,
                "header": header,
                "key": key or header,
                "dtype": None,
                "fmt": None,
                "required": None,
                "source": None,
                "notes": None,
            }
        )
    return out


def _normalize_spec_to_columns(spec: Any) -> List[Dict[str, Any]]:
    if spec is None:
        return []

    cols = _obj_get(spec, "columns")
    if cols is not None:
        return [_normalize_column(c) for c in _as_list(cols)]

    if isinstance(spec, (list, tuple)):
        if spec and all(isinstance(x, (str, bytes)) for x in spec):
            headers = [str(x) for x in spec]
            return _headers_keys_to_columns(headers, headers)
        return [_normalize_column(c) for c in spec]

    if isinstance(spec, Mapping):
        if "columns" in spec:
            return [_normalize_column(c) for c in _as_list(spec.get("columns"))]
        if "headers" in spec or "keys" in spec:
            headers = [str(x) for x in _as_list(spec.get("headers"))]
            keys = [str(x) for x in _as_list(spec.get("keys"))]
            return _headers_keys_to_columns(headers, keys or headers)
        for alt in ("fields", "schema"):
            if alt in spec:
                return [_normalize_column(c) for c in _as_list(spec.get(alt))]

    for alt in ("fields", "schema"):
        cols = _obj_get(spec, alt)
        if cols is not None:
            return [_normalize_column(c) for c in _as_list(cols)]

    headers = _as_list(_obj_get(spec, "headers"))
    keys = _as_list(_obj_get(spec, "keys"))
    if headers or keys:
        return _headers_keys_to_columns(
            [str(x) for x in headers],
            [str(x) for x in (keys or headers)],
        )

    return []


def _normalize_sheet_spec(sheet_name: str, spec: Any) -> Dict[str, Any]:
    columns = _normalize_spec_to_columns(spec)
    headers = [str(c.get("header") or "") for c in columns]
    keys = [str(c.get("key") or c.get("header") or "") for c in columns]

    return {
        "sheet": sheet_name,
        "headers": headers,
        "keys": keys,
        "columns": columns,
        "column_count": len(columns),
    }


def _find_spec_in_registry(sheet_name: str) -> Any:
    registry_map = _schema_registry_mapping()
    if registry_map:
        for candidate in (
            sheet_name,
            sheet_name.lower(),
            sheet_name.upper(),
        ):
            if candidate in registry_map:
                return registry_map[candidate]

        for k, v in registry_map.items():
            try:
                if str(k).strip().lower() == sheet_name.lower():
                    return v
            except Exception:
                continue

    if callable(_get_sheet_spec_callable):
        try:
            return _get_sheet_spec_callable(sheet_name)
        except Exception:
            pass

    return None


def _get_sheet_spec_safe(sheet_name: str) -> Dict[str, Any]:
    spec = _find_spec_in_registry(sheet_name)
    if spec is not None:
        normalized = _normalize_sheet_spec(sheet_name, spec)
        if normalized["column_count"] > 0:
            return normalized

    inferred_specs = _infer_specs_from_data_dictionary()
    if sheet_name in inferred_specs:
        return inferred_specs[sheet_name]

    for name, spec in inferred_specs.items():
        if str(name).lower() == sheet_name.lower():
            return spec

    raise KeyError(f"Unknown sheet: {sheet_name}")


def _get_all_sheet_specs() -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for page in _get_all_pages():
        try:
            grouped[page] = _get_sheet_spec_safe(page)
        except Exception:
            continue
    return grouped


# --------------------------------------------------------------------------------------
# Data dictionary helpers
# --------------------------------------------------------------------------------------

def _get_data_dictionary_headers_and_keys() -> Tuple[List[str], List[str]]:
    try:
        spec = _get_sheet_spec_safe("Data_Dictionary")
        headers = list(spec["headers"])
        keys = list(spec["keys"])
        if headers and keys and len(headers) == len(keys):
            return headers, keys
    except Exception:
        pass

    return list(_DATA_DICTIONARY_HEADERS), list(_DATA_DICTIONARY_KEYS)


def _normalize_row_dict(row: Any, keys: Sequence[str]) -> Dict[str, Any]:
    if isinstance(row, Mapping):
        out: Dict[str, Any] = {}
        for k in keys:
            out[k] = row.get(k)
            if out[k] is None:
                out[k] = row.get(k.title())
        return out

    if hasattr(row, "__dict__"):
        return {k: getattr(row, k, None) for k in keys}

    if isinstance(row, (list, tuple)):
        values = list(row)
        return {k: (values[i] if i < len(values) else None) for i, k in enumerate(keys)}

    out = {k: None for k in keys}
    if keys:
        out[str(keys[0])] = row
    return out


def _rows_to_dicts(rows: Any, keys: Sequence[str]) -> List[Dict[str, Any]]:
    return [_normalize_row_dict(row, keys) for row in _as_list(rows)]


def _normalize_values_matrix(values: Any, expected_cols: int) -> List[List[Any]]:
    raw_rows = _as_list(values)
    normalized: List[List[Any]] = []

    for row in raw_rows:
        if isinstance(row, list):
            cur = list(row)
        elif isinstance(row, tuple):
            cur = list(row)
        elif isinstance(row, Mapping):
            cur = list(row.values())
        else:
            cur = [row]

        if len(cur) < expected_cols:
            cur = cur + [None] * (expected_cols - len(cur))
        elif len(cur) > expected_cols:
            cur = cur[:expected_cols]

        normalized.append(cur)

    return normalized


def _first_row_matches_headers(values: List[List[Any]], headers: Sequence[str]) -> bool:
    if not values:
        return False
    first = [str(x).strip() if x is not None else "" for x in values[0]]
    target = [str(x).strip() for x in headers]
    return first == target


def _build_data_dictionary_rows_safe(include_meta_sheet: bool) -> List[Dict[str, Any]]:
    _, keys = _get_data_dictionary_headers_and_keys()

    if callable(build_data_dictionary_rows):
        try:
            raw_rows = build_data_dictionary_rows(include_meta_sheet=include_meta_sheet)
        except TypeError:
            raw_rows = build_data_dictionary_rows()
        rows = _rows_to_dicts(raw_rows, keys)
    else:
        rows = []
        all_specs = _get_all_sheet_specs()
        for sheet_name, spec in all_specs.items():
            if not include_meta_sheet and sheet_name == "Data_Dictionary":
                continue
            for col in spec["columns"]:
                rows.append(
                    {
                        "sheet": sheet_name,
                        "group": col.get("group"),
                        "header": col.get("header"),
                        "key": col.get("key"),
                        "dtype": col.get("dtype"),
                        "fmt": col.get("fmt"),
                        "required": col.get("required"),
                        "source": col.get("source"),
                        "notes": col.get("notes"),
                    }
                )

    if callable(validate_data_dictionary_output):
        try:
            validate_data_dictionary_output(rows, enforce_unique_sheet_key=True)
        except TypeError:
            try:
                validate_data_dictionary_output(rows)
            except Exception:
                pass
        except Exception:
            pass

    ordered_rows: List[Dict[str, Any]] = []
    for row in rows:
        if not include_meta_sheet and str(row.get("sheet") or "") == "Data_Dictionary":
            continue
        ordered_rows.append({k: row.get(k) for k in keys})

    return ordered_rows


def _build_data_dictionary_values_safe(include_meta_sheet: bool) -> List[List[Any]]:
    headers, keys = _get_data_dictionary_headers_and_keys()

    if callable(build_data_dictionary_values):
        try:
            raw_values = build_data_dictionary_values(
                include_header_row=True,
                include_meta_sheet=include_meta_sheet,
            )
        except TypeError:
            raw_values = build_data_dictionary_values()

        values = _normalize_values_matrix(raw_values, expected_cols=len(headers))
        if not _first_row_matches_headers(values, headers):
            values = [list(headers)] + values
        elif values:
            values[0] = list(headers)
        else:
            values = [list(headers)]
        return values

    rows = _build_data_dictionary_rows_safe(include_meta_sheet=include_meta_sheet)
    return [list(headers)] + [[row.get(k) for k in keys] for row in rows]


# --------------------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------------------

@router.get("/data-dictionary")
def get_data_dictionary(
    request: Request,
    format: str = Query(
        default="rows",
        description="rows (default) or values (2D array including header row)",
    ),
    include_meta_sheet: Optional[str] = Query(
        default="true",
        description="Include Data_Dictionary sheet itself in rows",
    ),
    token: Optional[str] = Query(default=None, description="Optional token if auth is enabled"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> BestJSONResponse:
    request_id = _get_request_id(request)

    if not _auth_passed(token=token, x_app_token=x_app_token, authorization=authorization):
        return _error(401, "Invalid token", request_id=request_id)

    include_meta = _bool_q(include_meta_sheet, True)
    fmt = (format or "rows").strip().lower()

    if fmt not in {"rows", "values"}:
        return _error(400, "format must be 'rows' or 'values'", request_id=request_id)

    try:
        headers, keys = _get_data_dictionary_headers_and_keys()
        generated_at_utc = _now_utc()

        if fmt == "values":
            values = _build_data_dictionary_values_safe(include_meta_sheet=include_meta)
            return BestJSONResponse(
                status_code=200,
                content={
                    "status": "success",
                    "endpoint": "data-dictionary",
                    "format": "values",
                    "headers": headers,
                    "keys": keys,
                    "values": values,
                    "version": SCHEMA_ROUTE_VERSION,
                    "schema_version": SCHEMA_VERSION,
                    "generated_at_utc": generated_at_utc,
                    "count": max(0, len(values) - 1),
                    "include_meta_sheet": include_meta,
                    "request_id": request_id,
                },
            )

        rows = _build_data_dictionary_rows_safe(include_meta_sheet=include_meta)
        rows_matrix = [[row.get(k) for k in keys] for row in rows]

        return BestJSONResponse(
            status_code=200,
            content={
                "status": "success",
                "endpoint": "data-dictionary",
                "format": "rows",
                "headers": headers,
                "keys": keys,
                "rows": rows,
                "rows_matrix": rows_matrix,
                "version": SCHEMA_ROUTE_VERSION,
                "schema_version": SCHEMA_VERSION,
                "generated_at_utc": generated_at_utc,
                "count": len(rows),
                "include_meta_sheet": include_meta,
                "request_id": request_id,
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        return _error(500, f"{type(e).__name__}: {e}", request_id=request_id)


@router.get("/sheet-spec")
def get_sheet_spec_route(
    request: Request,
    sheet: Optional[str] = Query(
        default=None,
        description="Optional canonical sheet name or alias. If omitted, all sheets are returned.",
    ),
    format: str = Query(
        default="grouped",
        description="grouped (default), flat, headers, or keys",
    ),
    token: Optional[str] = Query(default=None, description="Optional token if auth is enabled"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> BestJSONResponse:
    request_id = _get_request_id(request)

    if not _auth_passed(token=token, x_app_token=x_app_token, authorization=authorization):
        return _error(401, "Invalid token", request_id=request_id)

    fmt = (format or "grouped").strip().lower()
    if fmt not in {"grouped", "flat", "headers", "keys"}:
        return _error(
            400,
            "format must be 'grouped', 'flat', 'headers', or 'keys'",
            request_id=request_id,
        )

    try:
        generated_at_utc = _now_utc()
        normalized_sheet = _normalize_page_candidate(sheet)

        if normalized_sheet:
            spec = _get_sheet_spec_safe(normalized_sheet)
            specs = {normalized_sheet: spec}
        else:
            specs = _get_all_sheet_specs()

        if not specs:
            return _error(404, "No sheet specifications available", request_id=request_id)

        page_aliases = _get_page_aliases()

        if fmt == "headers":
            payload: Any = {name: spec["headers"] for name, spec in specs.items()}
        elif fmt == "keys":
            payload = {name: spec["keys"] for name, spec in specs.items()}
        elif fmt == "flat":
            flat_rows: List[Dict[str, Any]] = []
            for name, spec in specs.items():
                for col in spec["columns"]:
                    flat_rows.append(
                        {
                            "sheet": name,
                            "group": col.get("group"),
                            "header": col.get("header"),
                            "key": col.get("key"),
                            "dtype": col.get("dtype"),
                            "fmt": col.get("fmt"),
                            "required": col.get("required"),
                            "source": col.get("source"),
                            "notes": col.get("notes"),
                        }
                    )
            payload = flat_rows
        else:
            payload = specs

        response: Dict[str, Any] = {
            "status": "success",
            "endpoint": "sheet-spec",
            "format": fmt,
            "sheet": normalized_sheet,
            "count": len(specs),
            "pages": list(specs.keys()),
            "items": list(specs.keys()),
            "results": list(specs.keys()),
            "aliases": page_aliases,
            "data": payload,
            "version": SCHEMA_ROUTE_VERSION,
            "schema_version": SCHEMA_VERSION,
            "generated_at_utc": generated_at_utc,
            "request_id": request_id,
        }

        if normalized_sheet and normalized_sheet in specs:
            response["headers"] = specs[normalized_sheet]["headers"]
            response["keys"] = specs[normalized_sheet]["keys"]
            response["columns"] = specs[normalized_sheet]["columns"]
            response["column_count"] = specs[normalized_sheet]["column_count"]

        return BestJSONResponse(status_code=200, content=response)

    except KeyError as e:
        return _error(404, str(e), request_id=request_id)
    except HTTPException:
        raise
    except Exception as e:
        return _error(500, f"{type(e).__name__}: {e}", request_id=request_id)


@router.get("/pages")
def get_schema_pages(
    request: Request,
    include_aliases: Optional[str] = Query(
        default="true",
        description="Include aliases mapping in the response",
    ),
    token: Optional[str] = Query(default=None, description="Optional token if auth is enabled"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> BestJSONResponse:
    request_id = _get_request_id(request)

    if not _auth_passed(token=token, x_app_token=x_app_token, authorization=authorization):
        return _error(401, "Invalid token", request_id=request_id)

    try:
        pages = _get_all_pages()
        aliases = _get_page_aliases()
        include_alias_map = _bool_q(include_aliases, True)

        payload: Dict[str, Any] = {
            "status": "success",
            "endpoint": "pages",
            "pages": pages,
            "items": pages,
            "results": pages,
            "count": len(pages),
            "version": SCHEMA_ROUTE_VERSION,
            "schema_version": SCHEMA_VERSION,
            "generated_at_utc": _now_utc(),
            "request_id": request_id,
        }

        if include_alias_map:
            payload["aliases"] = aliases

        return BestJSONResponse(status_code=200, content=payload)

    except HTTPException:
        raise
    except Exception as e:
        return _error(500, f"{type(e).__name__}: {e}", request_id=request_id)


__all__ = ["router", "SCHEMA_ROUTE_VERSION"]
