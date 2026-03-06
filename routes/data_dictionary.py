#!/usr/bin/env python3
# routes/data_dictionary.py
"""
================================================================================
Schema Router — v2.0.0 (UNIFIED / SHEET-SPEC FIXED / DATA-DICTIONARY SAFE)
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
- ✅ FIX: /v1/schema/sheet-spec now returns real sheet specifications
- ✅ FIX: No accidental data-dictionary payload shape for sheet-spec
- ✅ FIX: Stable Apps-Script friendly response shapes
- ✅ FIX: Handles object-style / dict-style schema columns safely
- ✅ FIX: Adds optional filtering by sheet
- ✅ FIX: Adds consistent metadata across all schema endpoints
- ✅ SAFE: No quote engine / provider / network calls
- ✅ SAFE: Optional auth only, open-mode aware
- ✅ SAFE: Import-hardened for partial repo states

Response philosophy
-------------------
- "data-dictionary" returns flattened dictionary rows
- "sheet-spec" returns grouped per-sheet schema definitions
- "pages" returns supported pages and aliases metadata where available

Notes
-----
- This module is intentionally self-contained and defensive.
- It avoids assuming too much about helper signatures in other modules.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse

try:
    from fastapi.responses import ORJSONResponse as BestJSONResponse  # type: ignore
except Exception:  # pragma: no cover
    BestJSONResponse = JSONResponse  # type: ignore

# --------------------------------------------------------------------------------------
# Optional imports (kept defensive)
# --------------------------------------------------------------------------------------

try:
    from core.sheets.data_dictionary import (  # type: ignore
        build_data_dictionary_rows,
        build_data_dictionary_values,
    )
except Exception:  # pragma: no cover
    build_data_dictionary_rows = None  # type: ignore
    build_data_dictionary_values = None  # type: ignore

try:
    from core.sheets.data_dictionary import validate_data_dictionary_output  # type: ignore
except Exception:  # pragma: no cover
    validate_data_dictionary_output = None  # type: ignore

try:
    from core.sheets.schema_registry import SCHEMA_VERSION, get_sheet_spec  # type: ignore
except Exception:  # pragma: no cover
    SCHEMA_VERSION = "unknown"  # type: ignore
    get_sheet_spec = None  # type: ignore

# Optional page catalog helpers
try:
    from core.sheets.page_catalog import normalize_page_name as _normalize_page_name  # type: ignore
except Exception:  # pragma: no cover
    _normalize_page_name = None  # type: ignore

try:
    from core.sheets.page_catalog import PAGE_ALIASES as _PAGE_ALIASES  # type: ignore
except Exception:  # pragma: no cover
    _PAGE_ALIASES = None  # type: ignore

try:
    from core.sheets.page_catalog import SUPPORTED_PAGES as _SUPPORTED_PAGES  # type: ignore
except Exception:  # pragma: no cover
    _SUPPORTED_PAGES = None  # type: ignore

try:
    from core.sheets.page_catalog import PAGES as _PAGES  # type: ignore
except Exception:  # pragma: no cover
    _PAGES = None  # type: ignore

# Optional auth helpers
try:
    from core.config import auth_ok, is_open_mode  # type: ignore
except Exception:  # pragma: no cover
    auth_ok = None  # type: ignore
    is_open_mode = None  # type: ignore


SCHEMA_ROUTE_VERSION = "2.0.0"

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
    # Open mode bypass if available
    try:
        if callable(is_open_mode) and bool(is_open_mode()):
            return True
    except Exception:
        pass

    # If auth helper is unavailable, keep schema endpoints open
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
    except Exception:
        return False


# --------------------------------------------------------------------------------------
# Page catalog helpers
# --------------------------------------------------------------------------------------

def _normalize_page_candidate(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    raw = str(name).strip()
    if not raw:
        return None

    if callable(_normalize_page_name):
        try:
            out = _normalize_page_name(raw)
            if out:
                return str(out)
        except Exception:
            pass

    # direct canonical match
    for p in _get_all_pages():
        if raw == p:
            return p

    # case-insensitive direct match
    lower_raw = raw.lower()
    for p in _get_all_pages():
        if p.lower() == lower_raw:
            return p

    # aliases fallback
    aliases = _get_page_aliases()
    canonical = aliases.get(lower_raw)
    if canonical:
        return canonical

    return raw


def _get_page_aliases() -> Dict[str, str]:
    aliases: Dict[str, str] = {}

    if isinstance(_PAGE_ALIASES, Mapping):
        for k, v in _PAGE_ALIASES.items():
            try:
                aliases[str(k).strip().lower()] = str(v).strip()
            except Exception:
                continue

    # Soft fallbacks for common display variants
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
            try:
                pages.append(str(item))
            except Exception:
                continue
        return pages

    return pages


def _get_all_pages() -> List[str]:
    pages: List[str] = []

    for candidate in (_SUPPORTED_PAGES, _PAGES):
        pages.extend(_extract_pages_from_object(candidate))

    if not pages:
        pages = list(_CANONICAL_PAGES_FALLBACK)

    # Keep canonical order when possible
    uniq: List[str] = []
    seen = set()
    for p in pages + _CANONICAL_PAGES_FALLBACK:
        if not p:
            continue
        s = str(p).strip()
        if s and s not in seen:
            seen.add(s)
            uniq.append(s)

    return uniq


# --------------------------------------------------------------------------------------
# Schema normalization helpers
# --------------------------------------------------------------------------------------

def _normalize_column(col: Any) -> Dict[str, Any]:
    return {
        "group": _obj_get(col, "group"),
        "header": _obj_get(col, "header"),
        "key": _obj_get(col, "key"),
        "dtype": _obj_get(col, "dtype"),
        "fmt": _obj_get(col, "fmt"),
        "required": _obj_get(col, "required"),
        "source": _obj_get(col, "source"),
        "notes": _obj_get(col, "notes"),
    }


def _normalize_spec_to_columns(spec: Any) -> List[Dict[str, Any]]:
    if spec is None:
        return []

    # Most likely shape: spec.columns
    cols = _obj_get(spec, "columns")
    if cols is not None:
        return [_normalize_column(c) for c in _as_list(cols)]

    # Sometimes the spec itself may be a list
    if isinstance(spec, (list, tuple)):
        return [_normalize_column(c) for c in spec]

    # Possible dict-style fallback
    for alt in ("fields", "schema"):
        cols = _obj_get(spec, alt)
        if cols is not None:
            return [_normalize_column(c) for c in _as_list(cols)]

    return []


def _normalize_sheet_spec(sheet_name: str, spec: Any) -> Dict[str, Any]:
    columns = _normalize_spec_to_columns(spec)
    headers = [str(c.get("header") or "") for c in columns]
    keys = [str(c.get("key") or "") for c in columns]

    return {
        "sheet": sheet_name,
        "headers": headers,
        "keys": keys,
        "columns": columns,
        "column_count": len(columns),
    }


def _get_sheet_spec_safe(sheet_name: str) -> Dict[str, Any]:
    if not callable(get_sheet_spec):
        raise RuntimeError("get_sheet_spec unavailable")

    spec = get_sheet_spec(sheet_name)
    if spec is None:
        raise KeyError(f"Unknown sheet: {sheet_name}")

    return _normalize_sheet_spec(sheet_name, spec)


def _get_all_sheet_specs() -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for page in _get_all_pages():
        try:
            grouped[page] = _get_sheet_spec_safe(page)
        except Exception:
            # Keep route resilient even if one spec is broken
            continue
    return grouped


# --------------------------------------------------------------------------------------
# Data dictionary helpers
# --------------------------------------------------------------------------------------

def _get_data_dictionary_headers_and_keys() -> Tuple[List[str], List[str]]:
    spec = _get_sheet_spec_safe("Data_Dictionary")
    headers = list(spec["headers"])
    keys = list(spec["keys"])

    if not headers or not keys or len(headers) != len(keys):
        raise RuntimeError("Data_Dictionary schema columns are invalid")

    return headers, keys


def _normalize_row_dict(row: Any, keys: Sequence[str]) -> Dict[str, Any]:
    """
    Force one row into a dict aligned to canonical keys.
    Handles:
    - dict rows
    - object rows
    - list/tuple rows (mapped by position)
    - scalar/string rows (placed into first key only)
    """
    if isinstance(row, Mapping):
        return {k: row.get(k) for k in keys}

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


def _build_data_dictionary_rows_safe(include_meta_sheet: bool) -> List[Dict[str, Any]]:
    headers, keys = _get_data_dictionary_headers_and_keys()

    if callable(build_data_dictionary_rows):
        raw_rows = build_data_dictionary_rows(include_meta_sheet=include_meta_sheet)
        rows = _rows_to_dicts(raw_rows, keys)
    else:
        # Fallback: derive from all sheet specs directly
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
            validate_data_dictionary_output(rows)

    # Keep canonical field order
    ordered_rows: List[Dict[str, Any]] = []
    for row in rows:
        ordered_rows.append({k: row.get(k) for k in keys})

    return ordered_rows


def _build_data_dictionary_values_safe(include_meta_sheet: bool) -> List[List[Any]]:
    headers, keys = _get_data_dictionary_headers_and_keys()

    if callable(build_data_dictionary_values):
        raw_values = build_data_dictionary_values(
            include_header_row=True,
            include_meta_sheet=include_meta_sheet,
        )
        values = _normalize_values_matrix(raw_values, expected_cols=len(headers))
        if values:
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
            payload = {name: spec["headers"] for name, spec in specs.items()}
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

        return BestJSONResponse(
            status_code=200,
            content={
                "status": "success",
                "endpoint": "sheet-spec",
                "format": fmt,
                "sheet": normalized_sheet,
                "count": len(specs),
                "pages": list(specs.keys()),
                "aliases": page_aliases,
                "data": payload,
                "version": SCHEMA_ROUTE_VERSION,
                "schema_version": SCHEMA_VERSION,
                "generated_at_utc": generated_at_utc,
                "request_id": request_id,
            },
        )

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
