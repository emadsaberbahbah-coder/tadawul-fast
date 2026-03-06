#!/usr/bin/env python3
# routes/data_dictionary.py
"""
================================================================================
Schema Data Dictionary Router — v1.2.0 (HARDENED / SHAPE-SAFE / APPS-SCRIPT FRIENDLY)
================================================================================

Endpoint:
  GET /v1/schema/data-dictionary

Purpose:
- Returns the authoritative Data_Dictionary using core/sheets/data_dictionary.py
- Never touches quote/enrichment engines
- Normalizes row/value output so response shape is stable for Apps Script, tests,
  and direct API debugging

What this revision fixes
- ✅ FIX: Prevents suspicious row-shape output (for example rows appearing as width 1)
- ✅ FIX: Supports object-style or dict-style schema specs safely
- ✅ FIX: Normalizes "rows" output to list[dict] using canonical keys
- ✅ FIX: Normalizes "values" output to a strict 2D matrix with consistent width
- ✅ FIX: Adds request metadata and defensive validation
- ✅ SAFE: Optional auth only, with open-mode bypass if available
- ✅ SAFE: No network calls at import time

Query params:
- format=rows   -> returns list[dict] (default)
- format=values -> returns 2D array including header row
- include_meta_sheet=true|false (default true)
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional

from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse

try:
    from fastapi.responses import ORJSONResponse as BestJSONResponse  # type: ignore
except Exception:  # pragma: no cover
    BestJSONResponse = JSONResponse  # type: ignore

from core.sheets.data_dictionary import (
    build_data_dictionary_rows,
    build_data_dictionary_values,
)

try:
    from core.sheets.data_dictionary import validate_data_dictionary_output  # type: ignore
except Exception:  # pragma: no cover
    validate_data_dictionary_output = None  # type: ignore

try:
    from core.sheets.schema_registry import SCHEMA_VERSION, get_sheet_spec  # type: ignore
except Exception:  # pragma: no cover
    SCHEMA_VERSION = "unknown"  # type: ignore
    get_sheet_spec = None  # type: ignore

# Optional auth helpers
try:
    from core.config import auth_ok, is_open_mode  # type: ignore
except Exception:  # pragma: no cover
    auth_ok = None  # type: ignore
    is_open_mode = None  # type: ignore


DATA_DICTIONARY_ROUTE_VERSION = "1.2.0"

router = APIRouter(prefix="/v1/schema", tags=["Schema"])


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _bool_q(v: Optional[str], default: bool) -> bool:
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


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


def _error(status_code: int, message: str, *, request_id: str) -> BestJSONResponse:
    return BestJSONResponse(
        status_code=status_code,
        content={
            "status": "error",
            "error": str(message),
            "request_id": request_id,
            "version": DATA_DICTIONARY_ROUTE_VERSION,
            "schema_version": SCHEMA_VERSION,
            "generated_at_utc": _now_utc(),
        },
    )


def _get_request_id(request: Optional[Request]) -> str:
    try:
        if request is not None:
            rid = request.headers.get("X-Request-ID")
            if rid:
                return str(rid).strip()
    except Exception:
        pass
    return "ddict"


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

    # If auth_ok is unavailable, treat as open for this helper route
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


def _get_headers_and_keys() -> tuple[List[str], List[str]]:
    if not callable(get_sheet_spec):
        raise RuntimeError("get_sheet_spec unavailable")

    spec = get_sheet_spec("Data_Dictionary")
    columns = _as_list(_obj_get(spec, "columns", []))

    headers: List[str] = []
    keys: List[str] = []

    for col in columns:
        headers.append(str(_obj_get(col, "header", "")))
        keys.append(str(_obj_get(col, "key", "")))

    if not headers or not keys or len(headers) != len(keys):
        raise RuntimeError("Data_Dictionary schema columns are invalid")

    return headers, keys


def _normalize_row_dict(row: Any, keys: List[str]) -> Dict[str, Any]:
    """
    Force one row to be a dict aligned to canonical keys.
    Handles:
    - already-correct dict rows
    - object rows with attributes
    - list/tuple rows (map by position)
    - scalar/string rows (place into first column only, rest null)
    """
    if isinstance(row, Mapping):
        return {k: row.get(k) for k in keys}

    # object-style row
    if hasattr(row, "__dict__"):
        return {k: getattr(row, k, None) for k in keys}

    # list/tuple row
    if isinstance(row, (list, tuple)):
        values = list(row)
        out: Dict[str, Any] = {}
        for i, key in enumerate(keys):
            out[key] = values[i] if i < len(values) else None
        return out

    # scalar fallback
    out = {k: None for k in keys}
    if keys:
        out[keys[0]] = row
    return out


def _rows_to_dicts(rows: Any, keys: List[str]) -> List[Dict[str, Any]]:
    src = _as_list(rows)
    return [_normalize_row_dict(row, keys) for row in src]


def _dicts_to_values(rows: List[Dict[str, Any]], keys: List[str], headers: List[str]) -> List[List[Any]]:
    values: List[List[Any]] = [list(headers)]
    for row in rows:
        values.append([row.get(k) for k in keys])
    return values


def _normalize_values_matrix(values: Any, expected_cols: int) -> List[List[Any]]:
    """
    Ensure a strict 2D array. Each row is padded/truncated to expected_cols.
    """
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


@router.get("/data-dictionary")
def get_data_dictionary(
    request: Request,
    format: str = Query(default="rows", description="rows (default) or values (2D array including header row)"),
    include_meta_sheet: Optional[str] = Query(default="true", description="Include Data_Dictionary sheet itself in rows"),
    token: Optional[str] = Query(default=None, description="Optional token if deployment enforces auth"),
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
        headers, keys = _get_headers_and_keys()
        generated_at_utc = _now_utc()

        if fmt == "values":
            raw_values = build_data_dictionary_values(
                include_header_row=True,
                include_meta_sheet=include_meta,
            )
            values = _normalize_values_matrix(raw_values, expected_cols=len(headers))

            # Ensure header row is canonical, regardless of builder behavior
            if values:
                values[0] = list(headers)
            else:
                values = [list(headers)]

            return BestJSONResponse(
                status_code=200,
                content={
                    "status": "success",
                    "format": "values",
                    "headers": headers,
                    "keys": keys,
                    "values": values,
                    "version": DATA_DICTIONARY_ROUTE_VERSION,
                    "schema_version": SCHEMA_VERSION,
                    "generated_at_utc": generated_at_utc,
                    "count": max(0, len(values) - 1),
                    "include_meta_sheet": include_meta,
                    "request_id": request_id,
                },
            )

        # fmt == "rows"
        raw_rows = build_data_dictionary_rows(include_meta_sheet=include_meta)
        rows = _rows_to_dicts(raw_rows, keys)

        # Optional strict validator if present
        if callable(validate_data_dictionary_output):
            try:
                validate_data_dictionary_output(rows, enforce_unique_sheet_key=True)
            except TypeError:
                validate_data_dictionary_output(rows)

        # Also publish row matrix to make shape visible/debuggable
        rows_matrix = [[row.get(k) for k in keys] for row in rows]

        return BestJSONResponse(
            status_code=200,
            content={
                "status": "success",
                "format": "rows",
                "headers": headers,
                "keys": keys,
                "rows": rows,
                "rows_matrix": rows_matrix,
                "version": DATA_DICTIONARY_ROUTE_VERSION,
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


__all__ = ["router", "DATA_DICTIONARY_ROUTE_VERSION"]
