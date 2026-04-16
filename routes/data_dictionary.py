#!/usr/bin/env python3
# routes/data_dictionary.py
"""
================================================================================
Schema Router — v2.5.0 (UNIFIED / REGISTRY-FIRST / FORMAT-COMPATIBLE)
================================================================================

Endpoints
---------
GET /v1/schema/data-dictionary
GET /v1/schema/sheet-spec
GET /v1/schema/spec                  (alias of /sheet-spec)
GET /v1/schema/pages
GET /v1/schema/health

Purpose
-------
Single schema router that exposes:
- authoritative Data_Dictionary output
- true sheet-spec output (grouped per sheet, never flattened by mistake)
- supported pages and aliases metadata
- stable, PowerShell / Apps Script / API-friendly response contracts

Why this revision
-----------------
- ✅ FIX: accepts format=json for backward compatibility in /data-dictionary
          and /sheet-spec /spec so PowerShell sweeps do not fail with 400
- ✅ FIX: /v1/schema/sheet-spec works with sheet / page / sheet_name / name /
          tab / worksheet aliases
- ✅ FIX: /v1/schema/spec remains a full alias of /sheet-spec
- ✅ FIX: single-sheet requests always project top-level headers/keys/columns
- ✅ FIX: grouped all-sheets response is stable and JSON-safe
- ✅ FIX: registry-first resolution with graceful Data_Dictionary inference fallback
- ✅ FIX: handles dict-style, object-style, list-style, headers/keys-style schema safely
- ✅ FIX: safer auth calling across multiple core.config.auth_ok signatures
- ✅ SAFE: no quote engine / provider / network calls
- ✅ SAFE: import-hardened for partial repo states
================================================================================
"""

from __future__ import annotations

import importlib
import os
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse

try:
    from fastapi.responses import ORJSONResponse as BestJSONResponse  # type: ignore
except Exception:  # pragma: no cover
    BestJSONResponse = JSONResponse  # type: ignore


SCHEMA_ROUTE_VERSION = "2.6.0"
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

_FORBIDDEN_PAGES_FALLBACK: List[str] = [
    "Advisor_Criteria",
    "KSA_Tadawul",
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

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled"}

_DATA_DICTIONARY_FORMAT_ALIASES: Dict[str, str] = {
    "rows": "rows",
    "row": "rows",
    "json": "rows",
    "records": "rows",
    "record": "rows",
    "table": "rows",
    "dict": "rows",
    "objects": "rows",
    "values": "values",
    "value": "values",
    "matrix": "values",
    "2d": "values",
    "2d-array": "values",
    "array": "values",
}

_SHEET_SPEC_FORMAT_ALIASES: Dict[str, str] = {
    "grouped": "grouped",
    "json": "grouped",
    "object": "grouped",
    "objects": "grouped",
    "spec": "grouped",
    "specs": "grouped",
    "flat": "flat",
    "rows": "flat",
    "row": "flat",
    "table": "flat",
    "headers": "headers",
    "header": "headers",
    "keys": "keys",
    "key": "keys",
}


# --------------------------------------------------------------------------------------
# Optional modules (kept defensive)
# --------------------------------------------------------------------------------------
def _safe_import(module_name: str) -> Any:
    try:
        return importlib.import_module(module_name)
    except Exception:  # pragma: no cover
        return None


# FIX v2.6.0: multi-path fallback for schema_registry
_schema_registry_mod = None
for _p in ("core.sheets.schema_registry", "core.schema_registry", "schema_registry"):
    _schema_registry_mod = _safe_import(_p)
    if _schema_registry_mod is not None:
        break

# FIX v2.6.0: multi-path fallback for data_dictionary
_data_dictionary_mod = None
for _p in ("core.sheets.data_dictionary", "core.data_dictionary", "data_dictionary"):
    _data_dictionary_mod = _safe_import(_p)
    if _data_dictionary_mod is not None:
        break

# FIX v2.6.0: multi-path fallback for page_catalog
_page_catalog_mod = None
for _p in ("core.sheets.page_catalog", "core.page_catalog", "page_catalog"):
    _page_catalog_mod = _safe_import(_p)
    if _page_catalog_mod is not None:
        break

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
_get_sheet_headers_callable = _resolve_attr(
    _schema_registry_mod,
    "get_sheet_headers",
    "get_headers_for_sheet",
)
_get_sheet_keys_callable = _resolve_attr(
    _schema_registry_mod,
    "get_sheet_keys",
    "get_keys_for_sheet",
)
_list_sheets_callable = _resolve_attr(
    _schema_registry_mod,
    "list_sheets",
    "get_sheet_names",
    "list_pages",
)

_normalize_page_name = _resolve_attr(_page_catalog_mod, "normalize_page_name")
_get_page_aliases_callable = _resolve_attr(_page_catalog_mod, "get_page_aliases")
_CANONICAL_PAGES = _resolve_attr(_page_catalog_mod, "CANONICAL_PAGES")
_SUPPORTED_PAGES = _resolve_attr(_page_catalog_mod, "SUPPORTED_PAGES")
_PAGES = _resolve_attr(_page_catalog_mod, "PAGES")
_PAGE_ALIASES = _resolve_attr(_page_catalog_mod, "PAGE_ALIASES")
_FORBIDDEN_PAGES = _resolve_attr(_page_catalog_mod, "FORBIDDEN_PAGES", default=None)

auth_ok = _resolve_attr(_config_mod, "auth_ok")
is_open_mode = _resolve_attr(_config_mod, "is_open_mode")
get_settings_cached = _resolve_attr(_config_mod, "get_settings_cached")
mask_settings = _resolve_attr(_config_mod, "mask_settings")


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
    if isinstance(value, Mapping):
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
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default


def _normalize_choice(raw: Optional[str], mapping: Dict[str, str], default: str) -> str:
    if raw is None:
        return default
    s = str(raw).strip().lower()
    if not s:
        return default
    return mapping.get(s, s)


def _get_request_id(request: Optional[Request]) -> str:
    try:
        if request is not None:
            rid = request.headers.get("X-Request-ID")
            if rid:
                return str(rid).strip()
            state_rid = getattr(getattr(request, "state", None), "request_id", None)
            if state_rid:
                return str(state_rid).strip()
    except Exception:
        pass
    return "schema"


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


def _empty_if_none(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _json_safe(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, str)):
        return value

    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            return None
        return value

    if isinstance(value, Decimal):
        try:
            f = float(value)
            if f != f or f in (float("inf"), float("-inf")):
                return None
            return f
        except Exception:
            return str(value)

    if isinstance(value, (datetime, date, dt_time)):
        try:
            return value.isoformat()
        except Exception:
            return str(value)

    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="replace")
        except Exception:
            return str(value)

    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]

    if hasattr(value, "model_dump") and callable(getattr(value, "model_dump")):
        try:
            return _json_safe(value.model_dump(mode="python"))
        except Exception:
            pass

    if hasattr(value, "dict") and callable(getattr(value, "dict")):
        try:
            return _json_safe(value.dict())
        except Exception:
            pass

    if hasattr(value, "__dict__"):
        try:
            return _json_safe(vars(value))
        except Exception:
            pass

    try:
        return str(value)
    except Exception:
        return None


def _response(
    *,
    status_code: int,
    status: str,
    endpoint: str,
    request_id: str,
    **content: Any,
) -> BestJSONResponse:
    payload = {
        "status": status,
        "endpoint": endpoint,
        "version": SCHEMA_ROUTE_VERSION,
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": _now_utc(),
        "request_id": request_id,
        **content,
    }
    return BestJSONResponse(status_code=status_code, content=_json_safe(payload))


def _error(status_code: int, message: str, *, endpoint: str, request_id: str) -> BestJSONResponse:
    return _response(
        status_code=status_code,
        status="error",
        endpoint=endpoint,
        request_id=request_id,
        error=str(message),
    )


# --------------------------------------------------------------------------------------
# Auth helpers
# --------------------------------------------------------------------------------------
def _is_public_path(path: str) -> bool:
    p = (path or "").strip()
    if not p:
        return False

    if p in {
        "/v1/schema/health",
        "/v1/schema/pages",
        "/v1/schema/sheet-spec",
        "/v1/schema/spec",
        "/v1/schema/data-dictionary",
    }:
        return True

    env_paths = os.getenv("PUBLIC_PATHS", "") or os.getenv("AUTH_PUBLIC_PATHS", "")
    for raw in env_paths.split(","):
        candidate = raw.strip()
        if not candidate:
            continue
        if candidate.endswith("*") and p.startswith(candidate[:-1]):
            return True
        if p == candidate:
            return True

    return False


def _auth_passed(
    *,
    request: Request,
    token: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
) -> bool:
    try:
        if callable(is_open_mode) and bool(is_open_mode()):
            return True
    except Exception:
        pass

    if _is_public_path(str(getattr(getattr(request, "url", None), "path", "") or "")):
        return True

    if auth_ok is None:
        return True

    auth_token = (x_app_token or "").strip()
    authz = (authorization or "").strip()

    if authz.lower().startswith("bearer "):
        auth_token = authz.split(" ", 1)[1].strip()
    elif token and not auth_token:
        auth_token = token.strip()

    headers_dict = dict(request.headers)
    path = str(getattr(getattr(request, "url", None), "path", "") or "")

    settings = None
    try:
        if callable(get_settings_cached):
            settings = get_settings_cached()
    except Exception:
        settings = None

    call_attempts = [
        {
            "token": auth_token or None,
            "authorization": authorization,
            "headers": headers_dict,
            "path": path,
            "request": request,
            "settings": settings,
        },
        {
            "token": auth_token or None,
            "authorization": authorization,
            "headers": headers_dict,
            "path": path,
            "request": request,
        },
        {
            "token": auth_token or None,
            "authorization": authorization,
            "headers": headers_dict,
            "path": path,
        },
        {
            "token": auth_token or None,
            "authorization": authorization,
            "headers": headers_dict,
        },
        {
            "token": auth_token or None,
            "authorization": authorization,
        },
        {
            "token": auth_token or None,
        },
    ]

    for kwargs in call_attempts:
        try:
            return bool(auth_ok(**kwargs))
        except TypeError:
            continue
        except Exception:
            return False

    return False


# --------------------------------------------------------------------------------------
# Page catalog helpers
# --------------------------------------------------------------------------------------
def _get_page_aliases() -> Dict[str, str]:
    aliases: Dict[str, str] = {}

    if callable(_get_page_aliases_callable):
        try:
            raw_aliases = _get_page_aliases_callable()
            if isinstance(raw_aliases, Mapping):
                for k, v in raw_aliases.items():
                    aliases[str(k).strip().lower()] = str(v).strip()
        except Exception:
            pass

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
    aliases.setdefault("top10", "Top_10_Investments")
    aliases.setdefault("data dictionary", "Data_Dictionary")

    return aliases


def _get_forbidden_pages() -> List[str]:
    pages: List[str] = []

    if isinstance(_FORBIDDEN_PAGES, (list, tuple, set)):
        pages.extend(str(x).strip() for x in _FORBIDDEN_PAGES if str(x).strip())

    pages.extend(_FORBIDDEN_PAGES_FALLBACK)
    return _dedupe_keep_order(pages)


def _normalize_page_candidate(name: Optional[str]) -> Optional[str]:
    if not name:
        return None

    raw = str(name).strip()
    if not raw:
        return None

    if callable(_normalize_page_name):
        try:
            normalized = _normalize_page_name(raw, allow_output_pages=True)
            if normalized:
                return str(normalized)
        except TypeError:
            try:
                normalized = _normalize_page_name(raw)
                if normalized:
                    return str(normalized)
            except Exception:
                pass
        except Exception:
            pass

    aliases = _get_page_aliases()
    alias_hit = aliases.get(raw.lower())
    if alias_hit:
        return alias_hit

    for page in _get_all_pages():
        if raw == page or raw.lower() == page.lower():
            return page

    return raw


def _resolve_requested_sheet_name(
    *,
    sheet: Optional[str] = None,
    page: Optional[str] = None,
    sheet_name: Optional[str] = None,
    name: Optional[str] = None,
    tab: Optional[str] = None,
    worksheet: Optional[str] = None,
) -> Optional[str]:
    for candidate in (sheet, page, sheet_name, name, tab, worksheet):
        normalized = _normalize_page_candidate(candidate)
        if normalized:
            return normalized
    return None


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
                    or item.get("sheet_name")
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

    header_s = _empty_if_none(header)
    key_s = _empty_if_none(key) or header_s

    return {
        "group": _empty_if_none(group) or None,
        "header": header_s,
        "key": key_s,
        "dtype": _empty_if_none(dtype) or None,
        "fmt": _empty_if_none(fmt) or None,
        "required": required,
        "source": _empty_if_none(source) or None,
        "notes": _empty_if_none(notes) or None,
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
                "header": _empty_if_none(header),
                "key": _empty_if_none(key) or _empty_if_none(header),
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


def _registry_pages() -> List[str]:
    pages: List[str] = []

    if callable(_list_sheets_callable):
        try:
            pages.extend(_extract_pages_from_object(_list_sheets_callable()))
        except Exception:
            pass

    for candidate in (_CANONICAL_PAGES, _SUPPORTED_PAGES, _PAGES):
        pages.extend(_extract_pages_from_object(candidate))

    registry_map = _schema_registry_mapping()
    pages.extend(_extract_pages_from_object(registry_map))

    inferred_specs = _infer_specs_from_data_dictionary()
    pages.extend(_extract_pages_from_object(inferred_specs))

    pages.extend(_CANONICAL_PAGES_FALLBACK)

    return _dedupe_keep_order(pages)


def _get_all_pages() -> List[str]:
    return _registry_pages()


def _find_spec_in_registry(sheet_name: str) -> Any:
    if callable(_get_sheet_spec_callable):
        for call in (
            lambda: _get_sheet_spec_callable(sheet_name),
            lambda: _get_sheet_spec_callable(sheet=sheet_name),
            lambda: _get_sheet_spec_callable(page=sheet_name),
            lambda: _get_sheet_spec_callable(sheet_name=sheet_name),
            lambda: _get_sheet_spec_callable(name=sheet_name),
            lambda: _get_sheet_spec_callable(tab=sheet_name),
            lambda: _get_sheet_spec_callable(worksheet=sheet_name),
        ):
            try:
                return call()
            except TypeError:
                continue
            except Exception:
                pass

    headers = []
    keys = []

    if callable(_get_sheet_headers_callable):
        for call in (
            lambda: _get_sheet_headers_callable(sheet_name),
            lambda: _get_sheet_headers_callable(sheet=sheet_name),
            lambda: _get_sheet_headers_callable(page=sheet_name),
            lambda: _get_sheet_headers_callable(sheet_name=sheet_name),
        ):
            try:
                headers = [str(x) for x in _as_list(call())]
                if headers:
                    break
            except TypeError:
                continue
            except Exception:
                pass

    if callable(_get_sheet_keys_callable):
        for call in (
            lambda: _get_sheet_keys_callable(sheet_name),
            lambda: _get_sheet_keys_callable(sheet=sheet_name),
            lambda: _get_sheet_keys_callable(page=sheet_name),
            lambda: _get_sheet_keys_callable(sheet_name=sheet_name),
        ):
            try:
                keys = [str(x) for x in _as_list(call())]
                if keys:
                    break
            except TypeError:
                continue
            except Exception:
                pass

    if headers or keys:
        return {"headers": headers, "keys": keys or headers}

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


def _build_flat_sheet_spec_rows(specs: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
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
    return flat_rows


# --------------------------------------------------------------------------------------
# Health
# --------------------------------------------------------------------------------------
@router.get("/health")
def get_schema_health(request: Request) -> BestJSONResponse:
    request_id = _get_request_id(request)

    settings_summary = None
    try:
        if callable(mask_settings) and callable(get_settings_cached):
            settings = get_settings_cached()
            masked = mask_settings(settings)
            if isinstance(masked, Mapping):
                settings_summary = {
                    "open_mode_effective": masked.get("open_mode_effective"),
                    "token_count": masked.get("token_count"),
                }
    except Exception:
        settings_summary = None

    pages = _get_all_pages()
    inferred_specs = _infer_specs_from_data_dictionary()
    registry_map = _schema_registry_mapping()

    return _response(
        status_code=200,
        status="ok",
        endpoint="health",
        request_id=request_id,
        schema_registry_available=_schema_registry_mod is not None,
        data_dictionary_module_available=_data_dictionary_mod is not None,
        page_catalog_available=_page_catalog_mod is not None,
        pages_count=len(pages),
        pages=pages,
        canonical_pages=pages,
        forbidden_pages=_get_forbidden_pages(),
        settings=settings_summary,
        capabilities={
            "get_sheet_spec_callable": callable(_get_sheet_spec_callable),
            "get_sheet_headers_callable": callable(_get_sheet_headers_callable),
            "get_sheet_keys_callable": callable(_get_sheet_keys_callable),
            "list_sheets_callable": callable(_list_sheets_callable),
            "build_data_dictionary_rows": callable(build_data_dictionary_rows),
            "build_data_dictionary_values": callable(build_data_dictionary_values),
            "validate_data_dictionary_output": callable(validate_data_dictionary_output),
        },
        registry_counts={
            "registry_map_count": len(registry_map),
            "inferred_specs_count": len(inferred_specs),
            "fallback_pages_count": len(_CANONICAL_PAGES_FALLBACK),
        },
        response_contract={
            "data_dictionary_rows_shape": "headers + keys + rows + rows_matrix",
            "sheet_spec_grouped_shape": "data + optional top-level headers/keys/columns for single sheet",
            "pages_shape": "canonical_pages + pages + items + results + sheets + page_items",
        },
    )


# --------------------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------------------
@router.get("/data-dictionary")
def get_data_dictionary(
    request: Request,
    format: str = Query(
        default="rows",
        description="rows (default), json (alias of rows), or values (2D array including header row)",
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

    if not _auth_passed(request=request, token=token, x_app_token=x_app_token, authorization=authorization):
        return _error(401, "Invalid token", endpoint="data-dictionary", request_id=request_id)

    include_meta = _bool_q(include_meta_sheet, True)
    fmt = _normalize_choice(format, _DATA_DICTIONARY_FORMAT_ALIASES, "rows")

    if fmt not in {"rows", "values"}:
        return _error(
            400,
            "format must be one of: rows, json, records, table, values, matrix",
            endpoint="data-dictionary",
            request_id=request_id,
        )

    try:
        headers, keys = _get_data_dictionary_headers_and_keys()

        if fmt == "values":
            values = _build_data_dictionary_values_safe(include_meta_sheet=include_meta)
            return _response(
                status_code=200,
                status="success",
                endpoint="data-dictionary",
                request_id=request_id,
                format="values",
                headers=headers,
                keys=keys,
                display_headers=headers,
                values=values,
                rows_matrix=values[1:] if len(values) > 1 else [],
                count=max(0, len(values) - 1),
                include_meta_sheet=include_meta,
                response_contract={
                    "header_row_in_values": True,
                    "rows_matrix_excludes_header_row": True,
                    "json_safe_payload": True,
                },
            )

        rows = _build_data_dictionary_rows_safe(include_meta_sheet=include_meta)
        rows_matrix = [[row.get(k) for k in keys] for row in rows]

        return _response(
            status_code=200,
            status="success",
            endpoint="data-dictionary",
            request_id=request_id,
            format="rows",
            headers=headers,
            keys=keys,
            display_headers=headers,
            rows=rows,
            rows_matrix=rows_matrix,
            count=len(rows),
            include_meta_sheet=include_meta,
            response_contract={
                "headers_are_display_headers": True,
                "keys_present": True,
                "rows_are_projected_to_keys": True,
                "rows_matrix_present": True,
                "json_safe_payload": True,
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        return _error(500, f"{type(e).__name__}: {e}", endpoint="data-dictionary", request_id=request_id)


def _sheet_spec_response(
    *,
    request: Request,
    endpoint_name: str,
    sheet: Optional[str],
    page: Optional[str],
    sheet_name: Optional[str],
    name: Optional[str],
    tab: Optional[str],
    worksheet: Optional[str],
    format: str,
    include_aliases: Optional[str],
    token: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
) -> BestJSONResponse:
    request_id = _get_request_id(request)

    if not _auth_passed(request=request, token=token, x_app_token=x_app_token, authorization=authorization):
        return _error(401, "Invalid token", endpoint=endpoint_name, request_id=request_id)

    fmt = _normalize_choice(format, _SHEET_SPEC_FORMAT_ALIASES, "grouped")
    if fmt not in {"grouped", "flat", "headers", "keys"}:
        return _error(
            400,
            "format must be one of: grouped, json, flat, headers, keys",
            endpoint=endpoint_name,
            request_id=request_id,
        )

    try:
        include_alias_map = _bool_q(include_aliases, True)
        normalized_sheet = _resolve_requested_sheet_name(
            sheet=sheet,
            page=page,
            sheet_name=sheet_name,
            name=name,
            tab=tab,
            worksheet=worksheet,
        )

        if normalized_sheet:
            spec = _get_sheet_spec_safe(normalized_sheet)
            specs = {normalized_sheet: spec}
        else:
            specs = _get_all_sheet_specs()

        if not specs:
            return _error(404, "No sheet specifications available", endpoint=endpoint_name, request_id=request_id)

        page_aliases = _get_page_aliases()

        if fmt == "headers":
            payload: Any = {name_: spec_["headers"] for name_, spec_ in specs.items()}
        elif fmt == "keys":
            payload = {name_: spec_["keys"] for name_, spec_ in specs.items()}
        elif fmt == "flat":
            payload = _build_flat_sheet_spec_rows(specs)
        else:
            payload = specs

        page_names = list(specs.keys())

        response: Dict[str, Any] = {
            "format": fmt,
            "sheet": normalized_sheet,
            "page": normalized_sheet,
            "sheet_name": normalized_sheet,
            "name": normalized_sheet,
            "tab": normalized_sheet,
            "worksheet": normalized_sheet,
            "count": len(specs),
            "pages": page_names,
            "items": page_names,
            "results": page_names,
            "sheets": page_names,
            "page_items": page_names,
            "canonical_pages": page_names if normalized_sheet else _get_all_pages(),
            "forbidden_pages": _get_forbidden_pages(),
            "data": payload,
            "specs": specs,
            "sheet_specs": specs,
            "response_contract": {
                "single_sheet_top_level_projection": True,
                "grouped_data_shape": "sheet -> {headers, keys, columns, column_count}",
                "flat_data_shape": "list[dict]",
                "json_safe_payload": True,
            },
        }

        if include_alias_map:
            response["aliases"] = page_aliases

        if normalized_sheet and normalized_sheet in specs:
            response["headers"] = specs[normalized_sheet]["headers"]
            response["keys"] = specs[normalized_sheet]["keys"]
            response["columns"] = specs[normalized_sheet]["columns"]
            response["column_count"] = specs[normalized_sheet]["column_count"]

            if fmt == "grouped":
                response["data"] = specs[normalized_sheet]

        return _response(
            status_code=200,
            status="success",
            endpoint=endpoint_name,
            request_id=request_id,
            **response,
        )

    except KeyError as e:
        return _error(404, str(e), endpoint=endpoint_name, request_id=request_id)
    except HTTPException:
        raise
    except Exception as e:
        return _error(500, f"{type(e).__name__}: {e}", endpoint=endpoint_name, request_id=request_id)


@router.get("/sheet-spec")
def get_sheet_spec_route(
    request: Request,
    sheet: Optional[str] = Query(
        default=None,
        description="Optional canonical sheet name or alias. Also accepts page/sheet_name/name/tab/worksheet aliases.",
    ),
    page: Optional[str] = Query(default=None, description="Alias of 'sheet'"),
    sheet_name: Optional[str] = Query(default=None, description="Alias of 'sheet'"),
    name: Optional[str] = Query(default=None, description="Alias of 'sheet'"),
    tab: Optional[str] = Query(default=None, description="Alias of 'sheet'"),
    worksheet: Optional[str] = Query(default=None, description="Alias of 'sheet'"),
    format: str = Query(
        default="grouped",
        description="grouped (default), json (alias of grouped), flat, headers, or keys",
    ),
    include_aliases: Optional[str] = Query(
        default="true",
        description="Include aliases mapping in response",
    ),
    token: Optional[str] = Query(default=None, description="Optional token if auth is enabled"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> BestJSONResponse:
    return _sheet_spec_response(
        request=request,
        endpoint_name="sheet-spec",
        sheet=sheet,
        page=page,
        sheet_name=sheet_name,
        name=name,
        tab=tab,
        worksheet=worksheet,
        format=format,
        include_aliases=include_aliases,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
    )


@router.get("/spec")
def get_spec_alias_route(
    request: Request,
    sheet: Optional[str] = Query(
        default=None,
        description="Optional canonical sheet name or alias. Also accepts page/sheet_name/name/tab/worksheet aliases.",
    ),
    page: Optional[str] = Query(default=None, description="Alias of 'sheet'"),
    sheet_name: Optional[str] = Query(default=None, description="Alias of 'sheet'"),
    name: Optional[str] = Query(default=None, description="Alias of 'sheet'"),
    tab: Optional[str] = Query(default=None, description="Alias of 'sheet'"),
    worksheet: Optional[str] = Query(default=None, description="Alias of 'sheet'"),
    format: str = Query(
        default="grouped",
        description="grouped (default), json (alias of grouped), flat, headers, or keys",
    ),
    include_aliases: Optional[str] = Query(
        default="true",
        description="Include aliases mapping in response",
    ),
    token: Optional[str] = Query(default=None, description="Optional token if auth is enabled"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> BestJSONResponse:
    return _sheet_spec_response(
        request=request,
        endpoint_name="spec",
        sheet=sheet,
        page=page,
        sheet_name=sheet_name,
        name=name,
        tab=tab,
        worksheet=worksheet,
        format=format,
        include_aliases=include_aliases,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
    )


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

    if not _auth_passed(request=request, token=token, x_app_token=x_app_token, authorization=authorization):
        return _error(401, "Invalid token", endpoint="pages", request_id=request_id)

    try:
        pages = _get_all_pages()
        aliases = _get_page_aliases()
        include_alias_map = _bool_q(include_aliases, True)
        forbidden_pages = _get_forbidden_pages()

        page_items = []
        for p in pages:
            page_items.append(
                {
                    "page": p,
                    "sheet": p,
                    "sheet_name": p,
                    "name": p,
                    "has_aliases": any(v == p for v in aliases.values()),
                }
            )

        payload: Dict[str, Any] = {
            "canonical_pages": pages,
            "forbidden_pages": forbidden_pages,
            "pages": pages,
            "items": pages,
            "results": pages,
            "sheets": pages,
            "page_items": page_items,
            "count": len(pages),
            "response_contract": {
                "canonical_pages_present": True,
                "forbidden_pages_present": True,
                "pages_arrays_present": True,
                "page_items_present": True,
                "json_safe_payload": True,
            },
        }

        if include_alias_map:
            payload["aliases"] = aliases

        return _response(
            status_code=200,
            status="success",
            endpoint="pages",
            request_id=request_id,
            **payload,
        )

    except HTTPException:
        raise
    except Exception as e:
        return _error(500, f"{type(e).__name__}: {e}", endpoint="pages", request_id=request_id)


__all__ = [
    "router",
    "SCHEMA_ROUTE_VERSION",
]
