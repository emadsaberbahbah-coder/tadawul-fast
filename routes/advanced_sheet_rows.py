#!/usr/bin/env python3
# routes/advanced_sheet_rows.py
"""
================================================================================
Advanced Sheet-Rows Router — v2.3.0
================================================================================
SCHEMA-FIRST • CANONICAL PAGE DISPATCH • ANALYSIS-ALIGNED • SPECIAL-PAGE SAFE
PATH-AWARE AUTH • PUBLIC-PATH AWARE • STABLE RESPONSE SHAPE • LIVE-TEST READY

Endpoints
---------
GET  /v1/advanced/health
POST /v1/advanced/sheet-rows

Primary role
------------
Advanced sheet-rows behavior aligned with the rest of the TFB stack so that:
- page normalization is identical
- schema headers/keys are always authoritative
- special pages never fall back to the generic 80-column instrument schema
- instrument pages use engine-backed retrieval consistently
- table/special page behavior stays deterministic and stable

What this revision improves
---------------------------
- ✅ FIX: table mode no longer double-applies limit/offset after engine payloads
- ✅ FIX: symbol mode now honors offset/limit after top_n normalization
- ✅ FIX: table-mode payload extraction now accepts row_objects / quotes / results
- ✅ FIX: insights status now respects builder-reported warn/partial/error states
- ✅ FIX: table-mode dispatch now reports the real source used
- ✅ FIX: schema-only empty table fallback now reports warn instead of silent success
- ✅ FIX: better metadata for requested vs returned symbols and rows
- ✅ SAFE: No network I/O at import time
- ✅ SAFE: Optional auth only if core.config.auth_ok exists
================================================================================
"""

from __future__ import annotations

import importlib
import inspect
import os
import time
import uuid
from dataclasses import is_dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, status

# -----------------------------------------------------------------------------
# Schema registry (authoritative)
# -----------------------------------------------------------------------------
try:
    from core.sheets.schema_registry import get_sheet_spec  # type: ignore
except Exception as e:  # pragma: no cover
    get_sheet_spec = None  # type: ignore
    _SCHEMA_IMPORT_ERROR = repr(e)
else:
    _SCHEMA_IMPORT_ERROR = None

# -----------------------------------------------------------------------------
# Page catalog helpers (authoritative normalization / dispatch)
# -----------------------------------------------------------------------------
try:
    from core.sheets.page_catalog import (  # type: ignore
        CANONICAL_PAGES,
        FORBIDDEN_PAGES,
        allowed_pages,
        get_route_family,
        is_instrument_page,
        normalize_page_name,
    )
except Exception:  # pragma: no cover
    CANONICAL_PAGES = []  # type: ignore
    FORBIDDEN_PAGES = {"KSA_Tadawul", "Advisor_Criteria"}  # type: ignore

    def allowed_pages() -> List[str]:  # type: ignore
        return list(CANONICAL_PAGES) if CANONICAL_PAGES else []

    def normalize_page_name(name: str, allow_output_pages: bool = True) -> str:  # type: ignore
        return (name or "").strip().replace(" ", "_")

    def get_route_family(name: str) -> str:  # type: ignore
        if name == "Insights_Analysis":
            return "insights"
        if name == "Top_10_Investments":
            return "top10"
        if name == "Data_Dictionary":
            return "dictionary"
        return "instrument"

    def is_instrument_page(name: str) -> bool:  # type: ignore
        return get_route_family(name) == "instrument"


# -----------------------------------------------------------------------------
# Optional auth/config
# -----------------------------------------------------------------------------
try:
    from core.config import auth_ok, get_settings_cached, mask_settings  # type: ignore
except Exception:  # pragma: no cover
    auth_ok = None  # type: ignore
    mask_settings = None  # type: ignore

    def get_settings_cached(*args, **kwargs):  # type: ignore
        return None


# -----------------------------------------------------------------------------
# Optional legacy shared helper
# -----------------------------------------------------------------------------
try:
    from core.data_engine import get_sheet_rows as core_get_sheet_rows  # type: ignore
except Exception:  # pragma: no cover
    core_get_sheet_rows = None  # type: ignore


ADVANCED_SHEET_ROWS_VERSION = "2.3.0"
router = APIRouter(prefix="/v1/advanced", tags=["Advanced Sheet Rows"])


# =============================================================================
# Generic helpers
# =============================================================================
def _strip(v: Any) -> str:
    try:
        return str(v).strip()
    except Exception:
        return ""


def _as_list(v: Any) -> List[Any]:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, tuple):
        return list(v)
    if isinstance(v, set):
        return list(v)
    if isinstance(v, str):
        return [v]
    if isinstance(v, dict):
        return [v]
    if isinstance(v, Iterable):
        try:
            return list(v)
        except Exception:
            return [v]
    return [v]


def _dedupe_keep_order(items: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for item in items:
        s = _strip(item)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _pick_page_from_body(body: Mapping[str, Any]) -> str:
    for k in ("sheet", "page", "sheet_name", "sheetName", "name", "tab"):
        s = _strip(body.get(k))
        if s:
            return s
    return ""


def _maybe_int(v: Any, default: int) -> int:
    try:
        if v is None or isinstance(v, bool):
            return default
        if isinstance(v, int):
            return v
        if isinstance(v, float):
            return int(v)
        s = _strip(v)
        if not s:
            return default
        return int(float(s))
    except Exception:
        return default


def _maybe_bool(v: Any, default: bool) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        try:
            return bool(int(v))
        except Exception:
            return default
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "y", "on"}
    return default


def _get_list(body: Mapping[str, Any], *keys: str) -> List[str]:
    for k in keys:
        v = body.get(k)
        if isinstance(v, list):
            out: List[str] = []
            for item in v:
                s = _strip(item)
                if s:
                    out.append(s)
            if out:
                return out
        if isinstance(v, str) and v.strip():
            parts = [p.strip() for p in v.replace(";", ",").replace("|", ",").split(",") if p.strip()]
            if parts:
                return parts
    return []


def _to_plain_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj

    try:
        if hasattr(obj, "model_dump") and callable(getattr(obj, "model_dump")):
            d = obj.model_dump(mode="python")  # type: ignore
            return d if isinstance(d, dict) else {}
    except Exception:
        pass

    try:
        if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
            d = obj.dict()  # type: ignore
            return d if isinstance(d, dict) else {}
    except Exception:
        pass

    try:
        if is_dataclass(obj):
            dd = getattr(obj, "__dict__", None)
            if isinstance(dd, dict):
                return {k: v for k, v in dd.items() if not str(k).startswith("_")}
    except Exception:
        pass

    try:
        dd = getattr(obj, "__dict__", None)
        if isinstance(dd, dict):
            return dict(dd)
    except Exception:
        pass

    return {}


async def _maybe_await(x: Any) -> Any:
    try:
        if inspect.isawaitable(x):
            return await x
    except Exception:
        pass
    return x


def _slice(rows: List[Dict[str, Any]], *, limit: int, offset: int) -> List[Dict[str, Any]]:
    start = max(0, int(offset))
    if limit <= 0:
        return rows[start:]
    end = start + max(0, int(limit))
    return rows[start:end]


def _slice_values(values: Sequence[str], *, limit: int, offset: int) -> List[str]:
    start = max(0, int(offset))
    vals = list(values)
    if limit <= 0:
        return vals[start:]
    end = start + max(0, int(limit))
    return vals[start:end]


def _rows_to_matrix(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    return [[r.get(k) for k in keys] for r in rows]


def _schema_headers_keys(page: str) -> Tuple[List[str], List[str], Any]:
    if get_sheet_spec is None:
        raise KeyError("schema_registry unavailable")

    spec = get_sheet_spec(page)
    cols = getattr(spec, "columns", None) or []

    headers: List[str] = []
    keys: List[str] = []
    for c in cols:
        h = _strip(getattr(c, "header", None))
        k = _strip(getattr(c, "key", None))
        if not h or not k:
            continue
        headers.append(h)
        keys.append(k)

    return headers, keys, spec


def _normalize_to_schema_keys(
    *,
    schema_keys: Sequence[str],
    schema_headers: Sequence[str],
    raw: Mapping[str, Any],
) -> Dict[str, Any]:
    raw = dict(raw or {})
    raw_ci = {str(k).strip().lower(): v for k, v in raw.items()}

    header_by_key: Dict[str, str] = {}
    for k, h in zip(schema_keys, schema_headers):
        header_by_key[str(k)] = str(h)

    out: Dict[str, Any] = {}
    for k in schema_keys:
        ks = str(k)
        v = None

        if ks in raw:
            v = raw.get(ks)
        else:
            v = raw_ci.get(ks.lower())

        if v is None:
            h = header_by_key.get(ks, "")
            if h:
                if h in raw:
                    v = raw.get(h)
                else:
                    v = raw_ci.get(h.strip().lower())

        out[ks] = v

    return out


def _extract_rows_like(payload: Any) -> List[Dict[str, Any]]:
    """
    Accept rows from various helper shapes:
    - {"rows": [...]}
    - {"row_objects": [...]}
    - {"quotes": [...]}
    - {"results": [...]}
    - {"data": [...]}
    - {"items": [...]}
    - {"records": [...]}
    - {"data": {"rows": [...]}}
    - [...]
    """
    if payload is None:
        return []

    if isinstance(payload, list):
        return [_to_plain_dict(r) for r in payload]

    if isinstance(payload, dict):
        for name in ("rows", "row_objects", "quotes", "results", "data", "items", "records"):
            value = payload.get(name)
            if isinstance(value, list):
                return [_to_plain_dict(r) for r in value]
            if isinstance(value, dict):
                nested = _extract_rows_like(value)
                if nested:
                    return nested

    return []


def _extract_matrix_like(payload: Any) -> Optional[List[List[Any]]]:
    if isinstance(payload, dict):
        value = payload.get("rows_matrix")
        if isinstance(value, list):
            return [list(r) if isinstance(r, (list, tuple)) else [r] for r in value]

        nested = payload.get("data")
        if isinstance(nested, dict):
            value2 = nested.get("rows_matrix")
            if isinstance(value2, list):
                return [list(r) if isinstance(r, (list, tuple)) else [r] for r in value2]
    return None


def _matrix_to_rows(matrix: Sequence[Sequence[Any]], keys: Sequence[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in matrix:
        vals = list(row) if isinstance(row, (list, tuple)) else [row]
        item: Dict[str, Any] = {}
        for idx, k in enumerate(keys):
            item[k] = vals[idx] if idx < len(vals) else None
        rows.append(item)
    return rows


def _coerce_status(value: Any, default: str = "success") -> str:
    s = _strip(value).lower()
    return s if s in {"success", "warn", "warning", "partial", "error", "failed"} else default


def _normalize_status_word(value: str) -> str:
    v = _strip(value).lower()
    if v == "warning":
        return "warn"
    if v == "failed":
        return "error"
    return v or "success"


def _payload_meta(payload: Any) -> Dict[str, Any]:
    if isinstance(payload, dict):
        m = payload.get("meta")
        if isinstance(m, dict):
            return m
    return {}


def _payload_matches_requested_window(payload: Any, *, limit: int, offset: int) -> bool:
    meta = _payload_meta(payload)
    meta_limit = meta.get("limit", payload.get("limit") if isinstance(payload, dict) else None)
    meta_offset = meta.get("offset", payload.get("offset") if isinstance(payload, dict) else None)

    try:
        if meta_limit is not None and int(meta_limit) != int(limit):
            return False
        if meta_offset is not None and int(meta_offset) != int(offset):
            return False
        return meta_limit is not None or meta_offset is not None
    except Exception:
        return False


# =============================================================================
# Auth helpers
# =============================================================================
def _settings_get_bool(settings: Any, *names: str, default: bool = False) -> bool:
    for name in names:
        try:
            if settings is not None and hasattr(settings, name):
                return bool(getattr(settings, name))
        except Exception:
            continue
    return default


def _settings_get_list(settings: Any, *names: str) -> List[str]:
    for name in names:
        try:
            if settings is None or not hasattr(settings, name):
                continue
            v = getattr(settings, name)
            if isinstance(v, (list, tuple, set)):
                return [_strip(x) for x in v if _strip(x)]
            if isinstance(v, str) and v.strip():
                return [x.strip() for x in v.split(",") if x.strip()]
        except Exception:
            continue
    return []


def _path_is_public(path: str, settings: Any) -> bool:
    p = _strip(path)
    if not p:
        return False

    public_paths = _settings_get_list(
        settings,
        "PUBLIC_PATHS",
        "public_paths",
        "AUTH_PUBLIC_PATHS",
        "auth_public_paths",
        "public_routes",
    )

    public_prefixes = _settings_get_list(
        settings,
        "PUBLIC_PATH_PREFIXES",
        "public_path_prefixes",
        "AUTH_PUBLIC_PREFIXES",
        "auth_public_prefixes",
    )

    defaults = {
        "/",
        "/health",
        "/livez",
        "/readyz",
        "/ping",
        "/meta",
        "/docs",
        "/redoc",
        "/openapi.json",
        "/v1/advanced/health",
    }

    if p in defaults:
        return True
    if p in set(public_paths):
        return True

    for pref in public_prefixes:
        if pref and p.startswith(pref):
            return True

    return False


def _allow_query_token(settings: Any, request: Request) -> bool:
    try:
        if settings is not None:
            return bool(
                getattr(settings, "ALLOW_QUERY_TOKEN", False)
                or getattr(settings, "allow_query_token", False)
            )
    except Exception:
        pass

    v = (os.getenv("ALLOW_QUERY_TOKEN", "") or "").strip().lower()
    if v in {"1", "true", "yes", "y", "on"}:
        return True

    try:
        hv = _strip(request.headers.get("X-Allow-Query-Token"))
        if hv.lower() in {"1", "true", "yes"}:
            return True
    except Exception:
        pass

    return False


def _extract_auth_token(
    *,
    token_query: Optional[str],
    x_app_token: Optional[str],
    x_api_key: Optional[str],
    authorization: Optional[str],
    settings: Any,
    request: Request,
) -> str:
    auth_token = _strip(x_app_token) or _strip(x_api_key)

    if authorization and authorization.strip().lower().startswith("bearer "):
        auth_token = authorization.strip().split(" ", 1)[1].strip()

    if token_query and not auth_token and _allow_query_token(settings, request):
        auth_token = _strip(token_query)

    return auth_token


def _ensure_authorized(
    *,
    request: Request,
    settings: Any,
    token_query: Optional[str],
    x_app_token: Optional[str],
    x_api_key: Optional[str],
    authorization: Optional[str],
) -> None:
    path = str(getattr(getattr(request, "url", None), "path", "") or "")

    if _settings_get_bool(settings, "OPEN_MODE", "open_mode", default=False):
        return

    if _path_is_public(path, settings):
        return

    if auth_ok is None:
        return

    auth_token = _extract_auth_token(
        token_query=token_query,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
        settings=settings,
        request=request,
    )

    try:
        ok = auth_ok(
            token=auth_token,
            authorization=authorization,
            headers=request.headers,
            path=path,
            request=request,
            settings=settings,
        )
    except TypeError:
        try:
            ok = auth_ok(
                token=auth_token,
                authorization=authorization,
                headers=request.headers,
                path=path,
            )
        except TypeError:
            ok = auth_ok(token=auth_token, authorization=authorization, headers=request.headers)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": "Auth check failed", "detail": str(e)},
        )

    if not ok:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")


def _ensure_page_allowed(page: str) -> None:
    forbidden = set(FORBIDDEN_PAGES or set())
    if page in forbidden:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": f"Forbidden/removed page: {page}", "forbidden_pages": sorted(list(forbidden))},
        )

    try:
        ap = allowed_pages()
        if ap and page not in set(ap):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"error": f"Unknown page: {page}", "allowed_pages": ap},
            )
    except HTTPException:
        raise
    except Exception:
        return


# =============================================================================
# Engine access
# =============================================================================
async def _get_engine(request: Request) -> Optional[Any]:
    try:
        st = getattr(request.app, "state", None)
        if st and getattr(st, "engine", None):
            return st.engine
    except Exception:
        pass

    for modpath in ("core.data_engine_v2", "core.data_engine"):
        try:
            mod = __import__(modpath, fromlist=["get_engine"])
            get_engine = getattr(mod, "get_engine", None)
            if callable(get_engine):
                eng = get_engine()
                eng = await _maybe_await(eng)
                return eng
        except Exception:
            continue

    return None


async def _call_engine(fn: Any, *args: Any, **kwargs: Any) -> Any:
    res = fn(*args, **kwargs)
    return await _maybe_await(res)


def _dict_is_symbol_map(d: Dict[str, Any], symbols: Sequence[str]) -> bool:
    if not isinstance(d, dict) or not symbols:
        return False

    symset = set(symbols)
    keys = [k for k in d.keys() if isinstance(k, str)]
    if not keys:
        return False

    hit = sum(1 for k in keys if k in symset)
    if hit == len(symset) and len(symset) > 0:
        return True
    if hit >= max(1, min(3, len(symset) // 3 if len(symset) > 0 else 1)):
        return True
    return False


async def _fetch_advanced_rows(
    engine: Any,
    symbols: List[str],
    *,
    mode: str,
    settings: Any,
    schema: Any,
) -> Dict[str, Any]:
    if not symbols:
        return {}

    computations_enabled = _settings_get_bool(settings, "computations_enabled", default=True)
    forecasting_enabled = _settings_get_bool(settings, "forecasting_enabled", default=True)
    scoring_enabled = _settings_get_bool(settings, "scoring_enabled", default=True)
    want_advanced = computations_enabled and (forecasting_enabled or scoring_enabled)

    preferred: List[str] = []
    if want_advanced:
        preferred += [
            "get_analysis_rows_batch",
            "get_analysis_quotes_batch",
            "get_enriched_quotes_batch",
        ]
    else:
        preferred += ["get_enriched_quotes_batch"]

    preferred += ["get_enriched_quotes", "get_quotes_batch", "quotes_batch"]

    for method in preferred:
        fn = getattr(engine, method, None)
        if not callable(fn):
            continue

        try:
            try:
                res = await _call_engine(fn, symbols, mode=mode, schema=schema)
            except TypeError:
                try:
                    res = await _call_engine(fn, symbols, schema=schema)
                except TypeError:
                    try:
                        res = await _call_engine(fn, symbols, mode=mode)
                    except TypeError:
                        res = await _call_engine(fn, symbols)

            if isinstance(res, dict):
                if _dict_is_symbol_map(res, symbols):
                    return res

                data = res.get("data") or res.get("rows") or res.get("items") or res.get("quotes")
                if isinstance(data, dict) and _dict_is_symbol_map(data, symbols):
                    return data
                if isinstance(data, list):
                    return {s: r for s, r in zip(symbols, data)}

            elif isinstance(res, list):
                return {s: r for s, r in zip(symbols, res)}

        except Exception:
            continue

    out: Dict[str, Any] = {}
    per_dict_fn = getattr(engine, "get_enriched_quote_dict", None) or getattr(engine, "get_analysis_row_dict", None)
    per_fn = (
        getattr(engine, "get_enriched_quote", None)
        or getattr(engine, "get_analysis_row", None)
        or getattr(engine, "get_quote", None)
    )

    for s in symbols:
        try:
            if callable(per_dict_fn):
                try:
                    out[s] = await _call_engine(per_dict_fn, s, mode=mode, schema=schema)
                except TypeError:
                    try:
                        out[s] = await _call_engine(per_dict_fn, s, schema=schema)
                    except TypeError:
                        try:
                            out[s] = await _call_engine(per_dict_fn, s, mode=mode)
                        except TypeError:
                            out[s] = await _call_engine(per_dict_fn, s)
            elif callable(per_fn):
                try:
                    out[s] = await _call_engine(per_fn, s, mode=mode, schema=schema)
                except TypeError:
                    try:
                        out[s] = await _call_engine(per_fn, s, schema=schema)
                    except TypeError:
                        try:
                            out[s] = await _call_engine(per_fn, s, mode=mode)
                        except TypeError:
                            out[s] = await _call_engine(per_fn, s)
            else:
                out[s] = {"symbol": s, "error": "engine_missing_quote_method"}
        except Exception as e:
            out[s] = {"symbol": s, "error": str(e)}

    return out


async def _call_engine_sheet_rows_best_effort(
    *,
    engine: Any,
    page: str,
    limit: int,
    offset: int,
    mode: str,
    body: Dict[str, Any],
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if engine is None:
        return None, None

    candidates = [
        ("get_sheet_rows", {"sheet": page, "limit": limit, "offset": offset, "mode": mode, "body": body}),
        ("get_sheet_rows", {"sheet": page, "limit": limit, "offset": offset, "mode": mode}),
        ("get_sheet_rows", {"sheet": page, "limit": limit, "offset": offset}),
        ("get_page_rows", {"page": page, "limit": limit, "offset": offset, "mode": mode, "body": body}),
        ("get_page_rows", {"page": page, "limit": limit, "offset": offset, "mode": mode}),
        ("sheet_rows", {"sheet": page, "limit": limit, "offset": offset, "mode": mode, "body": body}),
        ("sheet_rows", {"sheet": page, "limit": limit, "offset": offset, "mode": mode}),
        ("build_sheet_rows", {"sheet": page, "limit": limit, "offset": offset, "mode": mode, "body": body}),
        ("build_sheet_rows", {"sheet": page, "limit": limit, "offset": offset, "mode": mode}),
    ]

    for method_name, kwargs in candidates:
        fn = getattr(engine, method_name, None)
        if not callable(fn):
            continue

        try:
            res = await _call_engine(fn, **kwargs)
            if isinstance(res, dict):
                return res, f"engine:{method_name}"
            if isinstance(res, list):
                return {"rows": res}, f"engine:{method_name}"
        except TypeError:
            continue
        except Exception:
            continue

    return None, None


# =============================================================================
# Lazy builder imports
# =============================================================================
def _import_first_available(mod_names: Sequence[str]):
    last_err: Optional[Exception] = None
    for mn in mod_names:
        try:
            return importlib.import_module(mn)
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(str(last_err) if last_err else "import failed")


async def _call_builder_best_effort(
    *,
    module_names: Sequence[str],
    function_names: Sequence[str],
    request: Request,
    settings: Any,
    mode: str,
    body: Dict[str, Any],
    schema_keys: Sequence[str],
    schema_headers: Sequence[str],
    friendly_name: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    try:
        mod = _import_first_available(module_names)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": f"{friendly_name} builder import failed", "detail": str(e)},
        )

    fn = None
    fn_name = ""
    for name in function_names:
        cand = getattr(mod, name, None)
        if callable(cand):
            fn = cand
            fn_name = name
            break

    if fn is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": f"{friendly_name} builder missing callable function"},
        )

    last_err: Optional[Exception] = None
    candidates: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = [
        ((), {"request": request, "settings": settings, "mode": mode, "body": body}),
        ((), {"request": request, "settings": settings, "mode": mode}),
        ((), {"settings": settings, "mode": mode, "body": body}),
        ((), {"settings": settings, "mode": mode}),
        ((), {"mode": mode, "body": body}),
        ((), {"mode": mode}),
        ((), {"body": body}),
        ((), {}),
    ]

    out = None
    for args, kwargs in candidates:
        try:
            res = fn(*args, **kwargs)
            out = await _maybe_await(res)
            break
        except TypeError as e:
            last_err = e
            continue
        except Exception as e:
            last_err = e
            continue

    if out is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": f"{friendly_name} builder call failed", "detail": str(last_err) if last_err else "unknown"},
        )

    rows = _extract_rows_like(out)
    matrix = _extract_matrix_like(out)
    if not rows and matrix:
        rows = _matrix_to_rows(matrix, schema_keys)

    rows_norm = [
        _normalize_to_schema_keys(schema_keys=schema_keys, schema_headers=schema_headers, raw=(r or {}))
        for r in rows
    ]

    meta: Dict[str, Any] = {
        "builder_module": getattr(mod, "__name__", ""),
        "builder_function": fn_name,
        "builder_payload_preserved": isinstance(out, dict),
    }
    if isinstance(out, dict):
        meta["builder_meta"] = out.get("meta")
        meta["builder_status"] = out.get("status")
        meta["builder_error"] = out.get("error")

    return rows_norm, meta


def _build_data_dictionary_rows_to_schema(
    *,
    schema_keys: Sequence[str],
    schema_headers: Sequence[str],
) -> List[Dict[str, Any]]:
    try:
        from core.sheets.data_dictionary import build_data_dictionary_rows  # type: ignore
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": "Data_Dictionary generator import failed", "detail": str(e)},
        )

    try:
        raw_rows = build_data_dictionary_rows(include_meta_sheet=True)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "Data_Dictionary generator failed", "detail": str(e)},
        )

    out: List[Dict[str, Any]] = []
    for r in _as_list(raw_rows):
        rd = _to_plain_dict(r)
        out.append(_normalize_to_schema_keys(schema_keys=schema_keys, schema_headers=schema_headers, raw=rd))
    return out


async def _call_core_sheet_rows_best_effort(
    *,
    page: str,
    limit: int,
    offset: int,
    mode: str,
    body: Dict[str, Any],
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if core_get_sheet_rows is None:
        return None, None

    last_err: Optional[Exception] = None
    candidates = [
        ((), {"sheet": page, "limit": limit, "offset": offset, "mode": mode, "body": body}),
        ((), {"sheet": page, "limit": limit, "offset": offset, "mode": mode}),
        ((), {"sheet": page, "limit": limit, "offset": offset}),
        ((page,), {"limit": limit, "offset": offset, "mode": mode, "body": body}),
        ((page,), {"limit": limit, "offset": offset, "mode": mode}),
        ((page,), {"limit": limit, "offset": offset}),
        ((page,), {}),
    ]

    for args, kwargs in candidates:
        try:
            res = core_get_sheet_rows(*args, **kwargs)
            res = await _maybe_await(res)
            if isinstance(res, dict):
                return res, "core:get_sheet_rows"
            return ({"rows": res} if isinstance(res, list) else {"rows": []}), "core:get_sheet_rows"
        except TypeError as e:
            last_err = e
            continue
        except Exception as e:
            last_err = e
            break

    if last_err is not None:
        return {"status": "error", "error": str(last_err), "rows": []}, "core:get_sheet_rows"
    return None, None


# =============================================================================
# Health
# =============================================================================
@router.get("/health")
async def advanced_sheet_rows_health(request: Request) -> Dict[str, Any]:
    settings = None
    try:
        settings = get_settings_cached()
    except Exception:
        settings = None

    auth_summary = None
    try:
        if callable(mask_settings) and settings is not None:
            masked = mask_settings(settings)
            auth_summary = {
                "open_mode_effective": masked.get("open_mode_effective"),
                "token_count": masked.get("token_count"),
            }
    except Exception:
        auth_summary = None

    engine_present = False
    try:
        engine_present = bool(getattr(getattr(request.app, "state", None), "engine", None))
    except Exception:
        engine_present = False

    return {
        "status": "ok",
        "service": "advanced_sheet_rows",
        "version": ADVANCED_SHEET_ROWS_VERSION,
        "schema_available": bool(get_sheet_spec is not None),
        "allowed_pages_count": len(allowed_pages()) if callable(allowed_pages) else len(CANONICAL_PAGES),
        "engine_present": engine_present,
        "auth": auth_summary,
        "path": str(getattr(getattr(request, "url", None), "path", "")),
    }


# =============================================================================
# Route
# =============================================================================
@router.post("/sheet-rows")
async def advanced_sheet_rows(
    request: Request,
    body: Dict[str, Any] = Body(...),
    mode: str = Query(default="", description="Optional mode hint for engine/provider"),
    include_matrix_q: Optional[bool] = Query(default=None, alias="include_matrix"),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    start = time.time()
    request_id = x_request_id or getattr(getattr(request, "state", None), "request_id", None) or str(uuid.uuid4())

    if get_sheet_spec is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "Schema registry unavailable",
                "module": "routes.advanced_sheet_rows",
                "schema_import_error": _SCHEMA_IMPORT_ERROR,
            },
        )

    try:
        settings = get_settings_cached()
    except Exception:
        settings = None

    _ensure_authorized(
        request=request,
        settings=settings,
        token_query=token,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
    )

    page_raw = _pick_page_from_body(body) or "Market_Leaders"
    try:
        page = normalize_page_name(page_raw, allow_output_pages=True)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": f"Invalid page: {str(e)}",
                "allowed_pages": allowed_pages() if callable(allowed_pages) else list(CANONICAL_PAGES),
            },
        )

    _ensure_page_allowed(page)
    route_family = str(get_route_family(page))

    include_matrix = _maybe_bool(body.get("include_matrix"), include_matrix_q if include_matrix_q is not None else True)
    limit = max(1, min(5000, _maybe_int(body.get("limit"), 2000)))
    offset = max(0, _maybe_int(body.get("offset"), 0))

    try:
        headers, keys, spec = _schema_headers_keys(page)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": f"Unknown page schema: {page}",
                "detail": str(e),
                "allowed_pages": allowed_pages() if callable(allowed_pages) else list(CANONICAL_PAGES),
            },
        )

    if not headers or not keys:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": f"Schema for page '{page}' is empty", "page": page},
        )

    raw_symbols = _get_list(body, "symbols", "tickers", "tickers_list")
    top_n = max(1, min(5000, _maybe_int(body.get("top_n"), len(raw_symbols) if raw_symbols else limit)))
    symbols_all = _dedupe_keep_order(raw_symbols)[:top_n]

    if route_family == "insights":
        rows, builder_meta = await _call_builder_best_effort(
            module_names=("core.analysis.insights_builder",),
            function_names=(
                "build_insights_analysis_rows",
                "build_insights_rows",
                "build_insights_analysis",
                "get_insights_rows",
                "build_rows",
            ),
            request=request,
            settings=settings,
            mode=(mode or ""),
            body=body,
            schema_keys=keys,
            schema_headers=headers,
            friendly_name="Insights_Analysis",
        )
        rows = _slice(rows, limit=limit, offset=offset)
        builder_status = _normalize_status_word(_coerce_status(builder_meta.get("builder_status") if isinstance(builder_meta, dict) else None, "success"))

        return {
            "status": builder_status,
            "page": page,
            "route_family": route_family,
            "headers": headers,
            "keys": keys,
            "rows": rows,
            "rows_matrix": _rows_to_matrix(rows, keys) if include_matrix else None,
            "version": ADVANCED_SHEET_ROWS_VERSION,
            "request_id": request_id,
            "meta": {
                "duration_ms": (time.time() - start) * 1000.0,
                "dispatch": "insights_builder",
                "mode": mode,
                "limit": limit,
                "offset": offset,
                "count": len(rows),
                **builder_meta,
            },
        }

    if route_family == "top10":
        rows, builder_meta = await _call_builder_best_effort(
            module_names=(
                "core.analysis.top10_selector",
                "core.analysis.top10_builder",
                "core.analysis.top_10_builder",
                "core.analysis.top_10_investments_builder",
                "core.analysis.top10_investments_builder",
            ),
            function_names=(
                "build_top10_rows",
                "build_top_10_investments_rows",
                "build_top10_investments_rows",
                "build_top_10_rows",
                "get_top10_rows",
                "select_top10_rows",
                "build_rows",
            ),
            request=request,
            settings=settings,
            mode=(mode or ""),
            body=body,
            schema_keys=keys,
            schema_headers=headers,
            friendly_name="Top_10_Investments",
        )
        rows = _slice(rows, limit=limit, offset=offset)

        status_out = "success" if rows else "warn"
        builder_status = _normalize_status_word(_coerce_status(builder_meta.get("builder_status") if isinstance(builder_meta, dict) else None, status_out))

        return {
            "status": builder_status,
            "page": page,
            "route_family": route_family,
            "headers": headers,
            "keys": keys,
            "rows": rows,
            "rows_matrix": _rows_to_matrix(rows, keys) if include_matrix else None,
            "version": ADVANCED_SHEET_ROWS_VERSION,
            "request_id": request_id,
            "meta": {
                "duration_ms": (time.time() - start) * 1000.0,
                "dispatch": "top10_builder",
                "mode": mode,
                "limit": limit,
                "offset": offset,
                "count": len(rows),
                **builder_meta,
            },
        }

    if route_family == "dictionary":
        rows = _build_data_dictionary_rows_to_schema(schema_keys=keys, schema_headers=headers)
        rows = _slice(rows, limit=limit, offset=offset)

        return {
            "status": "success",
            "page": page,
            "route_family": route_family,
            "headers": headers,
            "keys": keys,
            "rows": rows,
            "rows_matrix": _rows_to_matrix(rows, keys) if include_matrix else None,
            "version": ADVANCED_SHEET_ROWS_VERSION,
            "request_id": request_id,
            "meta": {
                "duration_ms": (time.time() - start) * 1000.0,
                "dispatch": "data_dictionary",
                "mode": mode,
                "limit": limit,
                "offset": offset,
                "count": len(rows),
            },
        }

    engine = await _get_engine(request)

    if not symbols_all:
        payload, payload_source = await _call_engine_sheet_rows_best_effort(
            engine=engine,
            page=page,
            limit=limit,
            offset=offset,
            mode=(mode or ""),
            body=body,
        )

        if payload is None:
            payload, payload_source = await _call_core_sheet_rows_best_effort(
                page=page,
                limit=limit,
                offset=offset,
                mode=(mode or ""),
                body=body,
            )

        if payload is None:
            return {
                "status": "warn",
                "page": page,
                "route_family": route_family,
                "headers": headers,
                "keys": keys,
                "rows": [],
                "rows_matrix": [] if include_matrix else None,
                "version": ADVANCED_SHEET_ROWS_VERSION,
                "request_id": request_id,
                "meta": {
                    "duration_ms": (time.time() - start) * 1000.0,
                    "mode": mode,
                    "limit": limit,
                    "offset": offset,
                    "count": 0,
                    "dispatch": "schema_only_table_mode",
                    "engine_present": bool(engine),
                    "warning": "no_table_payload_from_engine_or_core",
                },
            }

        raw_rows = _extract_rows_like(payload)
        matrix_rows = _extract_matrix_like(payload)
        if not raw_rows and matrix_rows:
            raw_rows = _matrix_to_rows(matrix_rows, keys)

        rows = [_normalize_to_schema_keys(schema_keys=keys, schema_headers=headers, raw=r) for r in raw_rows]
        if not _payload_matches_requested_window(payload, limit=limit, offset=offset):
            rows = _slice(rows, limit=limit, offset=offset)

        status_out = _normalize_status_word(_coerce_status(payload.get("status") if isinstance(payload, dict) else None, "success"))
        err_out = payload.get("error") if isinstance(payload, dict) else None

        return {
            "status": status_out,
            "page": page,
            "route_family": route_family,
            "headers": headers,
            "keys": keys,
            "rows": rows,
            "rows_matrix": _rows_to_matrix(rows, keys) if include_matrix else None,
            "error": err_out,
            "version": ADVANCED_SHEET_ROWS_VERSION,
            "request_id": request_id,
            "meta": {
                "duration_ms": (time.time() - start) * 1000.0,
                "mode": mode,
                "limit": limit,
                "offset": offset,
                "count": len(rows),
                "dispatch": payload_source or ("engine_sheet_rows" if engine is not None else "core_get_sheet_rows"),
                "engine_present": bool(engine),
                **_payload_meta(payload),
            },
        }

    if not is_instrument_page(page):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": f"Page '{page}' is not an instrument page for symbol-based retrieval."},
        )

    if not engine:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Data engine unavailable")

    symbols = _slice_values(symbols_all, limit=limit, offset=offset)
    data_map = await _fetch_advanced_rows(engine, symbols, mode=(mode or ""), settings=settings, schema=spec)

    normalize_fn = None
    try:
        from core.data_engine_v2 import normalize_row_to_schema as _n  # type: ignore
        normalize_fn = _n
    except Exception:
        normalize_fn = None

    normalized_rows: List[Dict[str, Any]] = []
    errors = 0

    for sym in symbols:
        raw_obj = data_map.get(sym)
        raw = _to_plain_dict(raw_obj)

        if isinstance(raw, dict) and raw.get("error"):
            errors += 1

        if callable(normalize_fn):
            try:
                row = normalize_fn(page, raw, keep_extras=True)
                if not isinstance(row, dict):
                    row = _normalize_to_schema_keys(schema_keys=keys, schema_headers=headers, raw=raw)
                else:
                    row = _normalize_to_schema_keys(schema_keys=keys, schema_headers=headers, raw=row)
            except Exception:
                row = _normalize_to_schema_keys(schema_keys=keys, schema_headers=headers, raw=raw)
        else:
            row = _normalize_to_schema_keys(schema_keys=keys, schema_headers=headers, raw=raw)

        if "symbol" in keys and not row.get("symbol"):
            row["symbol"] = sym
        if "ticker" in keys and not row.get("ticker"):
            row["ticker"] = sym

        normalized_rows.append(row)

    status_out = "success" if errors == 0 else ("partial" if errors < len(symbols) else "error")

    return {
        "status": status_out,
        "page": page,
        "route_family": route_family,
        "headers": headers,
        "keys": keys,
        "rows": normalized_rows,
        "rows_matrix": _rows_to_matrix(normalized_rows, keys) if include_matrix else None,
        "error": f"{errors} errors" if errors else None,
        "version": ADVANCED_SHEET_ROWS_VERSION,
        "request_id": request_id,
        "meta": {
            "duration_ms": (time.time() - start) * 1000.0,
            "requested_total": len(symbols_all),
            "requested_window": len(symbols),
            "returned": len(normalized_rows),
            "errors": errors,
            "mode": mode,
            "limit": limit,
            "offset": offset,
            "top_n": top_n,
            "schema_headers_always": bool(getattr(settings, "schema_headers_always", True)) if settings else True,
            "computations_enabled": _settings_get_bool(settings, "computations_enabled", default=True),
            "forecasting_enabled": _settings_get_bool(settings, "forecasting_enabled", default=True),
            "scoring_enabled": _settings_get_bool(settings, "scoring_enabled", default=True),
            "dispatch": "instrument_mode",
            "engine_present": True,
        },
    }


__all__ = ["router", "ADVANCED_SHEET_ROWS_VERSION"]
