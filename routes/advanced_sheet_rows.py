#!/usr/bin/env python3
# routes/advanced_sheet_rows.py
"""
================================================================================
Advanced Sheet-Rows Router — v2.0.0
================================================================================
SCHEMA-FIRST • CANONICAL PAGE DISPATCH • ANALYSIS-ALIGNED • SPECIAL-PAGE SAFE

Endpoint
--------
POST /v1/advanced/sheet-rows

Primary role
------------
Advanced sheet-rows behavior aligned with routes/analysis_sheet_rows.py so that:
- page normalization is identical
- schema headers/keys are always authoritative
- special pages never fall back to the generic 80-column instrument schema
- instrument pages use engine-backed row retrieval when symbols are provided
- table/special page behavior stays deterministic and stable

What this revision improves
---------------------------
- ✅ FIX: Aligns dispatch behavior with analysis_sheet_rows
- ✅ FIX: Canonical page normalization via page_catalog
- ✅ FIX: Explicit route-family dispatch:
    - Insights_Analysis  -> insights builder
    - Top_10_Investments -> top10 builder/selector
    - Data_Dictionary    -> data dictionary builder
    - instrument pages   -> engine-backed rows
- ✅ FIX: Better special-page isolation (no accidental standard-schema fallback)
- ✅ FIX: Stable response shape for all pages
- ✅ FIX: Stronger normalization from dict/object/list row shapes
- ✅ FIX: Table mode can use shared core_get_sheet_rows if available
- ✅ SAFE: No network I/O at import time
- ✅ SAFE: Optional auth only if core.config.auth_ok exists

Accepted body fields
--------------------
Page:
- sheet / page / sheet_name / sheetName / name / tab

Symbols:
- symbols / tickers / tickers_list

Optional:
- limit
- offset
- include_matrix
- top_n

Response
--------
{
  status,
  page,
  route_family,
  headers,
  keys,
  rows,
  rows_matrix?,
  error?,
  version,
  request_id,
  meta
}
================================================================================
"""

from __future__ import annotations

import importlib
import inspect
import os
import time
import uuid
from dataclasses import is_dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

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
# Optional auth
# -----------------------------------------------------------------------------
try:
    from core.config import auth_ok, get_settings_cached  # type: ignore
except Exception:  # pragma: no cover
    auth_ok = None  # type: ignore

    def get_settings_cached(*args, **kwargs):  # type: ignore
        return None

# -----------------------------------------------------------------------------
# Optional shared table-mode helper
# -----------------------------------------------------------------------------
try:
    from core.data_engine import get_sheet_rows as core_get_sheet_rows  # type: ignore
except Exception:  # pragma: no cover
    core_get_sheet_rows = None  # type: ignore

ADVANCED_SHEET_ROWS_VERSION = "2.0.0"
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
    if isinstance(v, Iterable):
        try:
            return list(v)
        except Exception:
            return [v]
    return [v]


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
    if isinstance(v, (int, float)):
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
            return out
        if isinstance(v, str) and v.strip():
            parts = [p.strip() for p in v.split(",") if p.strip()]
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


def _allow_query_token(settings: Any, request: Request) -> bool:
    try:
        if settings is not None:
            return bool(getattr(settings, "allow_query_token", False))
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
    authorization: Optional[str],
    settings: Any,
    request: Request,
) -> str:
    auth_token = _strip(x_app_token)

    if authorization and authorization.strip().lower().startswith("bearer "):
        auth_token = authorization.strip().split(" ", 1)[1].strip()

    if token_query and not auth_token and _allow_query_token(settings, request):
        auth_token = _strip(token_query)

    return auth_token


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


def _schema_headers_keys(page: str) -> Tuple[List[str], List[str], Any]:
    if get_sheet_spec is None:
        raise KeyError("schema_registry unavailable")

    spec = get_sheet_spec(page)
    cols = getattr(spec, "columns", None) or []

    headers = [str(getattr(c, "header", "")) for c in cols if getattr(c, "header", None)]
    keys = [str(getattr(c, "key", "")) for c in cols if getattr(c, "key", None)]

    return headers, keys, spec


def _slice(rows: List[Dict[str, Any]], *, limit: int, offset: int) -> List[Dict[str, Any]]:
    start = max(0, int(offset))
    if limit <= 0:
        return rows[start:]
    end = start + max(0, int(limit))
    return rows[start:end]


def _rows_to_matrix(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    return [[r.get(k) for k in keys] for r in rows]


def _normalize_to_schema_keys(
    *,
    schema_keys: Sequence[str],
    schema_headers: Sequence[str],
    raw: Mapping[str, Any],
) -> Dict[str, Any]:
    """
    Normalize raw dict to schema keys.
    Accepts:
    - schema keys directly
    - schema headers directly
    - case-insensitive variants
    """
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
    if hit >= max(1, min(3, len(symset) // 3)):
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

    computations_enabled = bool(getattr(settings, "computations_enabled", True)) if settings is not None else True
    forecasting_enabled = bool(getattr(settings, "forecasting_enabled", True)) if settings is not None else True
    scoring_enabled = bool(getattr(settings, "scoring_enabled", True)) if settings is not None else True
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

                data = res.get("data") or res.get("rows") or res.get("items")
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
) -> List[Dict[str, Any]]:
    try:
        mod = _import_first_available(module_names)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": f"{friendly_name} builder import failed", "detail": str(e)},
        )

    fn = None
    for name in function_names:
        cand = getattr(mod, name, None)
        if callable(cand):
            fn = cand
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

    rows: List[Dict[str, Any]] = []

    if isinstance(out, list):
        rows = [_to_plain_dict(r) for r in out]
    elif isinstance(out, dict):
        r2 = out.get("rows") or out.get("data") or out.get("items") or out.get("records")
        if isinstance(r2, list):
            rows = [_to_plain_dict(r) for r in r2]
        else:
            rows = [_to_plain_dict(out)]
    else:
        rows = []

    return [
        _normalize_to_schema_keys(schema_keys=schema_keys, schema_headers=schema_headers, raw=(r or {}))
        for r in rows
    ]


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
) -> Optional[Dict[str, Any]]:
    if core_get_sheet_rows is None:
        return None

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
                return res
            return {"rows": res} if isinstance(res, list) else {"rows": []}
        except TypeError as e:
            last_err = e
            continue
        except Exception as e:
            last_err = e
            break

    if last_err is not None:
        return {"status": "error", "error": str(last_err), "rows": []}
    return None


# =============================================================================
# Route
# =============================================================================
@router.post("/sheet-rows")
async def advanced_sheet_rows(
    request: Request,
    body: Dict[str, Any] = Body(...),
    mode: str = Query(default="", description="Optional mode hint for engine/provider"),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    start = time.time()
    request_id = x_request_id or str(uuid.uuid4())

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

    # -------------------------------------------------------------------------
    # Auth
    # -------------------------------------------------------------------------
    auth_token = _extract_auth_token(
        token_query=token,
        x_app_token=x_app_token,
        authorization=authorization,
        settings=settings,
        request=request,
    )

    if auth_ok is not None:
        if not auth_ok(
            token=auth_token,
            authorization=authorization,
            headers={"X-APP-TOKEN": x_app_token, "Authorization": authorization},
        ):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    # -------------------------------------------------------------------------
    # Page resolution
    # -------------------------------------------------------------------------
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

    include_matrix = _maybe_bool(body.get("include_matrix"), True)
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

    symbols = _get_list(body, "symbols", "tickers", "tickers_list")
    top_n = max(1, min(5000, _maybe_int(body.get("top_n"), 2000)))
    symbols = symbols[:top_n]

    # -------------------------------------------------------------------------
    # Explicit special-page dispatch
    # -------------------------------------------------------------------------
    if route_family == "insights":
        rows = await _call_builder_best_effort(
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
                "dispatch": "insights_builder",
                "mode": mode,
                "limit": limit,
                "offset": offset,
                "count": len(rows),
            },
        }

    if route_family == "top10":
        try:
            rows = await _call_builder_best_effort(
                module_names=(
                    "core.analysis.top10_selector",
                    "core.analysis.top10_builder",
                    "core.analysis.top_10_builder",
                    "core.analysis.top_10_investments_builder",
                    "core.analysis.top10_investments_builder",
                ),
                function_names=(
                    "build_top_10_investments_rows",
                    "build_top10_investments_rows",
                    "build_top_10_rows",
                    "build_top10_rows",
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
                    "dispatch": "top10_builder",
                    "mode": mode,
                    "limit": limit,
                    "offset": offset,
                    "count": len(rows),
                },
            }
        except HTTPException:
            # fall through to shared table-mode helper if available
            pass

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

    # -------------------------------------------------------------------------
    # Table mode for instrument pages (no symbols)
    # -------------------------------------------------------------------------
    if not symbols:
        payload = await _call_core_sheet_rows_best_effort(
            page=page,
            limit=limit,
            offset=offset,
            mode=(mode or ""),
            body=body,
        )

        if payload is None:
            return {
                "status": "success",
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
                    "dispatch": "schema_only_table_mode",
                },
            }

        raw_rows = payload.get("rows") or payload.get("data") or payload.get("items") or []
        raw_rows = [_to_plain_dict(r) for r in _as_list(raw_rows)]
        rows = [_normalize_to_schema_keys(schema_keys=keys, schema_headers=headers, raw=r) for r in raw_rows]
        rows = _slice(rows, limit=limit, offset=offset)

        status_out = payload.get("status") or "success"
        err_out = payload.get("error")

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
                "dispatch": "core_get_sheet_rows" if core_get_sheet_rows is not None else "table_mode_fallback",
                **(payload.get("meta") or {}),
            },
        }

    # -------------------------------------------------------------------------
    # Instrument mode (symbols provided)
    # -------------------------------------------------------------------------
    if not is_instrument_page(page):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": f"Page '{page}' is not an instrument page for symbol-based retrieval."},
        )

    engine = await _get_engine(request)
    if not engine:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Data engine unavailable")

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
                for k in keys:
                    if k not in row:
                        row[k] = None
            except Exception:
                row = _normalize_to_schema_keys(schema_keys=keys, schema_headers=headers, raw=raw)
        else:
            row = _normalize_to_schema_keys(schema_keys=keys, schema_headers=headers, raw=raw)

        if "symbol" in keys and not row.get("symbol"):
            row["symbol"] = sym

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
            "requested": len(symbols),
            "errors": errors,
            "mode": mode,
            "schema_headers_always": bool(getattr(settings, "schema_headers_always", True)) if settings else True,
            "computations_enabled": bool(getattr(settings, "computations_enabled", True)) if settings else True,
            "forecasting_enabled": bool(getattr(settings, "forecasting_enabled", True)) if settings else True,
            "scoring_enabled": bool(getattr(settings, "scoring_enabled", True)) if settings else True,
            "dispatch": "instrument_mode",
        },
    }


__all__ = ["router", "ADVANCED_SHEET_ROWS_VERSION"]
