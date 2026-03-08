#!/usr/bin/env python3
# routes/analysis_sheet_rows.py
"""
================================================================================
Analysis Sheet-Rows Router — v2.3.0
================================================================================
SCHEMA-FIRST • CANONICAL PAGE DISPATCH • SPECIAL-PAGE SAFE • ROUTE-CANONICAL
PATH-AWARE AUTH • STABLE RESPONSE SHAPE • POWERSHELL/LIVE-TEST FRIENDLY
GET+POST SAFE • BUILDER-FLEXIBLE • ENGINE-FLEXIBLE • HEADER/KEY STABLE
REAL-WORLD PAYLOAD TOLERANT • DATA-ENGINE FALLBACK SAFE

Endpoints
---------
GET  /v1/analysis/health
GET  /v1/analysis/sheet-rows
POST /v1/analysis/sheet-rows

Goals
-----
- Keep schema_registry as the authority for headers/keys
- Normalize page names through page_catalog
- Handle special pages explicitly:
    - Insights_Analysis
    - Top_10_Investments
    - Data_Dictionary
- Support GET and POST probing from browser / PowerShell / Apps Script
- Keep response envelopes stable even when builders or engine are partial
- Avoid import-time network I/O
"""

from __future__ import annotations

import importlib
import inspect
import logging
import os
import time
import uuid
from dataclasses import is_dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, status

logger = logging.getLogger("routes.analysis_sheet_rows")

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
# Optional shared table-mode helper
# -----------------------------------------------------------------------------
try:
    from core.data_engine import get_sheet_rows as core_get_sheet_rows  # type: ignore
except Exception:  # pragma: no cover
    core_get_sheet_rows = None  # type: ignore


ANALYSIS_SHEET_ROWS_VERSION = "2.3.0"
router = APIRouter(prefix="/v1/analysis", tags=["Analysis Sheet Rows"])


# =============================================================================
# Generic helpers
# =============================================================================
_FIELD_ALIAS_HINTS: Dict[str, List[str]] = {
    "symbol": ["ticker", "code", "instrument", "security", "symbol_code"],
    "ticker": ["symbol", "code", "instrument", "security"],
    "current_price": [
        "price",
        "last_price",
        "last",
        "close",
        "market_price",
        "current",
        "spot",
        "nav",
    ],
    "recommendation_reason": ["reason", "reco_reason", "recommendation_notes"],
    "top10_rank": ["rank", "top_rank"],
    "selection_reason": ["selection_notes", "selector_reason"],
    "criteria_snapshot": ["criteria", "criteria_json", "snapshot"],
}


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
        s = v.strip().lower()
        if s in {"1", "true", "yes", "y", "on"}:
            return True
        if s in {"0", "false", "no", "n", "off"}:
            return False
    return default


def _split_symbols_string(v: str) -> List[str]:
    raw = (v or "").replace(";", ",").replace("\n", ",").replace("\t", ",")
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    out: List[str] = []
    seen = set()
    for p in parts:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def _get_list(body: Mapping[str, Any], *keys: str) -> List[str]:
    for k in keys:
        v = body.get(k)
        if isinstance(v, list):
            out: List[str] = []
            seen = set()
            for item in v:
                s = _strip(item)
                if s and s not in seen:
                    seen.add(s)
                    out.append(s)
            return out
        if isinstance(v, str) and v.strip():
            return _split_symbols_string(v)
    return []


def _to_plain_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return dict(obj)

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


def _request_id(request: Request, x_request_id: Optional[str]) -> str:
    if x_request_id and _strip(x_request_id):
        return _strip(x_request_id)
    try:
        rid = _strip(getattr(request.state, "request_id", ""))
        if rid:
            return rid
    except Exception:
        pass
    return str(uuid.uuid4())[:12]


def _collect_get_body(request: Request) -> Dict[str, Any]:
    qp = request.query_params
    body: Dict[str, Any] = {}

    for key in ("sheet", "page", "sheet_name", "sheetName", "name", "tab"):
        v = _strip(qp.get(key))
        if v:
            body[key] = v

    for key in ("symbols", "tickers", "tickers_list"):
        vals = qp.getlist(key)
        if vals:
            if len(vals) == 1:
                body[key] = _split_symbols_string(vals[0])
            else:
                body[key] = [s.strip() for s in vals if _strip(s)]
            break

    for key in ("limit", "offset", "top_n", "include_matrix"):
        v = qp.get(key)
        if v is not None:
            body[key] = v

    return body


def _merge_body_with_query(body: Optional[Dict[str, Any]], request: Request) -> Dict[str, Any]:
    out = dict(body or {})
    qbody = _collect_get_body(request)
    for k, v in qbody.items():
        if k not in out or out.get(k) in (None, "", []):
            out[k] = v
    return out


def _is_public_path(settings: Any, path: str) -> bool:
    p = _strip(path)

    if p in {"/", "/health", "/readyz", "/livez", "/meta"}:
        return True
    if p.endswith("/health"):
        return True

    public_paths_env = os.getenv("PUBLIC_PATHS", "") or os.getenv("AUTH_PUBLIC_PATHS", "")
    env_paths = [x.strip() for x in public_paths_env.split(",") if x.strip()]
    for candidate in env_paths:
        if candidate.endswith("*"):
            if p.startswith(candidate[:-1]):
                return True
        elif p == candidate:
            return True

    if settings is None:
        return False

    for attr in ("public_paths", "PUBLIC_PATHS", "auth_public_paths", "AUTH_PUBLIC_PATHS"):
        try:
            value = getattr(settings, attr, None)
            for candidate in _as_list(value):
                c = _strip(candidate)
                if not c:
                    continue
                if c.endswith("*"):
                    if p.startswith(c[:-1]):
                        return True
                elif p == c:
                    return True
        except Exception:
            continue

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


def _auth_passed(
    *,
    request: Request,
    settings: Any,
    auth_token: str,
    authorization: Optional[str],
) -> bool:
    if auth_ok is None:
        return True

    path = str(getattr(getattr(request, "url", None), "path", "") or "")

    if _is_public_path(settings, path):
        return True

    headers_dict = dict(request.headers)

    call_attempts = [
        {
            "token": auth_token,
            "authorization": authorization,
            "headers": headers_dict,
            "path": path,
            "request": request,
            "settings": settings,
        },
        {
            "token": auth_token,
            "authorization": authorization,
            "headers": headers_dict,
            "path": path,
            "request": request,
        },
        {
            "token": auth_token,
            "authorization": authorization,
            "headers": headers_dict,
            "path": path,
        },
        {
            "token": auth_token,
            "authorization": authorization,
            "headers": headers_dict,
        },
        {
            "token": auth_token,
            "authorization": authorization,
        },
        {
            "token": auth_token,
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

    cols = getattr(spec, "columns", None)
    if cols is None and isinstance(spec, Mapping):
        cols = spec.get("columns")
    cols = _as_list(cols)

    headers: List[str] = []
    keys: List[str] = []

    for c in cols:
        cd = c if isinstance(c, Mapping) else _to_plain_dict(c)
        h = _strip(cd.get("header") if isinstance(cd, dict) else getattr(c, "header", None))
        k = _strip(cd.get("key") if isinstance(cd, dict) else getattr(c, "key", None))
        if h:
            headers.append(h)
        if k:
            keys.append(k)

    return headers, keys, spec


def _slice(rows: List[Dict[str, Any]], *, limit: int, offset: int) -> List[Dict[str, Any]]:
    start = max(0, int(offset))
    if limit <= 0:
        return rows[start:]
    end = start + max(0, int(limit))
    return rows[start:end]


def _rows_to_matrix(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    return [[r.get(k) for k in keys] for r in rows]


def _key_variants(key: str) -> List[str]:
    k = _strip(key)
    if not k:
        return []

    variants = [
        k,
        k.lower(),
        k.upper(),
        k.replace("_", " "),
        k.replace("_", "").lower(),
    ]
    for alias in _FIELD_ALIAS_HINTS.get(k, []):
        variants.extend([
            alias,
            alias.lower(),
            alias.upper(),
            alias.replace("_", " "),
            alias.replace("_", "").lower(),
        ])

    seen = set()
    out: List[str] = []
    for v in variants:
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _extract_from_raw(raw: Dict[str, Any], candidates: Sequence[str]) -> Any:
    raw_ci = {str(k).strip().lower(): v for k, v in raw.items()}

    for candidate in candidates:
        if candidate in raw:
            return raw.get(candidate)
        lc = candidate.lower()
        if lc in raw_ci:
            return raw_ci.get(lc)
    return None


def _normalize_to_schema_keys(
    *,
    schema_keys: Sequence[str],
    schema_headers: Sequence[str],
    raw: Mapping[str, Any],
) -> Dict[str, Any]:
    raw = dict(raw or {})

    header_by_key: Dict[str, str] = {}
    for k, h in zip(schema_keys, schema_headers):
        header_by_key[str(k)] = str(h)

    out: Dict[str, Any] = {}
    for k in schema_keys:
        ks = str(k)
        v = _extract_from_raw(raw, _key_variants(ks))

        if v is None:
            h = header_by_key.get(ks, "")
            if h:
                v = _extract_from_raw(raw, [h, h.lower(), h.upper()])

        out[ks] = v

    # Light enrichment / field harmonization
    if "symbol" in out and not out.get("symbol"):
        out["symbol"] = _extract_from_raw(raw, _key_variants("symbol"))
    if "ticker" in out and not out.get("ticker"):
        out["ticker"] = _extract_from_raw(raw, _key_variants("ticker"))

    if "current_price" in out and out.get("current_price") is None:
        out["current_price"] = _extract_from_raw(raw, _key_variants("current_price"))

    return out


def _extract_rows_like(payload: Any) -> List[Dict[str, Any]]:
    if payload is None:
        return []

    if isinstance(payload, list):
        if payload and isinstance(payload[0], (list, tuple)):
            return []
        return [_to_plain_dict(r) for r in payload]

    if isinstance(payload, dict):
        for name in ("rows", "data", "items", "records", "quotes"):
            value = payload.get(name)
            if isinstance(value, list):
                if value and isinstance(value[0], (list, tuple)):
                    continue
                return [_to_plain_dict(r) for r in value]

            if isinstance(value, dict):
                for inner_name in ("rows", "data", "items", "records", "quotes"):
                    inner = value.get(inner_name)
                    if isinstance(inner, list):
                        if inner and isinstance(inner[0], (list, tuple)):
                            continue
                        return [_to_plain_dict(r) for r in inner]

        for name in ("payload", "result"):
            nested = payload.get(name)
            if isinstance(nested, dict):
                nested_rows = _extract_rows_like(nested)
                if nested_rows:
                    return nested_rows

    return []


def _extract_matrix_like(payload: Any) -> Optional[List[List[Any]]]:
    if isinstance(payload, dict):
        for name in ("rows_matrix",):
            value = payload.get(name)
            if isinstance(value, list):
                return [list(r) if isinstance(r, (list, tuple)) else [r] for r in value]

        rows_value = payload.get("rows")
        if isinstance(rows_value, list) and rows_value and isinstance(rows_value[0], (list, tuple)):
            return [list(r) if isinstance(r, (list, tuple)) else [r] for r in rows_value]

        for name in ("data", "payload", "result"):
            nested = payload.get(name)
            if isinstance(nested, dict):
                mx = _extract_matrix_like(nested)
                if mx is not None:
                    return mx

    return None


def _extract_status_error(payload: Any) -> Tuple[str, Optional[str], Dict[str, Any]]:
    if not isinstance(payload, dict):
        return "success", None, {}

    status_out = _strip(payload.get("status")) or "success"
    error_out = payload.get("error")
    meta_out = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    return status_out, (str(error_out) if error_out is not None else None), meta_out


def _matrix_to_rows(matrix: Sequence[Sequence[Any]], keys: Sequence[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in matrix:
        vals = list(row) if isinstance(row, (list, tuple)) else [row]
        item: Dict[str, Any] = {}
        for idx, k in enumerate(keys):
            item[k] = vals[idx] if idx < len(vals) else None
        rows.append(item)
    return rows


def _empty_schema_row(keys: Sequence[str], *, symbol: str = "") -> Dict[str, Any]:
    row = {k: None for k in keys}
    if symbol:
        if "symbol" in row and not row.get("symbol"):
            row["symbol"] = symbol
        if "ticker" in row and not row.get("ticker"):
            row["ticker"] = symbol
    return row


def _payload(
    *,
    page: str,
    route_family: str,
    headers: Sequence[str],
    keys: Sequence[str],
    rows: Sequence[Mapping[str, Any]],
    include_matrix: bool,
    request_id: str,
    started_at: float,
    mode: str,
    status_out: str = "success",
    error_out: Optional[str] = None,
    meta_extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    rows_list = [dict(r) for r in rows]
    meta = {
        "duration_ms": (time.time() - started_at) * 1000.0,
        "mode": mode,
        "count": len(rows_list),
    }
    if meta_extra:
        meta.update(meta_extra)

    return {
        "status": status_out,
        "page": page,
        "route_family": route_family,
        "headers": list(headers),
        "keys": list(keys),
        "rows": rows_list,
        "rows_matrix": _rows_to_matrix(rows_list, keys) if include_matrix else None,
        "data": rows_list,
        "quotes": rows_list,
        "error": error_out,
        "version": ANALYSIS_SHEET_ROWS_VERSION,
        "request_id": request_id,
        "meta": meta,
    }


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
    if hit >= max(1, min(3, len(symset) // 3 if len(symset) > 2 else 1)):
        return True
    return False


async def _fetch_analysis_rows(
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
    want_analysis = computations_enabled and (forecasting_enabled or scoring_enabled)

    preferred: List[str] = []
    if want_analysis:
        preferred += [
            "get_analysis_rows_batch",
            "get_analysis_quotes_batch",
            "get_enriched_quotes_batch",
        ]
    else:
        preferred += ["get_enriched_quotes_batch"]

    preferred += [
        "get_enriched_quotes",
        "get_quotes_batch",
        "quotes_batch",
    ]

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
                out[s] = {"symbol": s, "ticker": s, "error": "engine_missing_quote_method"}
        except Exception as e:
            out[s] = {"symbol": s, "ticker": s, "error": str(e)}

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


async def _call_function_flexible(fn: Any, call_specs: Sequence[Tuple[Tuple[Any, ...], Dict[str, Any]]]) -> Any:
    last_err: Optional[Exception] = None
    for args, kwargs in call_specs:
        try:
            res = fn(*args, **kwargs)
            return await _maybe_await(res)
        except TypeError as e:
            last_err = e
            continue
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(str(last_err) if last_err else "call failed")


async def _call_builder_best_effort(
    *,
    module_names: Sequence[str],
    function_names: Sequence[str],
    request: Request,
    settings: Any,
    engine: Any,
    mode: str,
    body: Dict[str, Any],
    schema_keys: Sequence[str],
    schema_headers: Sequence[str],
    friendly_name: str,
) -> Tuple[List[Dict[str, Any]], str, Optional[str], Dict[str, Any]]:
    try:
        mod = _import_first_available(module_names)
    except Exception as e:
        return [], "partial", f"{friendly_name} builder import failed: {e}", {"builder_import_failed": True}

    fn = None
    chosen_name = ""
    for name in function_names:
        cand = getattr(mod, name, None)
        if callable(cand):
            fn = cand
            chosen_name = name
            break

    if fn is None:
        return [], "partial", f"{friendly_name} builder missing callable function", {"builder_missing": True}

    call_specs: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = [
        ((), {"request": request, "settings": settings, "engine": engine, "mode": mode, "body": body}),
        ((), {"request": request, "settings": settings, "data_engine": engine, "mode": mode, "body": body}),
        ((), {"request": request, "settings": settings, "quote_engine": engine, "mode": mode, "body": body}),
        ((), {"request": request, "settings": settings, "mode": mode, "body": body}),
        ((), {"request": request, "engine": engine, "mode": mode, "body": body}),
        ((), {"engine": engine, "mode": mode, "body": body}),
        ((), {"data_engine": engine, "mode": mode, "body": body}),
        ((), {"quote_engine": engine, "mode": mode, "body": body}),
        ((), {"request": request, "settings": settings, "mode": mode}),
        ((), {"settings": settings, "mode": mode, "body": body}),
        ((), {"mode": mode, "body": body}),
        ((), {"body": body}),
        ((), {"request": request}),
        ((), {}),
    ]

    try:
        out = await _call_function_flexible(fn, call_specs)
    except Exception as e:
        return [], "partial", f"{friendly_name} builder call failed: {e}", {"builder_call_failed": True, "builder_function": chosen_name}

    rows = _extract_rows_like(out)
    matrix = _extract_matrix_like(out)
    if not rows and matrix:
        rows = _matrix_to_rows(matrix, schema_keys)

    status_out, error_out, meta_out = _extract_status_error(out)
    normalized = [
        _normalize_to_schema_keys(schema_keys=schema_keys, schema_headers=schema_headers, raw=(r or {}))
        for r in rows
    ]
    meta_out = dict(meta_out or {})
    meta_out["builder_function"] = chosen_name
    return normalized, status_out, error_out, meta_out


def _build_data_dictionary_rows_to_schema(
    *,
    schema_keys: Sequence[str],
    schema_headers: Sequence[str],
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    try:
        from core.sheets.data_dictionary import build_data_dictionary_rows  # type: ignore
    except Exception as e:
        return [], f"Data_Dictionary generator import failed: {e}"

    try:
        raw_rows = build_data_dictionary_rows(include_meta_sheet=True)
    except TypeError:
        try:
            raw_rows = build_data_dictionary_rows()
        except Exception as e:
            return [], f"Data_Dictionary generator failed: {e}"
    except Exception as e:
        return [], f"Data_Dictionary generator failed: {e}"

    out: List[Dict[str, Any]] = []
    for r in _as_list(raw_rows):
        rd = _to_plain_dict(r)
        out.append(_normalize_to_schema_keys(schema_keys=schema_keys, schema_headers=schema_headers, raw=rd))
    return out, None


async def _call_core_sheet_rows_best_effort(
    *,
    engine: Any,
    page: str,
    limit: int,
    offset: int,
    mode: str,
    body: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    candidates: List[Any] = []

    if core_get_sheet_rows is not None:
        candidates.append(core_get_sheet_rows)

    if engine is not None:
        for name in ("get_sheet_rows", "sheet_rows", "build_sheet_rows"):
            fn = getattr(engine, name, None)
            if callable(fn):
                candidates.append(fn)

    for fn in candidates:
        call_specs = [
            ((), {"sheet": page, "limit": limit, "offset": offset, "mode": mode, "body": body}),
            ((), {"sheet": page, "limit": limit, "offset": offset, "mode": mode}),
            ((), {"sheet": page, "limit": limit, "offset": offset}),
            ((page,), {"limit": limit, "offset": offset, "mode": mode, "body": body}),
            ((page,), {"limit": limit, "offset": offset, "mode": mode}),
            ((page,), {"limit": limit, "offset": offset}),
            ((page,), {}),
        ]

        try:
            res = await _call_function_flexible(fn, call_specs)
            if isinstance(res, dict):
                return res
            if isinstance(res, list):
                return {"status": "success", "rows": res}
            return {"status": "success", "rows": []}
        except Exception:
            continue

    return None


# =============================================================================
# Health
# =============================================================================
@router.get("/health")
async def analysis_sheet_rows_health(request: Request) -> Dict[str, Any]:
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

    return {
        "status": "ok",
        "service": "analysis_sheet_rows",
        "version": ANALYSIS_SHEET_ROWS_VERSION,
        "schema_available": bool(get_sheet_spec is not None),
        "allowed_pages_count": len(allowed_pages()) if callable(allowed_pages) else len(CANONICAL_PAGES),
        "auth": auth_summary,
        "path": str(getattr(getattr(request, "url", None), "path", "")),
    }


# =============================================================================
# Internal implementation
# =============================================================================
async def _analysis_sheet_rows_impl(
    request: Request,
    body: Dict[str, Any],
    mode: str,
    include_matrix_q: Optional[bool],
    token: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
    x_request_id: Optional[str],
) -> Dict[str, Any]:
    start = time.time()
    request_id = _request_id(request, x_request_id)

    if get_sheet_spec is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "Schema registry unavailable",
                "module": "routes.analysis_sheet_rows",
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

    if not _auth_passed(
        request=request,
        settings=settings,
        auth_token=auth_token,
        authorization=authorization,
    ):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    # -------------------------------------------------------------------------
    # Page resolution
    # -------------------------------------------------------------------------
    merged_body = _merge_body_with_query(body, request)

    page_raw = _pick_page_from_body(merged_body) or "Market_Leaders"
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

    include_matrix = _maybe_bool(
        merged_body.get("include_matrix"),
        include_matrix_q if include_matrix_q is not None else True,
    )
    limit = max(1, min(5000, _maybe_int(merged_body.get("limit"), 2000)))
    offset = max(0, _maybe_int(merged_body.get("offset"), 0))

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

    symbols = _get_list(merged_body, "symbols", "tickers", "tickers_list")
    top_n = max(1, min(5000, _maybe_int(merged_body.get("top_n"), 2000)))
    symbols = symbols[:top_n]

    engine = await _get_engine(request)

    # -------------------------------------------------------------------------
    # Explicit special-page dispatch
    # -------------------------------------------------------------------------
    if route_family == "insights":
        rows, status_out, error_out, meta_out = await _call_builder_best_effort(
            module_names=("core.analysis.insights_builder",),
            function_names=(
                "build_insights_analysis_rows",
                "build_insights_rows",
                "build_insights_output_rows",
                "build_insights_analysis",
                "get_insights_rows",
                "build_rows",
            ),
            request=request,
            settings=settings,
            engine=engine,
            mode=(mode or ""),
            body=merged_body,
            schema_keys=keys,
            schema_headers=headers,
            friendly_name="Insights_Analysis",
        )

        rows = _slice(rows, limit=limit, offset=offset)

        return _payload(
            page=page,
            route_family=route_family,
            headers=headers,
            keys=keys,
            rows=rows,
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=start,
            mode=mode,
            status_out=status_out or "success",
            error_out=error_out,
            meta_extra={
                "dispatch": "insights_builder",
                "limit": limit,
                "offset": offset,
                **meta_out,
            },
        )

    if route_family == "top10":
        rows, status_out, error_out, meta_out = await _call_builder_best_effort(
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
                "build_top_10_output_rows",
                "build_top10_output_rows",
                "select_top10_rows",
                "get_top10_rows",
                "build_rows",
            ),
            request=request,
            settings=settings,
            engine=engine,
            mode=(mode or ""),
            body=merged_body,
            schema_keys=keys,
            schema_headers=headers,
            friendly_name="Top_10_Investments",
        )

        rows = _slice(rows, limit=limit, offset=offset)

        return _payload(
            page=page,
            route_family=route_family,
            headers=headers,
            keys=keys,
            rows=rows,
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=start,
            mode=mode,
            status_out=status_out or ("success" if rows else "partial"),
            error_out=error_out,
            meta_extra={
                "dispatch": "top10_builder",
                "limit": limit,
                "offset": offset,
                **meta_out,
            },
        )

    if route_family == "dictionary":
        rows, dd_error = _build_data_dictionary_rows_to_schema(schema_keys=keys, schema_headers=headers)
        rows = _slice(rows, limit=limit, offset=offset)

        return _payload(
            page=page,
            route_family=route_family,
            headers=headers,
            keys=keys,
            rows=rows,
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=start,
            mode=mode,
            status_out="success" if not dd_error else "partial",
            error_out=dd_error,
            meta_extra={
                "dispatch": "data_dictionary",
                "limit": limit,
                "offset": offset,
            },
        )

    # -------------------------------------------------------------------------
    # Table mode for instrument pages (no symbols)
    # -------------------------------------------------------------------------
    if not symbols:
        payload = await _call_core_sheet_rows_best_effort(
            engine=engine,
            page=page,
            limit=limit,
            offset=offset,
            mode=(mode or ""),
            body=merged_body,
        )

        if payload is None:
            return _payload(
                page=page,
                route_family=route_family,
                headers=headers,
                keys=keys,
                rows=[],
                include_matrix=include_matrix,
                request_id=request_id,
                started_at=start,
                mode=mode,
                status_out="success",
                error_out=None,
                meta_extra={
                    "limit": limit,
                    "offset": offset,
                    "dispatch": "schema_only_table_mode",
                },
            )

        raw_rows = _extract_rows_like(payload)
        matrix_rows = _extract_matrix_like(payload)
        if not raw_rows and matrix_rows:
            raw_rows = _matrix_to_rows(matrix_rows, keys)

        rows = [_normalize_to_schema_keys(schema_keys=keys, schema_headers=headers, raw=r) for r in raw_rows]
        rows = _slice(rows, limit=limit, offset=offset)

        status_out, err_out, meta_out = _extract_status_error(payload)

        return _payload(
            page=page,
            route_family=route_family,
            headers=headers,
            keys=keys,
            rows=rows,
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=start,
            mode=mode,
            status_out=status_out,
            error_out=err_out,
            meta_extra={
                "limit": limit,
                "offset": offset,
                "dispatch": "core_or_engine_get_sheet_rows",
                **meta_out,
            },
        )

    # -------------------------------------------------------------------------
    # Instrument mode (symbols provided)
    # -------------------------------------------------------------------------
    if not is_instrument_page(page):
        return _payload(
            page=page,
            route_family=route_family,
            headers=headers,
            keys=keys,
            rows=[],
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=start,
            mode=mode,
            status_out="error",
            error_out=f"Page '{page}' is not an instrument page for symbol-based retrieval.",
            meta_extra={"dispatch": "invalid_instrument_mode"},
        )

    if not engine:
        fallback_rows = [_empty_schema_row(keys, symbol=s) for s in symbols]
        return _payload(
            page=page,
            route_family=route_family,
            headers=headers,
            keys=keys,
            rows=fallback_rows,
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=start,
            mode=mode,
            status_out="partial",
            error_out="Data engine unavailable",
            meta_extra={
                "requested": len(symbols),
                "errors": len(symbols),
                "dispatch": "instrument_mode_no_engine",
            },
        )

    data_map = await _fetch_analysis_rows(engine, symbols, mode=(mode or ""), settings=settings, schema=spec)

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

        if not raw:
            raw = {"symbol": sym, "ticker": sym, "error": "missing_row"}
            errors += 1
        elif isinstance(raw, dict) and raw.get("error"):
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

    return _payload(
        page=page,
        route_family=route_family,
        headers=headers,
        keys=keys,
        rows=normalized_rows,
        include_matrix=include_matrix,
        request_id=request_id,
        started_at=start,
        mode=mode,
        status_out=status_out,
        error_out=f"{errors} errors" if errors else None,
        meta_extra={
            "requested": len(symbols),
            "errors": errors,
            "schema_headers_always": bool(getattr(settings, "schema_headers_always", True)) if settings else True,
            "computations_enabled": bool(getattr(settings, "computations_enabled", True)) if settings else True,
            "forecasting_enabled": bool(getattr(settings, "forecasting_enabled", True)) if settings else True,
            "scoring_enabled": bool(getattr(settings, "scoring_enabled", True)) if settings else True,
            "dispatch": "instrument_mode",
        },
    )


# =============================================================================
# Routes
# =============================================================================
@router.get("/sheet-rows")
async def analysis_sheet_rows_get(
    request: Request,
    page: str = Query(default="", description="sheet/page name"),
    sheet: str = Query(default="", description="sheet/page name"),
    sheet_name: str = Query(default="", description="sheet/page name"),
    name: str = Query(default="", description="sheet/page name"),
    tab: str = Query(default="", description="sheet/page name"),
    symbols: str = Query(default="", description="comma-separated symbols"),
    tickers: str = Query(default="", description="comma-separated tickers"),
    limit: Optional[int] = Query(default=None),
    offset: Optional[int] = Query(default=None),
    top_n: Optional[int] = Query(default=None),
    mode: str = Query(default="", description="Optional mode hint for engine/provider"),
    include_matrix_q: Optional[bool] = Query(default=None, alias="include_matrix"),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    body: Dict[str, Any] = {}
    for k, v in {
        "page": page,
        "sheet": sheet,
        "sheet_name": sheet_name,
        "name": name,
        "tab": tab,
        "symbols": symbols,
        "tickers": tickers,
        "limit": limit,
        "offset": offset,
        "top_n": top_n,
    }.items():
        if v not in (None, ""):
            body[k] = v

    return await _analysis_sheet_rows_impl(
        request=request,
        body=body,
        mode=mode,
        include_matrix_q=include_matrix_q,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
        x_request_id=x_request_id,
    )


@router.post("/sheet-rows")
async def analysis_sheet_rows(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    mode: str = Query(default="", description="Optional mode hint for engine/provider"),
    include_matrix_q: Optional[bool] = Query(default=None, alias="include_matrix"),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    return await _analysis_sheet_rows_impl(
        request=request,
        body=body,
        mode=mode,
        include_matrix_q=include_matrix_q,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
        x_request_id=x_request_id,
    )


__all__ = ["router", "ANALYSIS_SHEET_ROWS_VERSION"]
