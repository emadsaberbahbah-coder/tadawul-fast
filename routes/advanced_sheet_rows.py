#!/usr/bin/env python3
# routes/advanced_sheet_rows.py
"""
================================================================================
Advanced Sheet-Rows Router — v2.5.1
================================================================================
SCHEMA-FIRST • ADAPTER-FIRST • ROOT-ANALYSIS ALIGNED • SPECIAL-PAGE SAFE
PATH-AWARE AUTH • PUBLIC-PATH AWARE • STABLE RESPONSE SHAPE • LIVE-TEST READY

Endpoints
---------
GET  /v1/advanced/health
GET  /v1/advanced/sheet-rows
POST /v1/advanced/sheet-rows

v2.5.1 Fixes (vs v2.5.0)
------------------------
- FIX: _ensure_authorized now converts ANY exception from auth_ok into a clean
  HTTP 401 instead of letting non-TypeError errors escape uncaught. v2.5.0
  nested retries inside `except TypeError:` handlers — exceptions thrown from
  those inner retries bypassed the outer `except Exception as e:` sibling and
  propagated out of the function. Rewritten as a linear attempt-loop.
- FIX: _extract_matrix_like no longer early-returns on an EMPTY list at the
  first probed key. A payload like {"rows_matrix": [], "matrix": [[real]]}
  previously returned [] and never checked `matrix`, silently dropping real
  matrix-shaped data. Each probe now requires a non-empty list of row-like
  items.
- FIX: _extract_keys_like no longer stringifies dict entries in its scalar
  fallback branch. When keys=[{"foo": "bar"}] (a dict with no key/field/name/
  id), v2.5.0 fell through and produced ["{'foo': 'bar'}"]. The scalar branch
  now runs only when the list is NOT a list of Mappings.
- FIX: _canonicalize_page rewritten as a linear attempt-loop so the fallback
  normalize_page_name(s) call is protected the same way the primary one is.
- FIX: datetime.utcnow() replaced with datetime.now(timezone.utc) (deprecated
  in Python 3.12+, scheduled for removal). Added timezone to the datetime
  import.
- PERF: normalize_row_to_schema is now imported once at module load instead
  of on every request.
- CLEAN: Schema-registry and page-catalog discovery loops are encapsulated
  in _discover_schema_registry() / _discover_page_catalog() functions so
  their temporary loop locals (_sreg, _fh, _fgs, _il, _pcat, _ap, etc.) no
  longer leak into the module namespace.
- CLEAN: _build_placeholder_rows no longer calls _normalize_symbol_token
  twice per item in its filter-then-construct pattern.
- Public API, __all__, route paths, and all signatures preserved.

v2.5.0 Notes (unchanged)
------------------------
- ALIGN: prefers core.data_engine.get_sheet_rows adapter path before raw
  engine table helpers
- ALIGN: emits advanced_analysis-style envelopes (headers/display_headers/
  keys/rows/row_objects/items/records/data/quotes)
- Supports schema_only / headers_only on advanced route
- Table-mode payload extraction accepts rows / row_objects / items / records
  / quotes / results / rows_matrix
- Special pages use best-effort builder + adapter/engine payload comparison
- Data_Dictionary fails soft with guaranteed schema-driven rows
- Preserves adapter/engine meta and dispatch source for easier live
  diagnostics
- No network I/O at import time
================================================================================
"""

from __future__ import annotations

import importlib
import inspect
import logging
import math
import os
import re
import time
import uuid
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, status
from fastapi.encoders import jsonable_encoder

logger = logging.getLogger("routes.advanced_sheet_rows")
logger.addHandler(logging.NullHandler())

# -----------------------------------------------------------------------------
# Schema registry (authoritative)
# -----------------------------------------------------------------------------
get_sheet_spec = None  # type: ignore
_SCHEMA_IMPORT_ERROR: Optional[str] = None


def _discover_schema_registry() -> Tuple[Any, Optional[str]]:
    """v2.5.1: encapsulated so loop locals don't leak into module namespace."""
    last_err: Optional[str] = None
    for path in ("core.sheets.schema_registry", "core.schema_registry", "schema_registry"):
        try:
            mod = importlib.import_module(path)
        except Exception as e:
            last_err = repr(e)
            continue
        fgs = getattr(mod, "get_sheet_spec", None)
        if callable(fgs):
            return fgs, None
    return None, last_err


get_sheet_spec, _SCHEMA_IMPORT_ERROR = _discover_schema_registry()


# -----------------------------------------------------------------------------
# Page catalog helpers (authoritative normalization / dispatch)
# -----------------------------------------------------------------------------
CANONICAL_PAGES: List[str] = []
FORBIDDEN_PAGES: set = {"KSA_Tadawul", "Advisor_Criteria"}


def allowed_pages() -> List[str]:
    return list(CANONICAL_PAGES) if CANONICAL_PAGES else []


def normalize_page_name(name: str, allow_output_pages: bool = True) -> str:
    return (name or "").strip().replace(" ", "_")


def get_route_family(name: str) -> str:
    if name == "Insights_Analysis":
        return "insights"
    if name == "Top_10_Investments":
        return "top10"
    if name == "Data_Dictionary":
        return "dictionary"
    return "instrument"


def is_instrument_page(name: str) -> bool:
    return get_route_family(name) == "instrument"


def _discover_page_catalog() -> None:
    """v2.5.1: encapsulated discovery.

    Rebinds module-level allowed_pages / normalize_page_name /
    get_route_family / is_instrument_page and populates CANONICAL_PAGES /
    FORBIDDEN_PAGES from the first importable page_catalog module.
    """
    global allowed_pages, normalize_page_name, get_route_family, is_instrument_page
    for path in ("core.sheets.page_catalog", "core.page_catalog", "page_catalog"):
        try:
            mod = importlib.import_module(path)
        except Exception:
            continue
        ap = getattr(mod, "allowed_pages", None)
        np_ = getattr(mod, "normalize_page_name", None)
        grf = getattr(mod, "get_route_family", None)
        iip = getattr(mod, "is_instrument_page", None)
        if callable(ap):
            cp = getattr(mod, "CANONICAL_PAGES", None)
            fp = getattr(mod, "FORBIDDEN_PAGES", None)
            if cp is not None:
                CANONICAL_PAGES[:] = list(cp)
            if fp is not None:
                FORBIDDEN_PAGES.clear()
                FORBIDDEN_PAGES.update(fp)
            allowed_pages = ap
            if callable(np_):
                normalize_page_name = np_
            if callable(grf):
                get_route_family = grf
            if callable(iip):
                is_instrument_page = iip
            return


_discover_page_catalog()


# -----------------------------------------------------------------------------
# Optional auth/config
# -----------------------------------------------------------------------------
try:
    from core.config import auth_ok, get_settings_cached, is_open_mode, mask_settings  # type: ignore
except Exception:  # pragma: no cover
    auth_ok = None  # type: ignore
    is_open_mode = None  # type: ignore
    mask_settings = None  # type: ignore

    def get_settings_cached(*args: Any, **kwargs: Any) -> Any:  # type: ignore
        return None

# -----------------------------------------------------------------------------
# Optional adapter + engine access
# -----------------------------------------------------------------------------
try:
    from core.data_engine import get_sheet_rows as core_get_sheet_rows  # type: ignore
except Exception:  # pragma: no cover
    core_get_sheet_rows = None  # type: ignore

# v2.5.1: hoisted out of _run_advanced_sheet_rows_impl so we don't re-import
# on every request. Falls back to None when unavailable.
try:
    from core.data_engine_v2 import normalize_row_to_schema as _NORMALIZE_ROW_TO_SCHEMA  # type: ignore
except Exception:  # pragma: no cover
    _NORMALIZE_ROW_TO_SCHEMA = None  # type: ignore


ADVANCED_SHEET_ROWS_VERSION = "2.5.1"
ROOT_OWNER = "advanced_sheet_rows"
router = APIRouter(prefix="/v1/advanced", tags=["Advanced Sheet Rows"])

SPECIAL_PAGE_ENGINE_TIMEOUT_SEC = max(0.25, float(os.getenv("TFB_ADV_SPECIAL_PAGE_ENGINE_TIMEOUT_SEC", "8.0") or 8.0))
SPECIAL_PAGE_BUILDER_TIMEOUT_SEC = max(0.25, float(os.getenv("TFB_ADV_SPECIAL_PAGE_BUILDER_TIMEOUT_SEC", "10.0") or 10.0))

FIELD_ALIAS_HINTS: Dict[str, List[str]] = {
    "symbol": ["ticker", "code", "requested_symbol", "security", "instrument", "regularMarketSymbol"],
    "name": ["shortName", "longName", "displayName", "company_name", "instrument_name"],
    "current_price": ["price", "last", "last_price", "regularMarketPrice", "close", "nav", "value"],
    "previous_close": ["previousClose", "prev_close", "regularMarketPreviousClose"],
    "open_price": ["open", "openPrice", "regularMarketOpen"],
    "day_high": ["high", "dayHigh", "regularMarketDayHigh"],
    "day_low": ["low", "dayLow", "regularMarketDayLow"],
    "week_52_high": ["52WeekHigh", "fiftyTwoWeekHigh", "yearHigh", "week52High"],
    "week_52_low": ["52WeekLow", "fiftyTwoWeekLow", "yearLow", "week52Low"],
    "percent_change": ["changePercent", "pctChange", "regularMarketChangePercent"],
    "rank_overall": ["rank", "overallRank"],
    "recommendation_reason": ["reason", "summary", "analysis", "thesis"],
    "data_provider": ["provider", "source"],
    "top10_rank": ["rank", "top_rank"],
    "selection_reason": ["selection_notes", "selector_reason"],
    "criteria_snapshot": ["criteria", "criteria_json", "snapshot"],
    "sort_order": ["sortOrder", "order"],
}

TOP10_REQUIRED_FIELDS: Tuple[str, ...] = ("top10_rank", "selection_reason", "criteria_snapshot")
DATA_DICTIONARY_KEYS: List[str] = ["sheet", "group", "header", "key", "dtype", "fmt", "required", "source", "notes"]
EMERGENCY_PAGE_SYMBOLS: Dict[str, List[str]] = {
    "Market_Leaders": ["2222.SR", "1120.SR", "2010.SR", "7010.SR", "AAPL", "MSFT", "NVDA", "GOOGL"],
    "Global_Markets": ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AVGO"],
    "Commodities_FX": ["GC=F", "BZ=F", "SI=F", "EURUSD=X", "GBPUSD=X", "JPY=X", "SAR=X", "CL=F"],
    "Mutual_Funds": ["SPY", "QQQ", "VTI", "VOO", "IWM"],
    "My_Portfolio": ["2222.SR", "AAPL", "MSFT", "QQQ", "GC=F"],
    "My_Investments": ["2222.SR", "AAPL", "MSFT"],
    "Insights_Analysis": ["2222.SR", "AAPL", "GC=F"],
    "Top_10_Investments": ["2222.SR", "1120.SR", "AAPL", "MSFT", "NVDA"],
}


# =============================================================================
# Generic helpers
# =============================================================================
def _strip(v: Any) -> str:
    if v is None:
        return ""
    try:
        s = str(v).strip()
    except Exception:
        return ""
    return "" if s.lower() in {"none", "null", "nil", "undefined"} else s


def _safe_int(v: Any, default: int) -> int:
    try:
        if v is None or isinstance(v, bool):
            return default
        return int(float(v))
    except Exception:
        return default


def _safe_float(v: Any, default: float) -> float:
    try:
        if v is None or isinstance(v, bool):
            return default
        value = float(v)
        if math.isnan(value) or math.isinf(value):
            return default
        return value
    except Exception:
        return default


def _boolish(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    s = _strip(v).lower()
    if not s:
        return default
    if s in {"1", "true", "yes", "y", "on", "t"}:
        return True
    if s in {"0", "false", "no", "n", "off", "f"}:
        return False
    return default


def _safe_dict(v: Any) -> Dict[str, Any]:
    return dict(v) if isinstance(v, Mapping) else {}


def _json_safe(obj: Any) -> Any:
    try:
        return jsonable_encoder(obj)
    except Exception:
        pass
    if obj is None or isinstance(obj, (str, int, bool)):
        return obj
    if isinstance(obj, float):
        return None if math.isnan(obj) or math.isinf(obj) else obj
    if isinstance(obj, Decimal):
        try:
            f = float(obj)
            return None if math.isnan(f) or math.isinf(f) else f
        except Exception:
            return str(obj)
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, (datetime, date, dt_time)):
        try:
            return obj.isoformat()
        except Exception:
            return str(obj)
    if is_dataclass(obj):
        try:
            return {k: _json_safe(v) for k, v in asdict(obj).items()}
        except Exception:
            return str(obj)
    if isinstance(obj, Mapping):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_json_safe(v) for v in obj]
    try:
        return _json_safe(vars(obj))
    except Exception:
        return str(obj)


def _request_id(request: Request, x_request_id: Optional[str]) -> str:
    if _strip(x_request_id):
        return _strip(x_request_id)
    try:
        rid = _strip(getattr(request.state, "request_id", ""))
        if rid:
            return rid
    except Exception:
        pass
    return uuid.uuid4().hex[:12]


def _split_csv(text: str) -> List[str]:
    raw = (text or "").replace(";", ",").replace("\n", ",").replace("\t", ",").replace("|", ",")
    out: List[str] = []
    seen: Set[str] = set()
    for part in raw.split(","):
        s = _strip(part)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _normalize_symbol_token(sym: Any) -> str:
    s = _strip(sym).upper().replace(" ", "")
    if not s:
        return ""
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".SA"):
        s = s[:-3] + ".SR"
    if s.isdigit() and 3 <= len(s) <= 6:
        return f"{s}.SR"
    return s


def _dedupe_keep_order(items: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for item in items:
        s = _strip(item)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _get_list(body: Mapping[str, Any], *keys: str) -> List[str]:
    for k in keys:
        value = body.get(k)
        if isinstance(value, list):
            out: List[str] = []
            for item in value:
                s = _normalize_symbol_token(item) if ("symbol" in k or "ticker" in k or k in {"code", "requested_symbol"}) else _strip(item)
                if s:
                    out.append(s)
            if out:
                return out
        if isinstance(value, str) and value.strip():
            vals = _split_csv(value)
            vals = [_normalize_symbol_token(x) for x in vals] if ("symbol" in k or "ticker" in k or k in {"code", "requested_symbol"}) else vals
            vals = [x for x in vals if x]
            if vals:
                return vals
    return []


def _pick_page_from_body(body: Mapping[str, Any]) -> str:
    for k in ("sheet", "page", "sheet_name", "sheetName", "page_name", "name", "tab", "worksheet"):
        s = _strip(body.get(k))
        if s:
            return s
    return ""


def _extract_requested_symbols(body: Mapping[str, Any], limit: int) -> List[str]:
    symbols: List[str] = []
    for key in (
        "symbols", "tickers", "selected_symbols", "selected_tickers", "direct_symbols", "codes",
        "symbol", "ticker", "code", "requested_symbol",
    ):
        symbols.extend(_get_list(body, key))
    criteria = body.get("criteria") if isinstance(body.get("criteria"), Mapping) else None
    if criteria:
        for key in (
            "symbols", "tickers", "selected_symbols", "selected_tickers", "direct_symbols", "codes",
            "symbol", "ticker", "code", "requested_symbol",
        ):
            symbols.extend(_get_list(criteria, key))
    return _dedupe_keep_order([_normalize_symbol_token(s) for s in symbols if _normalize_symbol_token(s)])[:limit]


def _canonicalize_page(page_raw: str) -> str:
    """v2.5.1: linear attempt-loop so the fallback call is protected the same
    way the primary call is. Previously an unexpected non-TypeError raised by
    the kwargless form would propagate uncaught."""
    s = _strip(page_raw)
    if not s:
        return s
    for kwargs in ({"allow_output_pages": True}, {}):
        try:
            result = normalize_page_name(s, **kwargs)
            if isinstance(result, str) and result:
                return result
        except TypeError:
            continue
        except Exception:
            continue
    return s.replace("-", "_").replace(" ", "_")


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
    public_paths = _settings_get_list(settings, "PUBLIC_PATHS", "public_paths", "AUTH_PUBLIC_PATHS", "auth_public_paths", "public_routes")
    public_prefixes = _settings_get_list(settings, "PUBLIC_PATH_PREFIXES", "public_path_prefixes", "AUTH_PUBLIC_PREFIXES", "auth_public_prefixes")
    defaults = {"/", "/health", "/livez", "/readyz", "/ping", "/meta", "/docs", "/redoc", "/openapi.json", "/v1/advanced/health"}
    if p in defaults or p in set(public_paths):
        return True
    for pref in public_prefixes:
        if pref and p.startswith(pref):
            return True
    return False


def _allow_query_token(settings: Any, request: Request) -> bool:
    try:
        if settings is not None:
            return bool(getattr(settings, "ALLOW_QUERY_TOKEN", False) or getattr(settings, "allow_query_token", False))
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


def _extract_auth_token(*, token_query: Optional[str], x_app_token: Optional[str], x_api_key: Optional[str], authorization: Optional[str], settings: Any, request: Request) -> str:
    auth_token = _strip(x_app_token) or _strip(x_api_key)
    if authorization and authorization.strip().lower().startswith("bearer "):
        auth_token = authorization.strip().split(" ", 1)[1].strip()
    if token_query and not auth_token and _allow_query_token(settings, request):
        auth_token = _strip(token_query)
    return auth_token


def _ensure_authorized(*, request: Request, settings: Any, token_query: Optional[str], x_app_token: Optional[str], x_api_key: Optional[str], authorization: Optional[str]) -> None:
    """v2.5.1: rewritten as a linear attempt-loop so every auth_ok call is
    protected. v2.5.0 nested retries inside `except TypeError:` handlers;
    non-TypeError exceptions thrown from those inner calls bypassed the outer
    `except Exception as e:` sibling and propagated out of the function."""
    path = str(getattr(getattr(request, "url", None), "path", "") or "")
    try:
        if callable(is_open_mode) and bool(is_open_mode()):
            return
    except Exception:
        pass
    if _settings_get_bool(settings, "OPEN_MODE", "open_mode", default=False):
        return
    if _path_is_public(path, settings):
        return
    if auth_ok is None:
        return
    auth_token = _extract_auth_token(token_query=token_query, x_app_token=x_app_token, x_api_key=x_api_key, authorization=authorization, settings=settings, request=request)

    attempts = (
        {"token": auth_token, "authorization": authorization, "headers": request.headers, "path": path, "request": request, "settings": settings},
        {"token": auth_token, "authorization": authorization, "headers": request.headers, "path": path},
        {"token": auth_token, "authorization": authorization, "headers": request.headers},
    )
    ok: Any = False
    last_err: Optional[BaseException] = None
    for kwargs in attempts:
        try:
            ok = auth_ok(**kwargs)
            last_err = None
            break
        except TypeError:
            continue
        except Exception as e:
            last_err = e
            break
    if last_err is not None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail={"error": "Auth check failed", "detail": str(last_err)})
    if not ok:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")


def _ensure_page_allowed(page: str) -> None:
    forbidden = set(FORBIDDEN_PAGES or set())
    if page in forbidden:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"error": f"Forbidden/removed page: {page}", "forbidden_pages": sorted(list(forbidden))})
    try:
        ap = allowed_pages()
        if ap and page not in set(ap):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"error": f"Unknown page: {page}", "allowed_pages": ap})
    except HTTPException:
        raise
    except Exception:
        return


async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


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


def _safe_engine_type(engine: Any) -> str:
    try:
        return type(engine).__name__
    except Exception:
        return "unknown"


def _matrix_to_rows(matrix: Sequence[Sequence[Any]], keys: Sequence[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in matrix or []:
        row_list = list(row or [])
        obj: Dict[str, Any] = {}
        for idx, key in enumerate(keys):
            obj[str(key)] = _json_safe(row_list[idx] if idx < len(row_list) else None)
        out.append(obj)
    return out


# =============================================================================
# Schema / payload helpers
# =============================================================================
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


def _normalize_key_name(name: Any) -> str:
    s = _strip(name)
    if not s:
        return ""
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def _key_variants(key: str) -> List[str]:
    k = _strip(key)
    if not k:
        return []
    variants = [k, k.lower(), k.upper(), k.replace("_", " "), k.replace("_", "").lower()]
    for alias in FIELD_ALIAS_HINTS.get(k, []):
        variants.extend([alias, alias.lower(), alias.upper(), alias.replace("_", " "), alias.replace("_", "").lower()])
    out: List[str] = []
    seen: Set[str] = set()
    for v in variants:
        s = _strip(v)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _extract_from_raw(raw: Dict[str, Any], candidates: Sequence[str]) -> Any:
    raw_ci = {str(k).strip().lower(): v for k, v in raw.items()}
    raw_comp = {re.sub(r"[^a-z0-9]+", "", str(k).lower()): v for k, v in raw.items()}
    for candidate in candidates:
        if candidate in raw:
            return raw.get(candidate)
        lc = candidate.lower()
        if lc in raw_ci:
            return raw_ci.get(lc)
        cc = re.sub(r"[^a-z0-9]+", "", candidate.lower())
        if cc in raw_comp:
            return raw_comp.get(cc)
    return None


def _normalize_to_schema_keys(*, schema_keys: Sequence[str], schema_headers: Sequence[str], raw: Mapping[str, Any]) -> Dict[str, Any]:
    raw_dict = dict(raw or {})
    header_by_key = {str(k): str(h) for k, h in zip(schema_keys, schema_headers)}
    out: Dict[str, Any] = {}
    for key in schema_keys:
        ks = str(key)
        v = _extract_from_raw(raw_dict, _key_variants(ks))
        if v is None:
            header = header_by_key.get(ks, "")
            if header:
                v = _extract_from_raw(raw_dict, [header, header.lower(), header.upper()])
        out[ks] = _json_safe(v)
    if "symbol" in out and not out.get("symbol"):
        sym = _extract_from_raw(raw_dict, _key_variants("symbol"))
        out["symbol"] = _normalize_symbol_token(sym) if sym else sym
    if "ticker" in out and not out.get("ticker"):
        tic = _extract_from_raw(raw_dict, _key_variants("ticker"))
        out["ticker"] = _normalize_symbol_token(tic) if tic else tic
    return out


def _project_row(keys: Sequence[str], row: Dict[str, Any]) -> Dict[str, Any]:
    return {str(k): _json_safe(row.get(str(k), None)) for k in keys}


def _rows_to_matrix(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    return [[_json_safe(r.get(str(k))) for k in keys] for r in rows]


def _slice_rows(rows: Sequence[Mapping[str, Any]], offset: int, limit: int) -> List[Dict[str, Any]]:
    items = [dict(r) for r in rows]
    start = max(0, int(offset or 0))
    if int(limit or 0) <= 0:
        return items[start:]
    return items[start : start + int(limit)]


def _slice_values(values: Sequence[str], *, limit: int, offset: int) -> List[str]:
    vals = list(values)
    start = max(0, int(offset))
    if limit <= 0:
        return vals[start:]
    return vals[start : start + max(0, int(limit))]


def _extract_rows_like(payload: Any, depth: int = 0) -> List[Dict[str, Any]]:
    if payload is None or depth > 8:
        return []
    if isinstance(payload, list):
        if payload and all(isinstance(x, Mapping) for x in payload):
            return [dict(x) for x in payload]
        return []
    if not isinstance(payload, Mapping):
        return []
    for name in ("row_objects", "records", "items", "data", "quotes", "results", "rows"):
        value = payload.get(name)
        if isinstance(value, list) and value and isinstance(value[0], Mapping):
            return [dict(x) for x in value]
        if isinstance(value, Mapping):
            found = _extract_rows_like(value, depth + 1)
            if found:
                return found
    rows_value = payload.get("rows")
    if isinstance(rows_value, list) and rows_value and isinstance(rows_value[0], (list, tuple)):
        keys_like = _extract_keys_like(payload) or []
        if keys_like:
            return _matrix_to_rows(rows_value, keys_like)
    rows_matrix = payload.get("rows_matrix") or payload.get("matrix")
    if isinstance(rows_matrix, list) and rows_matrix and isinstance(rows_matrix[0], (list, tuple)):
        keys_like = _extract_keys_like(payload) or []
        if keys_like:
            return _matrix_to_rows(rows_matrix, keys_like)
    return []


def _extract_matrix_like(payload: Any, depth: int = 0) -> Optional[List[List[Any]]]:
    """v2.5.1: each probe now requires a NON-EMPTY list. v2.5.0 returned [] on
    the first list-shaped key even if empty, so {"rows_matrix": [],
    "matrix": [[real]]} silently lost the matrix data."""
    if depth > 8 or not isinstance(payload, Mapping):
        return None
    for name in ("rows_matrix", "matrix"):
        value = payload.get(name)
        if isinstance(value, list) and value:
            return [list(r) if isinstance(r, (list, tuple)) else [r] for r in value]
    rows_value = payload.get("rows")
    if isinstance(rows_value, list) and rows_value and isinstance(rows_value[0], (list, tuple)):
        return [list(r) if isinstance(r, (list, tuple)) else [r] for r in rows_value]
    for name in ("payload", "result", "response", "output", "data"):
        nested = payload.get(name)
        if isinstance(nested, Mapping):
            found = _extract_matrix_like(nested, depth + 1)
            if found is not None:
                return found
    return None


def _extract_keys_like(payload: Any, depth: int = 0) -> List[str]:
    """v2.5.1: the scalar-list fallback no longer runs on a list of dicts
    (which previously produced bogus str(dict)-valued entries when the dict
    form had no recognizable key/field/name/id field)."""
    if depth > 8 or not isinstance(payload, Mapping):
        return []
    for name in ("keys", "fields", "column_keys", "schema_keys", "columns"):
        value = payload.get(name)
        if isinstance(value, list):
            if value and isinstance(value[0], Mapping):
                keys = [_strip(v.get("key") or v.get("field") or v.get("name") or v.get("id")) for v in value if isinstance(v, Mapping)]
                keys = [k for k in keys if k]
                if keys:
                    return keys
                # v2.5.1: do NOT fall through to the scalar branch — stringifying
                # dict entries would produce garbage like "{'foo': 'bar'}".
            else:
                out = [_strip(x) for x in value if _strip(x)]
                if out:
                    return out
    for name in ("payload", "result", "response", "output", "data"):
        nested = payload.get(name)
        if isinstance(nested, Mapping):
            found = _extract_keys_like(nested, depth + 1)
            if found:
                return found
    return []


def _extract_status_error(payload: Any) -> Tuple[str, Optional[str], Dict[str, Any]]:
    if not isinstance(payload, Mapping):
        return "success", None, {}
    status_out = _strip(payload.get("status")) or "success"
    error_out = payload.get("error")
    if error_out in (None, ""):
        error_out = payload.get("detail") or payload.get("message")
    meta_out = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    return status_out, (str(error_out) if error_out is not None else None), meta_out


def _payload_has_real_rows(payload: Any) -> bool:
    return bool(_extract_rows_like(payload) or _extract_matrix_like(payload))


def _payload_quality_score(payload: Any, page: str = "") -> int:
    if payload is None:
        return -10
    if isinstance(payload, list):
        return 100 if payload else 0
    if not isinstance(payload, Mapping):
        return 0
    score = 0
    rows_like = _extract_rows_like(payload)
    matrix_like = _extract_matrix_like(payload)
    if rows_like:
        score += 100 + min(25, len(rows_like))
    if matrix_like:
        score += 85 + min(15, len(matrix_like))
    if _extract_keys_like(payload):
        score += 8
    status_out, error_out, _ = _extract_status_error(payload)
    if status_out.lower() == "success":
        score += 4
    elif status_out.lower() in {"partial", "warn", "warning"}:
        score += 2
    elif status_out.lower() in {"error", "failed", "fail"}:
        score -= 3
    if _strip(error_out):
        score -= 6
    if page == "Top_10_Investments" and rows_like:
        for field in TOP10_REQUIRED_FIELDS:
            if any(isinstance(r, Mapping) and r.get(field) not in (None, "", [], {}) for r in rows_like):
                score += 10
    return score


def _top10_fill_required(rows: List[Dict[str, Any]], offset: int = 0) -> None:
    for idx, row in enumerate(rows, start=offset + 1):
        row.setdefault("top10_rank", idx)
        row.setdefault("selection_reason", "Selected by advanced_sheet_rows fallback.")
        row.setdefault("criteria_snapshot", "{}")


def _payload_envelope(*, page: str, route_family: str, headers: Sequence[str], keys: Sequence[str], row_objects: Sequence[Mapping[str, Any]], include_matrix: bool, request_id: str, started_at: float, status_out: str, error_out: Optional[str], meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    hdrs = list(headers or [])
    ks = list(keys or [])
    rows_dict = [_project_row(ks, dict(r)) for r in (row_objects or [])]
    if page == "Top_10_Investments":
        _top10_fill_required(rows_dict, offset=0)
    matrix = _rows_to_matrix(rows_dict, ks) if include_matrix else []
    display_rows = [{hdrs[i]: _json_safe(row.get(ks[i])) for i in range(len(ks))} for row in rows_dict]
    payload = {
        "status": status_out,
        "page": page,
        "sheet": page,
        "sheet_name": page,
        "route_family": route_family,
        "headers": hdrs,
        "display_headers": hdrs,
        "sheet_headers": hdrs,
        "column_headers": hdrs,
        "keys": ks,
        "columns": ks,
        "fields": ks,
        "rows": matrix,
        "rows_matrix": matrix,
        "matrix": matrix,
        "row_objects": rows_dict,
        "items": rows_dict,
        "records": rows_dict,
        "data": rows_dict,
        "quotes": rows_dict,
        "display_row_objects": display_rows,
        "display_items": display_rows,
        "display_records": display_rows,
        "rows_dict_display": display_rows,
        "count": len(rows_dict),
        "detail": error_out or "",
        "error": error_out,
        "version": ADVANCED_SHEET_ROWS_VERSION,
        "request_id": request_id,
        "meta": {
            "duration_ms": round((time.time() - started_at) * 1000.0, 3),
            "count": len(rows_dict),
            "row_object_count": len(rows_dict),
            "matrix_row_count": len(matrix),
            **(meta or {}),
        },
    }
    return _json_safe(payload)


def _normalize_result_to_payload(*, result: Any, page: str, route_family: str, schema_headers: Sequence[str], schema_keys: Sequence[str], include_matrix: bool, request_id: str, started_at: float, dispatch: str, limit: int, offset: int, default_status: str = "success", default_error: Optional[str] = None, extra_meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    raw_rows = _extract_rows_like(result)
    if not raw_rows:
        mx = _extract_matrix_like(result)
        keys_like = _extract_keys_like(result) or list(schema_keys)
        if mx and keys_like:
            raw_rows = _matrix_to_rows(mx, keys_like)
    source_row_count = len(raw_rows)
    raw_rows = _slice_rows(raw_rows, offset=offset, limit=limit)
    rows: List[Dict[str, Any]] = []
    for idx, r in enumerate(raw_rows, start=offset + 1):
        rr = _normalize_to_schema_keys(schema_keys=schema_keys, schema_headers=schema_headers, raw=dict(r))
        if page == "Top_10_Investments":
            rr.setdefault("top10_rank", idx)
            rr.setdefault("selection_reason", "Selected by advanced_sheet_rows.")
            rr.setdefault("criteria_snapshot", "{}")
        rows.append(_project_row(schema_keys, rr))
    status_res, error_res, meta_in = _extract_status_error(result)
    final_status = _strip(status_res) or default_status
    if rows and final_status in {"error", "failed", "fail"}:
        final_status = "partial"
    if not rows and final_status == "success" and (error_res or default_error):
        final_status = "partial"
    return _payload_envelope(
        page=page,
        route_family=route_family,
        headers=schema_headers,
        keys=schema_keys,
        row_objects=rows,
        include_matrix=include_matrix,
        request_id=request_id,
        started_at=started_at,
        status_out=final_status or default_status,
        error_out=error_res or default_error,
        meta={
            "dispatch": dispatch,
            "result_payload_quality": _payload_quality_score(result, page=page),
            "source_row_count": source_row_count,
            "offset": offset,
            "limit": limit,
            **(meta_in or {}),
            **(extra_meta or {}),
        },
    )


def _placeholder_value_for_key(page: str, key: str, symbol: str, row_index: int) -> Any:
    kk = _normalize_key_name(key)
    if kk in {"symbol", "ticker"}:
        return symbol
    if kk == "name":
        return f"{page} {symbol}"
    if kk == "asset_class":
        return "Commodity" if symbol.endswith("=F") else "FX" if symbol.endswith("=X") else "Fund" if page == "Mutual_Funds" else "Equity"
    if kk == "exchange":
        if symbol.endswith(".SR"):
            return "Tadawul"
        if symbol.endswith("=F"):
            return "Futures"
        if symbol.endswith("=X"):
            return "FX"
        return "NASDAQ/NYSE"
    if kk == "currency":
        return "SAR" if symbol.endswith(".SR") else "USD"
    if kk == "country":
        return "Saudi Arabia" if symbol.endswith(".SR") else "Global"
    if kk == "data_provider":
        return "advanced_sheet_rows.placeholder_fallback"
    if kk == "last_updated_utc":
        # v2.5.1: datetime.utcnow() is deprecated; use timezone-aware now().
        return datetime.now(timezone.utc).isoformat()
    if kk == "last_updated_riyadh":
        # v2.5.1: datetime.utcnow() is deprecated; use timezone-aware now().
        return datetime.now(timezone.utc).isoformat()
    if kk == "recommendation":
        return "HOLD"
    if kk == "recommendation_reason":
        return "Placeholder fallback because live engine returned no usable rows."
    if kk in {"top10_rank", "rank_overall", "sort_order"}:
        return row_index
    if kk == "selection_reason":
        return "Placeholder fallback because live builder returned no usable rows."
    if kk == "criteria_snapshot":
        return "{}"
    if kk in {"warnings", "notes"}:
        return "placeholder"
    return None


def _build_placeholder_rows(*, page: str, keys: Sequence[str], requested_symbols: Sequence[str], limit: int, offset: int) -> List[Dict[str, Any]]:
    # v2.5.1: single-pass generator — was calling _normalize_symbol_token twice
    # per item (once in the predicate, once in the projection).
    symbols = [s for s in (_normalize_symbol_token(x) for x in requested_symbols) if s]
    if not symbols:
        symbols = [s for s in (_normalize_symbol_token(x) for x in EMERGENCY_PAGE_SYMBOLS.get(page, [])) if s]
    symbols = symbols[offset : offset + limit] if (offset or len(symbols) > limit) else symbols[:limit]
    rows: List[Dict[str, Any]] = []
    for idx, sym in enumerate(symbols, start=offset + 1):
        row = {str(k): _placeholder_value_for_key(page, str(k), sym, idx) for k in keys}
        rows.append(row)
    if page == "Top_10_Investments":
        _top10_fill_required(rows, offset=offset)
    return rows


# =============================================================================
# Engine access / calls
# =============================================================================
async def _get_engine(request: Request) -> Optional[Any]:
    try:
        st = getattr(request.app, "state", None)
        if st is not None:
            for attr in ("engine", "data_engine", "quote_engine", "cache_engine"):
                value = getattr(st, attr, None)
                if value is not None:
                    return value
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


async def _await_with_timeout(awaitable: Any, timeout_sec: float, label: str) -> Any:
    try:
        import asyncio
        async with asyncio.timeout(max(0.25, float(timeout_sec))):
            return await awaitable
    except TimeoutError as e:
        raise TimeoutError(f"{label} timed out after {timeout_sec:.2f}s") from e


async def _call_core_sheet_rows_best_effort(*, page: str, limit: int, offset: int, mode: str, body: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
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


async def _call_engine_sheet_rows_best_effort(*, engine: Any, page: str, limit: int, offset: int, mode: str, body: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
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
            res = fn(**kwargs)
            res = await _maybe_await(res)
            if isinstance(res, dict):
                return res, f"engine:{method_name}"
            if isinstance(res, list):
                return {"rows": res}, f"engine:{method_name}"
        except TypeError:
            continue
        except Exception:
            continue
    return None, None


async def _fetch_advanced_rows(engine: Any, symbols: List[str], *, mode: str, settings: Any, schema: Any) -> Dict[str, Any]:
    if not symbols:
        return {}
    computations_enabled = _settings_get_bool(settings, "computations_enabled", default=True)
    forecasting_enabled = _settings_get_bool(settings, "forecasting_enabled", default=True)
    scoring_enabled = _settings_get_bool(settings, "scoring_enabled", default=True)
    want_advanced = computations_enabled and (forecasting_enabled or scoring_enabled)
    preferred: List[str] = []
    if want_advanced:
        preferred += ["get_analysis_rows_batch", "get_analysis_quotes_batch", "get_enriched_quotes_batch"]
    else:
        preferred += ["get_enriched_quotes_batch"]
    preferred += ["get_enriched_quotes", "get_quotes_batch", "quotes_batch"]
    for method in preferred:
        fn = getattr(engine, method, None)
        if not callable(fn):
            continue
        try:
            try:
                res = fn(symbols, mode=mode, schema=schema)
                res = await _maybe_await(res)
            except TypeError:
                try:
                    res = fn(symbols, schema=schema)
                    res = await _maybe_await(res)
                except TypeError:
                    try:
                        res = fn(symbols, mode=mode)
                        res = await _maybe_await(res)
                    except TypeError:
                        res = fn(symbols)
                        res = await _maybe_await(res)
            if isinstance(res, dict):
                if all(isinstance(k, str) for k in res.keys()) and any(k in set(symbols) for k in res.keys()):
                    return res
                data = res.get("data") or res.get("rows") or res.get("items") or res.get("quotes")
                if isinstance(data, dict):
                    return data
                if isinstance(data, list):
                    return {s: r for s, r in zip(symbols, data)}
            elif isinstance(res, list):
                return {s: r for s, r in zip(symbols, res)}
        except Exception:
            continue
    out: Dict[str, Any] = {}
    per_dict_fn = getattr(engine, "get_enriched_quote_dict", None) or getattr(engine, "get_analysis_row_dict", None)
    per_fn = getattr(engine, "get_enriched_quote", None) or getattr(engine, "get_analysis_row", None) or getattr(engine, "get_quote", None)
    for s in symbols:
        try:
            if callable(per_dict_fn):
                try:
                    out[s] = await _maybe_await(per_dict_fn(s, mode=mode, schema=schema))
                except TypeError:
                    try:
                        out[s] = await _maybe_await(per_dict_fn(s, schema=schema))
                    except TypeError:
                        try:
                            out[s] = await _maybe_await(per_dict_fn(s, mode=mode))
                        except TypeError:
                            out[s] = await _maybe_await(per_dict_fn(s))
            elif callable(per_fn):
                try:
                    out[s] = await _maybe_await(per_fn(s, mode=mode, schema=schema))
                except TypeError:
                    try:
                        out[s] = await _maybe_await(per_fn(s, schema=schema))
                    except TypeError:
                        try:
                            out[s] = await _maybe_await(per_fn(s, mode=mode))
                        except TypeError:
                            out[s] = await _maybe_await(per_fn(s))
            else:
                out[s] = {"symbol": s, "error": "engine_missing_quote_method"}
        except Exception as e:
            out[s] = {"symbol": s, "error": str(e)}
    return out


# =============================================================================
# Builders / special pages
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


async def _call_builder_best_effort(*, module_names: Sequence[str], function_names: Sequence[str], request: Request, settings: Any, mode: str, body: Dict[str, Any], schema_keys: Sequence[str], schema_headers: Sequence[str], friendly_name: str) -> Tuple[Any, Dict[str, Any]]:
    imported_mod = _import_first_available(module_names)
    chosen_fn = None
    chosen_name = ""
    for name in function_names:
        cand = getattr(imported_mod, name, None)
        if callable(cand):
            chosen_fn = cand
            chosen_name = name
            break
    if chosen_fn is None:
        raise RuntimeError(f"{friendly_name} builder missing callable function")
    last_err: Optional[Exception] = None
    candidates = [
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
            res = chosen_fn(*args, **kwargs)
            out = await _maybe_await(res)
            break
        except TypeError as e:
            last_err = e
            continue
        except Exception as e:
            last_err = e
            continue
    if out is None:
        raise RuntimeError(str(last_err) if last_err else f"{friendly_name} builder call failed")
    meta: Dict[str, Any] = {
        "builder_module": getattr(imported_mod, "__name__", ""),
        "builder_function": chosen_name,
        "builder_payload_preserved": isinstance(out, dict),
    }
    if isinstance(out, dict):
        meta["builder_meta"] = out.get("meta")
        meta["builder_status"] = out.get("status")
        meta["builder_error"] = out.get("error")
    rows = _extract_rows_like(out)
    matrix = _extract_matrix_like(out)
    if not rows and matrix:
        rows = _matrix_to_rows(matrix, schema_keys)
    rows_norm = [_normalize_to_schema_keys(schema_keys=schema_keys, schema_headers=schema_headers, raw=(r or {})) for r in rows]
    payload = {"status": (out.get("status") if isinstance(out, dict) else "success") or "success", "row_objects": rows_norm, "error": out.get("error") if isinstance(out, dict) else None, "meta": out.get("meta") if isinstance(out, dict) and isinstance(out.get("meta"), dict) else {}}
    return payload, meta


def _build_data_dictionary_rows_to_schema(*, schema_keys: Sequence[str], schema_headers: Sequence[str]) -> List[Dict[str, Any]]:
    try:
        from core.sheets.data_dictionary import build_data_dictionary_rows  # type: ignore
    except Exception as e:
        logger.warning("Data_Dictionary generator import failed: %s", e)
        rows: List[Dict[str, Any]] = []
        try:
            pages = allowed_pages() if callable(allowed_pages) else list(CANONICAL_PAGES)
        except Exception:
            pages = []
        for page in pages:
            try:
                headers, keys, spec = _schema_headers_keys(page)
                cols = getattr(spec, "columns", None) or []
                if cols:
                    for c in cols:
                        raw = {
                            "sheet": page,
                            "group": getattr(c, "group", None),
                            "header": getattr(c, "header", None),
                            "key": getattr(c, "key", None),
                            "dtype": getattr(c, "dtype", None),
                            "fmt": getattr(c, "fmt", None),
                            "required": getattr(c, "required", None),
                            "source": getattr(c, "source", None) or "schema_registry",
                            "notes": getattr(c, "notes", None),
                        }
                        rows.append(_normalize_to_schema_keys(schema_keys=schema_keys, schema_headers=schema_headers, raw=raw))
                else:
                    for h, k in zip(headers, keys):
                        raw = {"sheet": page, "group": None, "header": h, "key": k, "dtype": None, "fmt": None, "required": False, "source": "fallback_contract", "notes": ""}
                        rows.append(_normalize_to_schema_keys(schema_keys=schema_keys, schema_headers=schema_headers, raw=raw))
            except Exception:
                continue
        return rows
    raw_rows = build_data_dictionary_rows(include_meta_sheet=True)
    out: List[Dict[str, Any]] = []
    for r in raw_rows or []:
        rd = _to_plain_dict(r)
        out.append(_normalize_to_schema_keys(schema_keys=schema_keys, schema_headers=schema_headers, raw=rd))
    return out


# =============================================================================
# Route internals
# =============================================================================
async def _run_advanced_sheet_rows_impl(*, request: Request, body: Dict[str, Any], mode: str, include_matrix_q: Optional[bool], token: Optional[str], x_app_token: Optional[str], x_api_key: Optional[str], authorization: Optional[str], x_request_id: Optional[str]) -> Dict[str, Any]:
    t0 = time.time()
    body = _safe_dict(body)
    request_id = _request_id(request, x_request_id)
    try:
        settings = get_settings_cached()
    except Exception:
        settings = None

    _ensure_authorized(request=request, settings=settings, token_query=token, x_app_token=x_app_token, x_api_key=x_api_key, authorization=authorization)

    if get_sheet_spec is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail={"error": "Schema registry unavailable", "schema_import_error": _SCHEMA_IMPORT_ERROR})

    page_raw = _pick_page_from_body(body) or "Market_Leaders"
    page = _canonicalize_page(page_raw)
    _ensure_page_allowed(page)
    route_family = str(get_route_family(page))

    include_matrix = include_matrix_q if isinstance(include_matrix_q, bool) else _boolish(body.get("include_matrix"), True)
    limit = max(1, min(5000, _safe_int(body.get("limit"), 2000)))
    offset = max(0, _safe_int(body.get("offset"), 0))
    fetch_limit = min(5000, limit + offset)
    top_n = max(1, min(5000, _safe_int(body.get("top_n"), fetch_limit)))
    schema_only = _boolish(body.get("schema_only"), False)
    headers_only = _boolish(body.get("headers_only"), False)

    try:
        headers, keys, spec = _schema_headers_keys(page)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"error": f"Unknown page schema: {page}", "detail": str(e), "allowed_pages": allowed_pages() if callable(allowed_pages) else list(CANONICAL_PAGES)})
    if not headers or not keys:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail={"error": f"Schema for page '{page}' is empty", "page": page})

    if schema_only or headers_only:
        status_word = "success" if headers and keys else "warn"
        return _payload_envelope(page=page, route_family=route_family, headers=headers, keys=keys, row_objects=[], include_matrix=bool(include_matrix), request_id=request_id, started_at=t0, status_out=status_word, error_out=None if headers and keys else f"Schema for page '{page}' is empty", meta={"dispatch": "schema_only", "schema_only": bool(schema_only), "headers_only": bool(headers_only), "schema_headers_always": True})

    requested_symbols = _extract_requested_symbols(body, limit=top_n)

    # ------------------------------------------------------------------
    # Data Dictionary
    # ------------------------------------------------------------------
    if route_family == "dictionary":
        try:
            rows = _build_data_dictionary_rows_to_schema(schema_keys=keys, schema_headers=headers)
            rows = _slice_rows(rows, offset=offset, limit=limit)
            return _payload_envelope(page=page, route_family=route_family, headers=headers, keys=keys, row_objects=rows, include_matrix=bool(include_matrix), request_id=request_id, started_at=t0, status_out="success" if rows else "partial", error_out=None if rows else "data_dictionary_empty", meta={"dispatch": "data_dictionary", "limit": limit, "offset": offset})
        except Exception as e:
            logger.exception("Data_Dictionary generation failed softly: %s", e)
            rows = _build_placeholder_rows(page=page, keys=keys, requested_symbols=[], limit=limit, offset=offset)
            return _payload_envelope(page=page, route_family=route_family, headers=headers, keys=keys, row_objects=rows, include_matrix=bool(include_matrix), request_id=request_id, started_at=t0, status_out="partial", error_out=str(e), meta={"dispatch": "data_dictionary_fail_soft", "limit": limit, "offset": offset})

    engine = await _get_engine(request)

    # ------------------------------------------------------------------
    # Special pages: compare builder vs adapter vs engine and keep best
    # ------------------------------------------------------------------
    if route_family in {"insights", "top10"}:
        special_errors: List[str] = []
        best_payload: Any = None
        best_dispatch = ""
        best_score = -9999
        if route_family == "insights":
            builder_modules = ("core.analysis.insights_builder", "core.analysis.insights_analysis", "routes.investment_advisor")
            builder_functions = ("build_insights_analysis_rows", "build_insights_rows", "build_insights_analysis", "get_insights_rows", "build_rows")
            friendly_name = "Insights_Analysis"
        else:
            builder_modules = ("core.analysis.top10_selector", "core.analysis.top10_builder", "core.analysis.top_10_builder", "core.analysis.top_10_investments_builder", "core.analysis.top10_investments_builder", "routes.investment_advisor")
            builder_functions = ("build_top10_rows", "build_top_10_investments_rows", "build_top10_investments_rows", "build_top_10_rows", "get_top10_rows", "select_top10_rows", "build_rows")
            friendly_name = "Top_10_Investments"

        try:
            builder_payload, builder_meta = await _await_with_timeout(
                _call_builder_best_effort(module_names=builder_modules, function_names=builder_functions, request=request, settings=settings, mode=(mode or ""), body=body, schema_keys=keys, schema_headers=headers, friendly_name=friendly_name),
                SPECIAL_PAGE_BUILDER_TIMEOUT_SEC,
                f"{friendly_name} builder",
            )
            score = _payload_quality_score(builder_payload, page=page)
            if score > best_score:
                best_payload, best_dispatch, best_score = builder_payload, "builder_special_best", score
        except Exception as e:
            special_errors.append(f"builder={e}")
            logger.warning("%s builder failed softly: %s", friendly_name, e)

        try:
            core_payload, core_source = await _await_with_timeout(
                _call_core_sheet_rows_best_effort(page=page, limit=fetch_limit, offset=0, mode=(mode or ""), body=body),
                SPECIAL_PAGE_ENGINE_TIMEOUT_SEC,
                f"{friendly_name} core adapter",
            )
            if core_payload is not None:
                score = _payload_quality_score(core_payload, page=page)
                if score > best_score:
                    best_payload, best_dispatch, best_score = core_payload, core_source or "core_special_best", score
        except Exception as e:
            special_errors.append(f"core={e}")
            logger.warning("%s core adapter failed softly: %s", friendly_name, e)

        try:
            engine_payload, engine_source = await _await_with_timeout(
                _call_engine_sheet_rows_best_effort(engine=engine, page=page, limit=fetch_limit, offset=0, mode=(mode or ""), body=body),
                SPECIAL_PAGE_ENGINE_TIMEOUT_SEC,
                f"{friendly_name} engine table",
            )
            if engine_payload is not None:
                score = _payload_quality_score(engine_payload, page=page)
                if score > best_score:
                    best_payload, best_dispatch, best_score = engine_payload, engine_source or "engine_special_best", score
        except Exception as e:
            special_errors.append(f"engine={e}")
            logger.warning("%s engine table failed softly: %s", friendly_name, e)

        if best_payload is None or not _payload_has_real_rows(best_payload):
            placeholder_rows = _build_placeholder_rows(page=page, keys=keys, requested_symbols=requested_symbols, limit=limit, offset=offset)
            return _payload_envelope(page=page, route_family=route_family, headers=headers, keys=keys, row_objects=placeholder_rows, include_matrix=bool(include_matrix), request_id=request_id, started_at=t0, status_out="partial", error_out="; ".join(special_errors) or "no_usable_special_payload", meta={"dispatch": f"{route_family}_placeholder_fallback", "limit": limit, "offset": offset, "engine_present": bool(engine), "engine_type": _safe_engine_type(engine) if engine else "none"})

        return _normalize_result_to_payload(result=best_payload, page=page, route_family=route_family, schema_headers=headers, schema_keys=keys, include_matrix=bool(include_matrix), request_id=request_id, started_at=t0, dispatch=best_dispatch, limit=limit, offset=offset, default_status="partial", extra_meta={"engine_present": bool(engine), "engine_type": _safe_engine_type(engine) if engine else "none", "special_error_parts": special_errors, "limit": limit, "offset": offset})

    # ------------------------------------------------------------------
    # Table mode for instrument-style pages: adapter-first, then engine
    # ------------------------------------------------------------------
    if not requested_symbols:
        payload = None
        payload_source = None
        core_payload, core_source = await _call_core_sheet_rows_best_effort(page=page, limit=limit, offset=offset, mode=(mode or ""), body=body)
        engine_payload, engine_source = await _call_engine_sheet_rows_best_effort(engine=engine, page=page, limit=limit, offset=offset, mode=(mode or ""), body=body)
        core_score = _payload_quality_score(core_payload, page=page) if core_payload is not None else -9999
        engine_score = _payload_quality_score(engine_payload, page=page) if engine_payload is not None else -9999
        if core_score >= engine_score and core_payload is not None:
            payload, payload_source = core_payload, core_source
        elif engine_payload is not None:
            payload, payload_source = engine_payload, engine_source
        if payload is None:
            return _payload_envelope(page=page, route_family=route_family, headers=headers, keys=keys, row_objects=[], include_matrix=bool(include_matrix), request_id=request_id, started_at=t0, status_out="warn", error_out="no_table_payload_from_engine_or_core", meta={"dispatch": "schema_only_table_mode", "engine_present": bool(engine), "engine_type": _safe_engine_type(engine) if engine else "none", "limit": limit, "offset": offset})
        return _normalize_result_to_payload(result=payload, page=page, route_family=route_family, schema_headers=headers, schema_keys=keys, include_matrix=bool(include_matrix), request_id=request_id, started_at=t0, dispatch=payload_source or ("engine_sheet_rows" if engine is not None else "core_get_sheet_rows"), limit=limit, offset=offset, default_status="success", extra_meta={"engine_present": bool(engine), "engine_type": _safe_engine_type(engine) if engine else "none", "adapter_score": core_score, "engine_score": engine_score})

    # ------------------------------------------------------------------
    # Symbol mode for instrument pages
    # ------------------------------------------------------------------
    if not is_instrument_page(page):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"error": f"Page '{page}' is not an instrument page for symbol-based retrieval."})
    if not engine:
        placeholder_rows = _build_placeholder_rows(page=page, keys=keys, requested_symbols=requested_symbols, limit=limit, offset=offset)
        return _payload_envelope(page=page, route_family=route_family, headers=headers, keys=keys, row_objects=placeholder_rows, include_matrix=bool(include_matrix), request_id=request_id, started_at=t0, status_out="partial", error_out="Data engine unavailable", meta={"dispatch": "placeholder_no_engine", "requested_total": len(requested_symbols), "requested_window": len(_slice_values(requested_symbols, limit=limit, offset=offset))})

    symbols = _slice_values(requested_symbols, limit=limit, offset=offset)
    data_map = await _fetch_advanced_rows(engine, symbols, mode=(mode or ""), settings=settings, schema=spec)
    # v2.5.1: normalize_fn hoisted to module load as _NORMALIZE_ROW_TO_SCHEMA.
    normalize_fn = _NORMALIZE_ROW_TO_SCHEMA
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
        normalized_rows.append(_project_row(keys, row))
    status_out = "success" if errors == 0 else ("partial" if errors < len(symbols) else "error")
    return _payload_envelope(page=page, route_family=route_family, headers=headers, keys=keys, row_objects=normalized_rows, include_matrix=bool(include_matrix), request_id=request_id, started_at=t0, status_out=status_out, error_out=f"{errors} errors" if errors else None, meta={"requested_total": len(requested_symbols), "requested_window": len(symbols), "returned": len(normalized_rows), "errors": errors, "mode": mode, "limit": limit, "offset": offset, "top_n": top_n, "schema_headers_always": bool(getattr(settings, "schema_headers_always", True)) if settings else True, "computations_enabled": _settings_get_bool(settings, "computations_enabled", default=True), "forecasting_enabled": _settings_get_bool(settings, "forecasting_enabled", default=True), "scoring_enabled": _settings_get_bool(settings, "scoring_enabled", default=True), "dispatch": "instrument_mode", "engine_present": True, "engine_type": _safe_engine_type(engine)})


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
            auth_summary = {"open_mode_effective": masked.get("open_mode_effective"), "token_count": masked.get("token_count")}
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
        "route_owner": ROOT_OWNER,
    }


# =============================================================================
# Routes
# =============================================================================
@router.get("/sheet-rows")
async def advanced_sheet_rows_get(
    request: Request,
    sheet: str = Query(default=""),
    sheet_name: str = Query(default=""),
    page: str = Query(default=""),
    page_name: str = Query(default=""),
    name: str = Query(default=""),
    tab: str = Query(default=""),
    worksheet: str = Query(default=""),
    symbols: str = Query(default=""),
    tickers: str = Query(default=""),
    symbol: str = Query(default=""),
    ticker: str = Query(default=""),
    code: str = Query(default=""),
    requested_symbol: str = Query(default=""),
    limit: Optional[int] = Query(default=None),
    offset: Optional[int] = Query(default=None),
    include_matrix: Optional[bool] = Query(default=None),
    schema_only: Optional[bool] = Query(default=None),
    headers_only: Optional[bool] = Query(default=None),
    top_n: Optional[int] = Query(default=None),
    mode: str = Query(default=""),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    body: Dict[str, Any] = {}
    for k, v in {
        "sheet": sheet,
        "sheet_name": sheet_name,
        "page": page,
        "page_name": page_name,
        "name": name,
        "tab": tab,
        "worksheet": worksheet,
        "symbols": symbols,
        "tickers": tickers,
        "symbol": symbol,
        "ticker": ticker,
        "code": code,
        "requested_symbol": requested_symbol,
        "limit": limit,
        "offset": offset,
        "schema_only": schema_only,
        "headers_only": headers_only,
        "top_n": top_n,
    }.items():
        if v not in (None, ""):
            body[k] = v
    return await _run_advanced_sheet_rows_impl(request=request, body=body, mode=mode or "", include_matrix_q=include_matrix, token=token, x_app_token=x_app_token, x_api_key=x_api_key, authorization=authorization, x_request_id=x_request_id)


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
    return await _run_advanced_sheet_rows_impl(request=request, body=body or {}, mode=mode or "", include_matrix_q=include_matrix_q, token=token, x_app_token=x_app_token, x_api_key=x_api_key, authorization=authorization, x_request_id=x_request_id)


__all__ = ["router", "ADVANCED_SHEET_ROWS_VERSION"]
