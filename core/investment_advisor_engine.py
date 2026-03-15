#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/investment_advisor_engine.py
================================================================================
INVESTMENT ADVISOR ENGINE — v3.1.0
================================================================================
SYNC-EXPORT SAFE • ASYNC-INTERNAL • EXPORT-HARDENED • ROUTE-TOLERANT
ENGINE-AWARE • NO-IMPORT-TIME-NETWORK • ADVISOR + SHEET-ROWS SAFE
TOP10 FIELD GUARANTEED • JSON-SAFE • 502-RESISTANT

What this revision fixes
------------------------
- ✅ FIX: public runner exports are now sync-compatible for route layers that call
         the advisor runner inside a worker thread and expect an immediate dict.
- ✅ FIX: keeps async internal execution for engine access and flexible runtime work.
- ✅ FIX: prevents uncaught exceptions from escaping normal production flows by
         returning structured error payloads instead of raising.
- ✅ FIX: normalizes engine page payloads into stable sheet-rows contracts:
         headers / display_headers / keys / rows / rows_matrix / data / items
- ✅ FIX: explicit non-Top10 sheet-rows requests now try direct engine page access
         first for *all* requested pages, not only a limited subset.
- ✅ FIX: adds lightweight derived-page fallbacks for:
         - Advisor_Criteria
         - AI_Opportunity_Report
         - KSA_TADAWUL
- ✅ FIX: schema resolution now supports sync or async schema functions.
- ✅ FIX: exported methods and singleton service objects remain compatibility-safe.

Purpose
-------
Restore the shared advisor engine exports expected by multiple route families and
provide a stable compatibility layer for:

- run_investment_advisor_engine(...)
- run_investment_advisor(...)
- run_advisor(...)
- execute_investment_advisor(...)
- execute_advisor(...)
- recommend(...)
- recommend_investments(...)
- get_recommendations(...)
- build_recommendations(...)

This module is intentionally tolerant to many calling styles used by the
existing route layer. It can:
- act as the runtime advisor runner for /run and /recommendations
- act as a sheet-rows source for /sheet-rows
- pass through engine page rows for explicit page requests
- build Top_10_Investments style outputs for derived Top10 requests
- avoid raising in normal production flows

Design notes
------------
- No external network calls at import time.
- Engine access is lazy and runtime only.
- Public exports are synchronous wrappers around an async core, so both:
    - sync callers
    - async-aware callers
  remain compatible through the route layer.
================================================================================
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import logging
import math
import threading
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger("core.investment_advisor_engine")
logger.addHandler(logging.NullHandler())

INVESTMENT_ADVISOR_ENGINE_VERSION = "3.1.0"
TOP10_PAGE_NAME = "Top_10_Investments"

_BASE_SOURCE_PAGES = [
    "Market_Leaders",
    "Global_Markets",
    "Mutual_Funds",
    "Commodities_FX",
    "My_Portfolio",
]

_PASSTHROUGH_PAGES = {
    "Market_Leaders",
    "Global_Markets",
    "Mutual_Funds",
    "Commodities_FX",
    "My_Portfolio",
    "Insights_Analysis",
    "Data_Dictionary",
}

_DERIVED_OR_NON_SOURCE_PAGES = {
    "KSA_TADAWUL",
    "Advisor_Criteria",
    "AI_Opportunity_Report",
    "Insights_Analysis",
    "Top_10_Investments",
    "Data_Dictionary",
}

_ENGINE_SHEET_METHOD_CANDIDATES = (
    "get_sheet_rows",
    "get_page_rows",
    "sheet_rows",
    "build_sheet_rows",
    "get_rows",
)

_SCHEMA_MODULE_CANDIDATES = (
    "core.sheets.schema_registry",
    "core.schema_registry",
    "core.page_catalog",
    "core.schemas",
    "core.schema",
)

_SCHEMA_FN_CANDIDATES = (
    "get_sheet_spec",
    "get_page_spec",
    "get_schema_for_page",
    "sheet_spec",
    "build_sheet_spec",
)


# =============================================================================
# Core helpers
# =============================================================================
def _s(v: Any) -> str:
    try:
        if v is None:
            return ""
        s = str(v).strip()
        return "" if s.lower() in {"none", "null", "nil"} else s
    except Exception:
        return ""


def _is_blank(v: Any) -> bool:
    return v is None or (isinstance(v, str) and not v.strip())


def _safe_int(v: Any, default: int) -> int:
    try:
        if isinstance(v, bool):
            return default
        return int(float(v))
    except Exception:
        return default


def _safe_float(v: Any, default: float) -> float:
    try:
        if isinstance(v, bool):
            return default
        return float(v)
    except Exception:
        return default


def _coerce_bool(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        s = v.strip().lower()
        if s in {"1", "true", "yes", "y", "on"}:
            return True
        if s in {"0", "false", "no", "n", "off"}:
            return False
    if isinstance(v, (int, float)):
        try:
            return bool(int(v))
        except Exception:
            return default
    return default


def _dedupe_keep_order(values: Iterable[Any]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        s = _s(value)
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _normalize_list(value: Any) -> List[str]:
    if value is None:
        return []

    if isinstance(value, str):
        seq = []
        for part in value.replace(";", ",").replace("\n", ",").split(","):
            p = part.strip()
            if p:
                seq.append(p)
    elif isinstance(value, (list, tuple, set)):
        seq = list(value)
    else:
        seq = [value]

    return _dedupe_keep_order(seq)


def _jsonable_snapshot(value: Any) -> Any:
    try:
        return json.loads(json.dumps(value, default=str, ensure_ascii=False))
    except Exception:
        try:
            return str(value)
        except Exception:
            return None


def _json_compact(value: Any) -> str:
    try:
        return json.dumps(_jsonable_snapshot(value), ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return str(value)


def _rows_to_matrix(rows: List[Dict[str, Any]], keys: List[str]) -> List[List[Any]]:
    return [[row.get(k) for k in keys] for row in rows]


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _callable(obj: Any) -> bool:
    try:
        return callable(obj)
    except Exception:
        return False


async def _call_maybe_async(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    if inspect.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)

    out = await asyncio.to_thread(fn, *args, **kwargs)
    if inspect.isawaitable(out):
        return await out
    return out


def _get_request_path(request: Any) -> str:
    try:
        return _s(getattr(getattr(request, "url", None), "path", ""))
    except Exception:
        return ""


def _get_request_id(request: Any = None) -> str:
    try:
        if request is not None:
            rid = getattr(getattr(request, "state", None), "request_id", None)
            if rid:
                return _s(rid)
            hdr = request.headers.get("X-Request-ID")
            if hdr:
                return _s(hdr)
    except Exception:
        pass
    return "investment_advisor_engine"


def _model_to_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return dict(obj)
    if isinstance(obj, Mapping):
        try:
            return dict(obj)
        except Exception:
            return {}

    try:
        if hasattr(obj, "model_dump") and callable(getattr(obj, "model_dump")):
            dumped = obj.model_dump(mode="python")
            if isinstance(dumped, dict):
                return dumped
    except Exception:
        pass

    try:
        if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
            dumped = obj.dict()
            if isinstance(dumped, dict):
                return dumped
    except Exception:
        pass

    try:
        if hasattr(obj, "__dict__"):
            d = vars(obj)
            if isinstance(d, dict):
                return dict(d)
    except Exception:
        pass

    return {"result": obj}


def _extract_rows_candidate(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [dict(x) for x in payload if isinstance(x, dict)]

    if not isinstance(payload, dict):
        return []

    for key in ("rows", "recommendations", "data", "items", "results", "records", "quotes"):
        value = payload.get(key)
        if isinstance(value, list) and value and isinstance(value[0], dict):
            return [dict(x) for x in value if isinstance(x, dict)]

    result = payload.get("result")
    if isinstance(result, list) and result and isinstance(result[0], dict):
        return [dict(x) for x in result if isinstance(x, dict)]

    if isinstance(payload.get("row"), dict):
        return [dict(payload["row"])]

    return []


def _extract_matrix_candidate(payload: Any) -> Optional[List[List[Any]]]:
    if not isinstance(payload, dict):
        return None

    for key in ("rows_matrix", "data_matrix", "matrix"):
        value = payload.get(key)
        if isinstance(value, list):
            out: List[List[Any]] = []
            for row in value:
                if isinstance(row, (list, tuple)):
                    out.append(list(row))
                else:
                    out.append([row])
            return out

    rows = payload.get("rows")
    if isinstance(rows, list) and rows and isinstance(rows[0], (list, tuple)):
        return [list(r) if isinstance(r, (list, tuple)) else [r] for r in rows]

    data = payload.get("data")
    if isinstance(data, list) and data and isinstance(data[0], (list, tuple)):
        return [list(r) if isinstance(r, (list, tuple)) else [r] for r in data]

    return None


def _extract_headers_keys(payload: Any) -> Tuple[List[str], List[str]]:
    if not isinstance(payload, dict):
        return [], []

    headers = (
        payload.get("display_headers")
        or payload.get("sheet_headers")
        or payload.get("column_headers")
        or payload.get("headers")
        or []
    )
    keys = payload.get("keys") or payload.get("fields") or []

    if not isinstance(headers, list):
        headers = []
    if not isinstance(keys, list):
        keys = []

    clean_headers = [_s(x) for x in headers if _s(x)]
    clean_keys = [_s(x) for x in keys if _s(x)]
    return clean_headers, clean_keys


def _matrix_to_rows(matrix: List[List[Any]], keys: List[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not keys:
        return out

    for row in matrix:
        if not isinstance(row, list):
            continue
        item: Dict[str, Any] = {}
        for idx, key in enumerate(keys):
            item[key] = row[idx] if idx < len(row) else None
        out.append(item)
    return out


def _derive_keys_from_rows(rows: List[Dict[str, Any]]) -> List[str]:
    out: List[str] = []
    seen = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in row.keys():
            k = _s(key)
            if k and k not in seen:
                seen.add(k)
                out.append(k)
    return out


def _has_tabular_shape(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    if isinstance(payload.get("rows"), list):
        return True
    if isinstance(payload.get("rows_matrix"), list):
        return True
    if isinstance(payload.get("headers"), list):
        return True
    if isinstance(payload.get("keys"), list):
        return True
    return False


def _route_family_for_page(page: str) -> str:
    return "top10" if page == TOP10_PAGE_NAME else "advisor"


def _make_error_payload(
    detail: str,
    *,
    request: Any = None,
    page: str = TOP10_PAGE_NAME,
    operation: str = "run",
    extra_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    meta = {
        "engine_version": INVESTMENT_ADVISOR_ENGINE_VERSION,
        "request_id": _get_request_id(request),
        "generated_at_utc": _now_utc_iso(),
        "operation": operation,
        "target_page": page,
    }
    if isinstance(extra_meta, dict):
        meta.update(extra_meta)

    return {
        "status": "error",
        "page": page,
        "sheet": page,
        "route_family": _route_family_for_page(page),
        "headers": [],
        "display_headers": [],
        "keys": [],
        "rows": [],
        "items": [],
        "data": [],
        "rows_matrix": [],
        "count": 0,
        "detail": detail,
        "meta": meta,
    }


def _run_coro_sync(coro: Any) -> Dict[str, Any]:
    try:
        asyncio.get_running_loop()
        running_loop = True
    except RuntimeError:
        running_loop = False

    if not running_loop:
        return asyncio.run(coro)

    box: Dict[str, Any] = {"result": None, "error": None}

    def _runner() -> None:
        try:
            box["result"] = asyncio.run(coro)
        except Exception as exc:  # pragma: no cover
            box["error"] = exc

    t = threading.Thread(target=_runner, daemon=True)
    t.start()
    t.join()

    if box["error"] is not None:
        raise box["error"]
    return box["result"]


# =============================================================================
# Schema helpers
# =============================================================================
def _ensure_top10_keys_present(keys: List[str], headers: List[str]) -> Tuple[List[str], List[str]]:
    extras = [
        ("top10_rank", "Top10 Rank"),
        ("selection_reason", "Selection Reason"),
        ("criteria_snapshot", "Criteria Snapshot"),
    ]

    out_keys = list(keys or [])
    out_headers = list(headers or [])

    for key, header in extras:
        if key not in out_keys:
            out_keys.append(key)
            out_headers.append(header)

    return out_keys, out_headers


def _extract_columns_from_spec(spec: Any) -> List[Tuple[str, str]]:
    columns: List[Tuple[str, str]] = []

    raw_columns = None
    if isinstance(spec, dict):
        raw_columns = spec.get("columns") or spec.get("fields")
    else:
        raw_columns = getattr(spec, "columns", None) or getattr(spec, "fields", None)

    if isinstance(raw_columns, list):
        for col in raw_columns:
            if isinstance(col, dict):
                key = _s(col.get("key") or col.get("field") or col.get("name") or col.get("id"))
                header = _s(col.get("header") or col.get("title") or col.get("label") or key)
            else:
                key = _s(getattr(col, "key", None) or getattr(col, "field", None) or getattr(col, "name", None))
                header = _s(getattr(col, "header", None) or getattr(col, "title", None) or getattr(col, "label", None) or key)

            if key:
                columns.append((key, header or key))

    if not columns and isinstance(spec, dict):
        keys = spec.get("keys") or []
        headers = spec.get("headers") or spec.get("display_headers") or []
        if isinstance(keys, list):
            for idx, key in enumerate(keys):
                k = _s(key)
                h = _s(headers[idx]) if isinstance(headers, list) and idx < len(headers) else k
                if k:
                    columns.append((k, h or k))

    return columns


async def _load_schema_defaults(page_name: str = TOP10_PAGE_NAME) -> Tuple[List[str], List[str]]:
    for mod_name in _SCHEMA_MODULE_CANDIDATES:
        try:
            mod = importlib.import_module(mod_name)
        except Exception:
            continue

        for fn_name in _SCHEMA_FN_CANDIDATES:
            fn = getattr(mod, fn_name, None)
            if not _callable(fn):
                continue

            attempts = [
                {"sheet": page_name},
                {"page": page_name},
                {"sheet_name": page_name},
                {"name": page_name},
            ]

            for kwargs in attempts:
                try:
                    spec = await _call_maybe_async(fn, **kwargs)
                    cols = _extract_columns_from_spec(spec)
                    if cols:
                        keys = [k for k, _ in cols]
                        headers = [h for _, h in cols]
                        if page_name == TOP10_PAGE_NAME:
                            keys, headers = _ensure_top10_keys_present(keys, headers)
                        return headers, keys
                except TypeError:
                    continue
                except Exception:
                    continue

    if page_name == TOP10_PAGE_NAME:
        keys = [
            "symbol",
            "name",
            "current_price",
            "expected_roi_3m",
            "forecast_confidence",
            "risk_score",
            "overall_score",
            "recommendation",
            "last_updated_riyadh",
            "top10_rank",
            "selection_reason",
            "criteria_snapshot",
        ]
        headers = [
            "Symbol",
            "Name",
            "Current Price",
            "Expected ROI 3M",
            "Forecast Confidence",
            "Risk Score",
            "Overall Score",
            "Recommendation",
            "Last Updated (Riyadh)",
            "Top10 Rank",
            "Selection Reason",
            "Criteria Snapshot",
        ]
        return headers, keys

    return [], []


# =============================================================================
# Context extraction
# =============================================================================
def _looks_like_engine(obj: Any) -> bool:
    if obj is None:
        return False
    for name in _ENGINE_SHEET_METHOD_CANDIDATES:
        try:
            if _callable(getattr(obj, name, None)):
                return True
        except Exception:
            continue
    return False


async def _get_engine_from_request(request: Any) -> Any:
    try:
        app = getattr(request, "app", None)
        st = getattr(app, "state", None)
        if st is not None and getattr(st, "engine", None) is not None:
            return st.engine
    except Exception:
        pass

    for modpath in ("core.data_engine_v2", "core.data_engine"):
        try:
            mod = importlib.import_module(modpath)
            get_engine = getattr(mod, "get_engine", None)
            if _callable(get_engine):
                eng = get_engine()
                if inspect.isawaitable(eng):
                    eng = await eng
                return eng
        except Exception:
            continue

    return None


def _merge_dicts(base: Dict[str, Any], extra: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in (extra or {}).items():
        if v is not None:
            out[k] = v
    return out


async def _extract_context(*args: Any, **kwargs: Any) -> Tuple[Any, Any, Any, Dict[str, Any]]:
    request = kwargs.get("request")
    engine = kwargs.get("engine") or kwargs.get("data_engine") or kwargs.get("quote_engine")
    settings = kwargs.get("settings")

    payload: Dict[str, Any] = {}

    for key in ("payload", "body", "request_data", "params"):
        val = kwargs.get(key)
        if isinstance(val, dict):
            payload = _merge_dicts(payload, val)

    criteria = kwargs.get("criteria")
    if isinstance(criteria, dict):
        payload["criteria"] = _merge_dicts(payload.get("criteria", {}) if isinstance(payload.get("criteria"), dict) else {}, criteria)

    for arg in args:
        if request is None and hasattr(arg, "headers") and hasattr(arg, "url"):
            request = arg
            continue

        if engine is None and _looks_like_engine(arg):
            engine = arg
            continue

        if isinstance(arg, dict):
            if not payload:
                payload = _merge_dicts(payload, arg)
            else:
                if "criteria" not in payload:
                    payload["criteria"] = {}
                if isinstance(payload.get("criteria"), dict):
                    payload["criteria"] = _merge_dicts(payload["criteria"], arg)
            continue

    if engine is None and request is not None:
        engine = await _get_engine_from_request(request)

    if settings is None:
        try:
            from core.config import get_settings_cached  # type: ignore
            settings = get_settings_cached()
        except Exception:
            settings = None

    return request, engine, settings, payload


# =============================================================================
# Normalization
# =============================================================================
def _flatten_criteria(payload: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    if isinstance(payload.get("criteria"), dict):
        out.update(payload["criteria"])

    if isinstance(payload.get("filters"), dict):
        out.update(payload["filters"])

    for key in (
        "pages_selected",
        "pages",
        "selected_pages",
        "sources",
        "page",
        "sheet",
        "sheet_name",
        "direct_symbols",
        "symbols",
        "symbol",
        "tickers",
        "limit",
        "top_n",
        "investment_period_days",
        "invest_period_days",
        "horizon_days",
        "risk_level",
        "risk_profile",
        "confidence_level",
        "confidence_bucket",
        "min_expected_roi",
        "min_roi",
        "min_confidence",
        "mode",
        "advisor_data_mode",
        "debug",
        "schema_only",
        "headers_only",
        "include_matrix",
        "format",
        "invest_amount",
        "allocation_strategy",
    ):
        if key in payload and payload.get(key) is not None:
            out[key] = payload.get(key)

    return out


def _normalize_sources(criteria: Dict[str, Any]) -> List[str]:
    pages = (
        _normalize_list(criteria.get("pages_selected"))
        or _normalize_list(criteria.get("pages"))
        or _normalize_list(criteria.get("selected_pages"))
        or _normalize_list(criteria.get("sources"))
    )

    page_single = _s(criteria.get("page")) or _s(criteria.get("sheet")) or _s(criteria.get("sheet_name"))
    if page_single and not pages:
        pages = [page_single]

    pages = _dedupe_keep_order(pages)

    out: List[str] = []
    for item in pages:
        if item.upper() == "ALL":
            out.extend(_BASE_SOURCE_PAGES)
        else:
            out.append(item)

    out = _dedupe_keep_order(out)
    out = [p for p in out if p and p not in _DERIVED_OR_NON_SOURCE_PAGES]

    if not out:
        out = list(_BASE_SOURCE_PAGES)

    return out


def _normalize_direct_symbols(criteria: Dict[str, Any]) -> List[str]:
    return (
        _normalize_list(criteria.get("direct_symbols"))
        or _normalize_list(criteria.get("symbols"))
        or _normalize_list(criteria.get("tickers"))
        or _normalize_list(criteria.get("symbol"))
    )


def _effective_limit(criteria: Dict[str, Any], default: int = 10) -> int:
    limit = _safe_int(criteria.get("limit") or criteria.get("top_n"), default)
    return max(1, min(200, limit))


def _normalize_mode(criteria: Dict[str, Any]) -> str:
    mode = _s(criteria.get("advisor_data_mode") or criteria.get("mode")).lower()
    if not mode or mode == "auto":
        return "live_quotes"
    if mode in {"live", "quotes"}:
        return "live_quotes"
    if mode in {"sheet", "live_sheet"}:
        return "live_sheet"
    if mode in {"snapshot", "snapshots"}:
        return "snapshot"
    return mode


def _normalize_target_page(criteria: Dict[str, Any]) -> str:
    target = _s(criteria.get("page")) or _s(criteria.get("sheet")) or _s(criteria.get("sheet_name"))
    return target or TOP10_PAGE_NAME


def _infer_operation(request: Any, criteria: Dict[str, Any]) -> str:
    path = _get_request_path(request).lower()

    if "sheet-rows" in path:
        return "sheet_rows"
    if "recommendations" in path:
        return "recommendations"
    if path.endswith("/run") or path.endswith("/advisor") or path.endswith("/investment-advisor"):
        return "run"

    fmt = _s(criteria.get("format")).lower()
    if fmt == "rows":
        return "sheet_rows"

    return "run"


# =============================================================================
# Engine page access
# =============================================================================
async def _call_engine_sheet_rows(
    engine: Any,
    *,
    page: str,
    limit: int,
    mode: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    if engine is None:
        return _make_error_payload(
            "Data engine unavailable",
            page=page,
            operation="sheet_rows",
            extra_meta={"dispatch": "engine.missing"},
        )

    fn = None
    fn_name = ""
    for name in _ENGINE_SHEET_METHOD_CANDIDATES:
        candidate = getattr(engine, name, None)
        if _callable(candidate):
            fn = candidate
            fn_name = name
            break

    if fn is None:
        return _make_error_payload(
            "Engine sheet-row handler not found",
            page=page,
            operation="sheet_rows",
            extra_meta={"dispatch": "engine.sheet_rows_missing"},
        )

    attempts = [
        {"page": page, "limit": limit, "format": "rows", "mode": mode, "body": payload},
        {"sheet": page, "limit": limit, "format": "rows", "mode": mode, "body": payload},
        {"sheet_name": page, "limit": limit, "format": "rows", "mode": mode, "body": payload},
        {"page": page, "limit": limit, "mode": mode},
        {"sheet": page, "limit": limit, "mode": mode},
        {"sheet_name": page, "limit": limit, "mode": mode},
        {"page": page, "limit": limit, "format": "rows"},
        {"sheet": page, "limit": limit, "format": "rows"},
        {"sheet_name": page, "limit": limit, "format": "rows"},
        {"page": page, "limit": limit},
        {"sheet": page, "limit": limit},
        {"sheet_name": page, "limit": limit},
    ]

    out: Any = None
    for kwargs in attempts:
        try:
            out = await _call_maybe_async(fn, **kwargs)
            break
        except TypeError:
            continue
        except Exception as e:
            return _make_error_payload(
                f"Engine page call failed: {type(e).__name__}: {e}",
                page=page,
                operation="sheet_rows",
                extra_meta={"dispatch": f"engine.{fn_name}"},
            )

    if out is None:
        try:
            out = await _call_maybe_async(fn, page, limit)
        except TypeError:
            try:
                out = await _call_maybe_async(fn, page)
            except Exception as e:
                return _make_error_payload(
                    f"Engine page call failed: {type(e).__name__}: {e}",
                    page=page,
                    operation="sheet_rows",
                    extra_meta={"dispatch": f"engine.{fn_name}"},
                )

    if isinstance(out, dict):
        result = dict(out)
    elif isinstance(out, list):
        if out and isinstance(out[0], dict):
            rows = [dict(x) for x in out if isinstance(x, dict)]
            keys = _derive_keys_from_rows(rows)
            headers = list(keys)
            result = {
                "status": "success" if rows else "partial",
                "page": page,
                "sheet": page,
                "headers": headers,
                "display_headers": headers,
                "keys": keys,
                "rows": rows,
                "data": rows,
                "items": rows,
                "rows_matrix": _rows_to_matrix(rows, keys) if keys else [],
            }
        else:
            result = {
                "status": "partial",
                "page": page,
                "sheet": page,
                "detail": "Engine returned non-dict list rows",
                "rows": [],
                "rows_matrix": [],
                "headers": [],
                "keys": [],
            }
    else:
        result = _model_to_dict(out)

    meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}
    meta = dict(meta)
    meta.setdefault("dispatch", f"engine.{fn_name}")
    result["meta"] = meta
    result.setdefault("page", page)
    result.setdefault("sheet", page)
    return result


def _normalize_engine_page_payload(result: Dict[str, Any], *, page: str) -> Dict[str, Any]:
    payload = dict(result or {})

    headers, keys = _extract_headers_keys(payload)
    rows = _extract_rows_candidate(payload)
    matrix = _extract_matrix_candidate(payload)

    if not rows and matrix:
        if not keys:
            keys = list(headers)
        if not keys and matrix and isinstance(matrix[0], list):
            keys = [f"col_{i+1}" for i in range(len(matrix[0]))]
        if keys:
            rows = _matrix_to_rows(matrix, keys)

    if rows and not keys:
        keys = _derive_keys_from_rows(rows)
    if keys and not headers:
        headers = list(keys)
    if rows and not matrix and keys:
        matrix = _rows_to_matrix(rows, keys)
    if matrix is None:
        matrix = []

    status_value = _s(payload.get("status"))
    if not status_value:
        status_value = "success" if rows or matrix or headers or keys else "partial"

    meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    meta = dict(meta)

    out = {
        "status": status_value,
        "page": _s(payload.get("page")) or page,
        "sheet": _s(payload.get("sheet")) or page,
        "route_family": _route_family_for_page(page),
        "headers": headers,
        "display_headers": headers,
        "sheet_headers": headers,
        "column_headers": headers,
        "keys": keys,
        "rows": rows,
        "items": rows,
        "data": rows,
        "rows_matrix": matrix,
        "count": max(len(rows), len(matrix)),
        "detail": _s(payload.get("detail") or payload.get("error") or payload.get("message")),
        "meta": meta,
    }
    return out


# =============================================================================
# Candidate collection
# =============================================================================
def _rows_from_engine_result(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = _extract_rows_candidate(result)
    if rows:
        return rows

    matrix = _extract_matrix_candidate(result)
    if matrix:
        headers, keys = _extract_headers_keys(result)
        if not keys:
            keys = headers
        if keys:
            return _matrix_to_rows(matrix, keys)

    return []


def _symbol_of(row: Dict[str, Any]) -> str:
    return (
        _s(row.get("symbol"))
        or _s(row.get("ticker"))
        or _s(row.get("code"))
        or _s(row.get("symbol_code"))
        or _s(row.get("instrument"))
    )


def _name_of(row: Dict[str, Any]) -> str:
    return (
        _s(row.get("name"))
        or _s(row.get("company_name"))
        or _s(row.get("description"))
        or _s(row.get("short_name"))
    )


async def _collect_candidate_rows(
    *,
    engine: Any,
    criteria: Dict[str, Any],
    mode: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    pages = _normalize_sources(criteria)
    direct_symbols = set([s.upper() for s in _normalize_direct_symbols(criteria)])
    limit = max(_effective_limit(criteria), 1)

    collected: List[Dict[str, Any]] = []
    by_symbol: Dict[str, Dict[str, Any]] = {}
    page_counts: Dict[str, int] = {}
    page_errors: Dict[str, str] = {}

    for page in pages:
        page_result = await _call_engine_sheet_rows(
            engine,
            page=page,
            limit=max(limit * 5, 25),
            mode=mode,
            payload=criteria,
        )

        page_rows = _rows_from_engine_result(page_result)
        page_counts[page] = len(page_rows)

        if not page_rows:
            detail = _s(page_result.get("detail") or page_result.get("error"))
            if detail:
                page_errors[page] = detail

        for row in page_rows:
            item = dict(row)
            item.setdefault("source_page", page)

            sym = _symbol_of(item)
            if not sym and direct_symbols:
                continue

            if direct_symbols and sym.upper() not in direct_symbols:
                continue

            key = sym.upper() if sym else f"__row_{len(collected)+1}"
            if key not in by_symbol:
                by_symbol[key] = item
                collected.append(item)
            else:
                existing = by_symbol[key]
                for k, v in item.items():
                    if existing.get(k) in (None, "", "None") and v not in (None, "", "None"):
                        existing[k] = v

    if direct_symbols and not collected:
        for sym in direct_symbols:
            collected.append(
                {
                    "symbol": sym,
                    "name": sym,
                    "source_page": "direct_symbols",
                }
            )

    meta = {
        "pages_effective": pages,
        "page_counts": page_counts,
        "page_errors": page_errors,
        "candidate_rows": len(collected),
        "direct_symbols_count": len(direct_symbols),
    }
    return collected, meta


# =============================================================================
# Scoring + recommendation
# =============================================================================
def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _maybe_ratio(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        x = float(value)
        if math.isnan(x) or math.isinf(x):
            return None
        return x
    except Exception:
        return None


def _extract_current_price(row: Dict[str, Any]) -> Optional[float]:
    for key in ("current_price", "price", "last_price", "close", "last", "market_price"):
        v = _maybe_ratio(row.get(key))
        if v is not None:
            return v
    return None


def _extract_target_price(row: Dict[str, Any]) -> Optional[float]:
    for key in ("target_price", "fair_value", "intrinsic_value", "forecast_price"):
        v = _maybe_ratio(row.get(key))
        if v is not None:
            return v
    return None


def _extract_expected_roi_3m(row: Dict[str, Any]) -> float:
    for key in ("expected_roi_3m", "expected_roi", "roi_3m", "expected_return_3m"):
        v = _maybe_ratio(row.get(key))
        if v is not None:
            return v

    cp = _extract_current_price(row)
    tp = _extract_target_price(row)
    if cp is not None and tp is not None and cp > 0:
        return (tp / cp) - 1.0

    roi1 = _maybe_ratio(row.get("expected_roi_1m"))
    if roi1 is not None:
        return roi1 * 3.0

    roi12 = _maybe_ratio(row.get("expected_roi_12m"))
    if roi12 is not None:
        return roi12 / 4.0

    upside = _maybe_ratio(row.get("upside_pct"))
    if upside is not None:
        return upside / 100.0 if abs(upside) > 1.0 else upside

    return 0.0


def _extract_confidence_score(row: Dict[str, Any]) -> float:
    for key in ("forecast_confidence", "confidence_score", "ai_confidence", "confidence"):
        raw = row.get(key)
        if isinstance(raw, str):
            s = raw.strip().lower().replace("%", "")
            if s in {"high", "strong"}:
                return 0.85
            if s in {"moderate", "medium"}:
                return 0.60
            if s in {"low", "weak"}:
                return 0.35
            try:
                raw = float(s)
            except Exception:
                raw = None

        v = _maybe_ratio(raw)
        if v is not None:
            if abs(v) > 1.0:
                return _clamp(v / 100.0, 0.0, 1.0)
            return _clamp(v, 0.0, 1.0)

    return 0.55


def _extract_risk_score(row: Dict[str, Any]) -> float:
    v = _maybe_ratio(row.get("risk_score"))
    if v is not None:
        if abs(v) <= 1.0:
            return _clamp(v * 100.0, 0.0, 100.0)
        return _clamp(v, 0.0, 100.0)

    vol = _maybe_ratio(row.get("volatility_90d"))
    if vol is not None:
        if abs(vol) <= 1.0:
            return _clamp(abs(vol) * 100.0, 0.0, 100.0)
        return _clamp(abs(vol), 0.0, 100.0)

    drawdown = _maybe_ratio(row.get("max_drawdown_1y"))
    if drawdown is not None:
        if abs(drawdown) <= 1.0:
            return _clamp(abs(drawdown) * 100.0, 0.0, 100.0)
        return _clamp(abs(drawdown), 0.0, 100.0)

    beta = _maybe_ratio(row.get("beta"))
    if beta is not None:
        return _clamp(abs(beta) * 30.0, 0.0, 100.0)

    return 40.0


def _confidence_bucket(score: float) -> str:
    if score >= 0.75:
        return "high"
    if score >= 0.50:
        return "moderate"
    return "low"


def _risk_bucket(score: float) -> str:
    if score <= 30.0:
        return "low"
    if score <= 60.0:
        return "moderate"
    return "high"


def _recommendation_from_scores(overall_score: float, roi_3m: float, confidence: float, risk_score: float) -> str:
    if overall_score >= 75.0 and roi_3m >= 0.05 and confidence >= 0.70 and risk_score <= 60.0:
        return "Strong Buy"
    if overall_score >= 60.0 and roi_3m > 0.0 and confidence >= 0.55:
        return "Buy"
    if overall_score >= 45.0:
        return "Hold"
    return "Avoid"


def _canonical_selection_reason(row: Dict[str, Any]) -> Optional[str]:
    recommendation = _s(row.get("recommendation"))
    confidence_bucket = _s(row.get("confidence_bucket"))
    risk_bucket = _s(row.get("risk_bucket"))

    score_parts: List[str] = []
    for label, key in (
        ("overall", "overall_score"),
        ("opportunity", "opportunity_score"),
        ("value", "value_score"),
        ("quality", "quality_score"),
        ("momentum", "momentum_score"),
        ("growth", "growth_score"),
    ):
        val = row.get(key)
        if isinstance(val, (int, float)):
            score_parts.append(f"{label}={round(float(val), 2)}")

    roi_parts: List[str] = []
    for label, key in (
        ("1M", "expected_roi_1m"),
        ("3M", "expected_roi_3m"),
        ("12M", "expected_roi_12m"),
    ):
        val = row.get(key)
        if isinstance(val, (int, float)):
            roi_parts.append(f"{label} ROI={round(float(val) * 100, 2)}%")

    reason_parts: List[str] = []
    if recommendation:
        reason_parts.append(f"Recommendation={recommendation}")
    if confidence_bucket:
        reason_parts.append(f"Confidence={confidence_bucket}")
    if risk_bucket:
        reason_parts.append(f"Risk={risk_bucket}")
    if score_parts:
        reason_parts.append(", ".join(score_parts[:3]))
    if roi_parts:
        reason_parts.append(", ".join(roi_parts[:2]))

    if not reason_parts:
        return None
    return " | ".join(reason_parts)


def _score_and_rank_rows(
    rows: List[Dict[str, Any]],
    *,
    criteria: Dict[str, Any],
    limit: int,
) -> List[Dict[str, Any]]:
    min_roi = _maybe_ratio(criteria.get("min_expected_roi"))
    if min_roi is None:
        min_roi = _maybe_ratio(criteria.get("min_roi"))
    if min_roi is None:
        min_roi = 0.0

    risk_pref = (_s(criteria.get("risk_level")) or _s(criteria.get("risk_profile")) or "moderate").lower()

    scored: List[Dict[str, Any]] = []
    criteria_snapshot = _json_compact(criteria)

    for row in rows:
        r = dict(row)

        symbol = _symbol_of(r)
        if symbol:
            r["symbol"] = symbol
        if not _s(r.get("name")):
            r["name"] = _name_of(r) or symbol

        current_price = _extract_current_price(r)
        if current_price is not None and r.get("current_price") in (None, "", "None"):
            r["current_price"] = current_price

        expected_roi_3m = _extract_expected_roi_3m(r)
        r["expected_roi_3m"] = expected_roi_3m

        confidence = _extract_confidence_score(r)
        risk_score = _extract_risk_score(r)

        roi_score = _clamp((expected_roi_3m * 100.0) + 50.0, 0.0, 100.0)
        confidence_score_pct = confidence * 100.0
        safety_score = 100.0 - risk_score

        if risk_pref == "low":
            safety_score = _clamp(safety_score + 10.0, 0.0, 100.0)
        elif risk_pref == "high":
            roi_score = _clamp(roi_score + 7.5, 0.0, 100.0)

        overall_score = (roi_score * 0.45) + (confidence_score_pct * 0.30) + (safety_score * 0.25)
        overall_score = _clamp(overall_score, 0.0, 100.0)

        if expected_roi_3m < min_roi:
            overall_score = _clamp(overall_score - 7.5, 0.0, 100.0)

        r["forecast_confidence"] = round(confidence, 4)
        r["confidence_bucket"] = _confidence_bucket(confidence)
        r["risk_score"] = round(risk_score, 2)
        r["risk_bucket"] = _risk_bucket(risk_score)
        r["overall_score"] = round(overall_score, 2)
        r["opportunity_score"] = round((roi_score * 0.60) + (confidence_score_pct * 0.40), 2)

        if _is_blank(r.get("recommendation")):
            r["recommendation"] = _recommendation_from_scores(overall_score, expected_roi_3m, confidence, risk_score)

        if _is_blank(r.get("selection_reason")):
            r["selection_reason"] = _canonical_selection_reason(r)

        if _is_blank(r.get("criteria_snapshot")):
            r["criteria_snapshot"] = criteria_snapshot

        scored.append(r)

    scored.sort(
        key=lambda x: (
            float(x.get("overall_score") or 0.0),
            float(x.get("expected_roi_3m") or 0.0),
            float(x.get("forecast_confidence") or 0.0),
        ),
        reverse=True,
    )

    out: List[Dict[str, Any]] = []
    for idx, row in enumerate(scored[: max(1, limit)], start=1):
        r = dict(row)
        r["top10_rank"] = idx
        if _is_blank(r.get("rank_overall")):
            r["rank_overall"] = idx
        out.append(r)

    return out


# =============================================================================
# Output builders
# =============================================================================
async def _build_projected_sheet_payload(
    page_name: str,
    rows: List[Dict[str, Any]],
    *,
    meta: Dict[str, Any],
    detail: str = "",
) -> Dict[str, Any]:
    headers, keys = await _load_schema_defaults(page_name)

    if not keys and rows:
        keys = _derive_keys_from_rows(rows)
    if not headers and keys:
        headers = list(keys)
    if page_name == TOP10_PAGE_NAME:
        keys, headers = _ensure_top10_keys_present(keys, headers)

    projected: List[Dict[str, Any]] = []
    if keys:
        for row in rows:
            projected.append({k: row.get(k, None) for k in keys})
    else:
        projected = [dict(r) for r in rows]

    matrix = _rows_to_matrix(projected, keys) if keys else []

    return {
        "status": "success" if projected or headers or keys else "partial",
        "page": page_name,
        "sheet": page_name,
        "route_family": _route_family_for_page(page_name),
        "headers": headers,
        "display_headers": headers,
        "sheet_headers": headers,
        "column_headers": headers,
        "keys": keys,
        "rows": projected,
        "data": projected,
        "items": projected,
        "rows_matrix": matrix,
        "count": len(projected),
        "detail": detail,
        "meta": meta,
    }


def _build_recommendations_payload(
    recommendations: List[Dict[str, Any]],
    *,
    meta: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "status": "success" if recommendations else "error",
        "page": TOP10_PAGE_NAME,
        "sheet": TOP10_PAGE_NAME,
        "route_family": "top10",
        "recommendations": recommendations,
        "rows": recommendations,
        "data": recommendations,
        "items": recommendations,
        "count": len(recommendations),
        "detail": "" if recommendations else "No advisor recommendations produced",
        "meta": meta,
    }


async def _build_criteria_sheet_payload(criteria: Dict[str, Any], *, meta: Dict[str, Any]) -> Dict[str, Any]:
    row = {
        "criteria": "active",
        "risk_profile": _s(criteria.get("risk_profile")) or _s(criteria.get("risk_level")) or "moderate",
        "risk_level": _s(criteria.get("risk_level")) or _s(criteria.get("risk_profile")) or "moderate",
        "allocation_strategy": _s(criteria.get("allocation_strategy")) or "maximum_sharpe",
        "horizon_days": _safe_int(criteria.get("horizon_days") or criteria.get("investment_period_days"), 90),
        "investment_period_days": _safe_int(criteria.get("investment_period_days") or criteria.get("horizon_days"), 90),
        "top_n": _effective_limit(criteria),
        "min_expected_roi": _maybe_ratio(criteria.get("min_expected_roi") or criteria.get("min_roi")),
        "min_confidence": _maybe_ratio(criteria.get("min_confidence")),
        "symbols": ",".join(_normalize_direct_symbols(criteria)),
        "sources": ",".join(_normalize_sources(criteria)),
        "advisor_data_mode": _normalize_mode(criteria),
        "generated_at_utc": _now_utc_iso(),
    }
    return await _build_projected_sheet_payload(
        "Advisor_Criteria",
        [row],
        meta=meta,
        detail="Criteria fallback payload built by advisor engine",
    )


def _filter_ksa_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        symbol = _symbol_of(row).upper()
        country = _s(row.get("country")).upper()
        exchange = _s(row.get("exchange")).upper()
        if symbol.endswith(".SR") or country in {"SA", "SAU", "KSA"} or "TADAWUL" in exchange:
            out.append(dict(row))
    return out


# =============================================================================
# Main executor
# =============================================================================
async def _execute_engine_async(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    request = None
    target_page = TOP10_PAGE_NAME
    operation = "run"

    try:
        request, engine, settings, payload = await _extract_context(*args, **kwargs)
        criteria = _flatten_criteria(payload)
        target_page = _normalize_target_page(criteria)
        mode = _normalize_mode(criteria)
        limit = _effective_limit(criteria, default=10)
        operation = _infer_operation(request, criteria)

        request_id = _get_request_id(request)

        base_meta: Dict[str, Any] = {
            "engine_version": INVESTMENT_ADVISOR_ENGINE_VERSION,
            "request_id": request_id,
            "generated_at_utc": _now_utc_iso(),
            "operation": operation,
            "target_page": target_page,
            "advisor_data_mode_effective": mode,
            "engine_present": bool(engine),
            "engine_type": type(engine).__name__ if engine is not None else "none",
        }

        # schema-only / headers-only fast path
        if _coerce_bool(criteria.get("schema_only")) or _coerce_bool(criteria.get("headers_only")):
            headers, keys = await _load_schema_defaults(target_page)
            if target_page == TOP10_PAGE_NAME:
                keys, headers = _ensure_top10_keys_present(keys, headers)

            return _jsonable_snapshot(
                {
                    "status": "success",
                    "page": target_page,
                    "sheet": target_page,
                    "route_family": _route_family_for_page(target_page),
                    "headers": headers,
                    "display_headers": headers,
                    "sheet_headers": headers,
                    "column_headers": headers,
                    "keys": keys,
                    "rows": [],
                    "items": [],
                    "data": [],
                    "rows_matrix": [],
                    "count": 0,
                    "detail": "Schema-only advisor response",
                    "meta": {**base_meta, "dispatch": "schema_only"},
                }
            )

        # Explicit non-Top10 sheet-rows requests: try direct engine page access first.
        if operation == "sheet_rows" and target_page != TOP10_PAGE_NAME:
            direct_result = await _call_engine_sheet_rows(
                engine,
                page=target_page,
                limit=limit,
                mode=mode,
                payload=criteria,
            )
            direct_payload = _normalize_engine_page_payload(direct_result, page=target_page)
            direct_payload["meta"] = {
                **(direct_payload.get("meta") if isinstance(direct_payload.get("meta"), dict) else {}),
                **base_meta,
                "dispatch": (
                    direct_payload.get("meta", {}).get("dispatch")
                    if isinstance(direct_payload.get("meta"), dict)
                    else "engine_direct_page"
                ) or "engine_direct_page",
            }

            direct_status = _s(direct_payload.get("status")).lower()
            if _has_tabular_shape(direct_payload) or direct_status not in {"error", "failed"}:
                return _jsonable_snapshot(direct_payload)

        candidate_rows, collect_meta = await _collect_candidate_rows(
            engine=engine,
            criteria=criteria,
            mode=mode,
        )

        base_meta.update(collect_meta)

        if not candidate_rows:
            if operation == "sheet_rows" and target_page == "Advisor_Criteria":
                return _jsonable_snapshot(
                    await _build_criteria_sheet_payload(
                        criteria,
                        meta={**base_meta, "dispatch": "criteria_fallback"},
                    )
                )

            return _jsonable_snapshot(
                _make_error_payload(
                    "No candidate rows found for advisor execution",
                    request=request,
                    page=(target_page if operation == "sheet_rows" else TOP10_PAGE_NAME),
                    operation=operation,
                    extra_meta=base_meta,
                )
            )

        recommendations = _score_and_rank_rows(
            candidate_rows,
            criteria=criteria,
            limit=limit,
        )

        meta = dict(base_meta)
        meta["dispatch"] = "advisor_scoring"
        meta["recommendation_count"] = len(recommendations)

        # Explicit derived page fallbacks
        if operation == "sheet_rows":
            if target_page == TOP10_PAGE_NAME:
                return _jsonable_snapshot(
                    await _build_projected_sheet_payload(
                        TOP10_PAGE_NAME,
                        recommendations,
                        meta=meta,
                    )
                )

            if target_page == "AI_Opportunity_Report":
                return _jsonable_snapshot(
                    await _build_projected_sheet_payload(
                        "AI_Opportunity_Report",
                        recommendations,
                        meta={**meta, "dispatch": "ai_opportunity_fallback"},
                        detail="AI Opportunity fallback built from advisor scoring",
                    )
                )

            if target_page == "Advisor_Criteria":
                return _jsonable_snapshot(
                    await _build_criteria_sheet_payload(
                        criteria,
                        meta={**meta, "dispatch": "criteria_fallback"},
                    )
                )

            if target_page == "KSA_TADAWUL":
                ksa_rows = _filter_ksa_rows(candidate_rows)
                return _jsonable_snapshot(
                    await _build_projected_sheet_payload(
                        "KSA_TADAWUL",
                        ksa_rows,
                        meta={**meta, "dispatch": "ksa_fallback"},
                        detail="KSA_TADAWUL fallback built from advisor source rows",
                    )
                )

            # Generic explicit page fallback: return projected scored rows
            return _jsonable_snapshot(
                await _build_projected_sheet_payload(
                    target_page,
                    recommendations,
                    meta={**meta, "dispatch": "generic_sheet_fallback"},
                    detail=f"{target_page} fallback built from advisor scoring",
                )
            )

        return _jsonable_snapshot(
            _build_recommendations_payload(
                recommendations,
                meta=meta,
            )
        )

    except Exception as e:
        logger.exception("Investment advisor engine execution failed: %s", e)
        return _jsonable_snapshot(
            _make_error_payload(
                f"{type(e).__name__}: {e}",
                request=request,
                page=target_page,
                operation=operation,
                extra_meta={"dispatch": "engine_exception"},
            )
        )


def _execute_engine_sync(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    try:
        return _run_coro_sync(_execute_engine_async(*args, **kwargs))
    except Exception as e:
        logger.exception("Investment advisor engine sync wrapper failed: %s", e)
        request = kwargs.get("request")
        payload = kwargs.get("payload") or kwargs.get("body") or kwargs.get("request_data") or kwargs.get("params") or {}
        page = TOP10_PAGE_NAME
        if isinstance(payload, dict):
            page = _s(payload.get("page") or payload.get("sheet") or payload.get("sheet_name")) or TOP10_PAGE_NAME
        return _make_error_payload(
            f"{type(e).__name__}: {e}",
            request=request,
            page=page,
            operation="run",
            extra_meta={"dispatch": "sync_wrapper_exception"},
        )


# =============================================================================
# Public engine class + factories
# =============================================================================
class InvestmentAdvisorEngine:
    """Compatibility object exposing the expected advisor runner methods."""

    def run_investment_advisor_engine(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return _execute_engine_sync(*args, **kwargs)

    def run_investment_advisor(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return _execute_engine_sync(*args, **kwargs)

    def run_advisor(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return _execute_engine_sync(*args, **kwargs)

    def execute_investment_advisor(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return _execute_engine_sync(*args, **kwargs)

    def execute_advisor(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return _execute_engine_sync(*args, **kwargs)

    def recommend(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return _execute_engine_sync(*args, **kwargs)

    def recommend_investments(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return _execute_engine_sync(*args, **kwargs)

    def get_recommendations(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return _execute_engine_sync(*args, **kwargs)

    def build_recommendations(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return _execute_engine_sync(*args, **kwargs)

    def __call__(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return _execute_engine_sync(*args, **kwargs)


_SINGLETON_ENGINE = InvestmentAdvisorEngine()

advisor_engine = _SINGLETON_ENGINE
investment_advisor_engine = _SINGLETON_ENGINE
advisor_service = _SINGLETON_ENGINE
investment_advisor_service = _SINGLETON_ENGINE
advisor_runner = _SINGLETON_ENGINE
investment_advisor_runner = _SINGLETON_ENGINE
advisor = _SINGLETON_ENGINE
investment_advisor = _SINGLETON_ENGINE


def create_investment_advisor(*args: Any, **kwargs: Any) -> InvestmentAdvisorEngine:
    return _SINGLETON_ENGINE


def get_investment_advisor(*args: Any, **kwargs: Any) -> InvestmentAdvisorEngine:
    return _SINGLETON_ENGINE


def build_investment_advisor(*args: Any, **kwargs: Any) -> InvestmentAdvisorEngine:
    return _SINGLETON_ENGINE


def create_advisor(*args: Any, **kwargs: Any) -> InvestmentAdvisorEngine:
    return _SINGLETON_ENGINE


def get_advisor(*args: Any, **kwargs: Any) -> InvestmentAdvisorEngine:
    return _SINGLETON_ENGINE


def build_advisor(*args: Any, **kwargs: Any) -> InvestmentAdvisorEngine:
    return _SINGLETON_ENGINE


class _EngineAdapter:
    """Lightweight passthrough adapter used by routes that optionally warm snapshots."""

    def __init__(self, engine: Any, cache_strategy: str = "memory", cache_ttl: int = 600) -> None:
        self.engine = engine
        self.cache_strategy = _s(cache_strategy).lower() or "memory"
        self.cache_ttl = _safe_int(cache_ttl, 600)

    def warm_cache(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return {
            "status": "ok",
            "cache_strategy": self.cache_strategy,
            "cache_ttl": self.cache_ttl,
            "warmed": False,
        }

    def warm_snapshots(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.warm_cache(*args, **kwargs)

    def preload_snapshots(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.warm_cache(*args, **kwargs)

    def build_snapshot_cache(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.warm_cache(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.engine, name)


def create_engine_adapter(engine: Any, cache_strategy: str = "memory", cache_ttl: int = 600, **kwargs: Any) -> _EngineAdapter:
    return _EngineAdapter(engine=engine, cache_strategy=cache_strategy, cache_ttl=cache_ttl)


# =============================================================================
# Direct function exports expected by routes
# =============================================================================
def run_investment_advisor_engine(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return _execute_engine_sync(*args, **kwargs)


def run_investment_advisor(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return _execute_engine_sync(*args, **kwargs)


def run_advisor(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return _execute_engine_sync(*args, **kwargs)


def execute_investment_advisor(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return _execute_engine_sync(*args, **kwargs)


def execute_advisor(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return _execute_engine_sync(*args, **kwargs)


def recommend(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return _execute_engine_sync(*args, **kwargs)


def recommend_investments(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return _execute_engine_sync(*args, **kwargs)


def get_recommendations(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return _execute_engine_sync(*args, **kwargs)


def build_recommendations(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return _execute_engine_sync(*args, **kwargs)


__all__ = [
    "INVESTMENT_ADVISOR_ENGINE_VERSION",
    "InvestmentAdvisorEngine",
    "advisor_engine",
    "investment_advisor_engine",
    "advisor_service",
    "investment_advisor_service",
    "advisor_runner",
    "investment_advisor_runner",
    "advisor",
    "investment_advisor",
    "create_investment_advisor",
    "get_investment_advisor",
    "build_investment_advisor",
    "create_advisor",
    "get_advisor",
    "build_advisor",
    "create_engine_adapter",
    "run_investment_advisor_engine",
    "run_investment_advisor",
    "run_advisor",
    "execute_investment_advisor",
    "execute_advisor",
    "recommend",
    "recommend_investments",
    "get_recommendations",
    "build_recommendations",
]
