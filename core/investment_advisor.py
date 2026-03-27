
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/investment_advisor.py
================================================================================
INVESTMENT ADVISOR ORCHESTRATOR — v4.2.0
================================================================================
LIVE-BY-DEFAULT • ENGINE-FIRST • SNAPSHOT-TOLERANT • ROUTE-COMPATIBLE
MODE-AWARE • SCHEMA-SAFE • JSON-SAFE • IMPORT-SAFE • WORKER-THREAD SAFE

What this revision fixes
------------------------
- ✅ FIX: keeps this module as an orchestration wrapper over the engine rather
         than becoming a second independent engine.
- ✅ FIX: supports aligned engine output shapes including:
         - row_objects / rowObjects
         - rows_matrix / matrix
         - nested payload / result / response payloads
- ✅ FIX: honors and normalizes offset locally, not only limit/top_n.
- ✅ FIX: hardens tolerant engine calling so only signature-mismatch TypeErrors
         trigger fallback retries; runtime exceptions now surface correctly.
- ✅ FIX: prevents snapshot cross-talk by keying cache with request shape
         (page + mode + symbols + limit + offset), not page alone.
- ✅ FIX: preserves stable JSON-safe envelopes and fallback recommendation logic.

Purpose
-------
This module is the advisor orchestration layer. It should:
- normalize payloads coming from routes
- choose effective data mode
- delegate execution to `core.investment_advisor_engine`
- optionally serve cached snapshots
- return stable, JSON-safe envelopes

It should NOT become a second independent engine.
================================================================================
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import inspect
import json
import logging
import math
import os
import threading
import time
from copy import deepcopy
from dataclasses import is_dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

logger = logging.getLogger("core.investment_advisor")
logger.addHandler(logging.NullHandler())

INVESTMENT_ADVISOR_VERSION = "4.2.0"
DEFAULT_PAGE = "Top_10_Investments"
DEFAULT_LIMIT = 10
DEFAULT_OFFSET = 0
DEFAULT_SNAPSHOT_TTL_SEC = 900

BASE_SOURCE_PAGES = {
    "Market_Leaders",
    "Global_Markets",
    "Mutual_Funds",
    "Commodities_FX",
    "My_Portfolio",
}
SPECIAL_PAGES = {
    "Top_10_Investments",
    "Insights_Analysis",
    "Data_Dictionary",
}
ALL_KNOWN_PAGES = BASE_SOURCE_PAGES | SPECIAL_PAGES | {
    "Advisor_Criteria",
    "AI_Opportunity_Report",
    "KSA_TADAWUL",
}

PAGE_ALIASES = {
    "top10": "Top_10_Investments",
    "top_10": "Top_10_Investments",
    "top_10_investments": "Top_10_Investments",
    "top-10-investments": "Top_10_Investments",
    "top10investments": "Top_10_Investments",
    "investment_advisor": "Top_10_Investments",
    "investment-advisor": "Top_10_Investments",
    "advisor": "Top_10_Investments",
    "insights": "Insights_Analysis",
    "insights_analysis": "Insights_Analysis",
    "insights-analysis": "Insights_Analysis",
    "data_dictionary": "Data_Dictionary",
    "data-dictionary": "Data_Dictionary",
    "dictionary": "Data_Dictionary",
    "market_leaders": "Market_Leaders",
    "market-leaders": "Market_Leaders",
    "global_markets": "Global_Markets",
    "global-markets": "Global_Markets",
    "mutual_funds": "Mutual_Funds",
    "mutual-funds": "Mutual_Funds",
    "commodities_fx": "Commodities_FX",
    "commodities-fx": "Commodities_FX",
    "my_portfolio": "My_Portfolio",
    "my-portfolio": "My_Portfolio",
}

ENGINE_MODULE_CANDIDATES = (
    "core.investment_advisor_engine",
    "core.investment_advisor",
)
ENGINE_FUNCTION_CANDIDATES = (
    "run_investment_advisor_engine",
    "run_investment_advisor",
    "run_advisor",
    "execute_investment_advisor",
    "execute_advisor",
    "recommend",
    "recommend_investments",
    "get_recommendations",
    "build_recommendations",
)
ENGINE_OBJECT_CANDIDATES = (
    "investment_advisor_engine",
    "advisor_engine",
    "investment_advisor_service",
    "advisor_service",
    "investment_advisor_runner",
    "advisor_runner",
    "investment_advisor",
    "advisor",
)
ENGINE_OBJECT_METHOD_CANDIDATES = ENGINE_FUNCTION_CANDIDATES

SCHEMA_MODULE_CANDIDATES = (
    "core.sheets.schema_registry",
    "core.sheets.page_catalog",
    "core.schema_registry",
    "core.schemas",
)
SCHEMA_FUNCTION_CANDIDATES = (
    "get_sheet_spec",
    "get_page_spec",
    "get_schema_for_page",
    "sheet_spec",
    "build_sheet_spec",
)

GENERIC_FALLBACK_HEADERS = [
    "Symbol",
    "Name",
    "Asset Class",
    "Exchange",
    "Currency",
    "Country",
    "Sector",
    "Industry",
    "Current Price",
    "Price Change",
    "Percent Change",
    "Risk Score",
    "Valuation Score",
    "Overall Score",
    "Opportunity Score",
    "Risk Bucket",
    "Confidence Bucket",
    "Recommendation",
]

_INSIGHTS_HEADERS = ["Section", "Item", "Metric", "Value", "Notes", "Source", "Updated At"]
_DICTIONARY_HEADERS = [
    "Sheet",
    "Column Key",
    "Display Header",
    "Section",
    "Type",
    "Description",
    "Example",
    "Required",
    "Notes",
]
_TOP10_EXTRA_KEYS = ["top10_rank", "selection_reason", "criteria_snapshot"]
_TOP10_EXTRA_HEADERS = ["Top10 Rank", "Selection Reason", "Criteria Snapshot"]

_SNAPSHOT_LOCK = threading.RLock()
_SNAPSHOT_STORE: Dict[str, Dict[str, Any]] = {}


# =============================================================================
# Generic helpers
# =============================================================================
def _s(value: Any) -> str:
    try:
        if value is None:
            return ""
        out = str(value).strip()
        return "" if out.lower() in {"none", "null", "nil"} else out
    except Exception:
        return ""


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if isinstance(value, bool):
            return default
        return int(float(value))
    except Exception:
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if isinstance(value, bool):
            return default
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        s = value.strip().lower()
        if s in {"1", "true", "yes", "y", "on"}:
            return True
        if s in {"0", "false", "no", "n", "off"}:
            return False
    if isinstance(value, (int, float)):
        try:
            return bool(int(value))
        except Exception:
            return default
    return default


def _is_blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


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
        raw = value.replace(";", ",").replace("\n", ",")
        parts = [part.strip() for part in raw.split(",")]
        return _dedupe_keep_order([p for p in parts if p])
    if isinstance(value, (list, tuple, set)):
        return _dedupe_keep_order(list(value))
    return _dedupe_keep_order([value])


def _jsonable(value: Any) -> Any:
    try:
        if is_dataclass(value):
            value = asdict(value)
        elif hasattr(value, "model_dump") and callable(getattr(value, "model_dump")):
            value = value.model_dump(mode="python")
        elif hasattr(value, "dict") and callable(getattr(value, "dict")):
            value = value.dict()  # type: ignore[assignment]
    except Exception:
        pass

    try:
        return json.loads(json.dumps(value, ensure_ascii=False, default=str))
    except Exception:
        try:
            return str(value)
        except Exception:
            return None


def _json_compact(value: Any) -> str:
    try:
        return json.dumps(_jsonable(value), ensure_ascii=False, separators=(",", ":"))
    except Exception:
        try:
            return str(value)
        except Exception:
            return ""


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _deepcopy_jsonable(value: Any) -> Any:
    try:
        return deepcopy(value)
    except Exception:
        return _jsonable(value)


def _rows_to_matrix(rows: List[Dict[str, Any]], keys: List[str]) -> List[List[Any]]:
    return [[row.get(key) for key in keys] for row in rows]


def _title_case_header(key: str) -> str:
    txt = _s(key).replace("_", " ").replace("-", " ").strip()
    return " ".join(part.capitalize() if part.upper() != part else part for part in txt.split()) if txt else ""


def _normalize_key(key: str) -> str:
    return _s(key).strip().lower().replace(" ", "_").replace("-", "_")


def _criteria_fingerprint(criteria: Mapping[str, Any]) -> str:
    payload = {
        "page": _normalize_page_name(criteria.get("page")) or DEFAULT_PAGE,
        "mode": _normalize_mode(criteria.get("advisor_data_mode") or criteria.get("mode")),
        "symbols": _normalize_list(criteria.get("symbols") or criteria.get("tickers")),
        "limit": max(1, _safe_int(criteria.get("limit") or criteria.get("top_n"), DEFAULT_LIMIT)),
        "offset": max(0, _safe_int(criteria.get("offset"), DEFAULT_OFFSET)),
    }
    encoded = _json_compact(payload)
    return hashlib.sha1(encoded.encode("utf-8")).hexdigest()


def _slice_rows(rows: List[Dict[str, Any]], offset: int, limit: int) -> List[Dict[str, Any]]:
    start = max(0, _safe_int(offset, DEFAULT_OFFSET))
    size = max(1, _safe_int(limit, DEFAULT_LIMIT))
    return rows[start:start + size]


# =============================================================================
# Page / mode normalization
# =============================================================================
def _normalize_page_name(page: Any) -> str:
    raw = _s(page)
    if not raw:
        return ""

    try:
        mod = importlib.import_module("core.sheets.page_catalog")
        for fn_name in ("normalize_page_name", "normalize_page", "resolve_page_name"):
            fn = getattr(mod, fn_name, None)
            if callable(fn):
                out = fn(raw)
                out_s = _s(out)
                if out_s:
                    return out_s
    except Exception:
        pass

    direct = PAGE_ALIASES.get(raw.lower())
    if direct:
        return direct

    if raw in ALL_KNOWN_PAGES:
        return raw

    compact = raw.replace(" ", "_")
    compact = PAGE_ALIASES.get(compact.lower(), compact)
    return compact or raw


def _normalize_mode(value: Any) -> str:
    mode = _s(value).lower()
    if not mode or mode == "auto":
        return "live_quotes"
    if mode in {"live", "quotes", "live_quotes", "live-quotes"}:
        return "live_quotes"
    if mode in {"sheet", "rows", "live_sheet", "live-sheet", "sheet_rows", "sheet-rows"}:
        return "live_sheet"
    if mode in {"snapshot", "snapshots"}:
        return "snapshot"
    return mode


def _default_mode_from_env() -> str:
    for env_name in (
        "ADVISOR_DATA_MODE",
        "AdvisorDataMode",
        "INVESTMENT_ADVISOR_MODE",
        "TFB_ADVISOR_MODE",
    ):
        value = os.getenv(env_name)
        if _s(value):
            return _normalize_mode(value)
    return "live_quotes"


# =============================================================================
# Schema helpers
# =============================================================================
def _extract_headers_from_spec(spec: Any) -> List[str]:
    if spec is None:
        return []

    if isinstance(spec, Mapping):
        for key in ("headers", "display_headers", "columns"):
            value = spec.get(key)
            if isinstance(value, list) and value:
                if key == "columns":
                    headers = []
                    for col in value:
                        if isinstance(col, Mapping):
                            headers.append(
                                _s(col.get("display_header") or col.get("header") or col.get("name") or col.get("key"))
                            )
                        else:
                            headers.append(_s(col))
                    headers = [h for h in headers if h]
                    if headers:
                        return headers
                else:
                    headers = [_s(v) for v in value if _s(v)]
                    if headers:
                        return headers
    return []


async def _load_headers_for_page(page: str) -> List[str]:
    normalized = _normalize_page_name(page) or DEFAULT_PAGE

    for module_name in SCHEMA_MODULE_CANDIDATES:
        try:
            module = importlib.import_module(module_name)
        except Exception:
            continue

        for fn_name in SCHEMA_FUNCTION_CANDIDATES:
            fn = getattr(module, fn_name, None)
            if not callable(fn):
                continue
            try:
                out = fn(normalized)
                if inspect.isawaitable(out):
                    out = await out
                headers = _extract_headers_from_spec(out)
                if headers:
                    return headers
            except TypeError:
                try:
                    out = fn(page=normalized)
                    if inspect.isawaitable(out):
                        out = await out
                    headers = _extract_headers_from_spec(out)
                    if headers:
                        return headers
                except Exception:
                    continue
            except Exception:
                continue

    if normalized == "Insights_Analysis":
        return list(_INSIGHTS_HEADERS)
    if normalized == "Data_Dictionary":
        return list(_DICTIONARY_HEADERS)
    if normalized == "Top_10_Investments":
        base = list(GENERIC_FALLBACK_HEADERS)
        return base + [h for h in _TOP10_EXTRA_HEADERS if h not in base]
    return list(GENERIC_FALLBACK_HEADERS)


def _headers_to_keys(headers: List[str]) -> List[str]:
    keys: List[str] = []
    seen = set()
    for idx, header in enumerate(headers):
        k = _normalize_key(header) or f"col_{idx + 1}"
        base = k
        n = 2
        while k in seen:
            k = f"{base}_{n}"
            n += 1
        seen.add(k)
        keys.append(k)
    return keys


# =============================================================================
# Payload normalization
# =============================================================================
def _merge_payloads(*candidates: Any) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    for candidate in candidates:
        if candidate is None:
            continue
        if isinstance(candidate, Mapping):
            merged.update(dict(candidate))
            continue
        if isinstance(candidate, str):
            txt = candidate.strip()
            if not txt:
                continue
            try:
                parsed = json.loads(txt)
                if isinstance(parsed, Mapping):
                    merged.update(dict(parsed))
                    continue
            except Exception:
                pass
        if hasattr(candidate, "model_dump") and callable(getattr(candidate, "model_dump")):
            try:
                dumped = candidate.model_dump(mode="python")
                if isinstance(dumped, Mapping):
                    merged.update(dict(dumped))
                    continue
            except Exception:
                pass
        if is_dataclass(candidate):
            try:
                dumped = asdict(candidate)
                if isinstance(dumped, Mapping):
                    merged.update(dict(dumped))
                    continue
            except Exception:
                pass
    return merged


def _normalize_payload(*, request: Any = None, body: Any = None, payload: Any = None, mode: Any = None, **kwargs: Any) -> Dict[str, Any]:
    out = _merge_payloads(payload, body, kwargs)

    try:
        request_state = getattr(request, "state", None)
        request_id = _s(getattr(request_state, "request_id", ""))
        if request_id and not out.get("request_id"):
            out["request_id"] = request_id
    except Exception:
        pass

    page = (
        out.get("page")
        or out.get("sheet")
        or out.get("sheet_name")
        or out.get("name")
        or out.get("tab")
        or DEFAULT_PAGE
    )
    normalized_page = _normalize_page_name(page) or DEFAULT_PAGE
    out["page"] = normalized_page
    out["sheet"] = normalized_page
    out["sheet_name"] = normalized_page

    symbol_values = (
        out.get("symbols")
        or out.get("tickers")
        or out.get("symbol")
        or out.get("ticker")
        or out.get("direct_symbols")
    )
    symbols = _normalize_list(symbol_values)
    if symbols:
        out["symbols"] = symbols
        out["tickers"] = list(symbols)

    effective_mode = _normalize_mode(
        mode
        or out.get("advisor_data_mode")
        or out.get("data_mode")
        or out.get("advisor_mode")
        or out.get("mode")
        or _default_mode_from_env()
    )
    out["mode"] = effective_mode
    out["advisor_mode"] = effective_mode
    out["data_mode"] = effective_mode
    out["advisor_data_mode"] = effective_mode

    limit = _safe_int(out.get("limit") or out.get("top_n") or DEFAULT_LIMIT, DEFAULT_LIMIT)
    limit = max(1, min(200, limit))
    offset = _safe_int(out.get("offset") or DEFAULT_OFFSET, DEFAULT_OFFSET)
    offset = max(0, offset)

    out["limit"] = limit
    out["top_n"] = limit
    out["offset"] = offset

    out["include_matrix"] = _coerce_bool(out.get("include_matrix"), True)
    out["include_headers"] = _coerce_bool(out.get("include_headers"), True)
    out.setdefault("format", "rows")
    return out


# =============================================================================
# Tolerant callable execution
# =============================================================================
async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _signature_typeerror_is_retryable(exc: TypeError) -> bool:
    msg = _s(exc).lower()
    retry_markers = (
        "unexpected keyword argument",
        "got an unexpected keyword argument",
        "takes ",
        "positional argument",
        "required positional argument",
        "missing 1 required positional argument",
        "too many positional arguments",
    )
    return any(marker in msg for marker in retry_markers)


async def _call_tolerant(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    attempts = [
        (args, kwargs),
        ((), kwargs),
        ((), {"request": kwargs.get("request"), "payload": kwargs.get("payload"), "body": kwargs.get("body"), "mode": kwargs.get("mode")}),
        ((), {"payload": kwargs.get("payload"), "mode": kwargs.get("mode")}),
        ((), {"body": kwargs.get("body"), "mode": kwargs.get("mode")}),
        ((), {"payload": kwargs.get("payload")}),
        ((), {"body": kwargs.get("body")}),
        ((), {}),
    ]

    last_error: Optional[Exception] = None
    for call_args, call_kwargs in attempts:
        call_kwargs = {k: v for k, v in call_kwargs.items() if v is not None}
        try:
            out = fn(*call_args, **call_kwargs)
            return await _maybe_await(out)
        except TypeError as exc:
            last_error = exc
            if _signature_typeerror_is_retryable(exc):
                continue
            raise
        except Exception:
            raise
    if last_error is not None:
        raise last_error
    return None


def _run_sync(awaitable: Any) -> Any:
    if not inspect.isawaitable(awaitable):
        return awaitable

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(awaitable)

    holder: Dict[str, Any] = {}

    def _runner() -> None:
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            holder["result"] = loop.run_until_complete(awaitable)
        except Exception as exc:  # pragma: no cover - defensive
            holder["error"] = exc
        finally:
            try:
                loop.close()
            except Exception:
                pass
            asyncio.set_event_loop(None)

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    thread.join()

    if "error" in holder:
        raise holder["error"]
    return holder.get("result")


# =============================================================================
# Snapshot cache helpers
# =============================================================================
def _snapshot_key(page: str, mode: str, criteria: Optional[Mapping[str, Any]] = None) -> str:
    normalized_page = _normalize_page_name(page) or DEFAULT_PAGE
    normalized_mode = _normalize_mode(mode)
    if not isinstance(criteria, Mapping):
        return f"{normalized_page}::{normalized_mode}"
    return f"{normalized_page}::{normalized_mode}::{_criteria_fingerprint(criteria)}"


def _snapshot_get(page: str, mode: str, criteria: Optional[Mapping[str, Any]] = None, ttl_sec: int = DEFAULT_SNAPSHOT_TTL_SEC) -> Optional[Dict[str, Any]]:
    key = _snapshot_key(page, mode, criteria=criteria)
    now = time.time()
    with _SNAPSHOT_LOCK:
        entry = _SNAPSHOT_STORE.get(key)
        if not entry:
            return None
        created = _safe_float(entry.get("ts"), 0.0)
        if ttl_sec > 0 and created > 0 and (now - created) > ttl_sec:
            _SNAPSHOT_STORE.pop(key, None)
            return None
        payload = entry.get("payload")
        if isinstance(payload, Mapping):
            return _deepcopy_jsonable(dict(payload))
        return None


def _snapshot_put(page: str, mode: str, criteria: Optional[Mapping[str, Any]], payload: Dict[str, Any]) -> None:
    if not isinstance(payload, Mapping):
        return
    key = _snapshot_key(page, mode, criteria=criteria)
    with _SNAPSHOT_LOCK:
        _SNAPSHOT_STORE[key] = {
            "ts": time.time(),
            "payload": _deepcopy_jsonable(dict(payload)),
        }


def _snapshot_summary() -> Dict[str, Any]:
    with _SNAPSHOT_LOCK:
        return {
            "entries": len(_SNAPSHOT_STORE),
            "keys": sorted(_SNAPSHOT_STORE.keys()),
        }


# =============================================================================
# Engine resolution + execution
# =============================================================================
def _resolve_callable_from_object(obj: Any) -> Optional[Callable[..., Any]]:
    if obj is None:
        return None
    if callable(obj):
        return obj
    for name in ENGINE_OBJECT_METHOD_CANDIDATES:
        fn = getattr(obj, name, None)
        if callable(fn):
            return fn
    return None


async def _resolve_engine_callable(request: Any = None) -> Tuple[Optional[Callable[..., Any]], Dict[str, Any]]:
    try:
        state = getattr(getattr(request, "app", None), "state", None)
    except Exception:
        state = None

    if state is not None:
        for name in ENGINE_FUNCTION_CANDIDATES:
            fn = getattr(state, name, None)
            if callable(fn):
                return fn, {"source": "app.state", "callable": name, "kind": "function"}
        for name in ENGINE_OBJECT_CANDIDATES:
            obj = getattr(state, name, None)
            fn = _resolve_callable_from_object(obj)
            if callable(fn):
                return fn, {"source": "app.state", "object": name, "callable": getattr(fn, "__name__", "callable"), "kind": "object_method"}

    for module_name in ENGINE_MODULE_CANDIDATES:
        try:
            module = importlib.import_module(module_name)
        except Exception:
            continue
        if module_name == __name__:
            continue
        for name in ENGINE_FUNCTION_CANDIDATES:
            fn = getattr(module, name, None)
            if callable(fn):
                return fn, {"source": module_name, "callable": name, "kind": "function"}
        for name in ENGINE_OBJECT_CANDIDATES:
            obj = getattr(module, name, None)
            fn = _resolve_callable_from_object(obj)
            if callable(fn):
                return fn, {"source": module_name, "object": name, "callable": getattr(fn, "__name__", "callable"), "kind": "object_method"}
    return None, {}


async def _execute_engine(request: Any, criteria: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    fn, meta = await _resolve_engine_callable(request)
    if not callable(fn):
        return None, meta
    try:
        out = await _call_tolerant(
            fn,
            request=request,
            payload=criteria,
            body=criteria,
            mode=criteria.get("advisor_data_mode") or criteria.get("mode"),
        )
        if isinstance(out, Mapping):
            return dict(out), meta
        if out is None:
            return None, meta
        return {"status": "success", "data": _jsonable(out)}, meta
    except Exception as exc:
        logger.warning("Investment advisor engine call failed: %s", exc, exc_info=True)
        return {
            "status": "error",
            "error": str(exc),
            "message": "Investment advisor engine execution failed",
        }, meta


# =============================================================================
# Result normalization + recommendation backfill
# =============================================================================
def _pick_first_mapping(result: Mapping[str, Any], *keys: str) -> Optional[Mapping[str, Any]]:
    for key in keys:
        value = result.get(key)
        if isinstance(value, Mapping):
            return value
    return None


def _normalize_rows(result: Mapping[str, Any], keys: List[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for key in ("row_objects", "rowObjects", "rows", "items", "records", "data", "quotes", "recommendations"):
        value = result.get(key)
        if isinstance(value, list) and value:
            if isinstance(value[0], Mapping):
                rows = [dict(v) for v in value if isinstance(v, Mapping)]
                break
            if isinstance(value[0], (list, tuple)):
                matrix_rows = []
                for row in value:
                    if isinstance(row, (list, tuple)):
                        matrix_rows.append({keys[idx]: row[idx] if idx < len(row) else None for idx in range(len(keys))})
                rows = matrix_rows
                break

    if rows:
        return rows

    for key in ("rows_matrix", "matrix"):
        value = result.get(key)
        if isinstance(value, list) and value and isinstance(value[0], (list, tuple)):
            matrix_rows = []
            for row in value:
                if isinstance(row, (list, tuple)):
                    matrix_rows.append({keys[idx]: row[idx] if idx < len(row) else None for idx in range(len(keys))})
            if matrix_rows:
                return matrix_rows

    nested = _pick_first_mapping(result, "payload", "result", "response")
    if nested:
        return _normalize_rows(dict(nested), keys)

    return []


def _normalize_headers_keys(result: Mapping[str, Any], page: str) -> Tuple[List[str], List[str], List[str]]:
    headers = [
        _s(v)
        for v in (result.get("display_headers") or result.get("headers") or [])
        if _s(v)
    ]
    keys = [_normalize_key(v) for v in (result.get("keys") or []) if _s(v)]

    if not headers and keys:
        headers = [_title_case_header(k) for k in keys]
    if headers and not keys:
        keys = _headers_to_keys(headers)

    if not headers or not keys or len(headers) != len(keys):
        fallback_headers = _run_sync(_load_headers_for_page(page))
        fallback_keys = _headers_to_keys(fallback_headers)
        if not headers:
            headers = fallback_headers
        if not keys:
            keys = fallback_keys
        if len(headers) != len(keys):
            headers = fallback_headers
            keys = fallback_keys

    display_headers = list(headers)
    return headers, display_headers, keys


def _score_recommendation(row: Mapping[str, Any]) -> Tuple[str, str, float]:
    opportunity = _safe_float(row.get("opportunity_score") or row.get("overall_score") or row.get("score"), 0.0)
    overall = _safe_float(row.get("overall_score"), opportunity)
    risk = _safe_float(row.get("risk_score"), 50.0)
    expected = _safe_float(
        row.get("expected_roi_3m")
        or row.get("expected_roi_1m")
        or row.get("expected_roi")
        or row.get("forecast_return_pct"),
        0.0,
    )

    composite = overall + (0.35 * opportunity) + (0.20 * expected) - (0.25 * risk)

    if composite >= 70:
        return "Strong Buy", "High score / attractive upside", composite
    if composite >= 55:
        return "Buy", "Favorable score / acceptable risk", composite
    if composite >= 45:
        return "Hold", "Balanced score / wait for confirmation", composite
    if composite >= 30:
        return "Reduce", "Weak score / elevated risk", composite
    return "Avoid", "Low score / unfavorable risk-reward", composite


def _risk_bucket_from_row(row: MutableMapping[str, Any]) -> str:
    existing = _s(row.get("risk_bucket"))
    if existing:
        return existing
    risk = _safe_float(row.get("risk_score"), math.nan)
    if math.isnan(risk):
        return "Moderate"
    if risk < 35:
        return "Low"
    if risk < 65:
        return "Moderate"
    return "High"


def _confidence_bucket_from_row(row: MutableMapping[str, Any]) -> str:
    existing = _s(row.get("confidence_bucket"))
    if existing:
        return existing
    score = _safe_float(row.get("overall_score") or row.get("opportunity_score"), math.nan)
    if math.isnan(score):
        return "Medium"
    if score >= 75:
        return "High"
    if score >= 50:
        return "Medium"
    return "Low"


def _ensure_top10_fields(rows: List[Dict[str, Any]], criteria: Dict[str, Any]) -> None:
    crit_txt = _json_compact(criteria)
    for idx, row in enumerate(rows, start=1):
        if _is_blank(row.get("top10_rank")):
            row["top10_rank"] = idx
        if _is_blank(row.get("selection_reason")):
            reco = _s(row.get("recommendation")) or "Candidate"
            row["selection_reason"] = f"{reco} based on live advisor scoring"
        if _is_blank(row.get("criteria_snapshot")):
            row["criteria_snapshot"] = crit_txt


def _backfill_rows(rows: List[Dict[str, Any]], page: str, criteria: Dict[str, Any]) -> List[Dict[str, Any]]:
    page = _normalize_page_name(page) or DEFAULT_PAGE
    if page == "Insights_Analysis":
        return rows
    if page == "Data_Dictionary":
        return rows

    for row in rows:
        reco = _s(row.get("recommendation"))
        if not reco:
            reco, reason, composite = _score_recommendation(row)
            row["recommendation"] = reco
            row.setdefault("selection_reason", reason)
            if _is_blank(row.get("overall_score")) and not _is_blank(composite):
                row["overall_score"] = round(composite, 2)
        row.setdefault("risk_bucket", _risk_bucket_from_row(row))
        row.setdefault("confidence_bucket", _confidence_bucket_from_row(row))

    if page == "Top_10_Investments":
        _ensure_top10_fields(rows, criteria)
    return rows


def _ensure_rows_cover_keys(rows: List[Dict[str, Any]], keys: List[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        normalized: Dict[str, Any] = {k: None for k in keys}
        for key in keys:
            if key in row:
                normalized[key] = row.get(key)
                continue
            header_style = _title_case_header(key)
            if header_style in row:
                normalized[key] = row.get(header_style)
                continue
            compact_key = key.replace("_", " ")
            for source_key, value in row.items():
                if _normalize_key(source_key) == key or _s(source_key).lower() == compact_key:
                    normalized[key] = value
                    break
        for source_key, value in row.items():
            nk = _normalize_key(source_key)
            if nk in normalized and normalized[nk] is None:
                normalized[nk] = value
        out.append(normalized)
    return out


def _build_special_fallback(page: str, criteria: Dict[str, Any]) -> Dict[str, Any]:
    page = _normalize_page_name(page) or DEFAULT_PAGE
    now = _now_utc()

    if page == "Insights_Analysis":
        symbols = criteria.get("symbols") or criteria.get("tickers") or []
        rows = [
            {
                "section": "Summary",
                "item": "Mode",
                "metric": "advisor_data_mode",
                "value": criteria.get("advisor_data_mode") or criteria.get("mode") or "live_quotes",
                "notes": "Fallback insight generated by advisor wrapper",
                "source": "core.investment_advisor",
                "updated_at": now,
            },
            {
                "section": "Summary",
                "item": "Universe",
                "metric": "symbols_count",
                "value": len(symbols) if isinstance(symbols, list) else 0,
                "notes": "Symbol count from request payload",
                "source": "core.investment_advisor",
                "updated_at": now,
            },
        ]
        headers = list(_INSIGHTS_HEADERS)
        keys = _headers_to_keys(headers)
        rows = _ensure_rows_cover_keys(rows, keys)
        return {
            "status": "warn",
            "page": page,
            "sheet": page,
            "sheet_name": page,
            "headers": headers,
            "display_headers": headers,
            "keys": keys,
            "rows": rows,
            "row_objects": rows,
            "rows_matrix": _rows_to_matrix(rows, keys),
            "meta": {
                "source": "core.investment_advisor",
                "fallback": True,
                "reason": "engine_unavailable_or_empty",
                "advisor_data_mode_effective": criteria.get("advisor_data_mode"),
            },
        }

    if page == "Data_Dictionary":
        headers = list(_DICTIONARY_HEADERS)
        keys = _headers_to_keys(headers)
        rows = [
            {
                "sheet": criteria.get("page") or DEFAULT_PAGE,
                "column_key": "recommendation",
                "display_header": "Recommendation",
                "section": "Advisor",
                "type": "string",
                "description": "Advisor recommendation label",
                "example": "Buy",
                "required": "No",
                "notes": "Fallback dictionary row generated by advisor wrapper",
            }
        ]
        rows = _ensure_rows_cover_keys(rows, keys)
        return {
            "status": "warn",
            "page": page,
            "sheet": page,
            "sheet_name": page,
            "headers": headers,
            "display_headers": headers,
            "keys": keys,
            "rows": rows,
            "row_objects": rows,
            "rows_matrix": _rows_to_matrix(rows, keys),
            "meta": {
                "source": "core.investment_advisor",
                "fallback": True,
                "reason": "engine_unavailable_or_empty",
            },
        }

    headers = _run_sync(_load_headers_for_page(page))
    keys = _headers_to_keys(headers)
    rows: List[Dict[str, Any]] = []
    symbols = _normalize_list(criteria.get("symbols") or criteria.get("tickers"))
    for idx, symbol in enumerate(symbols[: criteria.get("top_n", DEFAULT_LIMIT)], start=1):
        row = {k: None for k in keys}
        if "symbol" in row:
            row["symbol"] = symbol
        if "name" in row:
            row["name"] = symbol
        if "recommendation" in row:
            row["recommendation"] = "Hold"
        if "top10_rank" in row:
            row["top10_rank"] = idx
        if "selection_reason" in row:
            row["selection_reason"] = "Fallback candidate from supplied symbols"
        if "criteria_snapshot" in row:
            row["criteria_snapshot"] = _json_compact(criteria)
        rows.append(row)
    rows = _backfill_rows(rows, page, criteria)
    rows = _ensure_rows_cover_keys(rows, keys)
    rows = _slice_rows(rows, criteria.get("offset", DEFAULT_OFFSET), criteria.get("limit", DEFAULT_LIMIT))
    return {
        "status": "warn",
        "page": page,
        "sheet": page,
        "sheet_name": page,
        "headers": headers,
        "display_headers": headers,
        "keys": keys,
        "rows": rows,
        "row_objects": rows,
        "rows_matrix": _rows_to_matrix(rows, keys),
        "meta": {
            "source": "core.investment_advisor",
            "fallback": True,
            "reason": "engine_unavailable_or_empty",
            "advisor_data_mode_effective": criteria.get("advisor_data_mode"),
        },
    }


def _normalize_engine_result(result: Optional[Mapping[str, Any]], criteria: Dict[str, Any], resolver_meta: Dict[str, Any]) -> Dict[str, Any]:
    page = _normalize_page_name(criteria.get("page")) or DEFAULT_PAGE
    if not isinstance(result, Mapping) or not result:
        return _build_special_fallback(page, criteria)

    headers, display_headers, keys = _normalize_headers_keys(result, page)
    rows = _normalize_rows(result, keys)

    nested_meta = {}
    raw_meta = result.get("meta")
    if isinstance(raw_meta, Mapping):
        nested_meta = dict(raw_meta)

    if not rows and page in SPECIAL_PAGES:
        fallback = _build_special_fallback(page, criteria)
        fallback_meta = dict(fallback.get("meta") or {})
        fallback_meta.update({"engine_resolver": resolver_meta or None})
        fallback["meta"] = fallback_meta
        return fallback

    rows = _backfill_rows(rows, page, criteria)
    rows = _ensure_rows_cover_keys(rows, keys)

    total_rows_before_slice = len(rows)
    offset = criteria.get("offset", DEFAULT_OFFSET)
    limit = criteria.get("limit", DEFAULT_LIMIT)
    rows = _slice_rows(rows, offset, limit)

    out: Dict[str, Any] = {
        "status": _s(result.get("status")) or ("success" if rows else "warn"),
        "page": page,
        "sheet": page,
        "sheet_name": page,
        "route_family": _s(result.get("route_family")) or "advisor",
        "headers": headers,
        "display_headers": display_headers,
        "keys": keys,
        "rows": rows,
        "row_objects": rows,
        "rows_matrix": _rows_to_matrix(rows, keys),
        "data": rows,
        "items": rows,
        "records": rows,
        "quotes": rows if page in BASE_SOURCE_PAGES else [],
        "recommendations": rows if page == "Top_10_Investments" else [],
        "meta": {
            **nested_meta,
            "source": nested_meta.get("source") or "core.investment_advisor",
            "resolver": resolver_meta or None,
            "page": page,
            "advisor_data_mode_effective": criteria.get("advisor_data_mode"),
            "normalized_by": "core.investment_advisor",
            "timestamp_utc": _now_utc(),
            "offset": max(0, _safe_int(offset, DEFAULT_OFFSET)),
            "limit": max(1, _safe_int(limit, DEFAULT_LIMIT)),
            "rows_before_local_slice": total_rows_before_slice,
            "rows_after_local_slice": len(rows),
        },
    }

    for passthrough_key in (
        "message",
        "error",
        "warnings",
        "criteria",
        "criteria_snapshot",
        "summary",
        "stats",
    ):
        if passthrough_key in result and passthrough_key not in out:
            out[passthrough_key] = _jsonable(result.get(passthrough_key))

    return _jsonable(out)


# =============================================================================
# Async implementation
# =============================================================================
async def _run_investment_advisor_async(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    request = kwargs.get("request")
    body = kwargs.get("body")
    payload = kwargs.get("payload")
    mode = kwargs.get("mode")

    passthrough_kwargs = dict(kwargs)
    for reserved in ("request", "body", "payload", "mode"):
        passthrough_kwargs.pop(reserved, None)

    criteria = _normalize_payload(request=request, body=body, payload=payload, mode=mode, **passthrough_kwargs)
    page = criteria["page"]
    effective_mode = criteria["advisor_data_mode"]
    ttl_sec = max(
        0,
        _safe_int(
            criteria.get("snapshot_ttl")
            or os.getenv("ADVISOR_SNAPSHOT_TTL_SEC")
            or DEFAULT_SNAPSHOT_TTL_SEC,
            DEFAULT_SNAPSHOT_TTL_SEC,
        ),
    )

    if effective_mode == "snapshot":
        cached = _snapshot_get(page, effective_mode, criteria=criteria, ttl_sec=ttl_sec)
        if cached:
            meta = dict(cached.get("meta") or {})
            meta.update({
                "snapshot_hit": True,
                "advisor_data_mode_effective": effective_mode,
                "source": meta.get("source") or "core.investment_advisor.snapshot",
                "timestamp_utc": _now_utc(),
                "snapshot_key": _snapshot_key(page, effective_mode, criteria=criteria),
            })
            cached["meta"] = meta
            cached["status"] = _s(cached.get("status")) or "success"
            return _jsonable(cached)

    engine_result, resolver_meta = await _execute_engine(request, criteria)
    normalized = _normalize_engine_result(engine_result, criteria, resolver_meta)

    if normalized.get("rows"):
        _snapshot_put(page, effective_mode, criteria, normalized)
        if effective_mode != "snapshot":
            _snapshot_put(page, "snapshot", criteria, normalized)

    return normalized


# =============================================================================
# Public service class
# =============================================================================
class InvestmentAdvisor:
    """Compatibility service object for route-level resolution."""

    version = INVESTMENT_ADVISOR_VERSION

    def __call__(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.run_investment_advisor(*args, **kwargs)

    def run_investment_advisor(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return _run_sync(_run_investment_advisor_async(*args, **kwargs))

    def run_advisor(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.run_investment_advisor(*args, **kwargs)

    def execute_investment_advisor(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.run_investment_advisor(*args, **kwargs)

    def execute_advisor(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.run_investment_advisor(*args, **kwargs)

    def recommend(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.run_investment_advisor(*args, **kwargs)

    def recommend_investments(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.run_investment_advisor(*args, **kwargs)

    def get_recommendations(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.run_investment_advisor(*args, **kwargs)

    def build_recommendations(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.run_investment_advisor(*args, **kwargs)

    def warm_cache(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        payload = _normalize_payload(payload=kwargs.get("payload") or kwargs.get("body") or kwargs, mode=kwargs.get("mode"))
        page = payload.get("page") or DEFAULT_PAGE
        out = self.run_investment_advisor(payload=payload, mode=payload.get("advisor_data_mode") or "live_quotes")
        warmed = bool(out.get("rows"))
        return {
            "status": "ok" if warmed else "warn",
            "page": page,
            "warmed": warmed,
            "snapshot_summary": _snapshot_summary(),
            "advisor_data_mode_effective": payload.get("advisor_data_mode"),
        }

    def warm_snapshots(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        payload = _normalize_payload(payload=kwargs.get("payload") or kwargs.get("body") or kwargs, mode="snapshot")
        pages = _normalize_list(kwargs.get("pages") or payload.get("pages")) or [payload.get("page") or DEFAULT_PAGE]
        results = []
        for page in pages:
            local_payload = dict(payload)
            normalized_page = _normalize_page_name(page) or DEFAULT_PAGE
            local_payload.update({"page": normalized_page, "sheet": normalized_page, "sheet_name": normalized_page})
            result = self.run_investment_advisor(payload=local_payload, mode="live_quotes")
            if result.get("rows"):
                _snapshot_put(normalized_page, "snapshot", local_payload, result)
            results.append({
                "page": normalized_page,
                "rows": len(result.get("rows") or []),
                "status": result.get("status") or "success",
            })
        return {
            "status": "ok",
            "warmed": any(r.get("rows", 0) > 0 for r in results),
            "results": results,
            "snapshot_summary": _snapshot_summary(),
        }

    def preload_snapshots(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.warm_snapshots(*args, **kwargs)

    def build_snapshot_cache(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.warm_snapshots(*args, **kwargs)


# =============================================================================
# Factories + singleton exports
# =============================================================================
_SINGLETON = InvestmentAdvisor()

advisor = _SINGLETON
investment_advisor = _SINGLETON
advisor_service = _SINGLETON
investment_advisor_service = _SINGLETON
advisor_runner = _SINGLETON
investment_advisor_runner = _SINGLETON


def create_investment_advisor(*args: Any, **kwargs: Any) -> InvestmentAdvisor:
    return _SINGLETON


def get_investment_advisor(*args: Any, **kwargs: Any) -> InvestmentAdvisor:
    return _SINGLETON


def build_investment_advisor(*args: Any, **kwargs: Any) -> InvestmentAdvisor:
    return _SINGLETON


def create_advisor(*args: Any, **kwargs: Any) -> InvestmentAdvisor:
    return _SINGLETON


def get_advisor(*args: Any, **kwargs: Any) -> InvestmentAdvisor:
    return _SINGLETON


def build_advisor(*args: Any, **kwargs: Any) -> InvestmentAdvisor:
    return _SINGLETON


# =============================================================================
# Direct function exports expected by routes
# =============================================================================
async def _run_investment_advisor_impl(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _run_investment_advisor_async(*args, **kwargs)


async def _run_advisor_impl(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _run_investment_advisor_async(*args, **kwargs)


def run_investment_advisor(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return _SINGLETON.run_investment_advisor(*args, **kwargs)


def run_advisor(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return _SINGLETON.run_advisor(*args, **kwargs)


def execute_investment_advisor(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return _SINGLETON.execute_investment_advisor(*args, **kwargs)


def execute_advisor(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return _SINGLETON.execute_advisor(*args, **kwargs)


def recommend(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return _SINGLETON.recommend(*args, **kwargs)


def recommend_investments(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return _SINGLETON.recommend_investments(*args, **kwargs)


def get_recommendations(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return _SINGLETON.get_recommendations(*args, **kwargs)


def build_recommendations(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return _SINGLETON.build_recommendations(*args, **kwargs)


__all__ = [
    "INVESTMENT_ADVISOR_VERSION",
    "InvestmentAdvisor",
    "advisor",
    "investment_advisor",
    "advisor_service",
    "investment_advisor_service",
    "advisor_runner",
    "investment_advisor_runner",
    "create_investment_advisor",
    "get_investment_advisor",
    "build_investment_advisor",
    "create_advisor",
    "get_advisor",
    "build_advisor",
    "_run_investment_advisor_impl",
    "_run_advisor_impl",
    "run_investment_advisor",
    "run_advisor",
    "execute_investment_advisor",
    "execute_advisor",
    "recommend",
    "recommend_investments",
    "get_recommendations",
    "build_recommendations",
]
