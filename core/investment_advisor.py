#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/investment_advisor.py
================================================================================
INVESTMENT ADVISOR ORCHESTRATOR -- v4.5.0
================================================================================
LIVE-BY-DEFAULT • ENGINE-FIRST • SNAPSHOT-TOLERANT • ROUTE-COMPATIBLE
MODE-AWARE • SCHEMA-SAFE • JSON-SAFE • IMPORT-SAFE • WORKER-THREAD SAFE
SPECIAL-PAGE SAFE • CONTRACT-PRESERVING • DICTIONARY-HARDENED

What this revision fixes
------------------------
- FIX: keeps this module as an orchestration wrapper over the engine rather
         than becoming a second independent engine.
- FIX: supports aligned engine output shapes including:
         - row_objects / rowObjects
         - rows_matrix / matrix
         - nested payload / result / response payloads
- FIX: honors and normalizes offset locally, not only limit/top_n.
- FIX: hardens tolerant engine calling so only signature-mismatch TypeErrors
         trigger fallback retries; runtime exceptions now surface correctly.
- FIX: prevents snapshot cross-talk by keying cache with request shape
         (page + mode + symbols + limit + offset), not page alone.
- FIX: preserves stable JSON-safe envelopes and fallback recommendation logic.
- FIX: preserves exact canonical contracts for Top10 / Insights / Data Dictionary.
- FIX: uses real Data Dictionary builder rows when engine output is empty.
- FIX: reconstructs rows from nested payload + matrix payloads before fallback.
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
import re
import threading
import time
from copy import deepcopy
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

logger = logging.getLogger("core.investment_advisor")
logger.addHandler(logging.NullHandler())

INVESTMENT_ADVISOR_VERSION = "4.5.0"
try:
    from core.reco_normalize import (  # type: ignore
        RECO_STRONG_BUY,
        RECO_BUY,
        RECO_HOLD,
        RECO_REDUCE,
        RECO_SELL,
    )
except Exception:
    RECO_STRONG_BUY = "STRONG_BUY"
    RECO_BUY        = "BUY"
    RECO_HOLD       = "HOLD"
    RECO_REDUCE     = "REDUCE"
    RECO_SELL       = "SELL"
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

# FIX v4.5.0: added "schema_registry" and "page_catalog" repo-root fallbacks
SCHEMA_MODULE_CANDIDATES = (
    "core.sheets.schema_registry",
    "core.sheets.page_catalog",
    "core.schema_registry",
    "core.schemas",
    "schema_registry",   # repo-root fallback
    "page_catalog",      # repo-root fallback
)
SCHEMA_FUNCTION_CANDIDATES = (
    "get_sheet_spec",
    "get_page_spec",
    "get_schema_for_page",
    "sheet_spec",
    "build_sheet_spec",
)

_GENERIC_FALLBACK_HEADERS = [
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

# FIX v4.5.0: "Updated At" -> "Sort Order" (canonical Insights_Analysis schema key: sort_order)
_INSIGHTS_HEADERS = ["Section", "Item", "Metric", "Value", "Notes", "Source", "Sort Order"]
# FIX v4.5.0: aligned to canonical Data_Dictionary schema (schema_registry v3.0.0).
# Previous: Column Key, Display Header, Section, Type, Description, Example
# Canonical: Group, Header, Key, DType, Format (matches data_dictionary.py v3.3.0)
_DICTIONARY_HEADERS = [
    "Sheet",
    "Group",
    "Header",
    "Key",
    "DType",
    "Format",
    "Required",
    "Source",
    "Notes",
]
_TOP10_EXTRA_KEYS = ["top10_rank", "selection_reason", "criteria_snapshot"]
_TOP10_EXTRA_HEADERS = ["Top10 Rank", "Selection Reason", "Criteria Snapshot"]

_SNAPSHOT_LOCK = threading.RLock()
_SNAPSHOT_STORE: Dict[str, Dict[str, Any]] = {}

_FIELD_ALIAS_HINTS: Dict[str, List[str]] = {
    "symbol": ["ticker", "code", "requested_symbol"],
    "ticker": ["symbol", "code", "requested_symbol"],
    "name": ["company_name", "long_name", "instrument_name", "security_name", "title"],
    "current_price": ["price", "last_price", "last", "close", "market_price", "nav"],
    "price_change": ["change", "net_change"],
    "percent_change": ["change_pct", "change_percent", "pct_change"],
    "risk_score": ["risk", "riskscore"],
    "valuation_score": ["valuation", "valuationscore"],
    "overall_score": ["score", "overall", "totalscore"],
    "opportunity_score": ["opportunity", "opportunityscore"],
    "recommendation": ["signal", "rating", "action"],
    "selection_reason": ["reason", "recommendation_reason", "reco_reason"],
    "criteria_snapshot": ["criteria", "criteria_json", "snapshot"],
    "updated_at": ["updated_at_utc", "last_updated", "timestamp_utc"],
}


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
        out = float(value)
        return default if math.isnan(out) or math.isinf(out) else out
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


def _normalize_key_loose(key: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", _s(key).lower())


def _criteria_fingerprint(criteria: Mapping[str, Any]) -> str:
    payload = {
        "page": _normalize_page_name(criteria.get("page")) or DEFAULT_PAGE,
        "mode": _normalize_mode(criteria.get("advisor_data_mode") or criteria.get("mode")),
        "symbols": sorted(_normalize_list(criteria.get("symbols") or criteria.get("tickers"))),  # FIX v4.4.0
        "limit": max(1, _safe_int(criteria.get("limit") or criteria.get("top_n"), DEFAULT_LIMIT)),
        "offset": max(0, _safe_int(criteria.get("offset"), DEFAULT_OFFSET)),
    }
    encoded = _json_compact(payload)
    return hashlib.sha1(encoded.encode("utf-8")).hexdigest()


def _slice_rows(rows: List[Dict[str, Any]], offset: int, limit: int) -> List[Dict[str, Any]]:
    start = max(0, _safe_int(offset, DEFAULT_OFFSET))
    size = max(1, _safe_int(limit, DEFAULT_LIMIT))
    return rows[start:start + size]


def _row_value_for_aliases(row: Mapping[str, Any], aliases: Sequence[str]) -> Any:
    if not isinstance(row, Mapping):
        return None
    exact = {str(k): v for k, v in row.items()}
    lower = {str(k).lower(): v for k, v in row.items()}
    canon = {_normalize_key(str(k)): v for k, v in row.items()}
    loose = {_normalize_key_loose(str(k)): v for k, v in row.items()}

    expanded: List[str] = []
    seen = set()
    for alias in aliases:
        a = _s(alias)
        if not a:
            continue
        for candidate in [a, a.lower(), _normalize_key(a), _normalize_key_loose(a)] + _FIELD_ALIAS_HINTS.get(_normalize_key(a), []):
            c = _s(candidate)
            if c and c not in seen:
                seen.add(c)
                expanded.append(c)

    for alias in expanded:
        if alias in exact and exact[alias] is not None:
            return exact[alias]
        if alias.lower() in lower and lower[alias.lower()] is not None:
            return lower[alias.lower()]
        nk = _normalize_key(alias)
        if nk in canon and canon[nk] is not None:
            return canon[nk]
        nl = _normalize_key_loose(alias)
        if nl in loose and loose[nl] is not None:
            return loose[nl]
    return None


# =============================================================================
# Page / mode normalization
# =============================================================================
def _normalize_page_name(page: Any) -> str:
    raw = _s(page)
    if not raw:
        return ""

    # FIX v4.5.0: try multiple page_catalog paths (same pattern as config.py v5.9.0)
    for _pcat_mod_path in ("core.sheets.page_catalog", "core.page_catalog", "page_catalog"):
        try:
            mod = importlib.import_module(_pcat_mod_path)
        except Exception:
            continue
        for fn_name in ("normalize_page_name", "normalize_page", "resolve_page_name"):
            fn = getattr(mod, fn_name, None)
            if callable(fn):
                try:
                    out = fn(raw)
                    out_s = _s(out)
                    if out_s:
                        return out_s
                except Exception:
                    continue

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
        base = list(_GENERIC_FALLBACK_HEADERS)
        return base + [h for h in _TOP10_EXTRA_HEADERS if h not in base]
    return list(_GENERIC_FALLBACK_HEADERS)


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

    selected_pages = _normalize_list(out.get("source_pages") or out.get("pages_selected") or out.get("pages") or out.get("sources"))
    if selected_pages:
        out["pages_selected"] = [_normalize_page_name(p) for p in selected_pages]
        out["source_pages"] = [p for p in out["pages_selected"] if p in BASE_SOURCE_PAGES]

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


def _extract_payload_contract(result: Mapping[str, Any]) -> Tuple[List[str], List[str]]:
    headers = [_s(v) for v in (result.get("display_headers") or result.get("headers") or result.get("sheet_headers") or []) if _s(v)]
    keys = [_normalize_key(v) for v in (result.get("keys") or result.get("fields") or []) if _s(v)]
    if not headers and isinstance(result.get("columns"), list):
        for idx, col in enumerate(result.get("columns") or []):
            if isinstance(col, Mapping):
                key = _normalize_key(col.get("key") or col.get("field") or col.get("name"))
                header = _s(col.get("display_header") or col.get("header") or col.get("label") or col.get("title"))
            else:
                key = ""
                header = _s(col)
            if not key and header:
                key = f"column_{idx + 1}"
            if not header and key:
                header = _title_case_header(key)
            if header:
                headers.append(header)
            if key:
                keys.append(key)
    if headers and not keys:
        keys = _headers_to_keys(headers)
    if keys and not headers:
        headers = [_title_case_header(k) for k in keys]
    return headers, keys


def _matrix_rows_to_dicts(matrix: Any, keys: Sequence[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not isinstance(matrix, list):
        return out
    usable_keys = [str(k) for k in keys if _s(k)]
    if not usable_keys:
        return out
    for row in matrix:
        if isinstance(row, (list, tuple)):
            item = {usable_keys[idx]: row[idx] if idx < len(row) else None for idx in range(len(usable_keys))}
            out.append(item)
    return out


def _normalize_rows(result: Mapping[str, Any], keys: List[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for key in ("row_objects", "rowObjects", "rows", "items", "records", "data", "quotes", "recommendations"):
        value = result.get(key)
        if isinstance(value, list) and value:
            if isinstance(value[0], Mapping):
                rows = [dict(v) for v in value if isinstance(v, Mapping)]
                break
            if isinstance(value[0], (list, tuple)):
                return _matrix_rows_to_dicts(value, keys)

    if rows:
        return rows

    for key in ("rows_matrix", "matrix"):
        value = result.get(key)
        if isinstance(value, list) and value and isinstance(value[0], (list, tuple)):
            return _matrix_rows_to_dicts(value, keys)

    nested = _pick_first_mapping(result, "payload", "result", "response")
    if nested:
        nested_headers, nested_keys = _extract_payload_contract(dict(nested))
        return _normalize_rows(dict(nested), nested_keys or nested_headers or keys)

    return []


def _normalize_headers_keys(result: Mapping[str, Any], page: str) -> Tuple[List[str], List[str], List[str]]:
    page = _normalize_page_name(page) or DEFAULT_PAGE
    headers = [_s(v) for v in (result.get("display_headers") or result.get("headers") or []) if _s(v)]
    keys = [_normalize_key(v) for v in (result.get("keys") or []) if _s(v)]

    if not headers or not keys or len(headers) != len(keys):
        payload_headers, payload_keys = _extract_payload_contract(result)
        headers = headers or payload_headers
        keys = keys or payload_keys

    if not headers and keys:
        headers = [_title_case_header(k) for k in keys]
    if headers and not keys:
        keys = _headers_to_keys(headers)

    schema_headers = _run_sync(_load_headers_for_page(page))
    schema_keys = _headers_to_keys(schema_headers)

    if page in SPECIAL_PAGES or page == DEFAULT_PAGE:
        headers = list(schema_headers)
        keys = list(schema_keys)
    elif not headers or not keys or len(headers) != len(keys):
        headers = list(schema_headers)
        keys = list(schema_keys)

    if page == "Top_10_Investments":
        for extra_h, extra_k in zip(_TOP10_EXTRA_HEADERS, _TOP10_EXTRA_KEYS):
            if extra_h not in headers:
                headers.append(extra_h)
            if extra_k not in keys:
                keys.append(extra_k)

    display_headers = list(headers)
    return headers, display_headers, keys


def _score_recommendation(row: Mapping[str, Any]) -> Tuple[str, str, float]:
    opportunity = _safe_float(row.get("opportunity_score") or row.get("overall_score") or row.get("score"), 0.0)
    overall = _safe_float(row.get("overall_score"), opportunity)
    risk = _safe_float(row.get("risk_score"), 50.0)
    expected_raw = _safe_float(
        row.get("expected_roi_3m")
        or row.get("expected_roi_1m")
        or row.get("expected_roi")
        or row.get("forecast_return_pct"),
        0.0,
    )
    # FIX v4.5.0: expected_roi fields are dtype=pct stored as fractions (0.03 = 3%).
    # Without conversion the contribution is 0.20 * 0.03 = 0.006 (negligible).
    # Convert fraction -> pct-points (e.g. 0.03 -> 3.0) when value looks like a fraction.
    expected = expected_raw * 100.0 if abs(expected_raw) <= 1.5 and expected_raw != 0.0 else expected_raw

    composite = overall + (0.35 * opportunity) + (0.20 * expected) - (0.25 * risk)

    if composite >= 70:
        return RECO_STRONG_BUY, "High score / attractive upside", composite
    if composite >= 55:
        return RECO_BUY, "Favorable score / acceptable risk", composite
    if composite >= 45:
        return RECO_HOLD, "Balanced score / wait for confirmation", composite
    if composite >= 30:
        return RECO_REDUCE, "Weak score / elevated risk", composite
    return RECO_SELL, "Low score / unfavorable risk-reward", composite


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


def _ensure_rows_cover_keys(rows: List[Dict[str, Any]], keys: List[str], headers: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    headers = headers or []
    for row in rows:
        normalized: Dict[str, Any] = {k: None for k in keys}
        for idx, key in enumerate(keys):
            aliases = [key, key.replace("_", " "), key.replace("_", "-")]
            if idx < len(headers):
                aliases.extend([headers[idx], headers[idx].replace(" ", "_"), headers[idx].replace(" ", "-")])
            value = _row_value_for_aliases(row, aliases)
            normalized[key] = _jsonable(value)
        out.append(normalized)
    return out


def _build_data_dictionary_rows(criteria: Dict[str, Any]) -> List[Dict[str, Any]]:
    page = _normalize_page_name(criteria.get("page") or DEFAULT_PAGE)
    try:
        mod = importlib.import_module("core.sheets.data_dictionary")
        build_fn = getattr(mod, "build_data_dictionary_rows", None)
        if callable(build_fn):
            try:
                raw_rows = build_fn(include_meta_sheet=True)
            except TypeError:
                raw_rows = build_fn()
            out: List[Dict[str, Any]] = []
            for row in raw_rows if isinstance(raw_rows, list) else []:
                if isinstance(row, Mapping):
                    out.append(dict(row))
                else:
                    d = _jsonable(row)
                    if isinstance(d, Mapping):
                        out.append(dict(d))
            if out:
                return out
    except Exception:
        pass

    # FIX v4.5.0: use canonical Data_Dictionary keys (sheet, group, header, key, dtype,
    # fmt, required, source, notes) matching schema_registry v3.0.0 and data_dictionary.py v3.3.0.
    return [
        {
            "sheet":    criteria.get("page") or DEFAULT_PAGE,
            "group":    "Advisor",
            "header":   "Recommendation",
            "key":      "recommendation",
            "dtype":    "string",
            "fmt":      None,
            "required": False,
            "source":   "core.investment_advisor",
            "notes":    "Fallback dictionary row generated by advisor wrapper",
        }
    ]


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
                "sort_order": 1,  # FIX v4.5.0: was "updated_at" (non-canonical; canonical key is sort_order)
            },
            {
                "section": "Summary",
                "item": "Universe",
                "metric": "symbols_count",
                "value": len(symbols) if isinstance(symbols, list) else 0,
                "notes": "Symbol count from request payload",
                "source": "core.investment_advisor",
                "sort_order": 2,  # FIX v4.5.0: was "updated_at"
            },
        ]
        headers = list(_INSIGHTS_HEADERS)
        keys = _headers_to_keys(headers)
        rows = _ensure_rows_cover_keys(rows, keys, headers)
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
        rows = _ensure_rows_cover_keys(_build_data_dictionary_rows(criteria), keys, headers)
        return {
            "status": "warn" if rows else "error",
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
            row["recommendation"] = RECO_HOLD  # FIX v4.4.0: was "Hold" (non-canonical)
        if "top10_rank" in row:
            row["top10_rank"] = idx
        if "selection_reason" in row:
            row["selection_reason"] = "Fallback candidate from supplied symbols"
        if "criteria_snapshot" in row:
            row["criteria_snapshot"] = _json_compact(criteria)
        rows.append(row)
    rows = _backfill_rows(rows, page, criteria)
    rows = _ensure_rows_cover_keys(rows, keys, headers)
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
    rows = _ensure_rows_cover_keys(rows, keys, headers)

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
