#!/usr/bin/env python3
# core/enriched_quote.py
"""
===============================================================================
TFB Enriched Quote Core — v3.0.0
===============================================================================
IMPORT-SAFE • SCHEMA-AWARE • ROUTE-FRIENDLY • JSON-SAFE • SPECIAL-PAGE SAFE
ASYNC/THREAD SAFE • CONTRACT-HARDENED • BODY/QUERY MERGE • ENGINE-TOLERANT

Purpose
-------
Provide a single reusable core builder for enriched quote / sheet-row payloads
used by route wrappers, tests, and internal callers.

Design goals
------------
- No network calls at import time
- Accepts flexible inputs from GET/POST wrappers
- Handles sync and async engine methods safely
- Preserves canonical schema order when available
- Returns JSON-safe payloads only
- Degrades gracefully for special sheets and partial repo states

Typical use
-----------
    payload = await build_enriched_quote_payload(
        engine=engine,
        request=request,
        body=body,
        page="Market_Leaders",
        symbol="2222.SR",
    )

Public APIs
-----------
- build_enriched_quote_payload(...)
- get_enriched_quote_payload(...)
- enriched_quote(...)
- quote(...)
- build_enriched_quote_payload_sync(...)
"""

from __future__ import annotations

import asyncio
import datetime as _dt
import importlib
import inspect
import logging
import math
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

MODULE_VERSION = "3.0.0"

INSTRUMENT_DEFAULT_PAGES = {
    "Market_Leaders",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
    "My_Portfolio",
}

SPECIAL_PAGES = {
    "Top_10_Investments",
    "Insights_Analysis",
    "Data_Dictionary",
}

_PAGE_ALIASES = {
    "market-leaders": "Market_Leaders",
    "market_leaders": "Market_Leaders",
    "global-markets": "Global_Markets",
    "global_markets": "Global_Markets",
    "commodities-fx": "Commodities_FX",
    "commodities_fx": "Commodities_FX",
    "mutual-funds": "Mutual_Funds",
    "mutual_funds": "Mutual_Funds",
    "my-portfolio": "My_Portfolio",
    "my_portfolio": "My_Portfolio",
    "my-investments": "My_Portfolio",
    "my_investments": "My_Portfolio",
    "insights-analysis": "Insights_Analysis",
    "insights_analysis": "Insights_Analysis",
    "top-10-investments": "Top_10_Investments",
    "top_10_investments": "Top_10_Investments",
    "data-dictionary": "Data_Dictionary",
    "data_dictionary": "Data_Dictionary",
}

_BATCH_METHODS = (
    "get_enriched_quotes",
    "get_enriched_quotes_batch",
    "get_analysis_quotes_batch",
    "quotes_batch",
    "get_quotes_batch",
    "get_quotes",
    "fetch_quotes",
    "build_quotes",
)

_SINGLE_METHODS = (
    "get_enriched_quote",
    "enriched_quote",
    "get_quote",
    "quote",
    "get_quote_dict",
    "fetch_quote",
)

_SHEET_METHODS = (
    "get_sheet_rows",
    "build_sheet_rows",
    "get_page_rows",
    "build_page_rows",
    "rows_for_page",
    "sheet_rows",
    "get_rows_for_sheet",
    "build_output_rows",
    "build_rows_for_sheet",
)

_SNAPSHOT_METHODS = (
    "get_cached_sheet_snapshot",
    "get_sheet_snapshot",
    "get_cached_sheet_rows",
)

TOP10_MODULE_CANDIDATES = (
    "core.analysis.top10_selector",
    "core.top10_selector",
)
INSIGHTS_MODULE_CANDIDATES = (
    "core.analysis.insights_builder",
    "core.insights_builder",
    "core.analysis.insights_analysis",
)
DATA_DICTIONARY_MODULE_CANDIDATES = (
    "core.sheets.data_dictionary",
    "core.data_dictionary",
)

_TIMEOUT_SECONDS = 25.0


@dataclass
class SchemaColumn:
    key: str
    header: str
    dtype: Optional[str] = None
    fmt: Optional[str] = None
    required: bool = False
    group: Optional[str] = None
    source: Optional[str] = None
    notes: Optional[str] = None


def _strip(value: Any) -> str:
    if value is None:
        return ""
    try:
        return str(value).strip()
    except Exception:
        return ""


def _compact_dict(d: Mapping[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in d.items() if v is not None}


def _merge_dicts(*parts: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for part in parts:
        if isinstance(part, Mapping):
            out.update(dict(part))
    return out


def _is_true(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = _strip(value).lower()
    return text in {"1", "true", "yes", "y", "on"}


def _to_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return default


def _coerce_symbol(value: Any) -> Optional[str]:
    text = _strip(value)
    return text or None


def _coerce_symbols(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        raw = value.replace(";", ",").split(",")
        return [s for s in (_coerce_symbol(x) for x in raw) if s]
    if isinstance(value, (list, tuple, set)):
        out: List[str] = []
        for item in value:
            sym = _coerce_symbol(item)
            if sym:
                out.append(sym)
        return out
    sym = _coerce_symbol(value)
    return [sym] if sym else []


def _dedupe_keep_order(values: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            out.append(value)
    return out


def _sanitize_for_json(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, Decimal):
        try:
            f = float(value)
            if math.isnan(f) or math.isinf(f):
                return None
            return f
        except Exception:
            return str(value)
    if isinstance(value, (_dt.date, _dt.datetime, _dt.time)):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    if isinstance(value, Mapping):
        return {str(k): _sanitize_for_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_sanitize_for_json(v) for v in value]
    try:
        return str(value)
    except Exception:
        return None


def _normalize_page(page: Any) -> str:
    text = _strip(page)
    if not text:
        return "Market_Leaders"
    direct = _PAGE_ALIASES.get(text)
    if direct:
        return direct
    key = text.lower()
    if key in _PAGE_ALIASES:
        return _PAGE_ALIASES[key]
    key2 = key.replace(" ", "_")
    return _PAGE_ALIASES.get(key2, text)


def _extract_request_query(request: Any) -> Dict[str, Any]:
    if request is None:
        return {}
    try:
        qp = getattr(request, "query_params", None)
        if qp is not None:
            return dict(qp)
    except Exception:
        return {}
    return {}


def _extract_request_path_params(request: Any) -> Dict[str, Any]:
    if request is None:
        return {}
    try:
        pp = getattr(request, "path_params", None)
        if pp is not None:
            return dict(pp)
    except Exception:
        return {}
    return {}


def _extract_settings(settings: Any) -> Dict[str, Any]:
    if settings is None:
        return {}
    if isinstance(settings, Mapping):
        return dict(settings)
    try:
        return dict(vars(settings))
    except Exception:
        return {}


def _extract_symbols_from_inputs(*sources: Mapping[str, Any]) -> List[str]:
    symbols: List[str] = []
    for source in sources:
        for key in (
            "symbol", "symbols", "ticker", "tickers", "code", "codes",
            "instrument", "instruments", "security", "securities",
        ):
            if key in source:
                symbols.extend(_coerce_symbols(source.get(key)))
    return _dedupe_keep_order(symbols)


def _best_effort_limit(*sources: Mapping[str, Any], default: int = 50) -> int:
    for source in sources:
        for key in ("limit", "top", "max_rows", "max_results"):
            value = _to_int(source.get(key))
            if value and value > 0:
                return value
    return default


def _load_schema_columns(page: str) -> List[SchemaColumn]:
    module_candidates = (
        "core.sheets.schema_registry",
        "core.schema_registry",
    )
    for module_name in module_candidates:
        try:
            mod = importlib.import_module(module_name)
        except Exception:
            continue

        getters: List[Callable[..., Any]] = []
        for fn_name in (
            "get_sheet_schema",
            "get_schema_for_sheet",
            "get_sheet_columns",
            "get_columns_for_sheet",
        ):
            fn = getattr(mod, fn_name, None)
            if callable(fn):
                getters.append(fn)

        registry = getattr(mod, "SCHEMA_REGISTRY", None) or getattr(mod, "SHEET_SCHEMAS", None)

        if registry and isinstance(registry, Mapping):
            raw = registry.get(page)
            cols = _normalize_schema_columns(raw)
            if cols:
                return cols

        for getter in getters:
            try:
                raw = getter(page)
                cols = _normalize_schema_columns(raw)
                if cols:
                    return cols
            except Exception:
                continue
    return []


def _normalize_schema_columns(raw: Any) -> List[SchemaColumn]:
    if raw is None:
        return []
    if isinstance(raw, Mapping):
        raw = raw.get("columns") or raw.get("schema") or raw.get("fields") or raw
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return []

    out: List[SchemaColumn] = []
    for item in raw:
        if isinstance(item, Mapping):
            key = _strip(item.get("key") or item.get("field") or item.get("name"))
            header = _strip(item.get("header") or item.get("label") or key)
            if key:
                out.append(
                    SchemaColumn(
                        key=key,
                        header=header or key,
                        dtype=_strip(item.get("dtype")) or None,
                        fmt=_strip(item.get("fmt")) or None,
                        required=bool(item.get("required")),
                        group=_strip(item.get("group")) or None,
                        source=_strip(item.get("source")) or None,
                        notes=_strip(item.get("notes")) or None,
                    )
                )
        elif isinstance(item, str):
            key = _strip(item)
            if key:
                out.append(SchemaColumn(key=key, header=key))
    return out


def _headers_keys_display_from_schema(page: str) -> Tuple[List[str], List[str], List[str], List[Dict[str, Any]]]:
    cols = _load_schema_columns(page)
    if not cols:
        return [], [], [], []
    headers = [c.header or c.key for c in cols]
    keys = [c.key for c in cols]
    display_headers = list(headers)
    columns = [
        _sanitize_for_json(
            {
                "group": c.group,
                "header": c.header or c.key,
                "key": c.key,
                "dtype": c.dtype,
                "fmt": c.fmt,
                "required": c.required,
                "source": c.source,
                "notes": c.notes,
            }
        )
        for c in cols
    ]
    return headers, keys, display_headers, columns


def _project_row_to_keys(row: Mapping[str, Any], keys: Sequence[str]) -> Dict[str, Any]:
    projected: Dict[str, Any] = {}
    for key in keys:
        projected[key] = row.get(key)
    return projected


def _rows_to_matrix(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    matrix: List[List[Any]] = []
    for row in rows:
        matrix.append([_sanitize_for_json(row.get(key)) for key in keys])
    return matrix


def _normalize_engine_payload_to_rows(payload: Any) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    meta: Dict[str, Any] = {}
    if payload is None:
        return [], meta

    if isinstance(payload, Mapping):
        meta = dict(payload)

        for key in ("rows", "data", "items", "result", "results", "quotes"):
            maybe_rows = payload.get(key)
            if isinstance(maybe_rows, list):
                rows = [_coerce_row(x) for x in maybe_rows if _coerce_row(x)]
                return rows, meta

        if isinstance(payload.get("row"), Mapping):
            row = _coerce_row(payload.get("row"))
            return ([row] if row else []), meta

        if isinstance(payload.get("quote"), Mapping):
            row = _coerce_row(payload.get("quote"))
            return ([row] if row else []), meta

        if _looks_like_row(payload):
            row = _coerce_row(payload)
            return ([row] if row else []), {}

    if isinstance(payload, list):
        rows = [_coerce_row(x) for x in payload if _coerce_row(x)]
        return rows, meta

    return [], meta


def _looks_like_row(value: Any) -> bool:
    if not isinstance(value, Mapping):
        return False
    keys = {str(k).lower() for k in value.keys()}
    return bool(keys & {"symbol", "ticker", "current_price", "company_name", "name"})


def _coerce_row(value: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(value, Mapping):
        return None
    return {str(k): v for k, v in value.items()}


async def _call_maybe_async(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    if inspect.iscoroutinefunction(func):
        return await func(*args, **kwargs)
    return await asyncio.to_thread(func, *args, **kwargs)


async def _invoke_flexible_method(
    target: Any,
    method_names: Sequence[str],
    variants: Sequence[Dict[str, Any]],
) -> Any:
    for method_name in method_names:
        method = getattr(target, method_name, None)
        if not callable(method):
            continue
        for kwargs in variants:
            try:
                cleaned = _compact_dict(kwargs)
                return await _call_maybe_async(method, **cleaned)
            except TypeError:
                continue
            except Exception as exc:
                logger.warning("Method %s failed: %s", method_name, exc)
                break
    return None


def _build_call_variants(
    page: str,
    body: Mapping[str, Any],
    symbols: Sequence[str],
    limit: int,
    schema_only: bool,
    headers_only: bool,
    table_mode: bool,
    mode: str,
    request: Any = None,
    settings: Any = None,
) -> List[Dict[str, Any]]:
    symbol = symbols[0] if symbols else None
    common = {
        "page": page,
        "sheet": page,
        "sheet_name": page,
        "tab": page,
        "name": page,
        "body": dict(body),
        "request": request,
        "settings": settings,
        "limit": limit,
        "mode": mode,
        "schema_only": schema_only,
        "headers_only": headers_only,
        "table_mode": table_mode,
    }
    variants = [
        {**common, "symbol": symbol, "symbols": list(symbols), "ticker": symbol, "tickers": list(symbols)},
        {**common, "symbols": list(symbols)},
        {**common, "symbol": symbol},
        {**common},
        {"page": page, "symbols": list(symbols), "limit": limit, "body": dict(body)},
        {"sheet": page, "symbols": list(symbols), "limit": limit, "body": dict(body)},
        {"sheet_name": page, "symbols": list(symbols), "limit": limit},
        {"page": page, "symbol": symbol, "limit": limit},
        {"sheet": page, "symbol": symbol, "limit": limit},
        {"body": dict(body), "limit": limit},
        {"symbol": symbol},
        {"symbols": list(symbols)},
        {},
    ]
    seen = set()
    out: List[Dict[str, Any]] = []
    for item in variants:
        key = repr(sorted(item.items(), key=lambda kv: kv[0]))
        if key not in seen:
            seen.add(key)
            out.append(item)
    return out


async def _try_special_builder(
    page: str,
    engine: Any,
    request: Any,
    settings: Any,
    body: Mapping[str, Any],
) -> Any:
    if page == "Top_10_Investments":
        return await _try_module_build(
            TOP10_MODULE_CANDIDATES,
            (
                "build_top10_rows",
                "build_top10_output_rows",
                "select_top10",
                "select_top10_symbols",
            ),
            engine=engine,
            request=request,
            settings=settings,
            body=dict(body),
            page=page,
            sheet=page,
            mode=body.get("mode") or "sheet_rows",
            limit=_to_int(body.get("limit"), 10) or 10,
        )

    if page == "Insights_Analysis":
        result = await _try_module_build(
            INSIGHTS_MODULE_CANDIDATES,
            (
                "build_insights_rows",
                "build_insights_output_rows",
                "build_insights_analysis_rows",
                "build_insights_payload",
            ),
            engine=engine,
            request=request,
            settings=settings,
            body=dict(body),
            page=page,
            sheet=page,
            limit=_to_int(body.get("limit"), 50) or 50,
        )
        if result is not None:
            return result

    if page == "Data_Dictionary":
        result = await _try_module_build(
            DATA_DICTIONARY_MODULE_CANDIDATES,
            (
                "build_data_dictionary_rows",
                "generate_data_dictionary",
                "get_data_dictionary_rows",
            ),
            engine=engine,
            request=request,
            settings=settings,
            body=dict(body),
            page=page,
            sheet=page,
        )
        if result is not None:
            return result
        rows = _build_data_dictionary_from_schema()
        if rows:
            return rows

    return None


async def _try_module_build(
    module_candidates: Sequence[str],
    function_names: Sequence[str],
    **kwargs: Any,
) -> Any:
    for module_name in module_candidates:
        try:
            mod = importlib.import_module(module_name)
        except Exception:
            continue
        for fn_name in function_names:
            fn = getattr(mod, fn_name, None)
            if not callable(fn):
                continue
            try:
                return await asyncio.wait_for(_call_maybe_async(fn, **_compact_dict(kwargs)), timeout=_TIMEOUT_SECONDS)
            except TypeError:
                try:
                    minimal = {k: v for k, v in kwargs.items() if k in {"engine", "request", "settings", "body", "page", "sheet", "limit", "mode"}}
                    return await asyncio.wait_for(_call_maybe_async(fn, **_compact_dict(minimal)), timeout=_TIMEOUT_SECONDS)
                except Exception:
                    continue
            except Exception as exc:
                logger.warning("Special builder %s.%s failed: %s", module_name, fn_name, exc)
                continue
    return None


def _build_data_dictionary_from_schema() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    module_candidates = (
        "core.sheets.schema_registry",
        "core.schema_registry",
    )
    registry: Optional[Mapping[str, Any]] = None
    for module_name in module_candidates:
        try:
            mod = importlib.import_module(module_name)
        except Exception:
            continue
        registry = getattr(mod, "SCHEMA_REGISTRY", None) or getattr(mod, "SHEET_SCHEMAS", None)
        if isinstance(registry, Mapping):
            break

    if not isinstance(registry, Mapping):
        return rows

    for sheet_name, raw in registry.items():
        cols = _normalize_schema_columns(raw)
        for idx, col in enumerate(cols, start=1):
            rows.append(
                {
                    "sheet": sheet_name,
                    "group": col.group,
                    "header": col.header or col.key,
                    "key": col.key,
                    "dtype": col.dtype,
                    "fmt": col.fmt,
                    "required": col.required,
                    "source": col.source,
                    "notes": col.notes,
                    "column_order": idx,
                }
            )
    return rows


async def _engine_rows_fallback(
    engine: Any,
    page: str,
    body: Mapping[str, Any],
    symbols: Sequence[str],
    request: Any,
    settings: Any,
    schema_only: bool,
    headers_only: bool,
    table_mode: bool,
    mode: str,
    limit: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if engine is None:
        return [], {"warning": "engine_not_available"}

    variants = _build_call_variants(
        page=page,
        body=body,
        symbols=symbols,
        limit=limit,
        schema_only=schema_only,
        headers_only=headers_only,
        table_mode=table_mode,
        mode=mode,
        request=request,
        settings=settings,
    )

    if page in SPECIAL_PAGES:
        payload = await _invoke_flexible_method(engine, _SHEET_METHODS + _SNAPSHOT_METHODS, variants)
        rows, meta = _normalize_engine_payload_to_rows(payload)
        if rows:
            return rows, meta

    if symbols:
        method_names = _BATCH_METHODS if len(symbols) > 1 else (_SINGLE_METHODS + _BATCH_METHODS)
        payload = await _invoke_flexible_method(engine, method_names, variants)
        rows, meta = _normalize_engine_payload_to_rows(payload)
        if rows:
            return rows, meta

    payload = await _invoke_flexible_method(engine, _SHEET_METHODS + _SNAPSHOT_METHODS + _BATCH_METHODS + _SINGLE_METHODS, variants)
    rows, meta = _normalize_engine_payload_to_rows(payload)
    return rows, meta


def _derive_headers_keys_display(
    page: str,
    rows: Sequence[Mapping[str, Any]],
    source_meta: Mapping[str, Any],
) -> Tuple[List[str], List[str], List[str], List[Dict[str, Any]]]:
    headers, keys, display_headers, columns = _headers_keys_display_from_schema(page)
    if keys:
        return headers, keys, display_headers, columns

    maybe_headers = source_meta.get("headers")
    maybe_keys = source_meta.get("keys")
    maybe_display = source_meta.get("display_headers")
    if isinstance(maybe_headers, list) and isinstance(maybe_keys, list) and maybe_keys:
        return (
            [str(x) for x in maybe_headers],
            [str(x) for x in maybe_keys],
            [str(x) for x in (maybe_display or maybe_headers)],
            [],
        )

    ordered_keys: List[str] = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                ordered_keys.append(str(key))
    if not ordered_keys:
        ordered_keys = ["symbol", "current_price"]

    headers = list(ordered_keys)
    display_headers = list(headers)
    return headers, ordered_keys, display_headers, []


def _normalize_rows_for_page(
    page: str,
    rows: Sequence[Mapping[str, Any]],
    keys: Sequence[str],
) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        row_dict = dict(row)
        if "symbol" not in row_dict and "ticker" in row_dict:
            row_dict["symbol"] = row_dict.get("ticker")
        if "ticker" not in row_dict and "symbol" in row_dict:
            row_dict["ticker"] = row_dict.get("symbol")

        if keys:
            row_dict = _project_row_to_keys(row_dict, keys)
        normalized.append({k: _sanitize_for_json(v) for k, v in row_dict.items()})
    return normalized


def _response_status(rows: Sequence[Mapping[str, Any]], schema_only: bool, headers_only: bool, warnings: List[str]) -> str:
    if warnings:
        return "partial" if rows or schema_only or headers_only else "warn"
    return "ok"


async def build_enriched_quote_payload(
    engine: Any = None,
    request: Any = None,
    settings: Any = None,
    body: Optional[Mapping[str, Any]] = None,
    page: Optional[str] = None,
    symbol: Optional[str] = None,
    symbols: Optional[Sequence[str]] = None,
    mode: Optional[str] = None,
    limit: Optional[int] = None,
    schema_only: Optional[bool] = None,
    headers_only: Optional[bool] = None,
    table_mode: Optional[bool] = None,
    **extra: Any,
) -> Dict[str, Any]:
    query = _extract_request_query(request)
    path_params = _extract_request_path_params(request)
    settings_dict = _extract_settings(settings)
    body_dict = dict(body or {})
    extra_dict = dict(extra or {})

    merged = _merge_dicts(query, path_params, settings_dict, body_dict, extra_dict)

    resolved_page = _normalize_page(
        page
        or merged.get("page")
        or merged.get("sheet")
        or merged.get("sheet_name")
        or "Market_Leaders"
    )

    resolved_symbols = _dedupe_keep_order(
        [s for s in (symbols or []) if _coerce_symbol(s)] +
        ([symbol] if _coerce_symbol(symbol) else []) +
        _extract_symbols_from_inputs(merged)
    )

    resolved_limit = limit if isinstance(limit, int) and limit > 0 else _best_effort_limit(merged, default=50)
    resolved_mode = _strip(mode or merged.get("mode")) or "sheet_rows"
    resolved_schema_only = bool(schema_only) if isinstance(schema_only, bool) else _is_true(merged.get("schema_only"))
    resolved_headers_only = bool(headers_only) if isinstance(headers_only, bool) else _is_true(merged.get("headers_only"))
    resolved_table_mode = bool(table_mode) if isinstance(table_mode, bool) else _is_true(merged.get("table_mode"))

    warnings: List[str] = []
    source_meta: Dict[str, Any] = {}
    rows: List[Dict[str, Any]] = []

    if resolved_page in SPECIAL_PAGES:
        try:
            special_payload = await asyncio.wait_for(
                _try_special_builder(
                    page=resolved_page,
                    engine=engine,
                    request=request,
                    settings=settings,
                    body=merged,
                ),
                timeout=_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            special_payload = None
            warnings.append("special_builder_timeout")
        except Exception as exc:
            special_payload = None
            warnings.append(f"special_builder_error:{type(exc).__name__}")

        sp_rows, sp_meta = _normalize_engine_payload_to_rows(special_payload)
        if sp_rows:
            rows = sp_rows
            source_meta = sp_meta
        elif special_payload is not None and isinstance(special_payload, list):
            rows = [_coerce_row(x) for x in special_payload if _coerce_row(x)]
            source_meta = {}

    if not rows and not (resolved_schema_only or resolved_headers_only):
        engine_rows, engine_meta = await _engine_rows_fallback(
            engine=engine,
            page=resolved_page,
            body=merged,
            symbols=resolved_symbols,
            request=request,
            settings=settings,
            schema_only=resolved_schema_only,
            headers_only=resolved_headers_only,
            table_mode=resolved_table_mode,
            mode=resolved_mode,
            limit=resolved_limit,
        )
        rows = engine_rows
        source_meta = engine_meta or source_meta
        if not rows and engine is None:
            warnings.append("engine_not_available")
        elif not rows:
            warnings.append("no_rows_returned")

    headers, keys, display_headers, columns = _derive_headers_keys_display(
        resolved_page,
        rows,
        source_meta,
    )

    if not keys and resolved_page in INSTRUMENT_DEFAULT_PAGES and resolved_symbols:
        keys = ["symbol", "ticker", "company_name", "current_price"]
        headers = list(keys)
        display_headers = list(keys)

    if resolved_headers_only or resolved_schema_only:
        rows = []
    elif keys:
        rows = _normalize_rows_for_page(resolved_page, rows[:resolved_limit], keys)
    else:
        rows = _normalize_rows_for_page(resolved_page, rows[:resolved_limit], [])

    response = {
        "status": _response_status(rows, resolved_schema_only, resolved_headers_only, warnings),
        "module": "core.enriched_quote",
        "module_version": MODULE_VERSION,
        "mode": resolved_mode,
        "page": resolved_page,
        "sheet": resolved_page,
        "requested_symbols": resolved_symbols,
        "symbol": resolved_symbols[0] if len(resolved_symbols) == 1 else None,
        "symbols": resolved_symbols,
        "headers": headers,
        "keys": keys,
        "display_headers": display_headers or headers,
        "columns": columns,
        "row_count": len(rows),
        "rows": rows,
        "data": rows,
        "items": rows,
        "rows_matrix": _rows_to_matrix(rows, keys) if resolved_table_mode and keys and rows else [],
        "schema_only": resolved_schema_only,
        "headers_only": resolved_headers_only,
        "table_mode": resolved_table_mode,
        "warnings": warnings,
        "meta": _sanitize_for_json(
            {
                "source_meta_keys": sorted(list(source_meta.keys())) if isinstance(source_meta, Mapping) else [],
                "special_page": resolved_page in SPECIAL_PAGES,
                "has_engine": engine is not None,
            }
        ),
    }

    for key in (
        "criteria_fields",
        "criteria_snapshot",
        "selection_reason",
        "top10_rank",
        "supported_pages",
        "aliases",
    ):
        if isinstance(source_meta, Mapping) and key in source_meta:
            response[key] = _sanitize_for_json(source_meta.get(key))

    return _sanitize_for_json(response)


async def get_enriched_quote_payload(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await build_enriched_quote_payload(*args, **kwargs)


async def enriched_quote(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await build_enriched_quote_payload(*args, **kwargs)


async def quote(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await build_enriched_quote_payload(*args, **kwargs)


def build_enriched_quote_payload_sync(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(build_enriched_quote_payload(*args, **kwargs))
    raise RuntimeError(
        "build_enriched_quote_payload_sync() cannot run inside an active event loop. "
        "Use `await build_enriched_quote_payload(...)` instead."
    )


__all__ = [
    "MODULE_VERSION",
    "build_enriched_quote_payload",
    "build_enriched_quote_payload_sync",
    "get_enriched_quote_payload",
    "enriched_quote",
    "quote",
]
