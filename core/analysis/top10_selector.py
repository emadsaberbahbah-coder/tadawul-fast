#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/analysis/top10_selector.py
================================================================================
Top 10 Selector — v4.0.0
================================================================================
LIVE • SCHEMA-FIRST • ROUTE-COMPATIBLE • ENGINE-TOLERANT • JSON-SAFE
TOP10-METADATA GUARANTEED • SOURCE-PAGE SAFE • SNAPSHOT FALLBACK SAFE

Purpose
-------
Produce stable, schema-aligned Top_10_Investments rows for:
- routes/investment_advisor.py
- routes/analysis_sheet_rows.py
- routes/enriched_quote.py
- internal callers

Design goals
------------
- No network calls at import time
- Works with sync or async engines
- Accepts broad route-builder call shapes
- Prefers live/source-page selection, but can fall back safely
- Guarantees Top10-only fields:
    - top10_rank
    - selection_reason
    - criteria_snapshot
- Returns JSON-safe payloads only
- Degrades gracefully when repo/engine methods differ

Public APIs
-----------
- build_top10_rows(...)
- build_top10_output_rows(...)
- build_top10_investments_rows(...)
- build_top_10_investments_rows(...)
- get_top10_rows(...)
- select_top10(...)
- select_top10_symbols(...)
================================================================================
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import math
import os
import time
from decimal import Decimal
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger("core.analysis.top10_selector")
logger.addHandler(logging.NullHandler())

TOP10_SELECTOR_VERSION = "4.0.0"
OUTPUT_PAGE = "Top_10_Investments"

DEFAULT_SOURCE_PAGES = [
    "Market_Leaders",
    "Global_Markets",
    "Mutual_Funds",
    "Commodities_FX",
    "My_Portfolio",
]

DERIVED_OR_NON_SOURCE_PAGES = {
    "KSA_TADAWUL",
    "Advisor_Criteria",
    "AI_Opportunity_Report",
    "Insights_Analysis",
    "Top_10_Investments",
    "Data_Dictionary",
}

ROW_KEY_ALIASES: Dict[str, Tuple[str, ...]] = {
    "symbol": ("symbol", "ticker", "code", "instrument", "security"),
    "name": ("name", "company_name", "long_name", "instrument_name", "security_name"),
    "current_price": ("current_price", "price", "last_price", "last", "close", "market_price", "nav"),
    "expected_roi_1m": ("expected_roi_1m", "roi_1m", "expected_return_1m"),
    "expected_roi_3m": ("expected_roi_3m", "roi_3m", "expected_return_3m"),
    "expected_roi_12m": ("expected_roi_12m", "roi_12m", "expected_return_12m"),
    "forecast_confidence": ("forecast_confidence", "confidence_score", "ai_confidence"),
    "confidence_bucket": ("confidence_bucket", "confidence_level"),
    "risk_score": ("risk_score", "risk"),
    "risk_bucket": ("risk_bucket", "risk_level"),
    "volume": ("volume", "avg_volume", "trading_volume"),
    "liquidity_score": ("liquidity_score",),
    "recommendation": ("recommendation", "reco", "signal"),
    "overall_score": ("overall_score", "advisor_score", "score"),
    "opportunity_score": ("opportunity_score",),
    "value_score": ("value_score",),
    "quality_score": ("quality_score",),
    "momentum_score": ("momentum_score",),
    "growth_score": ("growth_score",),
    "selection_reason": ("selection_reason", "selector_reason"),
    "top10_rank": ("top10_rank", "rank"),
    "criteria_snapshot": ("criteria_snapshot", "criteria_json"),
}

DEFAULT_FALLBACK_KEYS = [
    "symbol",
    "name",
    "current_price",
    "expected_roi_1m",
    "expected_roi_3m",
    "expected_roi_12m",
    "forecast_confidence",
    "confidence_bucket",
    "risk_score",
    "risk_bucket",
    "liquidity_score",
    "overall_score",
    "opportunity_score",
    "value_score",
    "quality_score",
    "momentum_score",
    "growth_score",
    "recommendation",
    "last_updated_riyadh",
    "top10_rank",
    "selection_reason",
    "criteria_snapshot",
]

DEFAULT_FALLBACK_HEADERS = [
    "Symbol",
    "Name",
    "Current Price",
    "Expected ROI 1M",
    "Expected ROI 3M",
    "Expected ROI 12M",
    "Forecast Confidence",
    "Confidence Bucket",
    "Risk Score",
    "Risk Bucket",
    "Liquidity Score",
    "Overall Score",
    "Opportunity Score",
    "Value Score",
    "Quality Score",
    "Momentum Score",
    "Growth Score",
    "Recommendation",
    "Last Updated (Riyadh)",
    "Top10 Rank",
    "Selection Reason",
    "Criteria Snapshot",
]


# =============================================================================
# Basic helpers
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


def _safe_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if v is None:
            return default
        if isinstance(v, bool):
            return default
        if isinstance(v, (int, float)):
            f = float(v)
            if math.isnan(f) or math.isinf(f):
                return default
            return f
        s = _s(v).replace(",", "")
        if not s:
            return default
        if s.endswith("%"):
            f = float(s[:-1].strip()) / 100.0
        else:
            f = float(s)
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except Exception:
        return default


def _safe_ratio(v: Any, default: Optional[float] = None) -> Optional[float]:
    f = _safe_float(v, default)
    if f is None:
        return default
    if abs(f) > 1.5:
        return f / 100.0
    return f


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


def _json_safe(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, (bool, int, str)):
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

    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]

    try:
        return str(value)
    except Exception:
        return None


def _json_compact(value: Any) -> str:
    try:
        return json.dumps(_json_safe(value), ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return str(value)


def _normalize_list(value: Any) -> List[str]:
    if value is None:
        return []

    if isinstance(value, str):
        seq = [x.strip() for x in value.replace(";", ",").replace("\n", ",").split(",") if x.strip()]
    elif isinstance(value, (list, tuple, set)):
        seq = list(value)
    else:
        seq = [value]

    out: List[str] = []
    seen = set()
    for item in seq:
        s = _s(item)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


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


def _normalize_symbol(sym: Any) -> str:
    s = _s(sym).upper().replace(" ", "")
    if not s:
        return ""
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1]
    if s.endswith(".SA"):
        s = s[:-3] + ".SR"
    if s.isdigit():
        return f"{s}.SR"
    return s


def _safe_source_pages(values: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in values:
        s = _s(item)
        if not s or s in DERIVED_OR_NON_SOURCE_PAGES:
            continue
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _rows_to_matrix(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    return [[_json_safe(row.get(k)) for k in keys] for row in rows]


# =============================================================================
# Optional schema/page catalog
# =============================================================================
try:
    from core.sheets.schema_registry import get_sheet_spec as _get_sheet_spec  # type: ignore
except Exception:
    _get_sheet_spec = None  # type: ignore

try:
    from core.sheets.page_catalog import normalize_page_name as _normalize_page_name  # type: ignore
except Exception:
    _normalize_page_name = None  # type: ignore


def _normalize_page_name_safe(name: str) -> str:
    s = _s(name)
    if not s:
        return ""
    if callable(_normalize_page_name):
        try:
            return _normalize_page_name(s, allow_output_pages=False)
        except Exception:
            return s
    return s


def _load_schema_defaults() -> Tuple[List[str], List[str]]:
    if callable(_get_sheet_spec):
        try:
            spec = _get_sheet_spec(OUTPUT_PAGE)
            cols = getattr(spec, "columns", None) or []
            keys = [getattr(c, "key", "") for c in cols]
            headers = [getattr(c, "header", "") for c in cols]
            keys = [k for k in keys if isinstance(k, str) and k]
            headers = [h for h in headers if isinstance(h, str) and h]
            keys, headers = _ensure_top10_keys_present(keys, headers)
            if keys and headers:
                return headers, keys
        except Exception:
            pass

    return list(DEFAULT_FALLBACK_HEADERS), list(DEFAULT_FALLBACK_KEYS)


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


# =============================================================================
# Engine interaction helpers
# =============================================================================
async def _call_maybe_async(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    if inspect.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)

    result = await asyncio.to_thread(fn, *args, **kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


def _extract_rows_like(payload: Any) -> List[Dict[str, Any]]:
    if payload is None:
        return []

    if isinstance(payload, list):
        return [dict(x) for x in payload if isinstance(x, dict)]

    if not isinstance(payload, dict):
        return []

    # direct symbol map { "2222.SR": {...}, ... }
    maybe_symbol_map = True
    rows_from_map: List[Dict[str, Any]] = []
    if payload:
        for k, v in payload.items():
            if not isinstance(v, dict):
                maybe_symbol_map = False
                break
            row = dict(v)
            if _is_blank(row.get("symbol")) and _is_blank(row.get("ticker")):
                row["symbol"] = _normalize_symbol(k)
                row["ticker"] = _normalize_symbol(k)
            rows_from_map.append(row)
        if maybe_symbol_map and rows_from_map:
            return rows_from_map

    for key in ("rows", "recommendations", "data", "items", "results", "records", "quotes"):
        value = payload.get(key)
        if isinstance(value, list):
            return [dict(x) for x in value if isinstance(x, dict)]

    return []


def _extract_matrix_like(payload: Any) -> Optional[List[List[Any]]]:
    if not isinstance(payload, dict):
        return None

    rows_matrix = payload.get("rows_matrix")
    if isinstance(rows_matrix, list):
        out: List[List[Any]] = []
        for row in rows_matrix:
            if isinstance(row, (list, tuple)):
                out.append(list(row))
            else:
                out.append([row])
        return out

    rows = payload.get("rows")
    if isinstance(rows, list) and rows and isinstance(rows[0], (list, tuple)):
        return [list(r) if isinstance(r, (list, tuple)) else [r] for r in rows]

    return None


async def _call_engine_method(
    engine: Any,
    method_names: Sequence[str],
    attempts: Sequence[Tuple[Tuple[Any, ...], Dict[str, Any]]],
) -> Any:
    if engine is None:
        return None

    for method_name in method_names:
        fn = getattr(engine, method_name, None)
        if not callable(fn):
            continue

        for args, kwargs in attempts:
            try:
                return await _call_maybe_async(fn, *args, **kwargs)
            except TypeError:
                continue
            except Exception:
                break

    return None


async def _fetch_page_rows(engine: Any, page: str, limit: int, mode: str) -> List[Dict[str, Any]]:
    body = {
        "page": page,
        "sheet": page,
        "sheet_name": page,
        "limit": limit,
        "top_n": limit,
        "mode": mode or "",
    }

    attempts = [
        ((), {"page": page, "sheet": page, "sheet_name": page, "limit": limit, "mode": mode or "", "body": body}),
        ((), {"page": page, "sheet": page, "limit": limit, "mode": mode or ""}),
        ((), {"page": page, "limit": limit, "mode": mode or ""}),
        ((page,), {"limit": limit, "mode": mode or ""}),
        ((page,), {"limit": limit}),
        ((page,), {}),
    ]

    payload = await _call_engine_method(
        engine,
        ("get_sheet_rows", "get_page_rows", "sheet_rows", "build_sheet_rows"),
        attempts,
    )

    rows = _extract_rows_like(payload)
    if rows:
        return rows

    matrix = _extract_matrix_like(payload)
    if matrix:
        headers, keys = _load_schema_defaults()
        return [{k: (row[idx] if idx < len(row) else None) for idx, k in enumerate(keys)} for row in matrix]

    return []


async def _fetch_page_snapshot_rows(engine: Any, page: str) -> List[Dict[str, Any]]:
    attempts = [
        ((), {"sheet_name": page}),
        ((), {"sheet": page}),
        ((page,), {}),
    ]

    payload = await _call_engine_method(
        engine,
        ("get_cached_sheet_snapshot", "get_sheet_snapshot", "get_cached_sheet_rows"),
        attempts,
    )

    rows = _extract_rows_like(payload)
    if rows:
        return rows

    if isinstance(payload, dict):
        inner_rows = payload.get("rows")
        if isinstance(inner_rows, list):
            return [dict(x) for x in inner_rows if isinstance(x, dict)]

    return []


async def _fetch_direct_symbol_rows(engine: Any, symbols: Sequence[str], mode: str) -> List[Dict[str, Any]]:
    syms = [_normalize_symbol(s) for s in symbols if _normalize_symbol(s)]
    if not syms:
        return []

    attempts = [
        ((), {"symbols": syms, "mode": mode or "", "schema": OUTPUT_PAGE}),
        ((), {"symbols": syms, "mode": mode or ""}),
        ((), {"symbols": syms}),
        ((syms,), {"mode": mode or "", "schema": OUTPUT_PAGE}),
        ((syms,), {"mode": mode or ""}),
        ((syms,), {}),
        ((), {"tickers": syms, "mode": mode or ""}),
        ((), {"tickers": syms}),
    ]

    payload = await _call_engine_method(
        engine,
        ("get_enriched_quotes_batch", "get_analysis_quotes_batch", "get_quotes_batch", "quotes_batch", "get_quotes"),
        attempts,
    )

    rows = _extract_rows_like(payload)
    if rows:
        return rows

    # single-row fallback
    out: List[Dict[str, Any]] = []
    for sym in syms:
        single_attempts = [
            ((sym,), {"mode": mode or "", "schema": OUTPUT_PAGE}),
            ((sym,), {"mode": mode or ""}),
            ((sym,), {}),
            ((), {"symbol": sym, "mode": mode or ""}),
            ((), {"symbol": sym}),
        ]
        row_payload = await _call_engine_method(
            engine,
            ("get_enriched_quote", "get_quote", "get_quote_dict"),
            single_attempts,
        )
        single_rows = _extract_rows_like(row_payload)
        if single_rows:
            out.extend(single_rows)
        elif isinstance(row_payload, dict):
            out.append(dict(row_payload))

    return out


# =============================================================================
# Criteria normalization
# =============================================================================
def _merge_body_like(*parts: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for part in parts:
        if isinstance(part, Mapping):
            out.update(dict(part))
    return out


def _collect_criteria_from_kwargs(**kwargs: Any) -> Dict[str, Any]:
    body = _merge_body_like(
        kwargs.get("body"),
        kwargs.get("payload"),
        kwargs.get("request_data"),
        kwargs.get("params"),
    )

    criteria = {}
    if isinstance(kwargs.get("criteria"), Mapping):
        criteria.update(dict(kwargs["criteria"]))
    if isinstance(body.get("criteria"), Mapping):
        criteria.update(dict(body["criteria"]))
    if isinstance(body.get("filters"), Mapping):
        criteria.update(dict(body["filters"]))

    for k, v in body.items():
        if v is not None and k not in {"criteria", "filters"}:
            criteria.setdefault(k, v)

    for k in (
        "pages_selected",
        "pages",
        "selected_pages",
        "sources",
        "page",
        "sheet",
        "sheet_name",
        "symbols",
        "tickers",
        "direct_symbols",
        "top_n",
        "limit",
        "risk_level",
        "risk_profile",
        "confidence_bucket",
        "confidence_level",
        "invest_period_days",
        "investment_period_days",
        "horizon_days",
        "invest_period_label",
        "min_expected_roi",
        "min_roi",
        "min_confidence",
        "min_ai_confidence",
        "max_risk_score",
        "min_volume",
        "enrich_final",
        "schema_only",
        "mode",
    ):
        if kwargs.get(k) is not None:
            criteria[k] = kwargs.get(k)

    pages = (
        _normalize_list(criteria.get("pages_selected"))
        or _normalize_list(criteria.get("pages"))
        or _normalize_list(criteria.get("selected_pages"))
        or _normalize_list(criteria.get("sources"))
        or _normalize_list(criteria.get("page"))
        or _normalize_list(criteria.get("sheet"))
        or _normalize_list(criteria.get("sheet_name"))
    )
    pages = _safe_source_pages([_normalize_page_name_safe(p) for p in pages])

    if not pages:
        pages = _safe_source_pages(DEFAULT_SOURCE_PAGES)

    direct_symbols = _normalize_list(
        criteria.get("direct_symbols") or criteria.get("symbols") or criteria.get("tickers")
    )
    direct_symbols = [_normalize_symbol(s) for s in direct_symbols if _normalize_symbol(s)]

    limit = _safe_int(criteria.get("limit") or criteria.get("top_n") or kwargs.get("limit"), 10)
    limit = max(1, min(limit, _safe_int(os.getenv("TOP10_SELECTOR_MAX_LIMIT", "50"), 50)))

    horizon_days = _safe_int(
        criteria.get("horizon_days")
        or criteria.get("invest_period_days")
        or criteria.get("investment_period_days"),
        90,
    )

    risk_level = _s(criteria.get("risk_level") or criteria.get("risk_profile") or "moderate").lower()
    confidence_bucket = _s(criteria.get("confidence_bucket") or criteria.get("confidence_level")).lower()

    min_roi = criteria.get("min_expected_roi")
    if min_roi is None:
        min_roi = criteria.get("min_roi")
    min_roi_ratio = _safe_ratio(min_roi, None)

    normalized = dict(criteria)
    normalized["pages_selected"] = pages
    normalized["direct_symbols"] = direct_symbols
    normalized["limit"] = limit
    normalized["top_n"] = limit
    normalized["risk_level"] = risk_level
    normalized["risk_profile"] = risk_level
    normalized["confidence_bucket"] = confidence_bucket
    normalized["confidence_level"] = confidence_bucket
    normalized["horizon_days"] = horizon_days
    normalized["invest_period_days"] = horizon_days
    normalized["min_expected_roi"] = min_roi_ratio
    normalized["min_roi"] = min_roi_ratio
    normalized.setdefault("enrich_final", True)

    return normalized


# =============================================================================
# Row normalization / ranking
# =============================================================================
def _extract_value_by_aliases(row: Mapping[str, Any], key: str) -> Any:
    aliases = ROW_KEY_ALIASES.get(key, (key,))
    row_ci = {str(k).strip().lower(): v for k, v in row.items()}

    for alias in aliases:
        if alias in row:
            return row.get(alias)
        low = alias.lower()
        if low in row_ci:
            return row_ci.get(low)
    return None


def _normalize_candidate_row(raw: Mapping[str, Any]) -> Dict[str, Any]:
    row: Dict[str, Any] = dict(raw)

    for key in ROW_KEY_ALIASES.keys():
        if key not in row or row.get(key) is None:
            value = _extract_value_by_aliases(row, key)
            if value is not None:
                row[key] = value

    sym = _normalize_symbol(row.get("symbol") or row.get("ticker"))
    if sym:
        row["symbol"] = sym
        row.setdefault("ticker", sym)

    return row


def _row_richness(row: Mapping[str, Any]) -> int:
    return sum(1 for _, v in row.items() if not _is_blank(v))


def _choose_horizon_roi(row: Mapping[str, Any], horizon_days: int) -> Optional[float]:
    if horizon_days <= 31:
        return _safe_ratio(row.get("expected_roi_1m"), None) or _safe_ratio(row.get("expected_roi_3m"), None)
    if horizon_days <= 92:
        return _safe_ratio(row.get("expected_roi_3m"), None) or _safe_ratio(row.get("expected_roi_12m"), None)
    return _safe_ratio(row.get("expected_roi_12m"), None) or _safe_ratio(row.get("expected_roi_3m"), None)


def _confidence_bucket_match(row: Mapping[str, Any], wanted: str) -> bool:
    if not wanted:
        return True

    row_bucket = _s(row.get("confidence_bucket") or row.get("confidence_level")).lower()
    if row_bucket:
        return wanted in row_bucket or row_bucket in wanted

    score = _safe_float(row.get("forecast_confidence") or row.get("confidence_score"), None)
    if score is None:
        return True

    if "high" in wanted:
        return score >= 70
    if "moderate" in wanted or "medium" in wanted:
        return 45 <= score < 70
    if "low" in wanted:
        return score < 45
    return True


def _risk_level_match(row: Mapping[str, Any], wanted: str) -> bool:
    if not wanted:
        return True

    row_bucket = _s(row.get("risk_bucket") or row.get("risk_level")).lower()
    if row_bucket:
        return wanted in row_bucket or row_bucket in wanted

    risk = _safe_float(row.get("risk_score"), None)
    if risk is None:
        return True

    if "low" in wanted or "conservative" in wanted:
        return risk <= 35
    if "moderate" in wanted or "medium" in wanted:
        return 20 <= risk <= 65
    if "high" in wanted or "aggressive" in wanted:
        return risk >= 45
    return True


def _passes_filters(row: Mapping[str, Any], criteria: Mapping[str, Any]) -> bool:
    wanted_conf = _s(criteria.get("confidence_bucket") or criteria.get("confidence_level")).lower()
    wanted_risk = _s(criteria.get("risk_level") or criteria.get("risk_profile")).lower()
    horizon_days = _safe_int(criteria.get("horizon_days") or criteria.get("invest_period_days"), 90)

    if not _confidence_bucket_match(row, wanted_conf):
        return False
    if not _risk_level_match(row, wanted_risk):
        return False

    min_roi = _safe_ratio(criteria.get("min_expected_roi") or criteria.get("min_roi"), None)
    roi = _choose_horizon_roi(row, horizon_days)
    if min_roi is not None and roi is not None and roi < min_roi:
        return False

    min_conf = _safe_float(criteria.get("min_confidence") or criteria.get("min_ai_confidence"), None)
    row_conf = _safe_float(row.get("forecast_confidence") or row.get("confidence_score"), None)
    if min_conf is not None and row_conf is not None and row_conf < min_conf:
        return False

    max_risk = _safe_float(criteria.get("max_risk_score"), None)
    risk = _safe_float(row.get("risk_score"), None)
    if max_risk is not None and risk is not None and risk > max_risk:
        return False

    min_volume = _safe_float(criteria.get("min_volume"), None)
    volume = _safe_float(row.get("volume"), None)
    if min_volume is not None and volume is not None and volume < min_volume:
        return False

    return True


def _selector_score(row: Mapping[str, Any], criteria: Mapping[str, Any]) -> float:
    overall = _safe_float(row.get("overall_score"), None)
    opportunity = _safe_float(row.get("opportunity_score"), None)
    value = _safe_float(row.get("value_score"), None)
    quality = _safe_float(row.get("quality_score"), None)
    momentum = _safe_float(row.get("momentum_score"), None)
    growth = _safe_float(row.get("growth_score"), None)
    risk = _safe_float(row.get("risk_score"), None)
    conf = _safe_float(row.get("forecast_confidence") or row.get("confidence_score"), None)
    liquidity = _safe_float(row.get("liquidity_score"), None)
    roi = _choose_horizon_roi(row, _safe_int(criteria.get("horizon_days") or criteria.get("invest_period_days"), 90))

    score = 0.0

    if overall is not None:
        score += overall * 0.35
    if opportunity is not None:
        score += opportunity * 0.20
    if value is not None:
        score += value * 0.08
    if quality is not None:
        score += quality * 0.08
    if momentum is not None:
        score += momentum * 0.08
    if growth is not None:
        score += growth * 0.08
    if conf is not None:
        score += conf * 0.08
    if liquidity is not None:
        score += liquidity * 0.05
    if risk is not None:
        score += (100.0 - risk) * 0.08
    if roi is not None:
        score += roi * 100.0 * 0.20

    return float(score)


def _canonical_selection_reason(row: Dict[str, Any]) -> str:
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
        val = _safe_ratio(row.get(key), None)
        if isinstance(val, float):
            roi_parts.append(f"{label} ROI={round(val * 100, 2)}%")

    parts: List[str] = []
    if recommendation:
        parts.append(f"Recommendation={recommendation}")
    if confidence_bucket:
        parts.append(f"Confidence={confidence_bucket}")
    if risk_bucket:
        parts.append(f"Risk={risk_bucket}")
    if score_parts:
        parts.append(", ".join(score_parts[:3]))
    if roi_parts:
        parts.append(", ".join(roi_parts[:2]))

    return " | ".join(parts) if parts else "Selected by Top10 composite scoring."


def _rank_and_project_rows(
    rows: Sequence[Mapping[str, Any]],
    keys: Sequence[str],
    criteria: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    criteria_snapshot = _json_compact(criteria)
    out: List[Dict[str, Any]] = []

    for idx, raw in enumerate(rows, start=1):
        row = dict(raw)
        if _is_blank(row.get("top10_rank")):
            row["top10_rank"] = idx
        if _is_blank(row.get("rank_overall")):
            row["rank_overall"] = idx
        if _is_blank(row.get("selection_reason")):
            row["selection_reason"] = _canonical_selection_reason(row)
        if _is_blank(row.get("criteria_snapshot")):
            row["criteria_snapshot"] = criteria_snapshot

        projected = {k: _json_safe(row.get(k)) for k in keys}
        out.append(projected)

    return out


# =============================================================================
# Candidate collection
# =============================================================================
async def _collect_candidate_rows(
    engine: Any,
    criteria: Mapping[str, Any],
    mode: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    direct_symbols = _normalize_list(criteria.get("direct_symbols"))
    pages = _safe_source_pages(_normalize_list(criteria.get("pages_selected")))
    per_page_limit = max(10, min(_safe_int(os.getenv("TOP10_SELECTOR_SOURCE_PAGE_LIMIT", "250"), 250), 1000))

    meta: Dict[str, Any] = {
        "source_pages": pages,
        "direct_symbols_count": len(direct_symbols),
        "source_page_rows": {},
        "snapshot_rows": {},
        "direct_symbol_rows": 0,
    }

    candidates: Dict[str, Dict[str, Any]] = {}

    def _put_row(raw: Mapping[str, Any], source_page: str = "") -> None:
        row = _normalize_candidate_row(raw)
        sym = _normalize_symbol(row.get("symbol") or row.get("ticker"))
        if not sym:
            return

        if source_page and _is_blank(row.get("source_page")):
            row["source_page"] = source_page

        existing = candidates.get(sym)
        if existing is None or _row_richness(row) > _row_richness(existing):
            candidates[sym] = row

    # direct symbols first
    if direct_symbols:
        direct_rows = await _fetch_direct_symbol_rows(engine, direct_symbols, mode)
        meta["direct_symbol_rows"] = len(direct_rows)
        for row in direct_rows:
            _put_row(row, "")

    # source pages
    for page in pages:
        rows = await _fetch_page_rows(engine, page, per_page_limit, mode)
        meta["source_page_rows"][page] = len(rows)

        if not rows:
            snap_rows = await _fetch_page_snapshot_rows(engine, page)
            meta["snapshot_rows"][page] = len(snap_rows)
            rows = snap_rows

        for row in rows:
            _put_row(row, page)

    # final fallback to existing Top10 output page only if still empty
    if not candidates:
        fallback_rows = await _fetch_page_rows(engine, OUTPUT_PAGE, max(50, _safe_int(criteria.get("limit"), 10) * 3), mode)
        meta["top10_output_fallback_rows"] = len(fallback_rows)
        for row in fallback_rows:
            _put_row(row, OUTPUT_PAGE)

    return list(candidates.values()), meta


# =============================================================================
# Payload builder
# =============================================================================
def _build_payload(
    *,
    status: str,
    headers: List[str],
    keys: List[str],
    rows: List[Dict[str, Any]],
    meta: Dict[str, Any],
) -> Dict[str, Any]:
    return _json_safe(
        {
            "status": status,
            "page": OUTPUT_PAGE,
            "sheet": OUTPUT_PAGE,
            "route_family": "top10",
            "headers": headers,
            "display_headers": headers,
            "sheet_headers": headers,
            "column_headers": headers,
            "keys": keys,
            "rows": rows,
            "data": rows,
            "items": rows,
            "rows_matrix": _rows_to_matrix(rows, keys),
            "version": TOP10_SELECTOR_VERSION,
            "meta": meta,
        }
    )


# =============================================================================
# Public API
# =============================================================================
async def build_top10_rows(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    started = time.perf_counter()

    engine = kwargs.get("engine")
    if engine is None and args:
        for arg in args:
            if arg is not None and not isinstance(arg, Mapping):
                engine = arg
                break

    criteria = _collect_criteria_from_kwargs(**kwargs)
    mode = _s(kwargs.get("mode") or criteria.get("mode") or "")
    limit = max(1, _safe_int(criteria.get("limit") or kwargs.get("limit"), 10))

    headers, keys = _load_schema_defaults()

    if engine is None:
        return _build_payload(
            status="partial",
            headers=headers,
            keys=keys,
            rows=[],
            meta={
                "build_status": "DEGRADED",
                "dispatch": "top10_selector",
                "warning": "engine_unavailable",
                "criteria_used": _json_safe(criteria),
                "duration_ms": round((time.perf_counter() - started) * 1000.0, 3),
            },
        )

    try:
        candidates, collect_meta = await _collect_candidate_rows(engine, criteria, mode)
    except Exception as exc:
        logger.warning("Top10 candidate collection failed: %s", exc)
        return _build_payload(
            status="partial",
            headers=headers,
            keys=keys,
            rows=[],
            meta={
                "build_status": "DEGRADED",
                "dispatch": "top10_selector",
                "warning": f"candidate_collection_failed:{type(exc).__name__}",
                "criteria_used": _json_safe(criteria),
                "duration_ms": round((time.perf_counter() - started) * 1000.0, 3),
            },
        )

    filtered = [r for r in candidates if _passes_filters(r, criteria)]
    scored: List[Tuple[float, Dict[str, Any]]] = []
    for row in filtered:
        scored.append((_selector_score(row, criteria), dict(row)))

    scored.sort(
        key=lambda x: (
            x[0],
            _safe_ratio(x[1].get("expected_roi_3m"), 0.0) or 0.0,
            _safe_float(x[1].get("forecast_confidence"), 0.0) or 0.0,
            -(_safe_float(x[1].get("risk_score"), 999.0) or 999.0),
            _safe_float(x[1].get("liquidity_score"), 0.0) or 0.0,
        ),
        reverse=True,
    )

    top_rows = [row for _, row in scored[:limit]]
    projected_rows = _rank_and_project_rows(top_rows, keys, criteria)

    status = "success" if projected_rows else "partial"

    meta = {
        "build_status": "OK" if projected_rows else "WARN",
        "dispatch": "top10_selector",
        "criteria_used": _json_safe(criteria),
        "candidate_count": len(candidates),
        "filtered_count": len(filtered),
        "selected_count": len(projected_rows),
        "duration_ms": round((time.perf_counter() - started) * 1000.0, 3),
        **collect_meta,
    }

    if not projected_rows:
        meta["warning"] = "no_top10_rows_after_filtering"

    return _build_payload(
        status=status,
        headers=headers,
        keys=keys,
        rows=projected_rows,
        meta=meta,
    )


async def build_top10_output_rows(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await build_top10_rows(*args, **kwargs)


async def build_top10_investments_rows(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await build_top10_rows(*args, **kwargs)


async def build_top_10_investments_rows(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await build_top10_rows(*args, **kwargs)


async def get_top10_rows(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await build_top10_rows(*args, **kwargs)


async def select_top10(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await build_top10_rows(*args, **kwargs)


async def select_top10_symbols(*args: Any, **kwargs: Any) -> List[str]:
    payload = await build_top10_rows(*args, **kwargs)
    rows = payload.get("rows") if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        return []
    out: List[str] = []
    for row in rows:
        if isinstance(row, dict):
            sym = _normalize_symbol(row.get("symbol") or row.get("ticker"))
            if sym:
                out.append(sym)
    return _dedupe_keep_order(out)


__all__ = [
    "TOP10_SELECTOR_VERSION",
    "build_top10_rows",
    "build_top10_output_rows",
    "build_top10_investments_rows",
    "build_top_10_investments_rows",
    "get_top10_rows",
    "select_top10",
    "select_top10_symbols",
]
