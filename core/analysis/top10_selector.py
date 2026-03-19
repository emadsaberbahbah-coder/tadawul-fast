#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/analysis/top10_selector.py
================================================================================
Top 10 Selector — v4.3.0
================================================================================
LIVE • SCHEMA-FIRST • ROUTE-COMPATIBLE • ENGINE-SELF-RESOLVING • JSON-SAFE
TOP10-METADATA GUARANTEED • SOURCE-PAGE SAFE • SNAPSHOT FALLBACK SAFE
SYNC+ASYNC CALLER TOLERANT • MODULE/APP-STATE ENGINE DISCOVERY HARDENED
WRAPPER-PAYLOAD SAFE • STRICT ROW COERCION • HORIZON-AWARE FILTERING

Purpose
-------
Produce stable, schema-aligned Top_10_Investments rows for:
- routes/investment_advisor.py
- routes/analysis_sheet_rows.py
- routes/enriched_quote.py
- routes/advanced_analysis.py
- internal callers

What is improved in v4.3.0
--------------------------
- ✅ FIX: much stricter row extraction from nested/wrapper payloads
- ✅ FIX: prevents fake rows caused by wrapper dicts or meta dicts
- ✅ FIX: supports `keys`, `columns`, `fields`, `headers`, `display_headers`
- ✅ FIX: schema loading now tolerates dict/object/list spec shapes better
- ✅ FIX: Top10 payload now includes `columns` and `fields` aliases
- ✅ FIX: horizon-aware ROI filtering and horizon-aware ranking tie-breaks
- ✅ FIX: if `min_expected_roi` is requested and ROI is missing, the row is filtered
       out first, then relaxed only if filtering empties the whole pool
- ✅ FIX: model/object rows and batch quote outputs are reconstructed more safely
- ✅ FIX: richer metadata for debugging page rows / snapshot rows / direct symbols
================================================================================
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import logging
import math
import os
import sys
import time
from decimal import Decimal
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger("core.analysis.top10_selector")
logger.addHandler(logging.NullHandler())

TOP10_SELECTOR_VERSION = "4.3.0"
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

TOP10_REQUIRED_FIELDS = (
    "top10_rank",
    "selection_reason",
    "criteria_snapshot",
)

TOP10_REQUIRED_HEADERS = {
    "top10_rank": "Top10 Rank",
    "selection_reason": "Selection Reason",
    "criteria_snapshot": "Criteria Snapshot",
}

ROW_KEY_ALIASES: Dict[str, Tuple[str, ...]] = {
    "symbol": ("symbol", "ticker", "code", "instrument", "security", "symbol_normalized", "requested_symbol"),
    "name": ("name", "company_name", "long_name", "instrument_name", "security_name"),
    "asset_class": ("asset_class",),
    "exchange": ("exchange",),
    "currency": ("currency",),
    "country": ("country",),
    "sector": ("sector",),
    "industry": ("industry",),
    "current_price": ("current_price", "price", "last_price", "last", "close", "market_price", "nav"),
    "previous_close": ("previous_close",),
    "open_price": ("open_price", "open"),
    "day_high": ("day_high", "high"),
    "day_low": ("day_low", "low"),
    "week_52_high": ("week_52_high", "52w_high"),
    "week_52_low": ("week_52_low", "52w_low"),
    "price_change": ("price_change", "change"),
    "percent_change": ("percent_change", "change_pct", "pct_change"),
    "week_52_position_pct": ("week_52_position_pct",),
    "volume": ("volume", "avg_volume", "trading_volume"),
    "avg_volume_10d": ("avg_volume_10d",),
    "avg_volume_30d": ("avg_volume_30d",),
    "market_cap": ("market_cap",),
    "float_shares": ("float_shares",),
    "beta_5y": ("beta_5y",),
    "pe_ttm": ("pe_ttm",),
    "pe_forward": ("pe_forward",),
    "eps_ttm": ("eps_ttm",),
    "dividend_yield": ("dividend_yield",),
    "payout_ratio": ("payout_ratio",),
    "revenue_ttm": ("revenue_ttm",),
    "revenue_growth_yoy": ("revenue_growth_yoy",),
    "gross_margin": ("gross_margin",),
    "operating_margin": ("operating_margin",),
    "profit_margin": ("profit_margin",),
    "debt_to_equity": ("debt_to_equity",),
    "free_cash_flow_ttm": ("free_cash_flow_ttm",),
    "rsi_14": ("rsi_14",),
    "volatility_30d": ("volatility_30d",),
    "volatility_90d": ("volatility_90d",),
    "max_drawdown_1y": ("max_drawdown_1y",),
    "var_95_1d": ("var_95_1d",),
    "sharpe_1y": ("sharpe_1y",),
    "risk_score": ("risk_score", "risk"),
    "risk_bucket": ("risk_bucket", "risk_level"),
    "pb_ratio": ("pb_ratio",),
    "ps_ratio": ("ps_ratio",),
    "ev_ebitda": ("ev_ebitda",),
    "peg_ratio": ("peg_ratio",),
    "intrinsic_value": ("intrinsic_value",),
    "valuation_score": ("valuation_score",),
    "forecast_price_1m": ("forecast_price_1m",),
    "forecast_price_3m": ("forecast_price_3m",),
    "forecast_price_12m": ("forecast_price_12m",),
    "expected_roi_1m": ("expected_roi_1m", "roi_1m", "expected_return_1m"),
    "expected_roi_3m": ("expected_roi_3m", "roi_3m", "expected_return_3m"),
    "expected_roi_12m": ("expected_roi_12m", "roi_12m", "expected_return_12m"),
    "forecast_confidence": ("forecast_confidence", "confidence_score", "ai_confidence"),
    "confidence_score": ("confidence_score", "forecast_confidence", "ai_confidence"),
    "confidence_bucket": ("confidence_bucket", "confidence_level"),
    "value_score": ("value_score",),
    "quality_score": ("quality_score",),
    "momentum_score": ("momentum_score",),
    "growth_score": ("growth_score",),
    "overall_score": ("overall_score", "advisor_score", "score"),
    "opportunity_score": ("opportunity_score",),
    "rank_overall": ("rank_overall",),
    "recommendation": ("recommendation", "reco", "signal"),
    "recommendation_reason": ("recommendation_reason",),
    "horizon_days": ("horizon_days", "invest_period_days", "investment_period_days"),
    "invest_period_label": ("invest_period_label", "horizon_label"),
    "position_qty": ("position_qty",),
    "avg_cost": ("avg_cost",),
    "position_cost": ("position_cost",),
    "position_value": ("position_value",),
    "unrealized_pl": ("unrealized_pl",),
    "unrealized_pl_pct": ("unrealized_pl_pct",),
    "data_provider": ("data_provider",),
    "last_updated_utc": ("last_updated_utc",),
    "last_updated_riyadh": ("last_updated_riyadh",),
    "warnings": ("warnings", "warning"),
    "liquidity_score": ("liquidity_score",),
    "selection_reason": ("selection_reason", "selector_reason"),
    "top10_rank": ("top10_rank", "rank"),
    "criteria_snapshot": ("criteria_snapshot", "criteria_json"),
    "source_page": ("source_page", "page", "sheet", "sheet_name"),
}

DEFAULT_FALLBACK_KEYS = [
    "symbol",
    "name",
    "asset_class",
    "exchange",
    "currency",
    "country",
    "sector",
    "industry",
    "current_price",
    "previous_close",
    "open_price",
    "day_high",
    "day_low",
    "week_52_high",
    "week_52_low",
    "price_change",
    "percent_change",
    "week_52_position_pct",
    "volume",
    "avg_volume_10d",
    "avg_volume_30d",
    "market_cap",
    "float_shares",
    "beta_5y",
    "pe_ttm",
    "pe_forward",
    "eps_ttm",
    "dividend_yield",
    "payout_ratio",
    "revenue_ttm",
    "revenue_growth_yoy",
    "gross_margin",
    "operating_margin",
    "profit_margin",
    "debt_to_equity",
    "free_cash_flow_ttm",
    "rsi_14",
    "volatility_30d",
    "volatility_90d",
    "max_drawdown_1y",
    "var_95_1d",
    "sharpe_1y",
    "risk_score",
    "risk_bucket",
    "pb_ratio",
    "ps_ratio",
    "ev_ebitda",
    "peg_ratio",
    "intrinsic_value",
    "valuation_score",
    "forecast_price_1m",
    "forecast_price_3m",
    "forecast_price_12m",
    "expected_roi_1m",
    "expected_roi_3m",
    "expected_roi_12m",
    "forecast_confidence",
    "confidence_score",
    "confidence_bucket",
    "value_score",
    "quality_score",
    "momentum_score",
    "growth_score",
    "overall_score",
    "opportunity_score",
    "rank_overall",
    "recommendation",
    "recommendation_reason",
    "horizon_days",
    "invest_period_label",
    "position_qty",
    "avg_cost",
    "position_cost",
    "position_value",
    "unrealized_pl",
    "unrealized_pl_pct",
    "data_provider",
    "last_updated_utc",
    "last_updated_riyadh",
    "warnings",
    "top10_rank",
    "selection_reason",
    "criteria_snapshot",
]

DEFAULT_FALLBACK_HEADERS = [
    "Symbol",
    "Name",
    "Asset Class",
    "Exchange",
    "Currency",
    "Country",
    "Sector",
    "Industry",
    "Current Price",
    "Previous Close",
    "Open",
    "Day High",
    "Day Low",
    "52W High",
    "52W Low",
    "Price Change",
    "Percent Change",
    "52W Position %",
    "Volume",
    "Avg Volume 10D",
    "Avg Volume 30D",
    "Market Cap",
    "Float Shares",
    "Beta (5Y)",
    "P/E (TTM)",
    "P/E (Forward)",
    "EPS (TTM)",
    "Dividend Yield",
    "Payout Ratio",
    "Revenue (TTM)",
    "Revenue Growth YoY",
    "Gross Margin",
    "Operating Margin",
    "Profit Margin",
    "Debt/Equity",
    "Free Cash Flow (TTM)",
    "RSI (14)",
    "Volatility 30D",
    "Volatility 90D",
    "Max Drawdown 1Y",
    "VaR 95% (1D)",
    "Sharpe (1Y)",
    "Risk Score",
    "Risk Bucket",
    "P/B",
    "P/S",
    "EV/EBITDA",
    "PEG",
    "Intrinsic Value",
    "Valuation Score",
    "Forecast Price 1M",
    "Forecast Price 3M",
    "Forecast Price 12M",
    "Expected ROI 1M",
    "Expected ROI 3M",
    "Expected ROI 12M",
    "Forecast Confidence",
    "Confidence Score",
    "Confidence Bucket",
    "Value Score",
    "Quality Score",
    "Momentum Score",
    "Growth Score",
    "Overall Score",
    "Opportunity Score",
    "Rank (Overall)",
    "Recommendation",
    "Recommendation Reason",
    "Horizon Days",
    "Invest Period Label",
    "Position Qty",
    "Avg Cost",
    "Position Cost",
    "Position Value",
    "Unrealized P/L",
    "Unrealized P/L %",
    "Data Provider",
    "Last Updated (UTC)",
    "Last Updated (Riyadh)",
    "Warnings",
    "Top10 Rank",
    "Selection Reason",
    "Criteria Snapshot",
]

_ENGINE_CACHE: Optional[Any] = None
_ENGINE_CACHE_SOURCE: str = ""
_ENGINE_LOCK = asyncio.Lock()


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
        if v is None or isinstance(v, bool):
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
        if hasattr(value, "model_dump") and callable(getattr(value, "model_dump")):
            return _json_safe(value.model_dump(mode="python"))
    except Exception:
        pass

    try:
        if hasattr(value, "dict") and callable(getattr(value, "dict")):
            return _json_safe(value.dict())
    except Exception:
        pass

    try:
        if hasattr(value, "__dict__"):
            return _json_safe(dict(value.__dict__))
    except Exception:
        pass

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


def _looks_like_symbol_token(x: Any) -> bool:
    s = _s(x).upper()
    if not s or " " in s or len(s) > 24:
        return False
    return bool(__import__("re").fullmatch(r"[A-Z0-9\.\=\-\^:_/]{1,24}", s))


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


def _header_to_key(header: Any) -> str:
    s = _s(header)
    if not s:
        return ""
    out: List[str] = []
    prev_us = False
    for ch in s:
        if ch.isalnum():
            out.append(ch.lower())
            prev_us = False
        else:
            if not prev_us:
                out.append("_")
                prev_us = True
    key = "".join(out).strip("_")
    while "__" in key:
        key = key.replace("__", "_")
    key = key.replace("52w", "week_52")
    return key


def _is_mapping_like(obj: Any) -> bool:
    return isinstance(obj, Mapping)


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
            d = obj.model_dump(mode="python")
            if isinstance(d, dict):
                return d
    except Exception:
        pass

    try:
        if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
            d = obj.dict()
            if isinstance(d, dict):
                return d
    except Exception:
        pass

    try:
        if hasattr(obj, "__dict__"):
            d = getattr(obj, "__dict__", None)
            if isinstance(d, dict):
                return dict(d)
    except Exception:
        pass

    return {}


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
        except TypeError:
            try:
                return _normalize_page_name(s)
            except Exception:
                return s
        except Exception:
            return s
    return s


def _complete_schema_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    raw_headers = list(headers or [])
    raw_keys = list(keys or [])
    max_len = max(len(raw_headers), len(raw_keys))

    out_headers: List[str] = []
    out_keys: List[str] = []

    for i in range(max_len):
        h = _s(raw_headers[i]) if i < len(raw_headers) else ""
        k = _s(raw_keys[i]) if i < len(raw_keys) else ""

        if h and not k:
            k = _header_to_key(h)
        elif k and not h:
            h = k.replace("_", " ").title()
        elif not h and not k:
            h = f"Column_{i+1}"
            k = f"key_{i+1}"

        out_headers.append(h)
        out_keys.append(k)

    return out_headers, out_keys


def _schema_columns_from_any(spec: Any) -> List[Any]:
    if spec is None:
        return []

    if isinstance(spec, dict) and len(spec) == 1 and "columns" not in spec and "fields" not in spec:
        first_val = list(spec.values())[0]
        if isinstance(first_val, dict) and ("columns" in first_val or "fields" in first_val):
            spec = first_val

    cols = getattr(spec, "columns", None)
    if isinstance(cols, list) and cols:
        return cols

    fields = getattr(spec, "fields", None)
    if isinstance(fields, list) and fields:
        return fields

    if isinstance(spec, Mapping):
        cols2 = spec.get("columns") or spec.get("fields")
        if isinstance(cols2, list) and cols2:
            return cols2

    try:
        d = getattr(spec, "__dict__", None)
        if isinstance(d, dict):
            cols3 = d.get("columns") or d.get("fields")
            if isinstance(cols3, list) and cols3:
                return cols3
    except Exception:
        pass

    return []


def _schema_keys_headers_from_spec(spec: Any) -> Tuple[List[str], List[str]]:
    if isinstance(spec, dict) and len(spec) == 1 and not any(
        k in spec for k in ("columns", "fields", "headers", "keys", "display_headers")
    ):
        first_val = list(spec.values())[0]
        if isinstance(first_val, dict):
            spec = first_val

    headers: List[str] = []
    keys: List[str] = []

    cols = _schema_columns_from_any(spec)
    for c in cols:
        if isinstance(c, Mapping):
            h = _s(c.get("header") or c.get("display_header") or c.get("displayHeader") or c.get("label") or c.get("title"))
            k = _s(c.get("key") or c.get("field") or c.get("name") or c.get("id"))
        else:
            h = _s(
                getattr(
                    c,
                    "header",
                    getattr(c, "display_header", getattr(c, "displayHeader", getattr(c, "label", getattr(c, "title", None)))),
                )
            )
            k = _s(getattr(c, "key", getattr(c, "field", getattr(c, "name", getattr(c, "id", None)))))

        if h or k:
            headers.append(h or k.replace("_", " ").title())
            keys.append(k or _header_to_key(h))

    if not headers and not keys and isinstance(spec, Mapping):
        headers2 = spec.get("headers") or spec.get("display_headers")
        keys2 = spec.get("keys") or spec.get("columns") or spec.get("fields")
        if isinstance(headers2, list):
            headers = [_s(x) for x in headers2 if _s(x)]
        if isinstance(keys2, list):
            keys = [_s(x) for x in keys2 if _s(x)]

    return _complete_schema_contract(headers, keys)


def _ensure_top10_keys_present(keys: List[str], headers: List[str]) -> Tuple[List[str], List[str]]:
    out_keys = list(keys or [])
    out_headers = list(headers or [])

    for field in TOP10_REQUIRED_FIELDS:
        if field not in out_keys:
            out_keys.append(field)
            out_headers.append(TOP10_REQUIRED_HEADERS[field])

    return _complete_schema_contract(out_headers, out_keys)[1], _complete_schema_contract(out_headers, out_keys)[0]


def _load_schema_defaults() -> Tuple[List[str], List[str]]:
    if callable(_get_sheet_spec):
        try:
            spec = _get_sheet_spec(OUTPUT_PAGE)
            headers, keys = _schema_keys_headers_from_spec(spec)
            if keys:
                headers, keys = _complete_schema_contract(headers, keys)
                keys, headers = _ensure_top10_keys_present(keys, headers)
                return list(headers), list(keys)
        except Exception:
            pass

    headers = list(DEFAULT_FALLBACK_HEADERS)
    keys = list(DEFAULT_FALLBACK_KEYS)
    headers, keys = _complete_schema_contract(headers, keys)
    return headers, keys


# =============================================================================
# Engine detection / resolution
# =============================================================================
_ENGINE_METHOD_NAMES = (
    "get_sheet_rows",
    "get_page_rows",
    "sheet_rows",
    "build_sheet_rows",
    "execute_sheet_rows",
    "run_sheet_rows",
    "build_analysis_sheet_rows",
    "run_analysis_sheet_rows",
    "get_rows_for_sheet",
    "get_rows_for_page",
    "get_cached_sheet_snapshot",
    "get_sheet_snapshot",
    "get_cached_sheet_rows",
    "get_page_snapshot",
    "get_enriched_quotes_batch",
    "get_analysis_quotes_batch",
    "get_quotes_batch",
    "quotes_batch",
    "get_quotes",
    "get_quote",
    "get_quote_dict",
    "get_enriched_quote",
)

_ENGINE_HOLDER_ATTRS = (
    "engine",
    "data_engine",
    "quote_engine",
    "cache_engine",
    "_engine",
    "_data_engine",
    "service",
    "runner",
    "advisor_engine",
)

_APP_ATTRS = (
    "app",
    "application",
    "fastapi_app",
    "api",
)

_STATE_ATTRS = (
    "state",
    "app_state",
)

_MODULE_CANDIDATE_NAMES = (
    "main",
    "app",
    "core.data_engine_v2",
    "core.data_engine",
    "routes.analysis_sheet_rows",
    "routes.investment_advisor",
    "routes.advanced_analysis",
    "routes.enriched_quote",
    "routes.advisor",
)


def _looks_like_engine(obj: Any) -> bool:
    if obj is None:
        return False
    if isinstance(obj, (str, bytes, int, float, bool, list, tuple, set)):
        return False
    if isinstance(obj, Mapping):
        return False
    return any(callable(getattr(obj, m, None)) for m in _ENGINE_METHOD_NAMES)


def _safe_getattr(obj: Any, name: str, default: Any = None) -> Any:
    try:
        return getattr(obj, name, default)
    except Exception:
        return default


def _iter_mapping_values(mapping: Mapping[str, Any]) -> Iterable[Tuple[str, Any]]:
    try:
        for k, v in mapping.items():
            yield _s(k), v
    except Exception:
        return


def _iter_object_values(obj: Any) -> Iterable[Tuple[str, Any]]:
    if obj is None:
        return

    if isinstance(obj, Mapping):
        yield from _iter_mapping_values(obj)
        return

    names: List[str] = []
    try:
        if hasattr(obj, "__dict__") and isinstance(getattr(obj, "__dict__", None), dict):
            names.extend([n for n in obj.__dict__.keys() if isinstance(n, str)])
    except Exception:
        pass

    preferred = list(_ENGINE_HOLDER_ATTRS) + list(_APP_ATTRS) + list(_STATE_ATTRS)
    names = preferred + [n for n in names if n not in preferred]

    seen = set()
    for name in names:
        if not name or name in seen:
            continue
        seen.add(name)
        try:
            yield name, getattr(obj, name)
        except Exception:
            continue


def _collect_engine_candidates_from_object(
    obj: Any,
    prefix: str = "",
    seen: Optional[set] = None,
    depth: int = 0,
) -> List[Tuple[Any, str]]:
    if seen is None:
        seen = set()

    out: List[Tuple[Any, str]] = []

    if obj is None or depth > 4:
        return out

    obj_id = id(obj)
    if obj_id in seen:
        return out
    seen.add(obj_id)

    if _looks_like_engine(obj):
        out.append((obj, prefix or type(obj).__name__))

    if isinstance(obj, Mapping):
        for name, value in _iter_mapping_values(obj):
            if name in set(_ENGINE_HOLDER_ATTRS + _APP_ATTRS + _STATE_ATTRS) or _looks_like_engine(value):
                out.extend(
                    _collect_engine_candidates_from_object(
                        value,
                        f"{prefix}.{name}" if prefix else name,
                        seen,
                        depth + 1,
                    )
                )
        return out

    for attr in _ENGINE_HOLDER_ATTRS:
        val = _safe_getattr(obj, attr, None)
        if val is not None:
            out.extend(
                _collect_engine_candidates_from_object(
                    val,
                    f"{prefix}.{attr}" if prefix else attr,
                    seen,
                    depth + 1,
                )
            )

    for attr in _APP_ATTRS + _STATE_ATTRS:
        val = _safe_getattr(obj, attr, None)
        if val is not None:
            out.extend(
                _collect_engine_candidates_from_object(
                    val,
                    f"{prefix}.{attr}" if prefix else attr,
                    seen,
                    depth + 1,
                )
            )

    for name, value in _iter_object_values(obj):
        if value is None:
            continue
        if name in set(_ENGINE_HOLDER_ATTRS + _APP_ATTRS + _STATE_ATTRS) or _looks_like_engine(value):
            out.extend(
                _collect_engine_candidates_from_object(
                    value,
                    f"{prefix}.{name}" if prefix else name,
                    seen,
                    depth + 1,
                )
            )

    return out


async def _call_maybe_async(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    if inspect.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)

    result = await asyncio.to_thread(fn, *args, **kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


async def _safe_call_zero_arg(fn: Callable[..., Any]) -> Any:
    try:
        result = fn()
        if inspect.isawaitable(result):
            return await result
        return result
    except TypeError:
        return None
    except Exception:
        return None


async def _scan_module_for_engine(module: Any, module_name: str) -> List[Tuple[Any, str]]:
    out: List[Tuple[Any, str]] = []

    if module is None:
        return out

    if _looks_like_engine(module):
        out.append((module, module_name))

    for fn_name in ("get_engine", "resolve_engine", "load_engine", "build_engine", "create_engine"):
        fn = _safe_getattr(module, fn_name, None)
        if callable(fn):
            result = await _safe_call_zero_arg(fn)
            if _looks_like_engine(result):
                out.append((result, f"{module_name}.{fn_name}"))

    for attr in _ENGINE_HOLDER_ATTRS + _APP_ATTRS + _STATE_ATTRS + ("ENGINE", "engine", "_ENGINE"):
        value = _safe_getattr(module, attr, None)
        if value is not None:
            out.extend(_collect_engine_candidates_from_object(value, f"{module_name}.{attr}"))

    return out


async def _resolve_engine_from_modules() -> Tuple[Optional[Any], str]:
    global _ENGINE_CACHE, _ENGINE_CACHE_SOURCE

    if _looks_like_engine(_ENGINE_CACHE):
        return _ENGINE_CACHE, _ENGINE_CACHE_SOURCE or "engine_cache"

    async with _ENGINE_LOCK:
        if _looks_like_engine(_ENGINE_CACHE):
            return _ENGINE_CACHE, _ENGINE_CACHE_SOURCE or "engine_cache"

        candidates: List[Tuple[Any, str]] = []

        for module_name in _MODULE_CANDIDATE_NAMES:
            try:
                mod = importlib.import_module(module_name)
            except Exception:
                mod = None
            if mod is not None:
                candidates.extend(await _scan_module_for_engine(mod, module_name))

        loaded_names = sorted(
            name
            for name in sys.modules.keys()
            if isinstance(name, str)
            and (name == "main" or name == "app" or name.startswith("core.") or name.startswith("routes."))
        )

        seen_sources = set(src for _, src in candidates)
        for module_name in loaded_names:
            mod = sys.modules.get(module_name)
            if mod is None:
                continue
            scanned = await _scan_module_for_engine(mod, module_name)
            for candidate, source in scanned:
                if source not in seen_sources:
                    seen_sources.add(source)
                    candidates.append((candidate, source))

        if candidates:
            _ENGINE_CACHE, _ENGINE_CACHE_SOURCE = candidates[0]
            return _ENGINE_CACHE, _ENGINE_CACHE_SOURCE

    return None, "engine_unavailable"


async def _resolve_engine(*args: Any, **kwargs: Any) -> Tuple[Optional[Any], str]:
    for key in ("engine", "data_engine", "quote_engine", "cache_engine", "service", "runner", "request", "req", "app", "context"):
        if kwargs.get(key) is not None:
            candidates = _collect_engine_candidates_from_object(kwargs.get(key), key)
            if candidates:
                return candidates[0]

    for i, arg in enumerate(args):
        candidates = _collect_engine_candidates_from_object(arg, f"arg{i}")
        if candidates:
            return candidates[0]

    for key in ("body", "payload", "request_data", "params", "criteria"):
        val = kwargs.get(key)
        if isinstance(val, Mapping):
            candidates = _collect_engine_candidates_from_object(val, key)
            if candidates:
                return candidates[0]

    for i, arg in enumerate(args):
        if isinstance(arg, Mapping):
            candidates = _collect_engine_candidates_from_object(arg, f"arg{i}")
            if candidates:
                return candidates[0]

    return await _resolve_engine_from_modules()


# =============================================================================
# Payload / row extraction helpers
# =============================================================================
def _looks_like_explicit_row_dict(d: Any) -> bool:
    if not isinstance(d, dict) or not d:
        return False

    keyset = set(str(k) for k in d.keys())

    if keyset & {"symbol", "ticker", "code", "requested_symbol"}:
        return True
    if {"sheet", "header", "key"}.issubset(keyset):
        return True
    if {"section", "item"}.issubset(keyset):
        return True
    if {"top10_rank", "selection_reason"}.issubset(keyset):
        return True

    return False


def _payload_keys_like(payload: Mapping[str, Any]) -> List[str]:
    for name in ("keys", "columns", "fields"):
        keys = payload.get(name)
        if isinstance(keys, list):
            out = [_s(k) for k in keys if _s(k)]
            if out:
                return out

    headers = (
        payload.get("headers")
        or payload.get("display_headers")
        or payload.get("sheet_headers")
        or payload.get("column_headers")
    )
    if isinstance(headers, list):
        out = [_header_to_key(h) for h in headers if _header_to_key(h)]
        if out:
            return out

    return []


def _rows_from_matrix(rows_matrix: Any, cols: Sequence[str]) -> List[Dict[str, Any]]:
    if not isinstance(rows_matrix, list) or not rows_matrix or not cols:
        return []
    keys = [_s(c) for c in cols if _s(c)]
    if not keys:
        return []

    out: List[Dict[str, Any]] = []
    for row in rows_matrix:
        if not isinstance(row, (list, tuple)):
            continue
        vals = list(row)
        out.append({keys[i]: (vals[i] if i < len(vals) else None) for i in range(len(keys))})
    return out


def _extract_rows_like(payload: Any, depth: int = 0) -> List[Dict[str, Any]]:
    if payload is None or depth > 8:
        return []

    if isinstance(payload, list):
        if not payload:
            return []
        if all(isinstance(x, dict) for x in payload):
            return [dict(x) for x in payload if isinstance(x, dict)]

        model_rows = [_model_to_dict(x) for x in payload]
        model_rows = [r for r in model_rows if isinstance(r, dict) and r]
        if model_rows and all(_looks_like_explicit_row_dict(r) or _normalize_symbol(r.get("symbol") or r.get("ticker")) for r in model_rows):
            return model_rows

        return []

    if not isinstance(payload, Mapping):
        d = _model_to_dict(payload)
        return [d] if _looks_like_explicit_row_dict(d) else []

    if _looks_like_explicit_row_dict(payload):
        return [dict(payload)]

    maybe_symbol_map = True
    rows_from_symbol_map: List[Dict[str, Any]] = []
    symbol_like_keys = 0
    for k, v in payload.items():
        if not isinstance(v, dict):
            maybe_symbol_map = False
            break
        if not _looks_like_symbol_token(k):
            maybe_symbol_map = False
            break
        symbol_like_keys += 1
        row = dict(v)
        if _is_blank(row.get("symbol")) and _is_blank(row.get("ticker")):
            row["symbol"] = _normalize_symbol(k)
            row["ticker"] = _normalize_symbol(k)
        rows_from_symbol_map.append(row)
    if maybe_symbol_map and symbol_like_keys > 0 and rows_from_symbol_map:
        return rows_from_symbol_map

    for key in ("rows", "recommendations", "data", "items", "results", "records", "quotes"):
        value = payload.get(key)

        if isinstance(value, list):
            rows = _extract_rows_like(value, depth + 1)
            if rows:
                return rows

            if value and any(isinstance(x, (list, tuple)) for x in value):
                keys_like = _payload_keys_like(payload)
                rows = _rows_from_matrix(value, keys_like)
                if rows:
                    return rows

        if isinstance(value, Mapping):
            rows = _extract_rows_like(value, depth + 1)
            if rows:
                return rows

    rows_matrix = payload.get("rows_matrix") or payload.get("matrix")
    if isinstance(rows_matrix, list):
        keys_like = _payload_keys_like(payload)
        rows = _rows_from_matrix(rows_matrix, keys_like)
        if rows:
            return rows

    for key in ("payload", "result", "response", "output", "data"):
        value = payload.get(key)
        if value is not None and value is not payload:
            rows = _extract_rows_like(value, depth + 1)
            if rows:
                return rows

    return []


async def _call_engine_method(
    engine: Any,
    method_names: Sequence[str],
    attempts: Sequence[Tuple[Tuple[Any, ...], Dict[str, Any]]],
) -> Any:
    if engine is None:
        return None

    last_exc: Optional[Exception] = None

    for method_name in method_names:
        fn = getattr(engine, method_name, None)
        if not callable(fn):
            continue

        for args, kwargs in attempts:
            try:
                return await _call_maybe_async(fn, *args, **kwargs)
            except TypeError as exc:
                last_exc = exc
                continue
            except Exception as exc:
                last_exc = exc
                continue

    if last_exc is not None:
        logger.debug("Engine call attempts exhausted: %s", last_exc)
    return None


async def _fetch_page_rows(engine: Any, page: str, limit: int, mode: str) -> List[Dict[str, Any]]:
    body = {
        "page": page,
        "page_name": page,
        "sheet": page,
        "sheet_name": page,
        "tab": page,
        "worksheet": page,
        "name": page,
        "limit": limit,
        "top_n": limit,
        "mode": mode or "",
        "include_headers": True,
        "include_matrix": True,
        "schema_only": False,
        "headers_only": False,
    }

    attempts = [
        ((), {"page": page, "sheet": page, "sheet_name": page, "limit": limit, "mode": mode or "", "body": body}),
        ((), {"page_name": page, "sheet_name": page, "limit": limit, "mode": mode or "", "body": body}),
        ((), {"payload": body}),
        ((), {"body": body}),
        ((), {"page": page, "sheet": page, "sheet_name": page, "limit": limit, "mode": mode or ""}),
        ((), {"page_name": page, "sheet_name": page, "limit": limit, "mode": mode or ""}),
        ((), {"page": page, "sheet": page, "limit": limit, "mode": mode or ""}),
        ((), {"page": page, "limit": limit, "mode": mode or ""}),
        ((page,), {"limit": limit, "mode": mode or ""}),
        ((page,), {"limit": limit}),
        ((page,), {}),
    ]

    payload = await _call_engine_method(
        engine,
        (
            "get_sheet_rows",
            "get_page_rows",
            "sheet_rows",
            "build_sheet_rows",
            "execute_sheet_rows",
            "run_sheet_rows",
            "build_analysis_sheet_rows",
            "run_analysis_sheet_rows",
            "get_rows_for_sheet",
            "get_rows_for_page",
        ),
        attempts,
    )

    return _extract_rows_like(payload)


async def _fetch_page_snapshot_rows(engine: Any, page: str) -> List[Dict[str, Any]]:
    attempts = [
        ((), {"sheet_name": page}),
        ((), {"sheet": page}),
        ((), {"page": page}),
        ((), {"page_name": page}),
        ((page,), {}),
    ]

    payload = await _call_engine_method(
        engine,
        (
            "get_cached_sheet_snapshot",
            "get_sheet_snapshot",
            "get_cached_sheet_rows",
            "get_page_snapshot",
            "get_sheet_cache",
        ),
        attempts,
    )

    return _extract_rows_like(payload)


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
        (
            "get_enriched_quotes_batch",
            "get_analysis_quotes_batch",
            "get_quotes_batch",
            "quotes_batch",
            "get_quotes",
        ),
        attempts,
    )

    rows = _extract_rows_like(payload)
    if rows:
        return rows

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
        elif isinstance(row_payload, Mapping):
            d = dict(row_payload)
            if d:
                out.append(d)
        else:
            d = _model_to_dict(row_payload)
            if d:
                out.append(d)

    return out


# =============================================================================
# Criteria normalization
# =============================================================================
def _merge_mapping_like(*parts: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for part in parts:
        if isinstance(part, Mapping):
            out.update(dict(part))
    return out


def _collect_symbol_keys_from_mapping(mapping: Mapping[str, Any]) -> List[str]:
    out: List[str] = []
    for k, v in mapping.items():
        if isinstance(v, Mapping) and _looks_like_symbol_token(k):
            out.append(_normalize_symbol(k))
    return _dedupe_keep_order(out)


def _collect_criteria_from_inputs(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    positional_maps = [arg for arg in args if isinstance(arg, Mapping)]
    body = _merge_mapping_like(
        *positional_maps,
        kwargs.get("body"),
        kwargs.get("payload"),
        kwargs.get("request_data"),
        kwargs.get("params"),
    )

    criteria: Dict[str, Any] = {}
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
        "page_name",
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
        "headers_only",
        "include_headers",
        "include_matrix",
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
        or _normalize_list(criteria.get("page_name"))
        or _normalize_list(criteria.get("sheet"))
        or _normalize_list(criteria.get("sheet_name"))
    )
    pages = _safe_source_pages([_normalize_page_name_safe(p) for p in pages])

    if not pages:
        pages = _safe_source_pages(DEFAULT_SOURCE_PAGES)

    direct_symbols = _normalize_list(
        criteria.get("direct_symbols") or criteria.get("symbols") or criteria.get("tickers")
    )
    if not direct_symbols and isinstance(body, Mapping):
        direct_symbols = _collect_symbol_keys_from_mapping(body)

    direct_symbols = [_normalize_symbol(s) for s in direct_symbols if _normalize_symbol(s)]

    limit = _safe_int(criteria.get("limit") or criteria.get("top_n") or kwargs.get("limit"), 10)
    limit = max(1, min(limit, _safe_int(os.getenv("TOP10_SELECTOR_MAX_LIMIT", "50"), 50)))

    horizon_days = _safe_int(
        criteria.get("horizon_days")
        or criteria.get("invest_period_days")
        or criteria.get("investment_period_days"),
        90,
    )

    risk_level = _s(criteria.get("risk_level") or criteria.get("risk_profile") or "").lower()
    confidence_bucket = _s(criteria.get("confidence_bucket") or criteria.get("confidence_level") or "").lower()

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
    normalized["schema_only"] = _coerce_bool(normalized.get("schema_only"), False)
    normalized["headers_only"] = _coerce_bool(normalized.get("headers_only"), False)
    normalized["include_headers"] = _coerce_bool(normalized.get("include_headers"), True)
    normalized["include_matrix"] = _coerce_bool(normalized.get("include_matrix"), True)
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

    sym = _normalize_symbol(row.get("symbol") or row.get("ticker") or row.get("requested_symbol"))
    if sym:
        row["symbol"] = sym
        row.setdefault("ticker", sym)

    return row


def _row_richness(row: Mapping[str, Any]) -> int:
    return sum(1 for _, v in row.items() if not _is_blank(v))


def _choose_horizon_roi(row: Mapping[str, Any], horizon_days: int) -> Optional[float]:
    if horizon_days <= 31:
        return (
            _safe_ratio(row.get("expected_roi_1m"), None)
            or _safe_ratio(row.get("expected_roi_3m"), None)
            or _safe_ratio(row.get("expected_roi_12m"), None)
        )
    if horizon_days <= 92:
        return (
            _safe_ratio(row.get("expected_roi_3m"), None)
            or _safe_ratio(row.get("expected_roi_12m"), None)
            or _safe_ratio(row.get("expected_roi_1m"), None)
        )
    return (
        _safe_ratio(row.get("expected_roi_12m"), None)
        or _safe_ratio(row.get("expected_roi_3m"), None)
        or _safe_ratio(row.get("expected_roi_1m"), None)
    )


def _confidence_bucket_match(row: Mapping[str, Any], wanted: str) -> bool:
    if not wanted:
        return True

    row_bucket = _s(row.get("confidence_bucket") or row.get("confidence_level")).lower()
    if row_bucket:
        return wanted in row_bucket or row_bucket in wanted

    score = _safe_float(row.get("forecast_confidence") or row.get("confidence_score"), None)
    if score is None:
        return True

    if score <= 1.0:
        score *= 100.0

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
    if min_roi is not None:
        if roi is None:
            return False
        if roi < min_roi:
            return False

    min_conf = _safe_float(criteria.get("min_confidence") or criteria.get("min_ai_confidence"), None)
    row_conf = _safe_float(row.get("forecast_confidence") or row.get("confidence_score"), None)
    if row_conf is not None and row_conf <= 1.0:
        row_conf *= 100.0
    if min_conf is not None:
        if row_conf is None:
            return False
        if row_conf < min_conf:
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
    horizon_days = _safe_int(criteria.get("horizon_days") or criteria.get("invest_period_days"), 90)
    roi = _choose_horizon_roi(row, horizon_days)

    if conf is not None and conf <= 1.0:
        conf *= 100.0

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

    score += min(_row_richness(row), 120) * 0.03
    return float(score)


def _canonical_selection_reason(row: Dict[str, Any], criteria: Mapping[str, Any]) -> str:
    recommendation = _s(row.get("recommendation"))
    confidence_bucket = _s(row.get("confidence_bucket"))
    risk_bucket = _s(row.get("risk_bucket"))
    source_page = _s(row.get("source_page"))
    horizon_days = _safe_int(criteria.get("horizon_days") or criteria.get("invest_period_days"), 90)
    horizon_roi = _choose_horizon_roi(row, horizon_days)

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

    parts: List[str] = []
    if recommendation:
        parts.append(f"Recommendation={recommendation}")
    if confidence_bucket:
        parts.append(f"Confidence={confidence_bucket}")
    if risk_bucket:
        parts.append(f"Risk={risk_bucket}")
    if horizon_roi is not None:
        parts.append(f"Horizon ROI={round(horizon_roi * 100.0, 2)}%")
    if source_page:
        parts.append(f"Source={source_page}")
    if score_parts:
        parts.append(", ".join(score_parts[:3]))

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
            row["selection_reason"] = _canonical_selection_reason(row, criteria)
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
        "engine_source": _ENGINE_CACHE_SOURCE or "",
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

    if direct_symbols:
        direct_rows = await _fetch_direct_symbol_rows(engine, direct_symbols, mode)
        meta["direct_symbol_rows"] = len(direct_rows)
        for row in direct_rows:
            _put_row(row, "")

    for page in pages:
        rows = await _fetch_page_rows(engine, page, per_page_limit, mode)
        meta["source_page_rows"][page] = len(rows)

        if not rows:
            snap_rows = await _fetch_page_snapshot_rows(engine, page)
            meta["snapshot_rows"][page] = len(snap_rows)
            rows = snap_rows

        if rows:
            symbols_from_page = _dedupe_keep_order(
                _normalize_symbol(r.get("symbol") or r.get("ticker") or r.get("requested_symbol")) for r in rows
            )
            enriched_rows = await _fetch_direct_symbol_rows(engine, symbols_from_page, mode)
            if enriched_rows:
                page_map = {
                    _normalize_symbol(r.get("symbol") or r.get("ticker") or r.get("requested_symbol")): dict(r)
                    for r in rows
                    if _normalize_symbol(r.get("symbol") or r.get("ticker") or r.get("requested_symbol"))
                }
                for er in enriched_rows:
                    sym = _normalize_symbol(er.get("symbol") or er.get("ticker"))
                    if sym:
                        base = page_map.get(sym, {})
                        merged = dict(base)
                        for k, v in dict(er).items():
                            if v not in (None, "", [], {}):
                                merged[k] = v
                        page_map[sym] = merged
                rows = list(page_map.values())

        for row in rows:
            _put_row(row, page)

    if not candidates:
        fallback_rows = await _fetch_page_rows(engine, OUTPUT_PAGE, max(50, _safe_int(criteria.get("limit"), 10) * 3), mode)
        meta["top10_output_fallback_rows"] = len(fallback_rows)
        if not fallback_rows:
            fallback_rows = await _fetch_page_snapshot_rows(engine, OUTPUT_PAGE)
            meta["top10_output_snapshot_rows"] = len(fallback_rows)
        for row in fallback_rows:
            _put_row(row, OUTPUT_PAGE)

    meta["deduped_candidate_count"] = len(candidates)
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
    include_headers = _coerce_bool(meta.get("include_headers", True), True)
    include_matrix = _coerce_bool(meta.get("include_matrix", True), True)

    payload = {
        "status": status,
        "page": OUTPUT_PAGE,
        "sheet": OUTPUT_PAGE,
        "sheet_name": OUTPUT_PAGE,
        "route_family": "top10",
        "headers": headers if include_headers else [],
        "display_headers": headers if include_headers else [],
        "sheet_headers": headers if include_headers else [],
        "column_headers": headers if include_headers else [],
        "keys": keys,
        "columns": keys,
        "fields": keys,
        "rows": rows,
        "data": rows,
        "items": rows,
        "quotes": rows,
        "rows_matrix": _rows_to_matrix(rows, keys) if include_matrix else [],
        "version": TOP10_SELECTOR_VERSION,
        "meta": meta,
    }
    return _json_safe(payload)


# =============================================================================
# Core async implementation
# =============================================================================
async def _build_top10_rows_async(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    started = time.perf_counter()
    headers, keys = _load_schema_defaults()
    criteria = _collect_criteria_from_inputs(*args, **kwargs)
    mode = _s(kwargs.get("mode") or criteria.get("mode") or "")
    limit = max(1, _safe_int(criteria.get("limit") or kwargs.get("limit"), 10))

    if criteria.get("schema_only") or criteria.get("headers_only"):
        return _build_payload(
            status="success",
            headers=headers,
            keys=keys,
            rows=[],
            meta={
                "build_status": "OK",
                "dispatch": "top10_selector",
                "selector_version": TOP10_SELECTOR_VERSION,
                "schema_only": bool(criteria.get("schema_only")),
                "headers_only": bool(criteria.get("headers_only")),
                "criteria_used": _json_safe(criteria),
                "include_headers": criteria.get("include_headers", True),
                "include_matrix": criteria.get("include_matrix", True),
                "duration_ms": round((time.perf_counter() - started) * 1000.0, 3),
            },
        )

    engine, engine_source = await _resolve_engine(*args, **kwargs)

    if engine is None:
        return _build_payload(
            status="warn",
            headers=headers,
            keys=keys,
            rows=[],
            meta={
                "build_status": "DEGRADED",
                "dispatch": "top10_selector",
                "selector_version": TOP10_SELECTOR_VERSION,
                "warning": "engine_unavailable",
                "criteria_used": _json_safe(criteria),
                "include_headers": criteria.get("include_headers", True),
                "include_matrix": criteria.get("include_matrix", True),
                "engine_source": engine_source,
                "duration_ms": round((time.perf_counter() - started) * 1000.0, 3),
            },
        )

    try:
        candidates, collect_meta = await _collect_candidate_rows(engine, criteria, mode)
    except Exception as exc:
        logger.warning("Top10 candidate collection failed: %s", exc)
        return _build_payload(
            status="warn",
            headers=headers,
            keys=keys,
            rows=[],
            meta={
                "build_status": "DEGRADED",
                "dispatch": "top10_selector",
                "selector_version": TOP10_SELECTOR_VERSION,
                "warning": f"candidate_collection_failed:{type(exc).__name__}",
                "criteria_used": _json_safe(criteria),
                "include_headers": criteria.get("include_headers", True),
                "include_matrix": criteria.get("include_matrix", True),
                "engine_source": engine_source,
                "duration_ms": round((time.perf_counter() - started) * 1000.0, 3),
            },
        )

    filtered = [r for r in candidates if _passes_filters(r, criteria)]

    filter_relaxed = False
    selected_pool = filtered
    if not selected_pool and candidates:
        selected_pool = list(candidates)
        filter_relaxed = True

    horizon_days = _safe_int(criteria.get("horizon_days") or criteria.get("invest_period_days"), 90)

    scored: List[Tuple[float, Dict[str, Any]]] = []
    for row in selected_pool:
        scored.append((_selector_score(row, criteria), dict(row)))

    scored.sort(
        key=lambda x: (
            x[0],
            _choose_horizon_roi(x[1], horizon_days) or 0.0,
            _safe_float(x[1].get("opportunity_score"), 0.0) or 0.0,
            _safe_float(x[1].get("overall_score"), 0.0) or 0.0,
            (_safe_float(x[1].get("forecast_confidence"), 0.0) or 0.0),
            -(_safe_float(x[1].get("risk_score"), 999.0) or 999.0),
            _safe_float(x[1].get("liquidity_score"), 0.0) or 0.0,
            _row_richness(x[1]),
        ),
        reverse=True,
    )

    top_rows = [row for _, row in scored[:limit]]
    projected_rows = _rank_and_project_rows(top_rows, keys, criteria)

    status = "success" if projected_rows else "warn"

    meta = {
        "build_status": "OK" if projected_rows else "WARN",
        "dispatch": "top10_selector",
        "selector_version": TOP10_SELECTOR_VERSION,
        "criteria_used": _json_safe(criteria),
        "candidate_count": len(candidates),
        "filtered_count": len(filtered),
        "selected_count": len(projected_rows),
        "filter_relaxed": filter_relaxed,
        "include_headers": criteria.get("include_headers", True),
        "include_matrix": criteria.get("include_matrix", True),
        "engine_source": engine_source,
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


# =============================================================================
# Public API (sync+async tolerant)
# =============================================================================
def build_top10_rows(*args: Any, **kwargs: Any) -> Any:
    coro = _build_top10_rows_async(*args, **kwargs)
    try:
        asyncio.get_running_loop()
        return coro
    except RuntimeError:
        return asyncio.run(coro)


def build_top10_output_rows(*args: Any, **kwargs: Any) -> Any:
    return build_top10_rows(*args, **kwargs)


def build_top10_investments_rows(*args: Any, **kwargs: Any) -> Any:
    return build_top10_rows(*args, **kwargs)


def build_top_10_investments_rows(*args: Any, **kwargs: Any) -> Any:
    return build_top10_rows(*args, **kwargs)


def get_top10_rows(*args: Any, **kwargs: Any) -> Any:
    return build_top10_rows(*args, **kwargs)


def select_top10(*args: Any, **kwargs: Any) -> Any:
    return build_top10_rows(*args, **kwargs)


def select_top10_symbols(*args: Any, **kwargs: Any) -> Any:
    async def _inner() -> List[str]:
        payload = await _build_top10_rows_async(*args, **kwargs)
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

    try:
        asyncio.get_running_loop()
        return _inner()
    except RuntimeError:
        return asyncio.run(_inner())


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
