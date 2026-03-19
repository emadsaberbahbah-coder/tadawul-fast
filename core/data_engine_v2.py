#!/usr/bin/env python3
# core/data_engine_v2.py
"""
================================================================================
Data Engine V2 — GLOBAL-FIRST ORCHESTRATOR — v5.35.0
================================================================================

WHY v5.35.0
-----------
- FIX: introduces `_looks_like_explicit_row_dict()` to strictly prevent nested wrapper dicts from being coerced into fake rows.
- FIX: tightens symbol-map detection in `_coerce_rows_list()` to prevent treating non-symbol dict keys as rows.
- FIX: adds `columns` and `fields` aliases to `_finalize_payload()` for maximum route tolerance.
- FIX: makes Top10 ROI fallback filter horizon-aware (1m, 3m, 12m).
- FIX: exposes singleton engine through module-level aliases ENGINE, engine, _ENGINE
- FIX: adds compatibility aliases for sheet-row and snapshot methods expected by route wrappers/selectors
- FIX: Top10 engine path degrades more safely and supports richer fallback recovery with criteria filtering
- FIX: known sheet schema recovery is explicit and status/meta correctly reflect degraded paths
- FIX: optional support path for My_Investments as an instrument-style page

Provider routing constraint (unchanged)
---------------------------------------
- GLOBAL: EODHD primary + Yahoo/Finnhub fallback
- KSA: Tadawul + Argaam + Yahoo chart/history
================================================================================
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import logging
import math
import os
import pickle
import re
import sys
import time
import zlib
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone
from decimal import Decimal
from enum import Enum
from importlib import import_module
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple, Union

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

__version__ = "5.35.0"

logger = logging.getLogger("core.data_engine_v2")
logger.addHandler(logging.NullHandler())


# =============================================================================
# Small universal helpers
# =============================================================================
def _safe_str(x: Any, default: str = "") -> str:
    if x is None:
        return default
    try:
        s = str(x).strip()
        return s if s else default
    except Exception:
        return default


def _norm_key(x: Any) -> str:
    s = _safe_str(x).lower()
    if not s:
        return ""
    s = s.replace("-", "_").replace("/", "_").replace("&", "_")
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"__+", "_", s).strip("_")
    return s


def _norm_key_loose(x: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", _safe_str(x).lower())


def _dedupe_keep_order(items: Sequence[Any]) -> List[Any]:
    out: List[Any] = []
    seen: Set[Any] = set()
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


# =============================================================================
# JSON helpers
# =============================================================================
def _json_safe(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, str)):
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

    if isinstance(value, (datetime, date, dt_time)):
        try:
            return value.isoformat()
        except Exception:
            return str(value)

    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="replace")
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


try:
    import orjson  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)

    def json_dumps(obj: Any) -> str:
        return orjson.dumps(_json_safe(obj)).decode("utf-8")

except Exception:
    import json  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)

    def json_dumps(obj: Any) -> str:
        return json.dumps(_json_safe(obj), ensure_ascii=False, separators=(",", ":"), default=str)


# =============================================================================
# Schema
# =============================================================================
TOP10_REQUIRED_FIELDS: Tuple[str, ...] = (
    "top10_rank",
    "selection_reason",
    "criteria_snapshot",
)

TOP10_REQUIRED_HEADERS: Dict[str, str] = {
    "top10_rank": "Top10 Rank",
    "selection_reason": "Selection Reason",
    "criteria_snapshot": "Criteria Snapshot",
}

try:
    from core.sheets.schema_registry import SCHEMA_REGISTRY as _RAW_SCHEMA_REGISTRY  # type: ignore
    from core.sheets.schema_registry import get_sheet_spec as _RAW_GET_SHEET_SPEC  # type: ignore

    SCHEMA_REGISTRY = _RAW_SCHEMA_REGISTRY  # type: ignore
    _SCHEMA_AVAILABLE = True
except Exception:
    SCHEMA_REGISTRY = {}  # type: ignore
    _SCHEMA_AVAILABLE = False
    _RAW_GET_SHEET_SPEC = None  # type: ignore


def _complete_schema_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    raw_headers = list(headers or [])
    raw_keys = list(keys or [])
    max_len = max(len(raw_headers), len(raw_keys))

    hdrs: List[str] = []
    ks: List[str] = []

    for i in range(max_len):
        h = _safe_str(raw_headers[i]) if i < len(raw_headers) else ""
        k = _safe_str(raw_keys[i]) if i < len(raw_keys) else ""

        if h and not k:
            k = _norm_key(h)
        elif k and not h:
            h = k.replace("_", " ").title()
        elif not h and not k:
            h = f"Column_{i+1}"
            k = f"key_{i+1}"

        hdrs.append(h)
        ks.append(k)

    return hdrs, ks


def _usable_contract(headers: Sequence[str], keys: Sequence[str], sheet_name: str = "") -> bool:
    if not headers or not keys:
        return False
    if len(headers) != len(keys) or len(headers) == 0:
        return False
        
    if sheet_name:
        canon = _canonicalize_sheet_name(sheet_name)
        keyset = set(keys)

        if canon in {"Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds", "My_Portfolio", "My_Investments"}:
            has_symbol_anchor = bool(keyset & {"symbol", "requested_symbol", "ticker"})
            has_identity_anchor = bool(keyset & {"name", "current_price", "price"})
            if not has_symbol_anchor or not has_identity_anchor:
                return False

        elif canon == "Top_10_Investments":
            has_symbol_anchor = bool(keyset & {"symbol", "requested_symbol", "ticker"})
            has_required_top10 = {"top10_rank", "selection_reason", "criteria_snapshot"}.issubset(keyset)
            if not has_symbol_anchor or not has_required_top10:
                return False

        elif canon == "Data_Dictionary":
            has_required_dd = {"sheet", "header", "key"}.issubset(keyset)
            if not has_required_dd:
                return False

        elif canon == "Insights_Analysis":
            if not bool(keyset & {"section", "item", "value", "metric", "symbol"}):
                return False

    return True


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
        
    cols_fields = getattr(spec, "fields", None)
    if isinstance(cols_fields, list) and cols_fields:
        return cols_fields

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
    if isinstance(spec, dict) and len(spec) == 1 and not any(k in spec for k in ("columns", "fields", "headers", "keys", "display_headers")):
        first_val = list(spec.values())[0]
        if isinstance(first_val, dict):
            spec = first_val

    cols = _schema_columns_from_any(spec)
    headers: List[str] = []
    keys: List[str] = []

    for c in cols:
        if isinstance(c, Mapping):
            h = _safe_str(c.get("header") or c.get("display_header") or c.get("displayHeader") or c.get("label") or c.get("title"))
            k = _safe_str(c.get("key") or c.get("field") or c.get("name") or c.get("id"))
        else:
            h = _safe_str(getattr(c, "header", getattr(c, "display_header", getattr(c, "displayHeader", getattr(c, "label", getattr(c, "title", None))))))
            k = _safe_str(getattr(c, "key", getattr(c, "field", getattr(c, "name", getattr(c, "id", None)))))

        if h or k:
            headers.append(h or k.replace("_", " ").title())
            keys.append(k or _norm_key(h))

    if not headers and not keys and isinstance(spec, Mapping):
        headers2 = spec.get("headers") or spec.get("display_headers")
        keys2 = spec.get("keys") or spec.get("fields")
        if isinstance(headers2, list):
            headers = [_safe_str(x) for x in headers2 if _safe_str(x)]
        if isinstance(keys2, list):
            keys = [_safe_str(x) for x in keys2 if _safe_str(x)]

    return _complete_schema_contract(headers, keys)


def _ensure_top10_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    hdrs = list(headers or [])
    ks = list(keys or [])

    if not hdrs and not ks:
        return hdrs, ks

    for field in TOP10_REQUIRED_FIELDS:
        if field not in ks:
            ks.append(field)
            hdrs.append(TOP10_REQUIRED_HEADERS.get(field, field))

    return _complete_schema_contract(hdrs, ks)


def _registry_sheet_lookup(sheet: str) -> Any:
    if not isinstance(SCHEMA_REGISTRY, dict) or not SCHEMA_REGISTRY:
        return None

    candidates = [
        sheet,
        sheet.replace(" ", "_"),
        sheet.replace("_", " "),
        _norm_key(sheet),
        _norm_key(sheet).replace("_", " ").title().replace(" ", "_"),
        _norm_key_loose(sheet),
    ]

    registry_by_norm: Dict[str, Any] = {}
    for k, v in SCHEMA_REGISTRY.items():
        registry_by_norm[_norm_key(k)] = v
        registry_by_norm[_norm_key_loose(k)] = v

    for cand in candidates:
        if cand in SCHEMA_REGISTRY:
            return SCHEMA_REGISTRY.get(cand)
        nk = _norm_key(cand)
        if nk in registry_by_norm:
            return registry_by_norm[nk]
        nkl = _norm_key_loose(cand)
        if nkl in registry_by_norm:
            return registry_by_norm[nkl]

    return None


def get_sheet_spec(sheet: str) -> Any:  # type: ignore
    sheet2 = _safe_str(sheet)
    if not sheet2:
        raise KeyError("empty sheet name")

    if callable(_RAW_GET_SHEET_SPEC):
        direct_candidates = _dedupe_keep_order(
            [
                sheet2,
                sheet2.replace(" ", "_"),
                sheet2.replace("_", " "),
                _norm_key(sheet2),
                _norm_key(sheet2).replace("_", " ").title().replace(" ", "_"),
            ]
        )
        for cand in direct_candidates:
            try:
                return _RAW_GET_SHEET_SPEC(cand)  # type: ignore[misc]
            except Exception:
                continue

    spec = _registry_sheet_lookup(sheet2)
    if spec is not None:
        return spec

    raise KeyError(f"Unknown sheet spec: {sheet2}")


DEFAULT_ENGINE_KEYS: List[str] = [
    "symbol",
    "symbol_normalized",
    "requested_symbol",
    "name",
    "exchange",
    "currency",
    "asset_class",
    "country",
    "sector",
    "industry",
    "current_price",
    "price",
    "previous_close",
    "open_price",
    "day_high",
    "day_low",
    "week_52_high",
    "week_52_low",
    "volume",
    "avg_volume_10d",
    "avg_volume_30d",
    "market_cap",
    "float_shares",
    "beta_5y",
    "price_change",
    "percent_change",
    "week_52_position_pct",
    "change",
    "change_pct",
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
    "data_quality",
    "error",
    "warning",
    "warnings",
    "info",
    "data_sources",
    "provider_latency",
    "last_updated_utc",
    "last_updated_riyadh",
    "top10_rank",
    "selection_reason",
    "criteria_snapshot",
]


def _build_union_schema_keys() -> List[str]:
    keys: List[str] = []
    seen: Set[str] = set()

    if isinstance(SCHEMA_REGISTRY, dict) and SCHEMA_REGISTRY:
        for _, spec in SCHEMA_REGISTRY.items():
            try:
                _headers, spec_keys = _schema_keys_headers_from_spec(spec)
                for k in spec_keys:
                    kk = _safe_str(k)
                    if kk and kk not in seen:
                        seen.add(kk)
                        keys.append(kk)
            except Exception:
                continue

    for required in TOP10_REQUIRED_FIELDS:
        if required not in seen:
            seen.add(required)
            keys.append(required)

    for k in DEFAULT_ENGINE_KEYS:
        if k not in seen:
            seen.add(k)
            keys.append(k)

    return keys


_SCHEMA_UNION_KEYS: List[str] = _build_union_schema_keys()


def _row_alias_candidates(key: str, header: str = "") -> List[str]:
    key2 = _safe_str(key)
    header2 = _safe_str(header)

    vals = [
        key2,
        header2,
        key2.lower(),
        header2.lower(),
        key2.upper(),
        header2.upper(),
        key2.replace("_", " "),
        header2.replace("_", " "),
        key2.replace("_", ""),
        header2.replace(" ", "_"),
        _norm_key(key2),
        _norm_key(header2),
    ]

    alias_map: Dict[str, List[str]] = {
        "current_price": ["price", "last_price", "last", "close", "market_price", "current"],
        "price": ["current_price", "last", "close"],
        "previous_close": ["prev_close", "close_prev"],
        "open_price": ["open"],
        "day_high": ["high"],
        "day_low": ["low"],
        "week_52_high": ["52w_high", "high_52w"],
        "week_52_low": ["52w_low", "low_52w"],
        "price_change": ["change"],
        "percent_change": ["change_pct", "pct_change", "change_percent"],
        "name": ["company_name", "long_name", "instrument_name", "security_name"],
        "exchange": ["market", "mic"],
        "currency": ["ccy"],
        "country": ["country_name"],
        "sector": ["sector_name", "gics_sector"],
        "industry": ["industry_name", "gics_industry"],
        "pb_ratio": ["pb", "p_b", "price_to_book"],
        "ps_ratio": ["ps", "p_s", "price_to_sales"],
        "peg_ratio": ["peg"],
        "forecast_confidence": ["confidence", "confidence_score"],
        "top10_rank": ["rank", "top_rank"],
        "selection_reason": ["selection_notes", "selector_reason"],
        "criteria_snapshot": ["criteria", "criteria_json", "snapshot"],
    }

    for alias in alias_map.get(key2, []):
        vals.extend(
            [
                alias,
                alias.lower(),
                alias.upper(),
                alias.replace("_", " "),
                _norm_key(alias),
            ]
        )

    out: List[str] = []
    seen: Set[str] = set()
    for v in vals:
        s = _safe_str(v)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def normalize_row_to_schema(schema: Any, rowdict: Dict[str, Any], *, keep_extras: bool = True) -> Dict[str, Any]:
    raw = dict(rowdict or {})

    keys: List[str] = []
    headers: List[str] = []

    if schema is None:
        keys = list(_SCHEMA_UNION_KEYS)
        headers = list(_SCHEMA_UNION_KEYS)
    elif isinstance(schema, str):
        try:
            spec = get_sheet_spec(schema)
            headers, keys = _schema_keys_headers_from_spec(spec)
        except Exception:
            keys = list(_SCHEMA_UNION_KEYS)
            headers = list(_SCHEMA_UNION_KEYS)

        if _norm_key(schema) == _norm_key("Top_10_Investments"):
            headers, keys = _ensure_top10_contract(headers, keys)

    elif isinstance(schema, (list, tuple)):
        keys = [_safe_str(k) for k in schema if _safe_str(k)]
        headers = list(keys)

    else:
        headers, keys = _schema_keys_headers_from_spec(schema)
        if not keys:
            keys = list(_SCHEMA_UNION_KEYS)
            headers = list(_SCHEMA_UNION_KEYS)

    headers, keys = _complete_schema_contract(headers, keys)

    raw_ci = {str(k).strip().lower(): v for k, v in raw.items()}
    raw_loose = {_norm_key_loose(k): v for k, v in raw.items()}

    out: Dict[str, Any] = {}
    for idx, k in enumerate(keys):
        h = headers[idx] if idx < len(headers) else k
        value = None
        for cand in _row_alias_candidates(k, h):
            if cand in raw:
                value = raw.get(cand)
                break
            lc = cand.lower()
            if lc in raw_ci:
                value = raw_ci.get(lc)
                break
            loose = _norm_key_loose(cand)
            if loose in raw_loose:
                value = raw_loose.get(loose)
                break
        out[k] = value

    if keep_extras:
        for k, v in raw.items():
            if k not in out:
                out[k] = v

    return out


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
        try:
            d = obj.model_dump()
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

    return {"result": obj}


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


def _coerce_rows_list(out: Any) -> List[Dict[str, Any]]:
    if out is None:
        return []

    if isinstance(out, dict) and out:
        maybe_symbol_map = True
        rows_from_map: List[Dict[str, Any]] = []
        symbol_like_keys = 0

        for k, v in out.items():
            if not isinstance(v, dict):
                maybe_symbol_map = False
                break
            if not _looks_like_symbol_token(k):
                maybe_symbol_map = False
                break

            symbol_like_keys += 1
            row = dict(v)
            if not row.get("symbol") and not row.get("ticker"):
                row["symbol"] = str(k).strip()
                row["ticker"] = str(k).strip()
            rows_from_map.append(row)

        if maybe_symbol_map and symbol_like_keys > 0 and rows_from_map:
            return rows_from_map

    if isinstance(out, list):
        if not out:
            return []
        if isinstance(out[0], dict):
            return [dict(r) for r in out if isinstance(r, dict)]
        if isinstance(out[0], (list, tuple)):
            return []
        return [_model_to_dict(r) for r in out]

    if isinstance(out, dict):
        for key in ("rows", "data", "items", "records", "payload", "result", "quotes"):
            r2 = out.get(key)

            if isinstance(r2, list):
                if not r2:
                    continue
                if isinstance(r2[0], dict):
                    return [dict(r) for r in r2 if isinstance(r, dict)]
                if isinstance(r2[0], (list, tuple)):
                    cols = out.get("keys") or out.get("headers") or out.get("columns") or []
                    if isinstance(cols, list) and cols:
                        return _rows_from_matrix(r2, cols)
                    continue
                return [_model_to_dict(r) for r in r2]

            if isinstance(r2, dict):
                nested_rows = _coerce_rows_list(r2)
                if nested_rows and all(_looks_like_explicit_row_dict(x) for x in nested_rows):
                    return nested_rows

        rows_matrix = out.get("rows_matrix") or out.get("matrix")
        if isinstance(rows_matrix, list):
            cols = out.get("keys") or out.get("headers") or out.get("columns") or []
            if isinstance(cols, list) and cols:
                return _rows_from_matrix(rows_matrix, cols)

        if _looks_like_explicit_row_dict(out):
            return [dict(out)]

        return []

    try:
        d = _model_to_dict(out)
        return [d] if _looks_like_explicit_row_dict(d) else []
    except Exception:
        return []


def _extract_symbols_from_rows(rows: Sequence[Dict[str, Any]], limit: int = 5000) -> List[str]:
    raw: List[str] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        for key in ("symbol", "ticker", "code", "Symbol", "Ticker", "Code", "requested_symbol"):
            v = row.get(key)
            if v:
                raw.append(str(v).strip())
                break
    return _normalize_symbol_list(raw, limit=limit)


def _extract_symbols_from_payload(payload: Any, limit: int = 5000) -> List[str]:
    raw: List[str] = []

    if isinstance(payload, dict):
        if payload:
            maybe_symbol_map = True
            direct_keys: List[str] = []
            for k, v in payload.items():
                if not isinstance(v, dict):
                    maybe_symbol_map = False
                    break
                if _looks_like_symbol_token(k):
                    direct_keys.append(str(k).strip())
            if maybe_symbol_map and direct_keys:
                raw.extend(direct_keys)

        for key in (
            "symbols",
            "tickers",
            "selected_symbols",
            "direct_symbols",
            "codes",
            "top_symbols",
            "portfolio_symbols",
            "portfolio_tickers",
            "universe_symbols",
            "watchlist",
        ):
            raw.extend(_split_symbols(payload.get(key)))

        for nested_key in ("criteria", "settings", "data", "meta", "payload", "request", "result"):
            nested = payload.get(nested_key)
            if isinstance(nested, dict):
                for key in (
                    "symbols",
                    "tickers",
                    "selected_symbols",
                    "direct_symbols",
                    "codes",
                    "top_symbols",
                    "portfolio_symbols",
                    "portfolio_tickers",
                    "universe_symbols",
                    "watchlist",
                ):
                    raw.extend(_split_symbols(nested.get(key)))

        rows = _coerce_rows_list(payload)
        if rows:
            raw.extend(_extract_symbols_from_rows(rows, limit=limit * 2))

        _collect_symbol_candidates(payload, raw, depth=0, max_depth=5)

    elif isinstance(payload, list):
        rows = _coerce_rows_list(payload)
        if rows:
            raw.extend(_extract_symbols_from_rows(rows, limit=limit * 2))
        _collect_symbol_candidates(payload, raw, depth=0, max_depth=5)

    return _normalize_symbol_list(raw, limit=limit)


def _coerce_payload_keys_headers(payload: Any) -> Tuple[List[str], List[str]]:
    if not isinstance(payload, dict):
        return [], []

    headers = payload.get("display_headers") or payload.get("sheet_headers") or payload.get("column_headers") or payload.get("headers") or []
    keys = payload.get("keys") or []

    if isinstance(headers, list):
        headers = [str(h) for h in headers if _safe_str(h)]
    else:
        headers = []

    if isinstance(keys, list):
        keys = [str(k) for k in keys if _safe_str(k)]
    else:
        keys = []

    if not keys and headers:
        keys = [_norm_key(h) for h in headers if _norm_key(h)]

    return _complete_schema_contract(headers, keys)


def _list_sheet_names_best_effort() -> List[str]:
    if isinstance(SCHEMA_REGISTRY, dict) and SCHEMA_REGISTRY:
        try:
            return [str(k) for k in SCHEMA_REGISTRY.keys()]
        except Exception:
            pass
    try:
        from core.sheets.page_catalog import CANONICAL_PAGES  # type: ignore
        return [str(x) for x in CANONICAL_PAGES]
    except Exception:
        return []


def _apply_rank_overall(rows: List[Dict[str, Any]]) -> None:
    scored: List[Tuple[int, float]] = []
    for i, r in enumerate(rows):
        ov = _as_float(r.get("overall_score"))
        if ov is None:
            ov = _as_float(r.get("opportunity_score"))
        if ov is None:
            continue
        scored.append((i, ov))

    scored.sort(key=lambda t: t[1], reverse=True)
    rank = 1
    for idx, _ in scored:
        rows[idx]["rank_overall"] = rank
        rank += 1


def _top10_criteria_snapshot(criteria: Dict[str, Any]) -> str:
    if not isinstance(criteria, dict):
        return "{}"
    keep = {
        "top_n": criteria.get("top_n"),
        "pages_selected": criteria.get("pages_selected"),
        "horizon_days": criteria.get("horizon_days") or criteria.get("invest_period_days"),
        "risk_level": criteria.get("risk_level"),
        "min_expected_roi": criteria.get("min_expected_roi"),
        "confidence_level": criteria.get("confidence_level"),
        "direct_symbols": criteria.get("direct_symbols"),
    }
    keep = {k: v for k, v in keep.items() if v not in (None, "", [], {})}
    try:
        return json_dumps(keep)
    except Exception:
        return "{}"


def _top10_selection_reason(row: Dict[str, Any]) -> str:
    parts: List[str] = []
    ov = _as_float(row.get("overall_score"))
    op = _as_float(row.get("opportunity_score"))
    conf = _as_float(row.get("forecast_confidence"))
    if conf is None:
        conf = _as_float(row.get("confidence_score"))
    if conf is not None:
        conf = conf / 100.0 if conf > 1.5 else conf
    risk = _as_float(row.get("risk_score"))

    if ov is not None:
        parts.append(f"overall={round(ov, 1)}")
    if op is not None:
        parts.append(f"opportunity={round(op, 1)}")
    if conf is not None:
        parts.append(f"confidence={round(conf * 100.0, 1)}%")
    if risk is not None:
        parts.append(f"risk={round(risk, 1)}")

    if not parts:
        return "Selected by fallback ranking"
    return "Selected by fallback ranking: " + ", ".join(parts)


class _EngineSymbolsReaderProxy:
    def __init__(self, engine: "DataEngineV5") -> None:
        self._engine = engine

    async def get_symbols_for_sheet(self, sheet: str, limit: int = 5000) -> List[str]:
        return await self._engine.get_sheet_symbols(sheet, limit=limit)

    async def get_symbols_for_page(self, page: str, limit: int = 5000) -> List[str]:
        return await self._engine.get_page_symbols(page, limit=limit)

    async def list_symbols_for_page(self, page: str, limit: int = 5000) -> List[str]:
        return await self._engine.list_symbols_for_page(page, limit=limit)


# =========================== END OF PART 1 ===========================
# PART 2 must start directly with:
# class DataEngineV5:
class DataEngineV5:
    def __init__(self, settings: Any = None):
        self.settings = settings if settings is not None else _try_get_settings()
        self.flags = _feature_flags(self.settings)
        self.version = __version__

        if self.settings is not None:
            try:
                enabled = [str(x).lower() for x in (getattr(self.settings, "enabled_providers", None) or [])]
            except Exception:
                enabled = []
            try:
                ksa_list = [str(x).lower() for x in (getattr(self.settings, "ksa_providers", None) or [])]
            except Exception:
                ksa_list = []
            try:
                primary = str(getattr(self.settings, "primary_provider", "eodhd") or "eodhd").lower()
            except Exception:
                primary = "eodhd"
        else:
            enabled = _get_env_list("ENABLED_PROVIDERS", DEFAULT_PROVIDERS)
            ksa_list = _get_env_list("KSA_PROVIDERS", DEFAULT_KSA_PROVIDERS)
            primary = _get_env_str("PRIMARY_PROVIDER", "eodhd").lower()

        self.primary_provider = primary or "eodhd"
        self.enabled_providers = enabled or _get_env_list("ENABLED_PROVIDERS", DEFAULT_PROVIDERS)
        self.ksa_providers = ksa_list or _get_env_list("KSA_PROVIDERS", DEFAULT_KSA_PROVIDERS)
        self.global_providers = _get_env_list("GLOBAL_PROVIDERS", DEFAULT_GLOBAL_PROVIDERS)
        self.max_concurrency = _get_env_int("DATA_ENGINE_MAX_CONCURRENCY", 25)
        self.request_timeout = _get_env_float("DATA_ENGINE_TIMEOUT_SECONDS", 20.0)
        self.ksa_disallow_eodhd = _get_env_bool("KSA_DISALLOW_EODHD", True)
        self.schema_strict_sheet_rows = _get_env_bool("SCHEMA_STRICT_SHEET_ROWS", True)
        self.top10_force_full_schema = _get_env_bool("TOP10_FORCE_FULL_SCHEMA", True)
        self.rows_hydrate_external = _get_env_bool("ROWS_HYDRATE_EXTERNAL_READER", True)

        self._sem = asyncio.Semaphore(max(1, self.max_concurrency))
        self._singleflight = SingleFlight()
        self._registry = ProviderRegistry()
        self._cache = MultiLevelCache(
            name="data_engine",
            l1_ttl=_get_env_int("CACHE_L1_TTL", 60),
            l3_ttl=_get_env_int("CACHE_L3_TTL", 3600),
            max_l1_size=_get_env_int("CACHE_L1_MAX", 5000),
        )
        self._symbols_cache = MultiLevelCache(
            name="sheet_symbols",
            l1_ttl=_get_env_int("SHEET_SYMBOLS_L1_TTL", 300),
            l3_ttl=_get_env_int("SHEET_SYMBOLS_L3_TTL", 1800),
            max_l1_size=_get_env_int("SHEET_SYMBOLS_L1_MAX", 256),
        )

        self._symbols_reader_lock = asyncio.Lock()
        self._symbols_reader_ready = False
        self._symbols_reader_obj: Any = None
        self._symbols_reader_source = ""

        self._rows_reader_lock = asyncio.Lock()
        self._rows_reader_ready = False
        self._rows_reader_obj: Any = None
        self._rows_reader_source = ""

        self._sheet_snapshots: Dict[str, Dict[str, Any]] = {}
        self._sheet_symbol_resolution_meta: Dict[str, Dict[str, Any]] = {}

        self.symbols_reader = _EngineSymbolsReaderProxy(self)

    async def aclose(self) -> None:
        return

    # -------------------------------------------------------------------------
    # compatibility aliases
    # -------------------------------------------------------------------------
    async def execute_sheet_rows(self, *args, **kwargs) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    async def run_sheet_rows(self, *args, **kwargs) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    async def build_analysis_sheet_rows(self, *args, **kwargs) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    async def run_analysis_sheet_rows(self, *args, **kwargs) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    async def get_rows_for_sheet(self, *args, **kwargs) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    async def get_rows_for_page(self, *args, **kwargs) -> Dict[str, Any]:
        return await self.get_page_rows(*args, **kwargs)

    def get_page_snapshot(self, *args, **kwargs) -> Optional[Dict[str, Any]]:
        return self.get_cached_sheet_snapshot(*args, **kwargs)

    # -------------------------------------------------------------------------
    # small state helpers
    # -------------------------------------------------------------------------
    def _set_sheet_symbols_meta(self, sheet: str, source: str, count: int, note: Optional[str] = None) -> None:
        sheet2 = _canonicalize_sheet_name(sheet)
        if not sheet2:
            return
        self._sheet_symbol_resolution_meta[sheet2] = {
            "sheet": sheet2,
            "source": source or "",
            "count": int(count or 0),
            "note": note or "",
            "timestamp_utc": _now_utc_iso(),
        }

    def _get_sheet_symbols_meta(self, sheet: str) -> Dict[str, Any]:
        sheet2 = _canonicalize_sheet_name(sheet)
        if not sheet2:
            return {}
        meta = self._sheet_symbol_resolution_meta.get(sheet2)
        return dict(meta) if isinstance(meta, dict) else {}

    @staticmethod
    def _extract_row_symbol(row: Dict[str, Any]) -> str:
        if not isinstance(row, dict):
            return ""
        for k in ("symbol", "ticker", "code", "requested_symbol", "symbol_normalized", "Symbol", "Ticker", "Code"):
            v = row.get(k)
            if v:
                return str(v).strip()
        return ""

    @staticmethod
    def _row_non_null_score(row: Optional[Dict[str, Any]]) -> int:
        if not isinstance(row, dict):
            return 0
        return sum(1 for v in row.values() if v not in (None, "", [], {}))

    @staticmethod
    def _merge_rows_prefer_non_null(base: Optional[Dict[str, Any]], addon: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        out = dict(base or {})
        if not isinstance(addon, dict):
            return out
        for k, v in addon.items():
            if v not in (None, "", [], {}):
                out[k] = v
        return out

    async def _hydrate_rows_with_quotes(self, rows: List[Dict[str, Any]], schema: Any = None) -> List[Dict[str, Any]]:
        if not rows:
            return []

        symbols: List[str] = []
        for row in rows:
            sym = self._extract_row_symbol(row)
            if sym:
                symbols.append(sym)

        symbols = _normalize_symbol_list(symbols, limit=500)
        if not symbols:
            return [dict(r) for r in rows]

        quote_map: Dict[str, Dict[str, Any]] = {}
        try:
            quotes = await self.get_enriched_quotes(symbols, schema=None)
            for q in quotes:
                qd = _model_to_dict(q)
                qs = self._extract_row_symbol(qd)
                if qs:
                    quote_map[qs] = dict(qd)
                    qn = normalize_symbol(qs) if callable(normalize_symbol) else _fallback_normalize_symbol(qs)
                    if qn:
                        quote_map[str(qn)] = dict(qd)
        except Exception:
            return [normalize_row_to_schema(schema, dict(r), keep_extras=False) if schema is not None else dict(r) for r in rows]

        out: List[Dict[str, Any]] = []
        for row in rows:
            src_row = dict(row or {})
            sym = self._extract_row_symbol(src_row)
            norm = normalize_symbol(sym) if callable(normalize_symbol) else _fallback_normalize_symbol(sym)
            base = quote_map.get(sym) or quote_map.get(str(norm or "")) or {}

            merged = self._merge_rows_prefer_non_null(base if isinstance(base, dict) else {}, src_row)
            if sym and not merged.get("symbol"):
                merged["symbol"] = sym
            if norm and not merged.get("symbol_normalized"):
                merged["symbol_normalized"] = norm
            if schema is not None:
                merged = normalize_row_to_schema(schema, merged, keep_extras=False)
            out.append(merged)

        return out

    def _top10_schema_contract(self, headers: List[str], keys: List[str]) -> Tuple[List[str], List[str]]:
        if self.top10_force_full_schema:
            return _ensure_top10_contract(headers, keys)
        return _complete_schema_contract(headers, keys)

    def _project_rows_to_schema(
        self,
        rows: List[Dict[str, Any]],
        *,
        headers: List[str],
        keys: List[str],
    ) -> List[Dict[str, Any]]:
        headers, keys = _complete_schema_contract(headers, keys)
        if not keys:
            return []
        out: List[Dict[str, Any]] = []
        for r in rows or []:
            norm = _normalize_to_schema_keys(keys, headers, r)
            out.append(_strict_project_row(keys, norm))
        return out

    def _finalize_payload(
        self,
        *,
        sheet: str,
        headers: List[str],
        keys: List[str],
        rows: List[Dict[str, Any]],
        include_matrix: bool,
        status: str = "success",
        meta: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> Dict[str, Any]:
        headers, keys = _complete_schema_contract(headers, keys)
        payload = {
            "status": status,
            "sheet": sheet,
            "page": sheet,
            "sheet_name": sheet,
            "headers": headers,
            "display_headers": headers,
            "sheet_headers": headers,
            "column_headers": headers,
            "keys": keys,
            "columns": keys,
            "fields": keys,
            "rows": rows,
            "data": rows,
            "items": rows,
            "quotes": rows,
            "rows_matrix": _rows_matrix(rows, keys) if include_matrix else [],
            "meta": dict(meta or {}),
            "version": self.version,
        }
        if error:
            payload["error"] = error
        return _json_safe(payload)

    # -------------------------------------------------------------------------
    # provider routing
    # -------------------------------------------------------------------------
    def _providers_for(self, symbol: str) -> List[str]:
        info = get_symbol_info(symbol)
        is_ksa_sym = bool(info.get("is_ksa"))
        base = self.ksa_providers if is_ksa_sym else self.global_providers
        providers = [p for p in base if p in self.enabled_providers]

        if is_ksa_sym and self.ksa_disallow_eodhd:
            providers = [p for p in providers if p != "eodhd"]

        if self.primary_provider and (self.primary_provider in self.enabled_providers):
            if self.primary_provider in providers:
                providers = [p for p in providers if p != self.primary_provider]
                providers.insert(0, self.primary_provider)
            else:
                if (not is_ksa_sym) or (self.primary_provider != "eodhd") or (not self.ksa_disallow_eodhd):
                    providers.insert(0, self.primary_provider)

        seen: Set[str] = set()
        providers = [p for p in providers if not (p in seen or seen.add(p))]
        if providers:
            head = providers[0]
            tail = sorted(providers[1:], key=lambda p: PROVIDER_PRIORITIES.get(p, 999))
            return [head] + tail
        return providers

    def _provider_symbol(self, provider: str, symbol: str) -> str:
        if provider.startswith("yahoo"):
            try:
                return to_yahoo_symbol(symbol)  # type: ignore
            except Exception:
                return symbol
        return symbol

    # -------------------------------------------------------------------------
    # snapshots
    # -------------------------------------------------------------------------
    def _store_sheet_snapshot(self, sheet: str, payload: Dict[str, Any]) -> None:
        sheet2 = _canonicalize_sheet_name(sheet)
        if not sheet2 or not isinstance(payload, dict):
            return
        try:
            self._sheet_snapshots[sheet2] = dict(payload)
        except Exception:
            pass

    def get_cached_sheet_snapshot(
        self,
        sheet: Optional[str] = None,
        page: Optional[str] = None,
        sheet_name: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        target = _canonicalize_sheet_name(sheet or page or sheet_name or "")
        if not target:
            return None
        snap = self._sheet_snapshots.get(target)
        if isinstance(snap, dict):
            return dict(snap)
        return None

    def get_sheet_snapshot(
        self,
        page: Optional[str] = None,
        sheet: Optional[str] = None,
        sheet_name: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        return self.get_cached_sheet_snapshot(sheet=sheet, page=page, sheet_name=sheet_name)

    def get_cached_sheet_rows(
        self,
        sheet_name: Optional[str] = None,
        sheet: Optional[str] = None,
        page: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        return self.get_cached_sheet_snapshot(sheet=sheet, page=page, sheet_name=sheet_name)

    # -------------------------------------------------------------------------
    # symbols reader
    # -------------------------------------------------------------------------
    async def _init_symbols_reader(self) -> Tuple[Any, str]:
        if self._symbols_reader_ready:
            return self._symbols_reader_obj, self._symbols_reader_source

        async with self._symbols_reader_lock:
            if self._symbols_reader_ready:
                return self._symbols_reader_obj, self._symbols_reader_source

            obj: Any = None
            src = ""

            module_candidates = [
                "symbols_reader",
                "core.symbols_reader",
                "integrations.symbols_reader",
                "core.integrations.symbols_reader",
                "integrations.google_sheets_service",
                "core.integrations.google_sheets_service",
                "google_sheets_service",
                "core.google_sheets_service",
            ]

            for mod_path in module_candidates:
                try:
                    mod = import_module(mod_path)
                except Exception:
                    continue

                if any(
                    callable(getattr(mod, nm, None))
                    for nm in (
                        "get_symbols_for_sheet",
                        "read_symbols_for_sheet",
                        "get_sheet_symbols",
                        "get_symbols",
                        "list_symbols_for_page",
                        "get_symbols_for_page",
                        "read_symbols",
                        "load_symbols",
                        "read_sheet_symbols",
                    )
                ):
                    obj = mod
                    src = mod_path
                    break

                for attr_name in ("symbols_reader", "reader", "symbol_reader", "sheet_reader", "service"):
                    candidate_obj = getattr(mod, attr_name, None)
                    if candidate_obj is not None:
                        obj = candidate_obj
                        src = f"{mod_path}.{attr_name}"
                        break
                if obj is not None:
                    break

                for factory_name in ("get_reader", "build_reader", "create_reader", "get_service", "build_service"):
                    factory = getattr(mod, factory_name, None)
                    if callable(factory):
                        try:
                            obj = await _call_maybe_async(factory, settings=self.settings)
                        except TypeError:
                            try:
                                obj = await _call_maybe_async(factory)
                            except Exception:
                                obj = None
                        if obj is not None:
                            src = f"{mod_path}.{factory_name}"
                            break
                if obj is not None:
                    break

                for class_name in (
                    "SymbolsReader",
                    "SheetSymbolsReader",
                    "SymbolReader",
                    "GoogleSheetsService",
                    "SheetsService",
                ):
                    cls = getattr(mod, class_name, None)
                    if cls is None:
                        continue
                    try:
                        obj = cls(settings=self.settings)
                    except TypeError:
                        try:
                            obj = cls()
                        except Exception:
                            obj = None
                    if obj is not None:
                        src = f"{mod_path}.{class_name}"
                        break
                if obj is not None:
                    break

            self._symbols_reader_obj = obj
            self._symbols_reader_source = src
            self._symbols_reader_ready = True
            return obj, src

    async def _call_symbols_reader(self, obj: Any, sheet: str, limit: int) -> List[str]:
        if obj is None:
            return []

        if isinstance(obj, dict):
            for key in _sheet_lookup_candidates(sheet):
                if key in obj:
                    vals = obj.get(key)
                    syms = _normalize_symbol_list(_split_symbols(vals), limit=limit)
                    if syms:
                        return syms

        method_names = [
            "get_symbols_for_sheet",
            "read_symbols_for_sheet",
            "get_sheet_symbols",
            "get_symbols_for_page",
            "list_symbols_for_page",
            "get_symbols",
            "list_symbols",
            "read_symbols",
            "load_symbols",
            "read_sheet_symbols",
        ]

        timeout_s = _get_env_float("SHEET_SYMBOLS_TIMEOUT_SECONDS", 15.0)

        for name in method_names:
            fn = getattr(obj, name, None)
            if not callable(fn):
                continue

            call_variants = [
                (() , {"sheet": sheet, "limit": limit}),
                (() , {"sheet_name": sheet, "limit": limit}),
                (() , {"page": sheet, "limit": limit}),
                (() , {"tab": sheet, "limit": limit}),
                (() , {"name": sheet, "limit": limit}),
                (() , {"worksheet": sheet, "limit": limit}),
                ((sheet,), {"limit": limit}),
                ((sheet,), {}),
            ]

            for args, kwargs in call_variants:
                try:
                    async with asyncio.timeout(timeout_s):
                        res = await _call_maybe_async(fn, *args, **kwargs)
                except TypeError:
                    continue
                except TimeoutError:
                    logger.warning(f"Timeout calling symbols reader {name}")
                    continue
                except Exception:
                    continue

                syms = _extract_symbols_from_payload(res, limit=limit)
                if syms:
                    return syms

        return []

    async def _get_symbols_from_env(self, sheet: str, limit: int) -> List[str]:
        env_candidates: List[str] = []
        specific = PAGE_SYMBOL_ENV_KEYS.get(sheet)
        if specific:
            env_candidates.append(specific)

        for cand in _sheet_lookup_candidates(sheet):
            token = re.sub(r"[^A-Za-z0-9]+", "_", cand).strip("_").upper()
            if token:
                env_candidates.extend(
                    [
                        f"{token}_SYMBOLS",
                        f"{token}_TICKERS",
                        f"{token}_CODES",
                    ]
                )

        env_candidates.extend(
            [
                "TOP10_FALLBACK_SYMBOLS",
                "DEFAULT_PAGE_SYMBOLS",
                "DEFAULT_SYMBOLS",
            ]
        )

        seen = set()
        for env_key in env_candidates:
            if not env_key or env_key in seen:
                continue
            seen.add(env_key)
            raw = os.getenv(env_key, "") or ""
            if raw.strip():
                syms = _normalize_symbol_list(_split_symbols(raw), limit=limit)
                if syms:
                    return syms
        return []

    async def _get_symbols_from_settings(self, sheet: str, limit: int) -> List[str]:
        if self.settings is None:
            return []

        candidates = _sheet_lookup_candidates(sheet)

        try:
            attr_candidates = [
                f"{sheet.lower()}_symbols",
                f"{sheet.lower()}_tickers",
                f"{sheet.lower()}_codes",
                "default_symbols",
                "page_symbols",
                "sheet_symbols",
            ]
            for attr_name in attr_candidates:
                raw = getattr(self.settings, attr_name, None)
                if isinstance(raw, dict):
                    for cand in candidates:
                        vals = raw.get(cand)
                        syms = _normalize_symbol_list(_split_symbols(vals), limit=limit)
                        if syms:
                            return syms
                elif raw:
                    syms = _normalize_symbol_list(_split_symbols(raw), limit=limit)
                    if syms:
                        return syms
        except Exception:
            pass
        return []

    async def _get_symbols_from_page_catalog(self, sheet: str, limit: int) -> List[str]:
        module_candidates = [
            "core.sheets.page_catalog",
            "sheets.page_catalog",
        ]
        candidates = _sheet_lookup_candidates(sheet)

        for mod_path in module_candidates:
            try:
                mod = import_module(mod_path)
            except Exception:
                continue

            for attr_name in ("PAGE_SYMBOLS", "SHEET_SYMBOLS", "DEFAULT_PAGE_SYMBOLS", "PAGE_DEFAULT_SYMBOLS"):
                mapping = getattr(mod, attr_name, None)
                if isinstance(mapping, dict):
                    for cand in candidates:
                        vals = mapping.get(cand)
                        syms = _normalize_symbol_list(_split_symbols(vals), limit=limit)
                        if syms:
                            return syms

            for fn_name in ("get_default_symbols", "get_page_symbols", "get_symbols_for_page"):
                fn = getattr(mod, fn_name, None)
                if callable(fn):
                    for args, kwargs in [
                        ((sheet,), {"limit": limit}),
                        ((sheet,), {}),
                        ((), {"page": sheet, "limit": limit}),
                        ((), {"sheet": sheet, "limit": limit}),
                    ]:
                        try:
                            res = await _call_maybe_async(fn, *args, **kwargs)
                        except TypeError:
                            continue
                        except Exception:
                            continue
                        syms = _extract_symbols_from_payload(res, limit=limit)
                        if syms:
                            return syms
        return []

    async def _get_symbols_for_sheet_impl(self, sheet: str, limit: int = 5000, body: Optional[Dict[str, Any]] = None) -> List[str]:
        sheet2 = _canonicalize_sheet_name(sheet)
        if sheet2 in SPECIAL_SHEETS:
            self._set_sheet_symbols_meta(sheet2, "special_sheet", 0)
            return []
        if sheet2 and sheet2 not in INSTRUMENT_SHEETS:
            self._set_sheet_symbols_meta(sheet2, "non_instrument_sheet", 0)
            return []

        limit = max(1, min(5000, int(limit or 5000)))

        from_body = _extract_requested_symbols_from_body(body, limit=limit)
        if from_body:
            self._set_sheet_symbols_meta(sheet2, "body_symbols", len(from_body))
            return from_body

        cached = await self._symbols_cache.get(sheet=sheet2, limit=limit)
        if isinstance(cached, list) and cached:
            syms = _normalize_symbol_list(cached, limit=limit)
            self._set_sheet_symbols_meta(sheet2, "symbols_cache", len(syms))
            return syms

        obj, src = await self._init_symbols_reader()
        syms: List[str] = []

        if obj is not None:
            syms = await self._call_symbols_reader(obj, sheet2, limit=limit)
            if syms:
                self._set_sheet_symbols_meta(sheet2, f"symbols_reader:{src or 'unknown'}", len(syms))
                await self._symbols_cache.set(syms, sheet=sheet2, limit=limit)
                return syms

        syms = await self._get_symbols_from_page_catalog(sheet2, limit=limit)
        if syms:
            self._set_sheet_symbols_meta(sheet2, "page_catalog", len(syms))
            await self._symbols_cache.set(syms, sheet=sheet2, limit=limit)
            return syms

        syms = await self._get_symbols_from_env(sheet2, limit=limit)
        if syms:
            self._set_sheet_symbols_meta(sheet2, "env", len(syms))
            await self._symbols_cache.set(syms, sheet=sheet2, limit=limit)
            return syms

        syms = await self._get_symbols_from_settings(sheet2, limit=limit)
        if syms:
            self._set_sheet_symbols_meta(sheet2, "settings", len(syms))
            await self._symbols_cache.set(syms, sheet=sheet2, limit=limit)
            return syms

        snap = self.get_cached_sheet_snapshot(sheet=sheet2)
        snap_rows = _coerce_rows_list(snap)
        if snap_rows:
            syms = _extract_symbols_from_rows(snap_rows, limit=limit)
            if syms:
                self._set_sheet_symbols_meta(sheet2, "snapshot_rows", len(syms))
                await self._symbols_cache.set(syms, sheet=sheet2, limit=limit)
                return syms

        emergency = EMERGENCY_PAGE_SYMBOLS.get(sheet2) or []
        if emergency:
            syms = _normalize_symbol_list(emergency, limit=limit)
            if syms:
                self._set_sheet_symbols_meta(sheet2, "emergency_page_symbols", len(syms), note="last_resort_fallback")
                await self._symbols_cache.set(syms, sheet=sheet2, limit=limit)
                return syms

        self._set_sheet_symbols_meta(sheet2, "none", 0, note=(src or "no_source"))
        logger.info("No symbols resolved for sheet=%s source=%s", sheet2, src or "none")
        return []

    async def get_sheet_symbols(
        self,
        sheet: Optional[str] = None,
        *,
        sheet_name: Optional[str] = None,
        page: Optional[str] = None,
        limit: int = 5000,
        body: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        return await self._get_symbols_for_sheet_impl(page or sheet or sheet_name or "", limit=limit, body=body)

    async def get_page_symbols(
        self,
        page: Optional[str] = None,
        *,
        sheet: Optional[str] = None,
        sheet_name: Optional[str] = None,
        limit: int = 5000,
        body: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        return await self._get_symbols_for_sheet_impl(page or sheet or sheet_name or "", limit=limit, body=body)

    async def list_symbols_for_page(
        self,
        page: str,
        *,
        limit: int = 5000,
        body: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        return await self._get_symbols_for_sheet_impl(page, limit=limit, body=body)

    async def list_symbols(
        self,
        sheet: Optional[str] = None,
        *,
        page: Optional[str] = None,
        sheet_name: Optional[str] = None,
        limit: int = 5000,
        body: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        return await self._get_symbols_for_sheet_impl(page or sheet or sheet_name or "", limit=limit, body=body)

    async def get_symbols(
        self,
        sheet: Optional[str] = None,
        *,
        page: Optional[str] = None,
        sheet_name: Optional[str] = None,
        limit: int = 5000,
        body: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        return await self._get_symbols_for_sheet_impl(page or sheet or sheet_name or "", limit=limit, body=body)

    # -------------------------------------------------------------------------
    # optional external rows reader
    # -------------------------------------------------------------------------
    async def _init_rows_reader(self) -> Tuple[Any, str]:
        if self._rows_reader_ready:
            return self._rows_reader_obj, self._rows_reader_source

        async with self._rows_reader_lock:
            if self._rows_reader_ready:
                return self._rows_reader_obj, self._rows_reader_source

            obj: Any = None
            src = ""

            module_candidates = [
                "integrations.google_sheets_service",
                "core.integrations.google_sheets_service",
                "google_sheets_service",
                "core.google_sheets_service",
                "integrations.symbols_reader",
                "core.integrations.symbols_reader",
            ]

            for mod_path in module_candidates:
                try:
                    mod = import_module(mod_path)
                except Exception:
                    continue

                if any(
                    callable(getattr(mod, nm, None))
                    for nm in (
                        "get_rows_for_sheet",
                        "read_rows_for_sheet",
                        "get_sheet_rows",
                        "fetch_sheet_rows",
                        "sheet_rows",
                        "get_rows",
                    )
                ):
                    obj = mod
                    src = mod_path
                    break

                for attr_name in ("service", "reader", "rows_reader", "google_sheets_service"):
                    candidate_obj = getattr(mod, attr_name, None)
                    if candidate_obj is not None:
                        obj = candidate_obj
                        src = f"{mod_path}.{attr_name}"
                        break
                if obj is not None:
                    break

            self._rows_reader_obj = obj
            self._rows_reader_source = src
            self._rows_reader_ready = True
            return obj, src

    async def _call_rows_reader(self, obj: Any, sheet: str, limit: int) -> List[Dict[str, Any]]:
        if obj is None:
            return []

        method_names = [
            "get_rows_for_sheet",
            "read_rows_for_sheet",
            "get_sheet_rows",
            "fetch_sheet_rows",
            "sheet_rows",
            "get_rows",
        ]

        timeout_s = _get_env_float("ROWS_READER_TIMEOUT_SECONDS", 20.0)

        for name in method_names:
            fn = getattr(obj, name, None)
            if not callable(fn):
                continue

            call_variants = [
                (() , {"sheet": sheet, "limit": limit}),
                (() , {"sheet_name": sheet, "limit": limit}),
                (() , {"page": sheet, "limit": limit}),
                ((sheet,), {"limit": limit}),
                ((sheet,), {}),
            ]

            for args, kwargs in call_variants:
                try:
                    async with asyncio.timeout(timeout_s):
                        res = await _call_maybe_async(fn, *args, **kwargs)
                except TypeError:
                    continue
                except TimeoutError:
                    logger.warning(f"Timeout calling rows reader {name}")
                    continue
                except Exception:
                    continue

                rows = _coerce_rows_list(res)
                if rows:
                    return rows

        return []

    async def _get_rows_from_external_reader(self, sheet: str, limit: int) -> List[Dict[str, Any]]:
        obj, _ = await self._init_rows_reader()
        if obj is None:
            return []
        rows = await self._call_rows_reader(obj, sheet, limit)
        return rows[:limit] if rows else []

    # -------------------------------------------------------------------------
    # provider fetch / merge
    # -------------------------------------------------------------------------
    async def _fetch_patch(self, provider: str, symbol: str) -> Tuple[str, Optional[Dict[str, Any]], float, Optional[str]]:
        start = time.time()
        async with self._sem:
            module, stats = await self._registry.get_provider(provider)
            if stats.is_circuit_open:
                return provider, None, 0.0, "circuit_open"
            if module is None:
                err = stats.last_import_error or "provider module missing"
                await self._registry.record_failure(provider, err)
                return provider, None, (time.time() - start) * 1000.0, err

            fn = _pick_provider_callable(module, provider)
            if fn is None:
                err = f"no callable fetch function for provider '{provider}'"
                await self._registry.record_failure(provider, err)
                return provider, None, (time.time() - start) * 1000.0, err

            provider_symbol = self._provider_symbol(provider, symbol)
            
            call_variants = [
                ((provider_symbol,), {}),
                ((), {"symbol": provider_symbol}),
                ((), {"ticker": provider_symbol}),
                ((), {"requested_symbol": provider_symbol}),
                ((provider_symbol,), {"settings": self.settings}),
                ((), {"symbol": provider_symbol, "settings": self.settings}),
                ((), {"ticker": provider_symbol, "settings": self.settings}),
            ]
            
            res = None
            call_success = False
            collected_errs: List[str] = []
            
            for args, kwargs in call_variants:
                try:
                    async with asyncio.timeout(self.request_timeout):
                        res = await _call_maybe_async(fn, *args, **kwargs)
                    call_success = True
                    break
                except TimeoutError:
                    collected_errs.append("timeout")
                    break
                except Exception as e:
                    collected_errs.append(f"{type(e).__name__}: {str(e)[:100]}")
                    continue

            latency = (time.time() - start) * 1000.0

            if not call_success:
                err = " | ".join(collected_errs) if collected_errs else "provider_call_failed"
                await self._registry.record_failure(provider, err)
                return provider, None, latency, err

            if isinstance(res, dict) and res:
                patch = _normalize_patch_keys(_clean_patch(res))
                if _is_useful_patch(patch):
                    await self._registry.record_success(provider, latency)
                    return provider, patch, latency, None
                err = str(res.get("error") or "empty_result")
                await self._registry.record_failure(provider, err)
                return provider, None, latency, err
                
            err = "non_dict_or_empty"
            await self._registry.record_failure(provider, err)
            return provider, None, latency, err

    def _merge(self, requested_symbol: str, norm: str, patches: List[Tuple[str, Dict[str, Any], float]]) -> Dict[str, Any]:
        merged: Dict[str, Any] = {
            "symbol": norm,
            "symbol_normalized": norm,
            "requested_symbol": requested_symbol,
            "last_updated_utc": _now_utc_iso(),
            "last_updated_riyadh": _now_riyadh_iso(),
            "data_sources": [],
            "provider_latency": {},
        }
        protected = {"symbol", "symbol_normalized", "requested_symbol"}

        # richer / higher-priority patches first
        sorted_patches = sorted(
            patches,
            key=lambda item: (PROVIDER_PRIORITIES.get(item[0], 999), -self._row_non_null_score(item[1])),
        )

        for prov, patch, latency in sorted_patches:
            merged["data_sources"].append(prov)
            merged["provider_latency"][prov] = round(float(latency or 0.0), 2)
            for k, v in patch.items():
                if k in protected or v is None:
                    continue
                if k not in merged or merged.get(k) in (None, "", []):
                    merged[k] = v

        if merged.get("current_price") is None and merged.get("price") is not None:
            merged["current_price"] = merged.get("price")
        if merged.get("price") is None and merged.get("current_price") is not None:
            merged["price"] = merged.get("current_price")
        if merged.get("price_change") is None and merged.get("change") is not None:
            merged["price_change"] = merged.get("change")
        if merged.get("change") is None and merged.get("price_change") is not None:
            merged["change"] = merged.get("price_change")
        if merged.get("percent_change") is None and merged.get("change_pct") is not None:
            merged["percent_change"] = merged.get("change_pct")
        if merged.get("change_pct") is None and merged.get("percent_change") is not None:
            merged["change_pct"] = merged.get("percent_change")

        return merged

    def _data_quality(self, row: Dict[str, Any]) -> str:
        cp = row.get("current_price")
        if _as_float(cp) is None:
            return QuoteQuality.MISSING.value
        if any(row.get(k) is not None for k in ("overall_score", "forecast_price_3m", "pb_ratio", "volatility_90d")):
            return QuoteQuality.GOOD.value
        return QuoteQuality.FAIR.value

    async def _maybe_compute_risk_stats(
        self,
        row: Dict[str, Any],
        symbol_norm: str,
        is_ksa_sym: bool,
        providers_used: List[str],
    ) -> None:
        if not self.flags.get("risk_stats_enabled", True):
            return

        need = any(row.get(k) is None for k in ("volatility_90d", "max_drawdown_1y", "var_95_1d", "sharpe_1y"))
        if not need:
            return

        if is_ksa_sym:
            history_providers = [p for p in ["yahoo_chart", "tadawul", "argaam"] if p in self.enabled_providers]
        else:
            history_providers = [p for p in ["eodhd", "yahoo_chart", "finnhub"] if p in self.enabled_providers]

        hist_obj = None
        for p in history_providers:
            sym_p = self._provider_symbol(p, symbol_norm)
            hist_obj = await _fetch_history_from_provider(self._registry, p, sym_p, timeout_s=self.request_timeout)
            if hist_obj:
                break

        if not hist_obj:
            hist_obj = row.get("history") or row.get("prices") or row.get("price_history")

        prices = _extract_prices_from_history(hist_obj)
        if len(prices) < 30:
            return

        stats = _compute_risk_stats_from_prices(prices)
        for k, v in stats.items():
            if row.get(k) is None and v is not None:
                row[k] = _clamp(float(v), 0.0, 5.0) if k in ("volatility_30d", "volatility_90d", "max_drawdown_1y", "var_95_1d") else v

        if row.get("rsi_14") is None:
            rsi = _compute_rsi_from_prices(prices, period=14)
            if rsi is not None:
                row["rsi_14"] = rsi

    # -------------------------------------------------------------------------
    # quote APIs
    # -------------------------------------------------------------------------
    async def get_enriched_quote(self, symbol: str, use_cache: bool = True, *, schema: Any = None) -> UnifiedQuote:
        info = get_symbol_info(symbol)
        norm = info.get("normalized") or _safe_str(symbol)
        key = f"quote:{norm}:{'cache' if use_cache else 'live'}"
        
        raw_q = await self._singleflight.execute(
            key,
            lambda: self._get_enriched_quote_impl(symbol, use_cache),
        )
        
        if schema is not None:
            d = _model_to_dict(raw_q)
            d = normalize_row_to_schema(schema, d, keep_extras=False)
            return UnifiedQuote(**d)
        return raw_q

    async def get_enriched_quote_dict(self, symbol: str, use_cache: bool = True, *, schema: Any = None) -> Dict[str, Any]:
        q = await self.get_enriched_quote(symbol, use_cache=use_cache, schema=schema)
        return _model_to_dict(q)

    async def _get_enriched_quote_impl(self, symbol: str, use_cache: bool = True) -> UnifiedQuote:
        info = get_symbol_info(symbol)
        norm = info.get("normalized") or ""
        is_ksa_sym = bool(info.get("is_ksa"))

        if not norm:
            row = {
                "symbol": symbol,
                "symbol_normalized": None,
                "requested_symbol": symbol,
                "data_quality": QuoteQuality.MISSING.value,
                "error": "Invalid symbol",
                "last_updated_utc": _now_utc_iso(),
                "last_updated_riyadh": _now_riyadh_iso(),
            }
            _ensure_required_advanced_fields(row)
            row = normalize_row_to_schema(None, row)
            return UnifiedQuote(**row)  # type: ignore

        if use_cache:
            cached = await self._cache.get(symbol=norm)
            if cached:
                try:
                    if isinstance(cached, dict):
                        return UnifiedQuote(**cached)  # type: ignore
                    if isinstance(cached, UnifiedQuote):
                        return cached  # type: ignore
                except Exception:
                    pass

        providers = self._providers_for(norm)
        if not providers:
            row = {
                "symbol": norm,
                "symbol_normalized": norm,
                "requested_symbol": symbol,
                "data_quality": QuoteQuality.MISSING.value,
                "error": "No providers available",
                "data_sources": [],
                "provider_latency": {},
                "last_updated_utc": _now_utc_iso(),
                "last_updated_riyadh": _now_riyadh_iso(),
            }
            _ensure_required_advanced_fields(row)
            row = normalize_row_to_schema(None, row)
            return UnifiedQuote(**row)  # type: ignore

        top_n = _get_env_int("PROVIDER_TOP_N", 4)
        top = providers[: max(1, int(top_n))]
        gathered = await asyncio.gather(*[self._fetch_patch(p, norm) for p in top], return_exceptions=True)
        results: List[Tuple[str, Optional[Dict[str, Any]], float, Optional[str]]] = [
            r for r in gathered if isinstance(r, tuple) and len(r) == 4
        ]  # type: ignore
        patches_ok: List[Tuple[str, Dict[str, Any], float]] = [(p, patch, lat) for (p, patch, lat, _) in results if patch]

        if not patches_ok:
            stats = await self._registry.get_stats()
            err_detail = {
                "requested": symbol,
                "normalized": norm,
                "attempted_providers": top,
                "provider_stats": {k: stats.get(k) for k in top},
                "errors": [{"provider": p, "error": err, "latency_ms": round(lat, 2)} for (p, _, lat, err) in results],
            }
            row = {
                "symbol": norm,
                "symbol_normalized": norm,
                "requested_symbol": symbol,
                "data_quality": QuoteQuality.MISSING.value,
                "error": "No data available",
                "info": err_detail,
                "data_sources": [],
                "provider_latency": {},
                "last_updated_utc": _now_utc_iso(),
                "last_updated_riyadh": _now_riyadh_iso(),
            }
            _ensure_required_advanced_fields(row)
            row = normalize_row_to_schema(None, row)
            q = UnifiedQuote(**row)  # type: ignore
            if use_cache:
                await self._cache.set(_model_to_dict(q), symbol=norm)
            return q

        row = self._merge(symbol, norm, patches_ok)
        _compute_price_derivatives(row)
        _compute_portfolio_derivatives(row)
        _ensure_ksa_classification(row, is_ksa_sym)

        if self.flags.get("valuation_enabled", True):
            _compute_intrinsic_value_fallback(row)
            if row.get("valuation_score") is None:
                vs = _compute_valuation_score(row)
                if vs is not None:
                    row["valuation_score"] = vs

        await self._maybe_compute_risk_stats(row, norm, is_ksa_sym, providers_used=[p for p, _, _ in patches_ok])

        if self.flags.get("computations_enabled", True):
            if self.flags.get("forecasting_enabled", True):
                row = await _maybe_apply_forecast(row, self.settings)
            if self.flags.get("scoring_enabled", True):
                row = await _maybe_apply_scoring_module(row, self.settings)

        _compute_scores_fallback(row)
        _compute_recommendation(row)
        _ensure_required_advanced_fields(row)

        row["data_quality"] = self._data_quality(row)
        row["data_provider"] = (
            row.get("data_provider")
            or (row.get("data_sources")[0] if isinstance(row.get("data_sources"), list) and row["data_sources"] else "")
            or ""
        )

        row = normalize_row_to_schema(None, row)
        q = UnifiedQuote(**row)  # type: ignore

        if use_cache:
            await self._cache.set(_model_to_dict(q), symbol=norm)
        return q

    async def get_enriched_quotes(self, symbols: List[str], *, schema: Any = None) -> List[UnifiedQuote]:
        if not symbols:
            return []

        batch = _get_env_int("QUOTE_BATCH_SIZE", 25)
        try:
            if self.settings is not None and getattr(self.settings, "quote_batch_size", None):
                batch = int(getattr(self.settings, "quote_batch_size"))
        except Exception:
            pass
        batch = max(1, min(500, int(batch)))

        out: List[UnifiedQuote] = []
        for i in range(0, len(symbols), batch):
            part = symbols[i : i + batch]
            out.extend(await asyncio.gather(*[self.get_enriched_quote(s, schema=schema) for s in part]))
        return out

    async def get_enriched_quotes_batch(self, symbols: List[str], mode: str = "", *, schema: Any = None) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        if not symbols:
            return out

        norm_syms = _normalize_symbol_list(symbols, limit=len(symbols) + 10)
        quotes = await asyncio.gather(*[self.get_enriched_quote_dict(s, schema=schema) for s in norm_syms])

        for req_sym, qd in zip(norm_syms, quotes):
            out[req_sym] = qd
            norm = _safe_str(qd.get("symbol_normalized") or qd.get("symbol"))
            if norm:
                out[norm] = qd

        # preserve original requested keys too
        for req in symbols:
            req2 = _safe_str(req)
            if req2 and req2 not in out:
                norm = normalize_symbol(req2) if callable(normalize_symbol) else _fallback_normalize_symbol(req2)
                if norm in out:
                    out[req2] = out[norm]

        return out

    get_quote = get_enriched_quote
    get_quotes = get_enriched_quotes
    fetch_quote = get_enriched_quote
    fetch_quotes = get_enriched_quotes
    get_quotes_batch = get_enriched_quotes_batch
    get_analysis_quotes_batch = get_enriched_quotes_batch
    quotes_batch = get_enriched_quotes_batch
    get_quote_dict = get_enriched_quote_dict

    # -------------------------------------------------------------------------
    # Top10 fallback builder
    # -------------------------------------------------------------------------
    async def _build_top10_rows_fallback(
        self,
        headers: List[str],
        keys: List[str],
        body: Optional[Dict[str, Any]],
        limit: int,
        mode: str,
    ) -> Tuple[List[str], List[str], List[Dict[str, Any]]]:
        body = dict(body or {})
        criteria = _extract_nested_dict(body, "criteria")
        top_n = _safe_int(criteria.get("top_n") or body.get("top_n") or limit, 10, lo=1, hi=50) or 10

        headers, keys = self._top10_schema_contract(headers, keys)

        if not _usable_contract(headers, keys, "Top_10_Investments"):
            keys = list(_SCHEMA_UNION_KEYS)
            for field in TOP10_REQUIRED_FIELDS:
                if field not in keys:
                    keys.append(field)
            headers = [TOP10_REQUIRED_HEADERS.get(k, k.replace("_", " ").title()) for k in keys]
            headers, keys = _complete_schema_contract(headers, keys)

        direct_symbols = _extract_requested_symbols_from_body(body, limit=top_n * 25)
        pages_selected = _extract_top10_pages_from_body(body)
        if not pages_selected:
            pages_selected = [p for p in TOP10_ENGINE_DEFAULT_PAGES if p in INSTRUMENT_SHEETS]

        symbols: List[str] = []
        if direct_symbols:
            symbols = direct_symbols
        else:
            for page_name in pages_selected:
                syms = await self.get_sheet_symbols(page_name, limit=top_n * 15, body=body)
                symbols.extend(syms)

        symbols = _normalize_symbol_list(symbols, limit=max(top_n * 20, 50))
        if not symbols:
            return headers, keys, []

        quotes = await self.get_enriched_quotes(symbols, schema=None)
        rows: List[Dict[str, Any]] = [_model_to_dict(q) for q in quotes]
        if not rows:
            return headers, keys, []

        min_roi = _as_pct_fraction(criteria.get("min_expected_roi"))
        req_risk = _safe_str(criteria.get("risk_level")).upper()
        req_conf = _safe_str(criteria.get("confidence_level")).upper()
        horizon_days = _safe_int(
            criteria.get("horizon_days") 
            or criteria.get("invest_period_days") 
            or body.get("horizon_days") 
            or body.get("invest_period_days"), 
            365
        )

        filtered_rows = []
        for r in rows:
            if min_roi is not None:
                if horizon_days <= 30:
                    roi = _as_float(r.get("expected_roi_1m")) or _as_float(r.get("expected_roi_3m")) or _as_float(r.get("expected_roi_12m")) or -999.0
                elif horizon_days <= 90:
                    roi = _as_float(r.get("expected_roi_3m")) or _as_float(r.get("expected_roi_12m")) or _as_float(r.get("expected_roi_1m")) or -999.0
                else:
                    roi = _as_float(r.get("expected_roi_12m")) or _as_float(r.get("expected_roi_3m")) or _as_float(r.get("expected_roi_1m")) or -999.0
                    
                if roi < min_roi:
                    continue
            if req_risk and req_risk != "ALL":
                row_risk = _safe_str(r.get("risk_bucket")).upper()
                if row_risk and row_risk != req_risk:
                    continue
            if req_conf and req_conf != "ALL":
                row_conf = _safe_str(r.get("confidence_bucket")).upper()
                if row_conf and row_conf != req_conf:
                    continue
            filtered_rows.append(r)
        
        # Fallback to unfiltered if filtering removed everything
        if not filtered_rows and rows:
            filtered_rows = rows
        rows = filtered_rows

        def _sort_key(r: Dict[str, Any]) -> Tuple[float, float, float]:
            op = _as_float(r.get("opportunity_score"))
            ov = _as_float(r.get("overall_score"))
            conf = _as_float(r.get("forecast_confidence"))
            if conf is None:
                conf = _as_float(r.get("confidence_score"))
            if conf is not None and conf > 1.5:
                conf = conf / 100.0
            return (
                op if op is not None else -1.0,
                ov if ov is not None else -1.0,
                conf if conf is not None else -1.0,
            )

        rows.sort(key=_sort_key, reverse=True)
        rows = rows[:top_n]
        _apply_rank_overall(rows)

        criteria_snapshot = _top10_criteria_snapshot(criteria)

        final_rows: List[Dict[str, Any]] = []
        for i, row in enumerate(rows, start=1):
            row["top10_rank"] = i
            if not row.get("selection_reason"):
                row["selection_reason"] = _top10_selection_reason(row)
            if not row.get("criteria_snapshot"):
                row["criteria_snapshot"] = criteria_snapshot

            projected = _strict_project_row(keys, _normalize_to_schema_keys(keys, headers, row))
            if "top10_rank" in projected and projected.get("top10_rank") is None:
                projected["top10_rank"] = i
            if "selection_reason" in projected and not projected.get("selection_reason"):
                projected["selection_reason"] = row.get("selection_reason")
            if "criteria_snapshot" in projected and not projected.get("criteria_snapshot"):
                projected["criteria_snapshot"] = criteria_snapshot
            final_rows.append(projected)

        return headers, keys, final_rows

    # -------------------------------------------------------------------------
    # sheet/page APIs
    # -------------------------------------------------------------------------
    async def get_page_rows(
        self,
        page: Optional[str] = None,
        *,
        sheet: Optional[str] = None,
        sheet_name: Optional[str] = None,
        limit: int = 2000,
        offset: int = 0,
        mode: str = "",
        body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return await self.get_sheet_rows(
            page or sheet or sheet_name,
            limit=limit,
            offset=offset,
            mode=mode,
            body=body,
        )

    async def get_sheet(
        self,
        sheet_name: Optional[str] = None,
        *,
        sheet: Optional[str] = None,
        page: Optional[str] = None,
        limit: int = 2000,
        offset: int = 0,
        mode: str = "",
        body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return await self.get_sheet_rows(
            sheet_name or sheet or page,
            limit=limit,
            offset=offset,
            mode=mode,
            body=body,
        )

    async def get_sheet_rows(
        self,
        sheet: Optional[str] = None,
        *,
        sheet_name: Optional[str] = None,
        page: Optional[str] = None,
        limit: int = 2000,
        offset: int = 0,
        mode: str = "",
        body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        body = dict(body or {})
        limit = max(1, min(5000, int(limit or 2000)))
        offset = max(0, int(offset or 0))
        include_matrix = _safe_bool(body.get("include_matrix"), True)

        target_sheet = _canonicalize_sheet_name((sheet or sheet_name or page or "Market_Leaders").strip()) or "Market_Leaders"
        spec, headers, keys, schema_src = _schema_for_sheet(target_sheet)
        headers, keys = _complete_schema_contract(headers, keys)

        target_sheet_known = target_sheet in INSTRUMENT_SHEETS or target_sheet in SPECIAL_SHEETS or bool(spec)
        strict_req = bool(self.schema_strict_sheet_rows)

        contract_level = "canonical" if _usable_contract(headers, keys, target_sheet) else "partial"
        schema_warning = None
        recovered_from = None

        # ---------------------------------------------------------------------
        # SPECIAL SHEETS: Handle logic FIRST, before any global strict failure
        # ---------------------------------------------------------------------
        if target_sheet == "Data_Dictionary":
            raw_rows: List[Dict[str, Any]] = []
            dd_note = None
            try:
                from core.sheets.data_dictionary import build_data_dictionary_rows as _dd  # type: ignore
                raw_rows = _coerce_rows_list(_dd(include_meta_sheet=True))
                dd_note = "core.sheets.data_dictionary.build_data_dictionary_rows"
            except Exception:
                for sn in _list_sheet_names_best_effort():
                    sp, _, _, _ = _schema_for_sheet(sn)
                    cols = _schema_columns_from_any(sp)
                    if not cols:
                        continue
                    for c in cols:
                        if isinstance(c, Mapping):
                            rr = {
                                "sheet": sn,
                                "group": str(c.get("group", "")),
                                "header": str(c.get("header", "")),
                                "key": str(c.get("key", "")),
                                "dtype": str(c.get("dtype", "")),
                                "fmt": str(c.get("fmt", "")),
                                "required": bool(c.get("required", False)),
                                "source": str(c.get("source", "")),
                                "notes": str(c.get("notes", "")),
                            }
                        else:
                            rr = {
                                "sheet": sn,
                                "group": str(getattr(c, "group", "")),
                                "header": str(getattr(c, "header", "")),
                                "key": str(getattr(c, "key", "")),
                                "dtype": str(getattr(c, "dtype", "")),
                                "fmt": str(getattr(c, "fmt", "")),
                                "required": bool(getattr(c, "required", False)),
                                "source": str(getattr(c, "source", "")),
                                "notes": str(getattr(c, "notes", "")),
                            }
                        raw_rows.append(rr)
                dd_note = "fallback:internal"

            if not _usable_contract(headers, keys, target_sheet) and raw_rows and isinstance(raw_rows[0], dict):
                keys = list(raw_rows[0].keys())
                headers = [str(k).replace("_", " ").title() for k in keys]
                headers, keys = _complete_schema_contract(headers, keys)
                contract_level = "inferred"
                recovered_from = "builder_rows"

            if not _usable_contract(headers, keys, target_sheet):
                headers = ["Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes"]
                keys = ["sheet", "group", "header", "key", "dtype", "fmt", "required", "source", "notes"]
                headers, keys = _complete_schema_contract(headers, keys)
                if "fallback" not in schema_src:
                    schema_src = "fallback:standard_data_dictionary"
                    contract_level = "union_fallback"
                    recovered_from = "internal_fallback"

            base_meta = {
                "schema_source": schema_src,
                "contract_level": contract_level,
                "strict_requested": strict_req,
                "strict_enforced": False,
                "target_sheet_known": True,
                "builder": dd_note,
            }
            if recovered_from: base_meta["recovered_from"] = recovered_from

            if _is_schema_only_body(body):
                return self._finalize_payload(
                    sheet="Data_Dictionary",
                    headers=headers, keys=keys, rows=[],
                    include_matrix=include_matrix, status="success",
                    meta={**base_meta, "rows": 0, "limit": limit, "offset": offset, "built_from": "schema_only_special_path"},
                )

            rows = [_normalize_to_schema_keys(keys, headers, r) for r in raw_rows]
            full_rows = self._project_rows_to_schema(rows, headers=headers, keys=keys)
            payload_full = self._finalize_payload(
                sheet="Data_Dictionary",
                headers=headers, keys=keys, rows=full_rows,
                include_matrix=include_matrix, status="success",
                meta={**base_meta, "rows": len(full_rows), "limit": limit, "offset": offset},
            )
            self._store_sheet_snapshot("Data_Dictionary", payload_full)

            rows_page = full_rows[offset: offset + limit]
            return self._finalize_payload(
                sheet="Data_Dictionary",
                headers=headers, keys=keys, rows=rows_page,
                include_matrix=include_matrix, status="success",
                meta={**payload_full.get("meta", {}), "rows": len(rows_page)},
            )

        if target_sheet == "Insights_Analysis":
            rows0: List[Dict[str, Any]] = []
            builder_name = "core.analysis.insights_builder"
            try:
                from core.analysis.insights_builder import build_insights_analysis_rows  # type: ignore
                crit = body.get("criteria") if isinstance(body.get("criteria"), dict) else None
                universes = body.get("universes") if isinstance(body.get("universes"), dict) else None
                symbols = body.get("symbols") if isinstance(body.get("symbols"), list) else None
                payload = await build_insights_analysis_rows(
                    engine=self, criteria=crit, universes=universes,
                    symbols=symbols, mode=mode or "",
                )
                rows0 = _coerce_rows_list(payload)
            except Exception:
                rows0 = []

            if not rows0:
                rows0 = [
                    {
                        "section": "System",
                        "item": "Insights Status",
                        "symbol": "",
                        "metric": "status",
                        "value": "No live insight rows returned",
                        "notes": "Fallback row generated by data_engine_v2",
                        "last_updated_riyadh": _now_riyadh_iso(),
                    }
                ]
                builder_name = "fallback:system_row"

            if not _usable_contract(headers, keys, target_sheet) and rows0 and isinstance(rows0[0], dict):
                keys = list(rows0[0].keys())
                headers = [str(k).replace("_", " ").title() for k in keys]
                headers, keys = _complete_schema_contract(headers, keys)
                contract_level = "inferred"
                recovered_from = "builder_rows"

            base_meta = {
                "schema_source": schema_src,
                "contract_level": contract_level,
                "strict_requested": strict_req,
                "strict_enforced": False,
                "target_sheet_known": True,
                "builder": builder_name,
            }
            if recovered_from: base_meta["recovered_from"] = recovered_from

            if _is_schema_only_body(body):
                return self._finalize_payload(
                    sheet=target_sheet,
                    headers=headers, keys=keys, rows=[],
                    include_matrix=include_matrix, status="success",
                    meta={**base_meta, "rows": 0, "limit": limit, "offset": offset, "mode": mode, "built_from": "schema_only_special_path"},
                )

            full_rows = self._project_rows_to_schema(rows0, headers=headers, keys=keys)
            payload_full = self._finalize_payload(
                sheet=target_sheet,
                headers=headers, keys=keys, rows=full_rows,
                include_matrix=include_matrix, status="success",
                meta={**base_meta, "rows": len(full_rows), "limit": limit, "offset": offset, "mode": mode},
            )
            self._store_sheet_snapshot(target_sheet, payload_full)

            rows_page = full_rows[offset: offset + limit]
            return self._finalize_payload(
                sheet=target_sheet,
                headers=headers, keys=keys, rows=rows_page,
                include_matrix=include_matrix, status="success",
                meta={**payload_full.get("meta", {}), "rows": len(rows_page)},
            )

        if target_sheet == "Top_10_Investments":
            top10_body, route_warnings = _normalize_top10_body_for_engine(body, limit=max(1, min(limit, 50)))
            cached_before = self.get_cached_sheet_snapshot(sheet=target_sheet)

            if _is_schema_only_body(top10_body):
                if not _usable_contract(headers, keys, target_sheet):
                    keys = list(_SCHEMA_UNION_KEYS)
                    for field in TOP10_REQUIRED_FIELDS:
                        if field not in keys:
                            keys.append(field)
                    headers = [TOP10_REQUIRED_HEADERS.get(k, k.replace("_", " ").title()) for k in keys]
                    headers, keys = _complete_schema_contract(headers, keys)
                    schema_src = "schema_only_repaired_fallback"
                    contract_level = "union_fallback"
                    route_warnings.append("schema_repaired_for_schema_only_request")

                return self._finalize_payload(
                    sheet=target_sheet,
                    headers=headers, keys=keys, rows=[],
                    include_matrix=include_matrix, status="success",
                    meta={
                        "schema_source": schema_src,
                        "contract_level": contract_level,
                        "strict_requested": strict_req,
                        "strict_enforced": False,
                        "target_sheet_known": True,
                        "builder": "schema_only_fast_path",
                        "rows": 0, "limit": limit, "offset": offset, "mode": mode,
                        "warnings": route_warnings,
                    },
                )

            rows0: List[Dict[str, Any]] = []
            meta_extra: Dict[str, Any] = {}
            status_out = "success"
            builder_used = "core.analysis.top10_selector"

            try:
                from core.analysis.top10_selector import build_top10_rows  # type: ignore
                criteria = top10_body.get("criteria") if isinstance(top10_body.get("criteria"), dict) else None
                payload = await build_top10_rows(
                    engine=self, settings=self.settings, criteria=criteria,
                    body=dict(top10_body or {}),
                    limit=int(top10_body.get("limit") or top10_body.get("top_n") or min(limit, 10) or 10),
                    mode=mode or "",
                )
                rows0 = _coerce_rows_list(payload)
                if rows0:
                    rows0 = await self._hydrate_rows_with_quotes(rows0, schema=None)
                meta_extra = payload.get("meta") if isinstance(payload, dict) and isinstance(payload.get("meta"), dict) else {}
                status_out = payload.get("status") if isinstance(payload, dict) and payload.get("status") else "success"
            except Exception as e:
                rows0 = []
                meta_extra = {"top10_error": f"{type(e).__name__}: {e}"}
                status_out = "warn"

            if _usable_contract(headers, keys, target_sheet):
                full_rows = self._project_rows_to_schema(rows0, headers=headers, keys=keys)
            else:
                full_rows = []

            if not full_rows:
                fb_headers, fb_keys, fallback_rows = await self._build_top10_rows_fallback(headers, keys, top10_body, limit, mode)
                if fallback_rows:
                    headers, keys = fb_headers, fb_keys
                    full_rows = fallback_rows
                    builder_used = "fallback:live_ranker"
                    contract_level = "union_fallback"
                    route_warnings = route_warnings + ["selector_empty_used_live_ranker_fallback"]
                    status_out = "warn"

            if not full_rows and isinstance(cached_before, dict):
                cached_rows = _coerce_rows_list(cached_before)
                cached_headers = cached_before.get("headers") if isinstance(cached_before.get("headers"), list) else headers
                cached_keys = cached_before.get("keys") if isinstance(cached_before.get("keys"), list) else keys
                cached_headers, cached_keys = self._top10_schema_contract(list(cached_headers), list(cached_keys))
                if cached_rows and _usable_contract(cached_headers, cached_keys, target_sheet):
                    full_rows_cached = self._project_rows_to_schema(cached_rows, headers=cached_headers, keys=cached_keys)
                    rows_page = full_rows_cached[offset: offset + limit]
                    return self._finalize_payload(
                        sheet=target_sheet,
                        headers=cached_headers, keys=cached_keys, rows=rows_page,
                        include_matrix=include_matrix, status="warn",
                        meta={
                            "schema_source": schema_src,
                            "contract_level": "recovered",
                            "recovered_from": "cached_snapshot",
                            "strict_requested": strict_req,
                            "strict_enforced": False,
                            "target_sheet_known": True,
                            "builder": "cached_snapshot_fallback",
                            "rows": len(rows_page), "limit": limit, "offset": offset,
                            "mode": mode, "built_from": "cached_snapshot_fallback",
                            "warnings": route_warnings + ["top10_degraded_to_cached_snapshot"],
                            **(meta_extra or {}),
                        },
                    )

            payload_full = self._finalize_payload(
                sheet=target_sheet,
                headers=headers, keys=keys, rows=full_rows,
                include_matrix=include_matrix, status=status_out or ("success" if full_rows else "warn"),
                meta={
                    "schema_source": schema_src,
                    "contract_level": contract_level,
                    "strict_requested": strict_req,
                    "strict_enforced": False,
                    "target_sheet_known": True,
                    "builder": builder_used,
                    "rows": len(full_rows), "limit": limit, "offset": offset, "mode": mode,
                    "warnings": route_warnings,
                    **(meta_extra or {}),
                },
            )

            if full_rows:
                self._store_sheet_snapshot(target_sheet, payload_full)

            rows_page = full_rows[offset: offset + limit]
            return self._finalize_payload(
                sheet=target_sheet,
                headers=headers, keys=keys, rows=rows_page,
                include_matrix=include_matrix, status=payload_full.get("status", "success"),
                meta={**payload_full.get("meta", {}), "rows": len(rows_page)},
            )


        # ---------------------------------------------------------------------
        # SCHEMA CONTRACT RECOVERY FLOW (Instrument & Unknown Sheets)
        # ---------------------------------------------------------------------
        if contract_level != "canonical":
            recovered = False

            # a) Try to recover from cached snapshot contract
            cached_snap = self.get_cached_sheet_snapshot(sheet=target_sheet)
            if isinstance(cached_snap, dict):
                c_headers = cached_snap.get("headers") or cached_snap.get("display_headers")
                c_keys = cached_snap.get("keys") or cached_snap.get("fields")
                if c_headers or c_keys:
                    ch, ck = _complete_schema_contract(c_headers or [], c_keys or [])
                    if _usable_contract(ch, ck, target_sheet):
                        headers, keys = ch, ck
                        schema_src = "recovered_from_cache_contract"
                        recovered_from = "cache_contract"
                        contract_level = "recovered"
                        recovered = True

            # b) Infer from cached rows
            if not recovered:
                c_rows = _coerce_rows_list(cached_snap) if 'cached_snap' in locals() else None
                if c_rows and isinstance(c_rows[0], dict):
                    ch, ck = _complete_schema_contract([], list(c_rows[0].keys()))
                    if _usable_contract(ch, ck, target_sheet):
                        headers, keys = ch, ck
                        schema_src = "recovered_from_cached_rows"
                        recovered_from = "cached_rows"
                        contract_level = "inferred"
                        recovered = True

            # c) Infer from external rows (for instrument sheets)
            if not recovered and target_sheet in INSTRUMENT_SHEETS:
                try:
                    ext_rows = await self._get_rows_from_external_reader(target_sheet, 1)
                    if ext_rows and isinstance(ext_rows[0], dict):
                        ch, ck = _complete_schema_contract([], list(ext_rows[0].keys()))
                        if _usable_contract(ch, ck, target_sheet):
                            headers, keys = ch, ck
                            schema_src = "recovered_from_external_rows"
                            recovered_from = "external_rows"
                            contract_level = "inferred"
                            recovered = True
                except Exception:
                    pass

            # d) Infer from live quote (for instrument sheets)
            if not recovered and target_sheet in INSTRUMENT_SHEETS:
                try:
                    sample_syms = await self.get_sheet_symbols(target_sheet, limit=1, body=body)
                    if sample_syms:
                        sample_q = await self.get_enriched_quote_dict(sample_syms[0], schema=None)
                        if sample_q:
                            ch, ck = _complete_schema_contract([], list(sample_q.keys()))
                            if _usable_contract(ch, ck, target_sheet):
                                headers, keys = ch, ck
                                schema_src = "recovered_from_live_quote"
                                recovered_from = "live_quote"
                                contract_level = "inferred"
                                recovered = True
                except Exception:
                    pass
                    
            # e) Fallback to Union for known instrument sheets instead of immediate failure
            if not recovered and target_sheet in INSTRUMENT_SHEETS:
                keys = list(_SCHEMA_UNION_KEYS)
                headers = list(_SCHEMA_UNION_KEYS)
                schema_src = "fallback_union"
                recovered_from = "union_fallback"
                contract_level = "union_fallback"
                recovered = True

            # f) Strict Mode Hard Failure ONLY IF it's NOT a known sheet and NO recovery succeeded
            if not recovered and strict_req and _SCHEMA_AVAILABLE and not target_sheet_known:
                base_meta_err = {
                    "schema_source": schema_src,
                    "contract_level": "failed",
                    "strict_requested": True,
                    "strict_enforced": True,
                    "target_sheet_known": target_sheet_known,
                    "known_sheets": _list_sheet_names_best_effort(),
                }
                return self._finalize_payload(
                    sheet=target_sheet,
                    headers=[], keys=[], rows=[],
                    include_matrix=include_matrix, status="error",
                    meta=base_meta_err,
                    error=f"Unknown sheet or schema missing for '{target_sheet}'",
                )

        final_status = "success"
        if contract_level == "union_fallback" and target_sheet_known:
            final_status = "warn"
            schema_warning = schema_warning or "canonical_schema_unusable_used_union_schema"
        elif not target_sheet_known and not strict_req:
            final_status = "warn"
            schema_warning = schema_warning or "unknown_sheet_non_strict_mode"

        base_meta = {
            "schema_source": schema_src,
            "contract_level": contract_level,
            "strict_requested": strict_req,
            "strict_enforced": strict_req and not target_sheet_known and not _usable_contract(headers, keys, target_sheet),
            "target_sheet_known": target_sheet_known,
        }
        if recovered_from: base_meta["recovered_from"] = recovered_from
        if schema_warning: base_meta["schema_warning"] = schema_warning


        # ---------------------------------------------------------------------
        # SCHEMA ONLY FAST PATH FOR INSTRUMENT SHEETS
        # ---------------------------------------------------------------------
        if _is_schema_only_body(body):
            return self._finalize_payload(
                sheet=target_sheet,
                headers=headers, keys=keys, rows=[],
                include_matrix=include_matrix, status=final_status,
                meta={
                    **base_meta,
                    "rows": 0, "limit": limit, "offset": offset, "mode": mode,
                    "built_from": "schema_only_fast_path",
                },
            )


        # ---------------------------------------------------------------------
        # STANDARD INSTRUMENT SHEETS DATA HYDRATION
        # ---------------------------------------------------------------------
        requested_symbols = _extract_requested_symbols_from_body(body, limit=limit + offset)
        built_from = "body_symbols" if requested_symbols else "live_quotes"

        if requested_symbols:
            self._set_sheet_symbols_meta(target_sheet, "body_symbols", len(requested_symbols))

        if not requested_symbols and target_sheet in INSTRUMENT_SHEETS:
            requested_symbols = await self.get_sheet_symbols(target_sheet, limit=limit + offset, body=body)
            sym_meta0 = self._get_sheet_symbols_meta(target_sheet)
            built_from = sym_meta0.get("source") or ("auto_sheet_symbols" if requested_symbols else "empty")

        out_headers = headers[:] if headers else (keys[:] if keys else [])
        out_keys = keys[:] if keys else (headers[:] if headers else [])

        if not requested_symbols and target_sheet in INSTRUMENT_SHEETS:
            ext_rows = await self._get_rows_from_external_reader(target_sheet, limit + offset)
            if ext_rows and self.rows_hydrate_external:
                try:
                    ext_rows = await self._hydrate_rows_with_quotes(ext_rows, schema=None)
                except Exception:
                    pass
            if ext_rows:
                if not strict_req and not _usable_contract(out_headers, out_keys, target_sheet) and isinstance(ext_rows[0], dict):
                    out_keys = list(ext_rows[0].keys())
                    out_headers = [k.replace("_", " ").title() for k in out_keys]
                    out_headers, out_keys = _complete_schema_contract(out_headers, out_keys)

                full_rows = self._project_rows_to_schema(ext_rows, headers=out_headers, keys=out_keys)
                payload_full = self._finalize_payload(
                    sheet=target_sheet,
                    headers=out_headers, keys=out_keys, rows=full_rows,
                    include_matrix=include_matrix, status=final_status,
                    meta={
                        **base_meta,
                        "rows": len(full_rows), "limit": limit, "offset": offset, "mode": mode,
                        "built_from": "external_rows_reader",
                        "rows_reader_source": self._rows_reader_source,
                        "symbols_reader_source": self._symbols_reader_source,
                        "symbol_resolution_meta": self._get_sheet_symbols_meta(target_sheet),
                    },
                )
                self._store_sheet_snapshot(target_sheet, payload_full)
                rows_page = full_rows[offset: offset + limit]
                return self._finalize_payload(
                    sheet=target_sheet,
                    headers=out_headers, keys=out_keys, rows=rows_page,
                    include_matrix=include_matrix, status=final_status,
                    meta={**payload_full.get("meta", {}), "rows": len(rows_page)},
                )

        if not requested_symbols:
            cached_snap = self.get_cached_sheet_snapshot(sheet=target_sheet) if 'cached_snap' not in locals() else cached_snap
            cached_rows = _coerce_rows_list(cached_snap)
            if cached_rows:
                if not strict_req and not _usable_contract(out_headers, out_keys, target_sheet) and isinstance(cached_rows[0], dict):
                    out_keys = list(cached_rows[0].keys())
                    out_headers = [k.replace("_", " ").title() for k in out_keys]
                    out_headers, out_keys = _complete_schema_contract(out_headers, out_keys)

                full_rows = self._project_rows_to_schema(cached_rows, headers=out_headers, keys=out_keys)
                rows_page = full_rows[offset: offset + limit]
                return self._finalize_payload(
                    sheet=target_sheet,
                    headers=out_headers, keys=out_keys, rows=rows_page,
                    include_matrix=include_matrix, status=final_status,
                    meta={
                        **base_meta,
                        "rows": len(rows_page), "limit": limit, "offset": offset, "mode": mode,
                        "built_from": "cached_snapshot", "auto_symbols_count": 0,
                        "symbols_reader_source": self._symbols_reader_source,
                        "symbol_resolution_meta": self._get_sheet_symbols_meta(target_sheet),
                    },
                )

        rows_full: List[Dict[str, Any]] = []
        if requested_symbols:
            # We now process quotes agnostic of schema entirely, schema projection is explicit down below.
            quotes = await self.get_enriched_quotes(requested_symbols, schema=None)
            
            if not strict_req and not _usable_contract(out_headers, out_keys, target_sheet) and quotes:
                first_d = _model_to_dict(quotes[0])
                if first_d:
                    out_keys = list(first_d.keys())
                    out_headers = [k.replace("_", " ").title() for k in out_keys]
                    out_headers, out_keys = _complete_schema_contract(out_headers, out_keys)

            for q in quotes:
                d = _model_to_dict(q)
                d = _normalize_to_schema_keys(out_keys, out_headers, d) if out_keys else d
                rows_full.append(_strict_project_row(out_keys, d) if out_keys else d)

        if rows_full and ("rank_overall" in (out_keys or [])):
            _apply_rank_overall(rows_full)

        if rows_full:
            sym_meta = self._get_sheet_symbols_meta(target_sheet)
            payload_full = self._finalize_payload(
                sheet=target_sheet,
                headers=out_headers, keys=out_keys, rows=rows_full,
                include_matrix=include_matrix, status=final_status,
                meta={
                    **base_meta,
                    "rows": len(rows_full), "limit": limit, "offset": offset, "mode": mode,
                    "built_from": built_from,
                    "auto_symbols_count": len(requested_symbols) if built_from != "body_symbols" else 0,
                    "resolved_symbols_count": len(requested_symbols),
                    "symbols_reader_source": self._symbols_reader_source,
                    "symbol_resolution_meta": sym_meta,
                },
            )
            self._store_sheet_snapshot(target_sheet, payload_full)

            rows_page = rows_full[offset: offset + limit]
            return self._finalize_payload(
                sheet=target_sheet,
                headers=out_headers, keys=out_keys, rows=rows_page,
                include_matrix=include_matrix, status=final_status,
                meta={**payload_full.get("meta", {}), "rows": len(rows_page)},
            )

        return self._finalize_payload(
            sheet=target_sheet,
            headers=out_headers, keys=out_keys, rows=[],
            include_matrix=include_matrix, status=final_status,
            meta={
                **base_meta,
                "rows": 0, "limit": limit, "offset": offset, "mode": mode,
                "built_from": built_from,
                "auto_symbols_count": len(requested_symbols) if built_from != "body_symbols" else 0,
                "resolved_symbols_count": len(requested_symbols),
                "symbols_reader_source": self._symbols_reader_source,
                "symbol_resolution_meta": self._get_sheet_symbols_meta(target_sheet),
            },
        )

    async def sheet_rows(self, *args, **kwargs) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    async def build_sheet_rows(self, *args, **kwargs) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    # -------------------------------------------------------------------------
    # health / stats
    # -------------------------------------------------------------------------
    async def health(self) -> Dict[str, Any]:
        return {
            "status": "ok",
            "version": self.version,
            "schema_available": bool(_SCHEMA_AVAILABLE),
            "snapshot_sheets": len(self._sheet_snapshots),
        }

    async def get_health(self) -> Dict[str, Any]:
        return await self.health()

    async def health_check(self) -> Dict[str, Any]:
        return await self.health()

    async def get_stats(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "primary_provider": self.primary_provider,
            "enabled_providers": self.enabled_providers,
            "ksa_providers": self.ksa_providers,
            "global_providers": self.global_providers,
            "ksa_disallow_eodhd": self.ksa_disallow_eodhd,
            "flags": dict(self.flags),
            "provider_stats": await self._registry.get_stats(),
            "schema_available": bool(_SCHEMA_AVAILABLE),
            "schema_strict_sheet_rows": bool(self.schema_strict_sheet_rows),
            "top10_force_full_schema": bool(self.top10_force_full_schema),
            "rows_hydrate_external": bool(self.rows_hydrate_external),
            "symbols_reader_source": self._symbols_reader_source,
            "rows_reader_source": self._rows_reader_source,
            "snapshot_sheets": sorted(list(self._sheet_snapshots.keys())),
            "sheet_symbol_resolution_meta": dict(self._sheet_symbol_resolution_meta),
        }


_ENGINE_INSTANCE: Optional['DataEngineV5'] = None
ENGINE: Optional['DataEngineV5'] = None
engine: Optional['DataEngineV5'] = None
_ENGINE: Optional['DataEngineV5'] = None
_ENGINE_LOCK = asyncio.Lock()


async def get_engine() -> DataEngineV5:
    global _ENGINE_INSTANCE, ENGINE, engine, _ENGINE
    if _ENGINE_INSTANCE is None:
        async with _ENGINE_LOCK:
            if _ENGINE_INSTANCE is None:
                _ENGINE_INSTANCE = DataEngineV5()
    ENGINE = _ENGINE_INSTANCE
    engine = _ENGINE_INSTANCE
    _ENGINE = _ENGINE_INSTANCE
    return _ENGINE_INSTANCE


async def close_engine() -> None:
    global _ENGINE_INSTANCE, ENGINE, engine, _ENGINE
    if _ENGINE_INSTANCE:
        await _ENGINE_INSTANCE.aclose()
    _ENGINE_INSTANCE = None
    ENGINE = None
    engine = None
    _ENGINE = None


def get_engine_if_ready() -> Optional['DataEngineV5']:
    return _ENGINE_INSTANCE


def peek_engine() -> Optional['DataEngineV5']:
    return _ENGINE_INSTANCE


def get_cache() -> Any:
    global _ENGINE_INSTANCE
    return getattr(_ENGINE_INSTANCE, "_cache", None)


DataEngineV4 = DataEngineV5
DataEngineV3 = DataEngineV5
DataEngineV2 = DataEngineV5
DataEngine = DataEngineV5

__all__ = [
    "DataEngineV5",
    "DataEngineV4",
    "DataEngineV3",
    "DataEngineV2",
    "DataEngine",
    "ENGINE",
    "engine",
    "_ENGINE",
    "get_engine",
    "get_engine_if_ready",
    "peek_engine",
    "close_engine",
    "get_cache",
    "QuoteQuality",
    "DataSource",
    "__version__",
    "normalize_row_to_schema",
]
