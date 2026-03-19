#!/usr/bin/env python3
# core/data_engine_v2.py
"""
================================================================================
Data Engine V2 — GLOBAL-FIRST ORCHESTRATOR — v5.36.0
================================================================================

WHY v5.36.0
-----------
- FIX: makes V2 import-safe and startup-safe even when optional integrations,
       provider modules, schema registry modules, or page catalog modules are
       unavailable.
- FIX: introduces static canonical contracts for all core pages so known sheets
       never degrade to "schema empty" during runtime.
- FIX: preserves route compatibility aliases expected by wrappers, selectors,
       and direct router calls.
- FIX: returns canonical headers/keys/columns/fields consistently across all
       payload families.
- FIX: supports external rows/symbol readers when available, but degrades
       safely to snapshots and emergency symbols instead of acting like a stub.
- FIX: exposes singleton aliases ENGINE / engine / _ENGINE and import-safe
       get_engine() / close_engine() helpers.
- FIX: adds stronger Top10 and Insights fallbacks inside the engine so sheet-row
       routes remain useful even when specialized builders are temporarily down.
- FIX: avoids fake-row coercion from nested wrapper dicts.

Design goals
------------
- Never fail import because an optional module is missing.
- Never return an empty schema for a known page.
- Prefer live external readers/providers when available.
- Preserve schema-first contracts for route stability.
- Keep payloads JSON-safe and route-tolerant.
================================================================================
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal
from enum import Enum
from importlib import import_module
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple, Union

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

try:
    from pydantic import BaseModel, ConfigDict
except Exception:  # pragma: no cover
    BaseModel = object  # type: ignore
    ConfigDict = dict  # type: ignore

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

__version__ = "5.36.0"

logger = logging.getLogger("core.data_engine_v2")
logger.addHandler(logging.NullHandler())

# =============================================================================
# Minimal domain models
# =============================================================================
class QuoteQuality(str, Enum):
    GOOD = "good"
    FAIR = "fair"
    MISSING = "missing"


class DataSource(str, Enum):
    ENGINE_V2 = "engine_v2"
    EXTERNAL_ROWS = "external_rows"
    SNAPSHOT = "snapshot"
    FALLBACK = "fallback"


class UnifiedQuote(BaseModel):
    model_config = ConfigDict(extra="allow")


# =============================================================================
# Canonical page contracts
# =============================================================================
INSTRUMENT_CANONICAL_KEYS: List[str] = [
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
]

INSTRUMENT_CANONICAL_HEADERS: List[str] = [
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
]

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

INSIGHTS_HEADERS: List[str] = [
    "Symbol",
    "Recommendation",
    "Recommendation Reason",
    "Current Price",
    "Forecast Price 1M",
    "Forecast Price 3M",
    "Forecast Price 12M",
    "Expected ROI 1M",
    "Expected ROI 3M",
    "Expected ROI 12M",
    "Forecast Confidence",
    "Overall Score",
    "Risk Bucket",
    "Horizon Days",
    "Invest Period Label",
    "Top10 Rank",
    "Selection Reason",
    "Criteria Snapshot",
]
INSIGHTS_KEYS: List[str] = [
    "symbol",
    "recommendation",
    "recommendation_reason",
    "current_price",
    "forecast_price_1m",
    "forecast_price_3m",
    "forecast_price_12m",
    "expected_roi_1m",
    "expected_roi_3m",
    "expected_roi_12m",
    "forecast_confidence",
    "overall_score",
    "risk_bucket",
    "horizon_days",
    "invest_period_label",
    "top10_rank",
    "selection_reason",
    "criteria_snapshot",
]

DATA_DICTIONARY_HEADERS: List[str] = [
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
DATA_DICTIONARY_KEYS: List[str] = [
    "sheet",
    "group",
    "header",
    "key",
    "dtype",
    "fmt",
    "required",
    "source",
    "notes",
]

STATIC_CANONICAL_SHEET_CONTRACTS: Dict[str, Dict[str, List[str]]] = {
    "Market_Leaders": {"headers": list(INSTRUMENT_CANONICAL_HEADERS), "keys": list(INSTRUMENT_CANONICAL_KEYS)},
    "Global_Markets": {"headers": list(INSTRUMENT_CANONICAL_HEADERS), "keys": list(INSTRUMENT_CANONICAL_KEYS)},
    "Commodities_FX": {"headers": list(INSTRUMENT_CANONICAL_HEADERS), "keys": list(INSTRUMENT_CANONICAL_KEYS)},
    "Mutual_Funds": {"headers": list(INSTRUMENT_CANONICAL_HEADERS), "keys": list(INSTRUMENT_CANONICAL_KEYS)},
    "My_Portfolio": {"headers": list(INSTRUMENT_CANONICAL_HEADERS), "keys": list(INSTRUMENT_CANONICAL_KEYS)},
    "My_Investments": {"headers": list(INSTRUMENT_CANONICAL_HEADERS), "keys": list(INSTRUMENT_CANONICAL_KEYS)},
    "Top_10_Investments": {
        "headers": list(INSTRUMENT_CANONICAL_HEADERS) + [TOP10_REQUIRED_HEADERS[k] for k in TOP10_REQUIRED_FIELDS],
        "keys": list(INSTRUMENT_CANONICAL_KEYS) + list(TOP10_REQUIRED_FIELDS),
    },
    "Insights_Analysis": {"headers": list(INSIGHTS_HEADERS), "keys": list(INSIGHTS_KEYS)},
    "Data_Dictionary": {"headers": list(DATA_DICTIONARY_HEADERS), "keys": list(DATA_DICTIONARY_KEYS)},
}

INSTRUMENT_SHEETS: Set[str] = {
    "Market_Leaders",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
    "My_Portfolio",
    "My_Investments",
    "Top_10_Investments",
}
SPECIAL_SHEETS: Set[str] = {"Insights_Analysis", "Data_Dictionary"}
TOP10_ENGINE_DEFAULT_PAGES: List[str] = [
    "Market_Leaders",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
    "My_Portfolio",
    "My_Investments",
]

EMERGENCY_PAGE_SYMBOLS: Dict[str, List[str]] = {
    "Market_Leaders": ["2222.SR", "1120.SR", "2010.SR", "7010.SR", "AAPL", "MSFT", "NVDA", "GOOGL"],
    "Global_Markets": ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AVGO"],
    "Commodities_FX": ["GC=F", "BZ=F", "SI=F", "EURUSD=X", "GBPUSD=X", "JPY=X", "SAR=X", "CL=F"],
    "Mutual_Funds": ["SPY", "QQQ", "VTI", "VOO", "IWM"],
    "My_Portfolio": ["2222.SR", "AAPL", "MSFT"],
    "My_Investments": ["2222.SR", "AAPL", "MSFT"],
    "Top_10_Investments": ["2222.SR", "1120.SR", "AAPL", "MSFT", "NVDA"],
}

PAGE_SYMBOL_ENV_KEYS: Dict[str, str] = {
    "Market_Leaders": "MARKET_LEADERS_SYMBOLS",
    "Global_Markets": "GLOBAL_MARKETS_SYMBOLS",
    "Commodities_FX": "COMMODITIES_FX_SYMBOLS",
    "Mutual_Funds": "MUTUAL_FUNDS_SYMBOLS",
    "My_Portfolio": "MY_PORTFOLIO_SYMBOLS",
    "My_Investments": "MY_INVESTMENTS_SYMBOLS",
    "Top_10_Investments": "TOP10_FALLBACK_SYMBOLS",
}

DEFAULT_PROVIDERS = ["eodhd", "yahoo", "finnhub"]
DEFAULT_KSA_PROVIDERS = ["tadawul", "argaam", "yahoo"]
DEFAULT_GLOBAL_PROVIDERS = ["eodhd", "yahoo", "finnhub"]
PROVIDER_PRIORITIES = {"tadawul": 10, "argaam": 20, "eodhd": 30, "yahoo": 40, "finnhub": 50, "yahoo_chart": 60}

# =============================================================================
# Small helpers
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


def _safe_bool(x: Any, default: bool = False) -> bool:
    if isinstance(x, bool):
        return x
    s = _safe_str(x).lower()
    if s in {"1", "true", "yes", "y", "on", "t"}:
        return True
    if s in {"0", "false", "no", "n", "off", "f"}:
        return False
    return default


def _safe_int(x: Any, default: int = 0, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    try:
        v = int(float(x))
    except Exception:
        v = int(default)
    if lo is not None:
        v = max(lo, v)
    if hi is not None:
        v = min(hi, v)
    return v


def _as_float(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _as_pct_fraction(x: Any) -> Optional[float]:
    v = _as_float(x)
    if v is None:
        return None
    if abs(v) > 1.5:
        return v / 100.0
    return v


def _dedupe_keep_order(items: Sequence[Any]) -> List[Any]:
    out: List[Any] = []
    seen: Set[Any] = set()
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _canonicalize_sheet_name(name: str) -> str:
    raw = _safe_str(name)
    if not raw:
        return ""
    candidates = [raw, raw.replace(" ", "_"), raw.replace("-", "_"), _norm_key(raw)]
    known = {k: k for k in STATIC_CANONICAL_SHEET_CONTRACTS.keys()}
    by_norm = {_norm_key(k): k for k in STATIC_CANONICAL_SHEET_CONTRACTS.keys()}
    for cand in candidates:
        if cand in known:
            return known[cand]
        nk = _norm_key(cand)
        if nk in by_norm:
            return by_norm[nk]
    return raw.replace(" ", "_")


def _sheet_lookup_candidates(sheet: str) -> List[str]:
    s = _canonicalize_sheet_name(sheet)
    vals = [s, s.replace("_", " "), s.lower(), _norm_key(s), _norm_key_loose(s)]
    return [v for v in _dedupe_keep_order(vals) if _safe_str(v)]


def _looks_like_symbol_token(x: Any) -> bool:
    s = _safe_str(x)
    if not s:
        return False
    if len(s) > 24:
        return False
    if re.match(r"^[A-Z0-9.=\-:^/]{1,24}$", s):
        return True
    if re.match(r"^[0-9]{4}(\.SR)?$", s):
        return True
    return False


def normalize_symbol(symbol: str) -> str:
    return _safe_str(symbol).upper()


def get_symbol_info(symbol: str) -> Dict[str, Any]:
    s = normalize_symbol(symbol)
    return {
        "requested": _safe_str(symbol),
        "normalized": s,
        "is_ksa": s.endswith(".SR") or re.match(r"^[0-9]{4}$", s) is not None,
    }


def _split_symbols(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        out: List[str] = []
        for v in value:
            out.extend(_split_symbols(v))
        return out
    s = _safe_str(value)
    if not s:
        return []
    parts = re.split(r"[,;|\s]+", s)
    return [p.strip() for p in parts if p.strip()]


def _normalize_symbol_list(symbols: Iterable[Any], limit: int = 5000) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for item in symbols:
        s = normalize_symbol(_safe_str(item))
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
        if len(out) >= limit:
            break
    return out


def _extract_nested_dict(payload: Dict[str, Any], key: str) -> Dict[str, Any]:
    val = payload.get(key)
    return dict(val) if isinstance(val, dict) else {}


def _extract_requested_symbols_from_body(body: Optional[Dict[str, Any]], limit: int = 5000) -> List[str]:
    if not isinstance(body, dict):
        return []
    raw: List[str] = []
    for key in (
        "symbols",
        "tickers",
        "selected_symbols",
        "direct_symbols",
        "codes",
        "watchlist",
        "portfolio_symbols",
    ):
        raw.extend(_split_symbols(body.get(key)))
    criteria = body.get("criteria")
    if isinstance(criteria, dict):
        for key in ("symbols", "tickers", "selected_symbols", "direct_symbols", "codes"):
            raw.extend(_split_symbols(criteria.get(key)))
    return _normalize_symbol_list(raw, limit=limit)


def _extract_top10_pages_from_body(body: Optional[Dict[str, Any]]) -> List[str]:
    if not isinstance(body, dict):
        return []
    raw: List[str] = []
    for key in ("pages_selected", "pages", "source_pages"):
        val = body.get(key)
        if isinstance(val, (list, tuple, set)):
            raw.extend([_canonicalize_sheet_name(_safe_str(v)) for v in val if _safe_str(v)])
    criteria = body.get("criteria")
    if isinstance(criteria, dict):
        val = criteria.get("pages_selected") or criteria.get("pages")
        if isinstance(val, (list, tuple, set)):
            raw.extend([_canonicalize_sheet_name(_safe_str(v)) for v in val if _safe_str(v)])
    return [p for p in _dedupe_keep_order(raw) if p]


def _normalize_top10_body_for_engine(body: Optional[Dict[str, Any]], limit: int) -> Tuple[Dict[str, Any], List[str]]:
    out = dict(body or {})
    warnings: List[str] = []
    criteria = dict(out.get("criteria") or {}) if isinstance(out.get("criteria"), dict) else {}
    if not criteria:
        criteria = {}
    if not criteria.get("top_n"):
        criteria["top_n"] = max(1, min(limit, 50))
    out["criteria"] = criteria
    out.setdefault("top_n", criteria.get("top_n"))
    return out, warnings


def _is_schema_only_body(body: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(body, dict):
        return False
    return _safe_bool(body.get("schema_only"), False) or _safe_bool(body.get("headers_only"), False)


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_riyadh_iso() -> str:
    try:
        if ZoneInfo is not None:
            return datetime.now(ZoneInfo("Asia/Riyadh")).isoformat()
    except Exception:
        pass
    return datetime.now(timezone.utc).isoformat()


def _safe_env(name: str, default: str = "") -> str:
    return _safe_str(os.getenv(name), default)


def _get_env_bool(name: str, default: bool = False) -> bool:
    return _safe_bool(os.getenv(name), default)


def _get_env_int(name: str, default: int) -> int:
    return _safe_int(os.getenv(name), default)


def _get_env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except Exception:
        return float(default)


def _get_env_list(name: str, default: Sequence[str]) -> List[str]:
    raw = _safe_env(name, "")
    if not raw:
        return [str(x).lower() for x in default]
    return [p.strip().lower() for p in re.split(r"[,;|\s]+", raw) if p.strip()]


def _rows_matrix(rows: List[Dict[str, Any]], keys: List[str]) -> List[List[Any]]:
    out: List[List[Any]] = []
    for row in rows or []:
        out.append([_json_safe(row.get(k)) for k in keys])
    return out


def _rows_from_matrix(matrix: Any, cols: Sequence[Any]) -> List[Dict[str, Any]]:
    keys = [_safe_str(c) for c in cols if _safe_str(c)]
    out: List[Dict[str, Any]] = []
    for row in matrix or []:
        if not isinstance(row, (list, tuple)):
            continue
        d: Dict[str, Any] = {}
        for i, k in enumerate(keys):
            d[k] = row[i] if i < len(row) else None
        out.append(d)
    return out


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
            h = f"Column_{i + 1}"
            k = f"key_{i + 1}"
        hdrs.append(h)
        ks.append(k)
    return hdrs, ks


def _usable_contract(headers: Sequence[str], keys: Sequence[str], sheet_name: str = "") -> bool:
    if not headers or not keys:
        return False
    if len(headers) != len(keys) or len(headers) == 0:
        return False
    canon = _canonicalize_sheet_name(sheet_name)
    keyset = set(keys)
    if canon in INSTRUMENT_SHEETS - {"Top_10_Investments"}:
        if not ({"symbol", "ticker", "requested_symbol"} & keyset):
            return False
        if not ({"current_price", "price", "name"} & keyset):
            return False
    if canon == "Top_10_Investments":
        if not ({"symbol", "ticker", "requested_symbol"} & keyset):
            return False
        if not set(TOP10_REQUIRED_FIELDS).issubset(keyset):
            return False
    if canon == "Insights_Analysis":
        if not ({"symbol", "recommendation", "overall_score", "selection_reason"} & keyset):
            return False
    if canon == "Data_Dictionary":
        if not {"sheet", "header", "key"}.issubset(keyset):
            return False
    return True


def _ensure_top10_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    hdrs = list(headers or [])
    ks = list(keys or [])
    for field in TOP10_REQUIRED_FIELDS:
        if field not in ks:
            ks.append(field)
            hdrs.append(TOP10_REQUIRED_HEADERS[field])
    return _complete_schema_contract(hdrs, ks)


def _normalize_to_schema_keys(keys: Sequence[str], headers: Sequence[str], row: Dict[str, Any]) -> Dict[str, Any]:
    src = dict(row or {})
    src_ci = {str(k).strip().lower(): v for k, v in src.items()}
    src_loose = {_norm_key_loose(k): v for k, v in src.items()}
    out: Dict[str, Any] = {}
    for idx, key in enumerate(keys or []):
        header = headers[idx] if idx < len(headers) else key
        aliases = [
            key,
            header,
            _norm_key(key),
            _norm_key(header),
            key.lower(),
            header.lower(),
            key.replace("_", " "),
        ]
        val = None
        found = False
        for alias in aliases:
            if alias in src:
                val = src[alias]
                found = True
                break
            if alias.lower() in src_ci:
                val = src_ci[alias.lower()]
                found = True
                break
            loose = _norm_key_loose(alias)
            if loose in src_loose:
                val = src_loose[loose]
                found = True
                break
        out[key] = val if found else None
    return out


def _strict_project_row(keys: Sequence[str], row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: row.get(k) for k in keys}


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
        return _as_float(value)
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
        return str(value)
    except Exception:
        return None


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
    return {"result": obj}


def _looks_like_explicit_row_dict(d: Any) -> bool:
    if not isinstance(d, dict) or not d:
        return False
    keyset = {str(k) for k in d.keys()}
    if keyset & {"symbol", "ticker", "code", "requested_symbol"}:
        return True
    if {"sheet", "header", "key"}.issubset(keyset):
        return True
    if {"top10_rank", "selection_reason"}.issubset(keyset):
        return True
    if keyset & {"section", "item", "recommendation", "overall_score"}:
        return True
    return False


def _coerce_rows_list(out: Any) -> List[Dict[str, Any]]:
    if out is None:
        return []
    if isinstance(out, list):
        if not out:
            return []
        if isinstance(out[0], dict):
            return [dict(r) for r in out if isinstance(r, dict)]
        if isinstance(out[0], (list, tuple)):
            return []
        return [_model_to_dict(r) for r in out]
    if isinstance(out, dict):
        maybe_symbol_map = True
        rows_from_map: List[Dict[str, Any]] = []
        symbol_like_keys = 0
        if out:
            for k, v in out.items():
                if not isinstance(v, dict):
                    maybe_symbol_map = False
                    break
                if not _looks_like_symbol_token(k):
                    maybe_symbol_map = False
                    break
                symbol_like_keys += 1
                row = dict(v)
                if not row.get("symbol"):
                    row["symbol"] = _safe_str(k)
                rows_from_map.append(row)
        if maybe_symbol_map and symbol_like_keys > 0 and rows_from_map:
            return rows_from_map
        for key in ("rows", "data", "items", "records", "payload", "result", "quotes"):
            val = out.get(key)
            if isinstance(val, list):
                if val and isinstance(val[0], dict):
                    return [dict(r) for r in val if isinstance(r, dict)]
                if val and isinstance(val[0], (list, tuple)):
                    cols = out.get("keys") or out.get("headers") or out.get("columns") or []
                    if isinstance(cols, list) and cols:
                        return _rows_from_matrix(val, cols)
            if isinstance(val, dict):
                nested_rows = _coerce_rows_list(val)
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
    d = _model_to_dict(out)
    return [d] if _looks_like_explicit_row_dict(d) else []


def _extract_symbols_from_rows(rows: Sequence[Dict[str, Any]], limit: int = 5000) -> List[str]:
    raw: List[str] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        for key in ("symbol", "ticker", "code", "requested_symbol", "Symbol", "Ticker", "Code"):
            v = row.get(key)
            if v:
                raw.append(str(v).strip())
                break
    return _normalize_symbol_list(raw, limit=limit)


def _compute_scores_fallback(row: Dict[str, Any]) -> None:
    price = _as_float(row.get("current_price")) or _as_float(row.get("price"))
    pe = _as_float(row.get("pe_ttm"))
    pb = _as_float(row.get("pb_ratio"))
    div_yield = _as_float(row.get("dividend_yield"))
    roi_3m = _as_float(row.get("expected_roi_3m"))
    if roi_3m is None:
        roi_3m = _as_float(row.get("expected_roi_1m"))
    if roi_3m is None:
        roi_3m = _as_float(row.get("expected_roi_12m"))

    value_score = row.get("value_score")
    if value_score is None:
        value_score = 60.0
        if pe is not None:
            value_score += max(0.0, 20.0 - min(pe, 20.0))
        if pb is not None:
            value_score += max(0.0, 10.0 - min(pb, 10.0))
        if div_yield is not None:
            value_score += min(div_yield * 100.0, 10.0)
        row["value_score"] = round(_clamp(float(value_score), 0.0, 100.0), 2)

    quality_score = row.get("quality_score")
    if quality_score is None:
        quality_score = 55.0
        gm = _as_float(row.get("gross_margin"))
        pm = _as_float(row.get("profit_margin"))
        if gm is not None:
            quality_score += min(gm * 100.0, 20.0) if gm <= 1.5 else min(gm, 20.0)
        if pm is not None:
            quality_score += min(pm * 100.0, 15.0) if pm <= 1.5 else min(pm, 15.0)
        row["quality_score"] = round(_clamp(float(quality_score), 0.0, 100.0), 2)

    momentum_score = row.get("momentum_score")
    if momentum_score is None:
        pct = _as_float(row.get("percent_change"))
        if pct is None:
            pct = _as_float(row.get("change_pct"))
        if pct is None:
            pct = 0.0
        if abs(pct) > 1.5:
            pct = pct / 100.0
        momentum_score = 50.0 + (pct * 100.0)
        row["momentum_score"] = round(_clamp(float(momentum_score), 0.0, 100.0), 2)

    growth_score = row.get("growth_score")
    if growth_score is None:
        rg = _as_float(row.get("revenue_growth_yoy"))
        if rg is None:
            rg = 0.0
        if abs(rg) > 1.5:
            rg = rg / 100.0
        growth_score = 50.0 + (rg * 100.0)
        row["growth_score"] = round(_clamp(float(growth_score), 0.0, 100.0), 2)

    conf = _as_float(row.get("forecast_confidence"))
    if conf is None:
        conf = _as_float(row.get("confidence_score"))
    if conf is None:
        conf = 0.55
    if conf > 1.5:
        conf = conf / 100.0
    row.setdefault("forecast_confidence", round(_clamp(conf, 0.0, 1.0), 4))
    row.setdefault("confidence_score", round(_clamp(conf * 100.0, 0.0, 100.0), 2))

    risk_score = _as_float(row.get("risk_score"))
    if risk_score is None:
        vol = _as_float(row.get("volatility_90d"))
        beta = _as_float(row.get("beta_5y"))
        risk_score = 40.0
        if vol is not None:
            risk_score += min(max(vol, 0.0), 40.0)
        if beta is not None:
            risk_score += min(max(beta * 10.0, 0.0), 20.0)
        row["risk_score"] = round(_clamp(risk_score, 0.0, 100.0), 2)

    overall = row.get("overall_score")
    if overall is None:
        vals = [
            _as_float(row.get("value_score")),
            _as_float(row.get("quality_score")),
            _as_float(row.get("momentum_score")),
            _as_float(row.get("growth_score")),
        ]
        vals2 = [v for v in vals if v is not None]
        overall = sum(vals2) / len(vals2) if vals2 else 50.0
        row["overall_score"] = round(_clamp(float(overall), 0.0, 100.0), 2)

    opp = row.get("opportunity_score")
    if opp is None:
        base = _as_float(row.get("overall_score")) or 50.0
        boost = 0.0
        if roi_3m is not None:
            boost += (roi_3m * 100.0 if abs(roi_3m) <= 1.5 else roi_3m) * 0.3
        boost += ((_as_float(row.get("confidence_score")) or 50.0) - 50.0) * 0.2
        boost -= ((_as_float(row.get("risk_score")) or 50.0) - 50.0) * 0.2
        row["opportunity_score"] = round(_clamp(base + boost, 0.0, 100.0), 2)

    if not row.get("risk_bucket"):
        rs = _as_float(row.get("risk_score")) or 50.0
        if rs < 40:
            row["risk_bucket"] = "LOW"
        elif rs < 70:
            row["risk_bucket"] = "MODERATE"
        else:
            row["risk_bucket"] = "HIGH"

    if not row.get("confidence_bucket"):
        cs = _as_float(row.get("confidence_score")) or 55.0
        if cs >= 75:
            row["confidence_bucket"] = "HIGH"
        elif cs >= 55:
            row["confidence_bucket"] = "MODERATE"
        else:
            row["confidence_bucket"] = "LOW"

    if price is not None and row.get("forecast_price_3m") is None:
        row["forecast_price_3m"] = round(price * 1.03, 4)
    if price is not None and row.get("forecast_price_1m") is None:
        row["forecast_price_1m"] = round(price * 1.01, 4)
    if price is not None and row.get("forecast_price_12m") is None:
        row["forecast_price_12m"] = round(price * 1.08, 4)
    if price is not None and row.get("expected_roi_1m") is None:
        fp1 = _as_float(row.get("forecast_price_1m"))
        if fp1 is not None and price:
            row["expected_roi_1m"] = round((fp1 - price) / price, 6)
    if price is not None and row.get("expected_roi_3m") is None:
        fp3 = _as_float(row.get("forecast_price_3m"))
        if fp3 is not None and price:
            row["expected_roi_3m"] = round((fp3 - price) / price, 6)
    if price is not None and row.get("expected_roi_12m") is None:
        fp12 = _as_float(row.get("forecast_price_12m"))
        if fp12 is not None and price:
            row["expected_roi_12m"] = round((fp12 - price) / price, 6)


def _compute_recommendation(row: Dict[str, Any]) -> None:
    if row.get("recommendation"):
        return
    overall = _as_float(row.get("overall_score")) or 50.0
    conf = _as_float(row.get("confidence_score")) or 55.0
    risk = _as_float(row.get("risk_score")) or 50.0
    if overall >= 75 and conf >= 65 and risk <= 60:
        rec = "BUY"
    elif overall >= 60 and conf >= 55:
        rec = "ACCUMULATE"
    elif overall <= 35 or risk >= 85:
        rec = "REDUCE"
    else:
        rec = "HOLD"
    row["recommendation"] = rec
    row.setdefault("recommendation_reason", f"overall={round(overall,1)} confidence={round(conf,1)} risk={round(risk,1)}")


def _apply_rank_overall(rows: List[Dict[str, Any]]) -> None:
    scored: List[Tuple[int, float]] = []
    for i, row in enumerate(rows):
        score = _as_float(row.get("overall_score"))
        if score is None:
            score = _as_float(row.get("opportunity_score"))
        if score is None:
            continue
        scored.append((i, score))
    scored.sort(key=lambda t: t[1], reverse=True)
    for rank, (idx, _) in enumerate(scored, start=1):
        rows[idx]["rank_overall"] = rank


def _top10_selection_reason(row: Dict[str, Any]) -> str:
    parts: List[str] = []
    for key, label in (("overall_score", "overall"), ("opportunity_score", "opportunity"), ("confidence_score", "confidence"), ("risk_score", "risk")):
        val = _as_float(row.get(key))
        if val is None:
            continue
        suffix = "%" if key == "confidence_score" else ""
        parts.append(f"{label}={round(val, 1)}{suffix}")
    return "Selected by fallback ranking" if not parts else ("Selected by fallback ranking: " + ", ".join(parts))


def _top10_criteria_snapshot(criteria: Dict[str, Any]) -> str:
    keep = {
        "top_n": criteria.get("top_n"),
        "pages_selected": criteria.get("pages_selected"),
        "horizon_days": criteria.get("horizon_days") or criteria.get("invest_period_days"),
        "risk_level": criteria.get("risk_level"),
        "min_expected_roi": criteria.get("min_expected_roi"),
        "confidence_level": criteria.get("confidence_level"),
        "direct_symbols": criteria.get("direct_symbols") or criteria.get("symbols"),
    }
    keep = {k: v for k, v in keep.items() if v not in (None, "", [], {})}
    try:
        import json
        return json.dumps(_json_safe(keep), ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return "{}"


def _feature_flags(settings: Any) -> Dict[str, bool]:
    return {
        "computations_enabled": _safe_bool(getattr(settings, "computations_enabled", True), True),
        "forecasting_enabled": _safe_bool(getattr(settings, "forecasting_enabled", True), True),
        "scoring_enabled": _safe_bool(getattr(settings, "scoring_enabled", True), True),
    }


def _try_get_settings() -> Any:
    for mod_path in ("config", "core.config", "env"):
        try:
            mod = import_module(mod_path)
        except Exception:
            continue
        for fn_name in ("get_settings_cached", "get_settings"):
            fn = getattr(mod, fn_name, None)
            if callable(fn):
                try:
                    return fn()
                except Exception:
                    continue
    return None


def _call_maybe_async(fn: Any, *args: Any, **kwargs: Any) -> Any:
    result = fn(*args, **kwargs)
    if inspect.isawaitable(result):
        return result
    async def _wrap() -> Any:
        return result
    return _wrap()


# =============================================================================
# Schema registry helpers
# =============================================================================
try:
    from core.sheets.schema_registry import SCHEMA_REGISTRY as _RAW_SCHEMA_REGISTRY  # type: ignore
    from core.sheets.schema_registry import get_sheet_spec as _RAW_GET_SHEET_SPEC  # type: ignore
    _SCHEMA_AVAILABLE = True
except Exception:
    _RAW_SCHEMA_REGISTRY = {}
    _RAW_GET_SHEET_SPEC = None
    _SCHEMA_AVAILABLE = False

SCHEMA_REGISTRY = _RAW_SCHEMA_REGISTRY if isinstance(_RAW_SCHEMA_REGISTRY, dict) else {}


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
            h = _safe_str(c.get("header") or c.get("display_header") or c.get("label") or c.get("title"))
            k = _safe_str(c.get("key") or c.get("field") or c.get("name") or c.get("id"))
        else:
            h = _safe_str(getattr(c, "header", getattr(c, "display_header", getattr(c, "label", getattr(c, "title", None)))))
            k = _safe_str(getattr(c, "key", getattr(c, "field", getattr(c, "name", getattr(c, "id", None)))))
        if h or k:
            headers.append(h or k.replace("_", " ").title())
            keys.append(k or _norm_key(h))
    if not headers and not keys and isinstance(spec, Mapping):
        h2 = spec.get("headers") or spec.get("display_headers")
        k2 = spec.get("keys") or spec.get("fields")
        if isinstance(h2, list):
            headers = [_safe_str(x) for x in h2 if _safe_str(x)]
        if isinstance(k2, list):
            keys = [_safe_str(x) for x in k2 if _safe_str(x)]
    return _complete_schema_contract(headers, keys)


def _registry_sheet_lookup(sheet: str) -> Any:
    if not SCHEMA_REGISTRY:
        return None
    candidates = [sheet, sheet.replace(" ", "_"), sheet.replace("_", " "), _norm_key(sheet), _norm_key_loose(sheet)]
    by_norm = {_norm_key(k): v for k, v in SCHEMA_REGISTRY.items()}
    by_loose = {_norm_key_loose(k): v for k, v in SCHEMA_REGISTRY.items()}
    for cand in candidates:
        if cand in SCHEMA_REGISTRY:
            return SCHEMA_REGISTRY.get(cand)
        nk = _norm_key(cand)
        if nk in by_norm:
            return by_norm[nk]
        nkl = _norm_key_loose(cand)
        if nkl in by_loose:
            return by_loose[nkl]
    return None


def get_sheet_spec(sheet: str) -> Any:
    canon = _canonicalize_sheet_name(sheet)
    static_contract = STATIC_CANONICAL_SHEET_CONTRACTS.get(canon)
    if static_contract:
        return dict(static_contract)
    if callable(_RAW_GET_SHEET_SPEC):
        for cand in _dedupe_keep_order([canon, canon.replace("_", " "), _norm_key(canon), sheet]):
            try:
                spec = _RAW_GET_SHEET_SPEC(cand)  # type: ignore[misc]
                if spec is not None:
                    return spec
            except Exception:
                continue
    spec = _registry_sheet_lookup(canon)
    if spec is not None:
        return spec
    raise KeyError(f"Unknown sheet spec: {sheet}")


def _schema_for_sheet(sheet: str) -> Tuple[Any, List[str], List[str], str]:
    canon = _canonicalize_sheet_name(sheet)
    static_contract = STATIC_CANONICAL_SHEET_CONTRACTS.get(canon)
    if static_contract:
        h, k = _complete_schema_contract(static_contract.get("headers", []), static_contract.get("keys", []))
        if canon == "Top_10_Investments":
            h, k = _ensure_top10_contract(h, k)
        return dict(static_contract), h, k, "static_canonical_contract"
    try:
        spec = get_sheet_spec(canon)
        h, k = _schema_keys_headers_from_spec(spec)
        if canon == "Top_10_Investments":
            h, k = _ensure_top10_contract(h, k)
        if _usable_contract(h, k, canon):
            return spec, h, k, "schema_registry"
    except Exception:
        pass
    # final fallback for known pages
    if canon in STATIC_CANONICAL_SHEET_CONTRACTS:
        c = STATIC_CANONICAL_SHEET_CONTRACTS[canon]
        h, k = _complete_schema_contract(c["headers"], c["keys"])
        if canon == "Top_10_Investments":
            h, k = _ensure_top10_contract(h, k)
        return dict(c), h, k, "static_canonical_contract_fallback"
    return None, [], [], "missing"


def _list_sheet_names_best_effort() -> List[str]:
    names = list(STATIC_CANONICAL_SHEET_CONTRACTS.keys())
    try:
        from core.sheets.page_catalog import CANONICAL_PAGES  # type: ignore
        for item in list(CANONICAL_PAGES or []):
            s = _canonicalize_sheet_name(_safe_str(item))
            if s and s not in names:
                names.append(s)
    except Exception:
        pass
    if isinstance(SCHEMA_REGISTRY, dict):
        for k in SCHEMA_REGISTRY.keys():
            s = _canonicalize_sheet_name(_safe_str(k))
            if s and s not in names:
                names.append(s)
    return names


def _build_union_schema_keys() -> List[str]:
    keys: List[str] = []
    seen: Set[str] = set()
    for contract in STATIC_CANONICAL_SHEET_CONTRACTS.values():
        for key in contract.get("keys", []):
            k = _safe_str(key)
            if k and k not in seen:
                seen.add(k)
                keys.append(k)
    if isinstance(SCHEMA_REGISTRY, dict):
        for _, spec in SCHEMA_REGISTRY.items():
            try:
                _, spec_keys = _schema_keys_headers_from_spec(spec)
                for key in spec_keys:
                    k = _safe_str(key)
                    if k and k not in seen:
                        seen.add(k)
                        keys.append(k)
            except Exception:
                continue
    for field in TOP10_REQUIRED_FIELDS:
        if field not in seen:
            seen.add(field)
            keys.append(field)
    return keys


_SCHEMA_UNION_KEYS: List[str] = _build_union_schema_keys()

# =============================================================================
# Light async utilities
# =============================================================================
class SingleFlight:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._tasks: Dict[str, asyncio.Task[Any]] = {}

    async def execute(self, key: str, factory: Any) -> Any:
        async with self._lock:
            task = self._tasks.get(key)
            if task is None:
                task = asyncio.create_task(factory())
                self._tasks[key] = task
        try:
            return await task
        finally:
            async with self._lock:
                if self._tasks.get(key) is task:
                    self._tasks.pop(key, None)


class MultiLevelCache:
    def __init__(self, name: str, l1_ttl: int = 60, l3_ttl: int = 3600, max_l1_size: int = 5000) -> None:
        self.name = name
        self.l1_ttl = max(1, int(l1_ttl))
        self.max_l1_size = max(1, int(max_l1_size))
        self._data: Dict[str, Tuple[float, Any]] = {}
        self._lock = asyncio.Lock()

    def _key(self, **kwargs: Any) -> str:
        items = sorted((str(k), _safe_str(v)) for k, v in kwargs.items())
        return "|".join([self.name] + [f"{k}={v}" for k, v in items])

    async def get(self, **kwargs: Any) -> Any:
        key = self._key(**kwargs)
        async with self._lock:
            item = self._data.get(key)
            if not item:
                return None
            expires_at, value = item
            if expires_at < time.time():
                self._data.pop(key, None)
                return None
            return value

    async def set(self, value: Any, **kwargs: Any) -> None:
        key = self._key(**kwargs)
        async with self._lock:
            if len(self._data) >= self.max_l1_size:
                oldest_key = next(iter(self._data.keys()), None)
                if oldest_key:
                    self._data.pop(oldest_key, None)
            self._data[key] = (time.time() + self.l1_ttl, value)


class ProviderRegistry:
    def __init__(self) -> None:
        self._stats: Dict[str, Dict[str, Any]] = {}

    async def get_provider(self, provider: str) -> Tuple[Optional[Any], Any]:
        module = None
        candidates = [
            f"core.providers.{provider}",
            f"providers.{provider}",
            f"core.providers.{provider}_provider",
            f"providers.{provider}_provider",
        ]
        for mod_path in candidates:
            try:
                module = import_module(mod_path)
                break
            except Exception:
                continue
        stats = type("ProviderStats", (), {
            "is_circuit_open": False,
            "last_import_error": "" if module is not None else "provider module missing",
        })()
        return module, stats

    async def record_success(self, provider: str, latency_ms: float) -> None:
        stat = self._stats.setdefault(provider, {"success": 0, "failure": 0, "last_error": "", "latency_ms": 0.0})
        stat["success"] += 1
        stat["latency_ms"] = round(float(latency_ms or 0.0), 2)

    async def record_failure(self, provider: str, error: str) -> None:
        stat = self._stats.setdefault(provider, {"success": 0, "failure": 0, "last_error": "", "latency_ms": 0.0})
        stat["failure"] += 1
        stat["last_error"] = _safe_str(error)

    async def get_stats(self) -> Dict[str, Any]:
        return {k: dict(v) for k, v in self._stats.items()}


def _pick_provider_callable(module: Any, provider: str) -> Optional[Any]:
    for name in (
        "get_quote",
        "fetch_quote",
        "fetch_enriched_quote",
        "get_enriched_quote",
        "quote",
    ):
        fn = getattr(module, name, None)
        if callable(fn):
            return fn
    for attr in (provider, f"{provider}_quote", "client", "service"):
        obj = getattr(module, attr, None)
        if obj is not None:
            for name in ("get_quote", "fetch_quote", "get_enriched_quote"):
                fn = getattr(obj, name, None)
                if callable(fn):
                    return fn
    return None


# =============================================================================
# Engine symbols proxy
# =============================================================================
class _EngineSymbolsReaderProxy:
    def __init__(self, engine: "DataEngineV5") -> None:
        self._engine = engine

    async def get_symbols_for_sheet(self, sheet: str, limit: int = 5000) -> List[str]:
        return await self._engine.get_sheet_symbols(sheet, limit=limit)

    async def get_symbols_for_page(self, page: str, limit: int = 5000) -> List[str]:
        return await self._engine.get_page_symbols(page, limit=limit)

    async def list_symbols_for_page(self, page: str, limit: int = 5000) -> List[str]:
        return await self._engine.list_symbols_for_page(page, limit=limit)


# =============================================================================
# DataEngineV5
# =============================================================================
class DataEngineV5:
    def __init__(self, settings: Any = None):
        self.settings = settings if settings is not None else _try_get_settings()
        self.flags = _feature_flags(self.settings)
        self.version = __version__
        self.primary_provider = _safe_str(getattr(self.settings, "primary_provider", "eodhd") if self.settings is not None else _safe_env("PRIMARY_PROVIDER", "eodhd")).lower() or "eodhd"
        self.enabled_providers = _get_env_list("ENABLED_PROVIDERS", DEFAULT_PROVIDERS)
        self.ksa_providers = _get_env_list("KSA_PROVIDERS", DEFAULT_KSA_PROVIDERS)
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
        self._cache = MultiLevelCache(name="data_engine", l1_ttl=_get_env_int("CACHE_L1_TTL", 60), l3_ttl=_get_env_int("CACHE_L3_TTL", 3600), max_l1_size=_get_env_int("CACHE_L1_MAX", 5000))
        self._symbols_cache = MultiLevelCache(name="sheet_symbols", l1_ttl=_get_env_int("SHEET_SYMBOLS_L1_TTL", 300), l3_ttl=_get_env_int("SHEET_SYMBOLS_L3_TTL", 1800), max_l1_size=_get_env_int("SHEET_SYMBOLS_L1_MAX", 256))

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

    # ---------------------------------------------------------------------
    # compatibility aliases
    # ---------------------------------------------------------------------
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

    # ---------------------------------------------------------------------
    # snapshot helpers
    # ---------------------------------------------------------------------
    def _store_sheet_snapshot(self, sheet: str, payload: Dict[str, Any]) -> None:
        target = _canonicalize_sheet_name(sheet)
        if not target or not isinstance(payload, dict):
            return
        self._sheet_snapshots[target] = dict(payload)

    def get_cached_sheet_snapshot(self, sheet: Optional[str] = None, page: Optional[str] = None, sheet_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        target = _canonicalize_sheet_name(sheet or page or sheet_name or "")
        if not target:
            return None
        snap = self._sheet_snapshots.get(target)
        return dict(snap) if isinstance(snap, dict) else None

    def get_sheet_snapshot(self, page: Optional[str] = None, sheet: Optional[str] = None, sheet_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        return self.get_cached_sheet_snapshot(sheet=sheet, page=page, sheet_name=sheet_name)

    def get_cached_sheet_rows(self, sheet_name: Optional[str] = None, sheet: Optional[str] = None, page: Optional[str] = None) -> Optional[Dict[str, Any]]:
        return self.get_cached_sheet_snapshot(sheet=sheet, page=page, sheet_name=sheet_name)

    # ---------------------------------------------------------------------
    # meta helpers
    # ---------------------------------------------------------------------
    def _set_sheet_symbols_meta(self, sheet: str, source: str, count: int, note: Optional[str] = None) -> None:
        target = _canonicalize_sheet_name(sheet)
        if not target:
            return
        self._sheet_symbol_resolution_meta[target] = {
            "sheet": target,
            "source": source or "",
            "count": int(count or 0),
            "note": note or "",
            "timestamp_utc": _now_utc_iso(),
        }

    def _get_sheet_symbols_meta(self, sheet: str) -> Dict[str, Any]:
        target = _canonicalize_sheet_name(sheet)
        meta = self._sheet_symbol_resolution_meta.get(target)
        return dict(meta) if isinstance(meta, dict) else {}

    @staticmethod
    def _extract_row_symbol(row: Dict[str, Any]) -> str:
        if not isinstance(row, dict):
            return ""
        for k in ("symbol", "ticker", "code", "requested_symbol", "Symbol", "Ticker", "Code"):
            v = row.get(k)
            if v:
                return _safe_str(v)
        return ""

    # ---------------------------------------------------------------------
    # final payload
    # ---------------------------------------------------------------------
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
            "count": len(rows),
            "meta": dict(meta or {}),
            "version": self.version,
        }
        if error is not None:
            payload["error"] = error
        return _json_safe(payload)

    # ---------------------------------------------------------------------
    # rows reader discovery
    # ---------------------------------------------------------------------
    async def _init_rows_reader(self) -> Tuple[Any, str]:
        if self._rows_reader_ready:
            return self._rows_reader_obj, self._rows_reader_source
        async with self._rows_reader_lock:
            if self._rows_reader_ready:
                return self._rows_reader_obj, self._rows_reader_source
            obj: Any = None
            source = ""
            for mod_path in (
                "integrations.google_sheets_service",
                "core.integrations.google_sheets_service",
                "google_sheets_service",
                "core.google_sheets_service",
                "integrations.symbols_reader",
                "core.integrations.symbols_reader",
            ):
                try:
                    mod = import_module(mod_path)
                except Exception:
                    continue
                if any(callable(getattr(mod, nm, None)) for nm in ("get_rows_for_sheet", "read_rows_for_sheet", "get_sheet_rows", "fetch_sheet_rows", "sheet_rows", "get_rows")):
                    obj = mod
                    source = mod_path
                    break
                for attr_name in ("service", "reader", "rows_reader", "google_sheets_service"):
                    candidate = getattr(mod, attr_name, None)
                    if candidate is not None:
                        obj = candidate
                        source = f"{mod_path}.{attr_name}"
                        break
                if obj is not None:
                    break
            self._rows_reader_obj = obj
            self._rows_reader_source = source
            self._rows_reader_ready = True
            return obj, source

    async def _call_rows_reader(self, obj: Any, sheet: str, limit: int) -> List[Dict[str, Any]]:
        if obj is None:
            return []
        for name in ("get_rows_for_sheet", "read_rows_for_sheet", "get_sheet_rows", "fetch_sheet_rows", "sheet_rows", "get_rows"):
            fn = getattr(obj, name, None)
            if not callable(fn):
                continue
            variants = [
                ((), {"sheet": sheet, "limit": limit}),
                ((), {"sheet_name": sheet, "limit": limit}),
                ((), {"page": sheet, "limit": limit}),
                ((sheet,), {"limit": limit}),
                ((sheet,), {}),
            ]
            for args, kwargs in variants:
                try:
                    async with asyncio.timeout(_get_env_float("ROWS_READER_TIMEOUT_SECONDS", 20.0)):
                        result = await _call_maybe_async(fn, *args, **kwargs)
                    rows = _coerce_rows_list(result)
                    if rows:
                        return rows[:limit]
                except TypeError:
                    continue
                except Exception:
                    continue
        return []

    async def _get_rows_from_external_reader(self, sheet: str, limit: int) -> List[Dict[str, Any]]:
        obj, _ = await self._init_rows_reader()
        if obj is None:
            return []
        rows = await self._call_rows_reader(obj, sheet, limit)
        return rows[:limit] if rows else []

    # ---------------------------------------------------------------------
    # symbols reader discovery
    # ---------------------------------------------------------------------
    async def _init_symbols_reader(self) -> Tuple[Any, str]:
        if self._symbols_reader_ready:
            return self._symbols_reader_obj, self._symbols_reader_source
        async with self._symbols_reader_lock:
            if self._symbols_reader_ready:
                return self._symbols_reader_obj, self._symbols_reader_source
            obj: Any = None
            source = ""
            for mod_path in (
                "symbols_reader",
                "core.symbols_reader",
                "integrations.symbols_reader",
                "core.integrations.symbols_reader",
                "integrations.google_sheets_service",
                "core.integrations.google_sheets_service",
                "google_sheets_service",
                "core.google_sheets_service",
            ):
                try:
                    mod = import_module(mod_path)
                except Exception:
                    continue
                if any(callable(getattr(mod, nm, None)) for nm in ("get_symbols_for_sheet", "read_symbols_for_sheet", "get_sheet_symbols", "get_symbols", "list_symbols_for_page", "get_symbols_for_page", "read_symbols", "load_symbols", "read_sheet_symbols")):
                    obj = mod
                    source = mod_path
                    break
                for attr_name in ("symbols_reader", "reader", "symbol_reader", "sheet_reader", "service"):
                    candidate = getattr(mod, attr_name, None)
                    if candidate is not None:
                        obj = candidate
                        source = f"{mod_path}.{attr_name}"
                        break
                if obj is not None:
                    break
            self._symbols_reader_obj = obj
            self._symbols_reader_source = source
            self._symbols_reader_ready = True
            return obj, source

    async def _call_symbols_reader(self, obj: Any, sheet: str, limit: int) -> List[str]:
        if obj is None:
            return []
        if isinstance(obj, dict):
            for key in _sheet_lookup_candidates(sheet):
                vals = obj.get(key)
                syms = _normalize_symbol_list(_split_symbols(vals), limit=limit)
                if syms:
                    return syms
        for name in ("get_symbols_for_sheet", "read_symbols_for_sheet", "get_sheet_symbols", "get_symbols_for_page", "list_symbols_for_page", "get_symbols", "list_symbols", "read_symbols", "load_symbols", "read_sheet_symbols"):
            fn = getattr(obj, name, None)
            if not callable(fn):
                continue
            variants = [
                ((), {"sheet": sheet, "limit": limit}),
                ((), {"sheet_name": sheet, "limit": limit}),
                ((), {"page": sheet, "limit": limit}),
                ((sheet,), {"limit": limit}),
                ((sheet,), {}),
            ]
            for args, kwargs in variants:
                try:
                    async with asyncio.timeout(_get_env_float("SHEET_SYMBOLS_TIMEOUT_SECONDS", 15.0)):
                        result = await _call_maybe_async(fn, *args, **kwargs)
                    syms = _normalize_symbol_list(_split_symbols(result), limit=limit)
                    if not syms and isinstance(result, (dict, list)):
                        syms = _extract_symbols_from_rows(_coerce_rows_list(result), limit=limit)
                    if syms:
                        return syms
                except TypeError:
                    continue
                except Exception:
                    continue
        return []

    async def _get_symbols_from_env(self, sheet: str, limit: int) -> List[str]:
        env_candidates: List[str] = []
        specific = PAGE_SYMBOL_ENV_KEYS.get(sheet)
        if specific:
            env_candidates.append(specific)
        for cand in _sheet_lookup_candidates(sheet):
            token = re.sub(r"[^A-Za-z0-9]+", "_", cand).strip("_").upper()
            if token:
                env_candidates.extend([f"{token}_SYMBOLS", f"{token}_TICKERS", f"{token}_CODES"])
        env_candidates.extend(["TOP10_FALLBACK_SYMBOLS", "DEFAULT_PAGE_SYMBOLS", "DEFAULT_SYMBOLS"])
        seen: Set[str] = set()
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
        for attr_name in (f"{sheet.lower()}_symbols", f"{sheet.lower()}_tickers", f"{sheet.lower()}_codes", "default_symbols", "page_symbols", "sheet_symbols"):
            try:
                raw = getattr(self.settings, attr_name, None)
            except Exception:
                raw = None
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
        return []

    async def _get_symbols_from_page_catalog(self, sheet: str, limit: int) -> List[str]:
        candidates = _sheet_lookup_candidates(sheet)
        for mod_path in ("core.sheets.page_catalog", "sheets.page_catalog"):
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
                if not callable(fn):
                    continue
                for args, kwargs in [((sheet,), {"limit": limit}), ((sheet,), {}), ((), {"page": sheet, "limit": limit}), ((), {"sheet": sheet, "limit": limit})]:
                    try:
                        result = await _call_maybe_async(fn, *args, **kwargs)
                        syms = _normalize_symbol_list(_split_symbols(result), limit=limit)
                        if syms:
                            return syms
                    except TypeError:
                        continue
                    except Exception:
                        continue
        return []

    async def _get_symbols_for_sheet_impl(self, sheet: str, limit: int = 5000, body: Optional[Dict[str, Any]] = None) -> List[str]:
        target = _canonicalize_sheet_name(sheet)
        if target in SPECIAL_SHEETS:
            self._set_sheet_symbols_meta(target, "special_sheet", 0)
            return []
        limit = max(1, min(5000, int(limit or 5000)))
        from_body = _extract_requested_symbols_from_body(body, limit=limit)
        if from_body:
            self._set_sheet_symbols_meta(target, "body_symbols", len(from_body))
            return from_body
        cached = await self._symbols_cache.get(sheet=target, limit=limit)
        if isinstance(cached, list) and cached:
            syms = _normalize_symbol_list(cached, limit=limit)
            self._set_sheet_symbols_meta(target, "symbols_cache", len(syms))
            return syms
        obj, src = await self._init_symbols_reader()
        if obj is not None:
            syms = await self._call_symbols_reader(obj, target, limit=limit)
            if syms:
                self._set_sheet_symbols_meta(target, f"symbols_reader:{src or 'unknown'}", len(syms))
                await self._symbols_cache.set(syms, sheet=target, limit=limit)
                return syms
        syms = await self._get_symbols_from_page_catalog(target, limit=limit)
        if syms:
            self._set_sheet_symbols_meta(target, "page_catalog", len(syms))
            await self._symbols_cache.set(syms, sheet=target, limit=limit)
            return syms
        syms = await self._get_symbols_from_env(target, limit=limit)
        if syms:
            self._set_sheet_symbols_meta(target, "env", len(syms))
            await self._symbols_cache.set(syms, sheet=target, limit=limit)
            return syms
        syms = await self._get_symbols_from_settings(target, limit=limit)
        if syms:
            self._set_sheet_symbols_meta(target, "settings", len(syms))
            await self._symbols_cache.set(syms, sheet=target, limit=limit)
            return syms
        snap = self.get_cached_sheet_snapshot(sheet=target)
        snap_rows = _coerce_rows_list(snap)
        if snap_rows:
            syms = _extract_symbols_from_rows(snap_rows, limit=limit)
            if syms:
                self._set_sheet_symbols_meta(target, "snapshot_rows", len(syms))
                await self._symbols_cache.set(syms, sheet=target, limit=limit)
                return syms
        emergency = EMERGENCY_PAGE_SYMBOLS.get(target) or []
        if emergency:
            syms = _normalize_symbol_list(emergency, limit=limit)
            self._set_sheet_symbols_meta(target, "emergency_page_symbols", len(syms), note="last_resort_fallback")
            await self._symbols_cache.set(syms, sheet=target, limit=limit)
            return syms
        self._set_sheet_symbols_meta(target, "none", 0, note=(src or "no_source"))
        return []

    async def get_sheet_symbols(self, sheet: Optional[str] = None, *, sheet_name: Optional[str] = None, page: Optional[str] = None, limit: int = 5000, body: Optional[Dict[str, Any]] = None) -> List[str]:
        return await self._get_symbols_for_sheet_impl(page or sheet or sheet_name or "", limit=limit, body=body)

    async def get_page_symbols(self, page: Optional[str] = None, *, sheet: Optional[str] = None, sheet_name: Optional[str] = None, limit: int = 5000, body: Optional[Dict[str, Any]] = None) -> List[str]:
        return await self._get_symbols_for_sheet_impl(page or sheet or sheet_name or "", limit=limit, body=body)

    async def list_symbols_for_page(self, page: str, *, limit: int = 5000, body: Optional[Dict[str, Any]] = None) -> List[str]:
        return await self._get_symbols_for_sheet_impl(page, limit=limit, body=body)

    async def list_symbols(self, sheet: Optional[str] = None, *, page: Optional[str] = None, sheet_name: Optional[str] = None, limit: int = 5000, body: Optional[Dict[str, Any]] = None) -> List[str]:
        return await self._get_symbols_for_sheet_impl(page or sheet or sheet_name or "", limit=limit, body=body)

    async def get_symbols(self, sheet: Optional[str] = None, *, page: Optional[str] = None, sheet_name: Optional[str] = None, limit: int = 5000, body: Optional[Dict[str, Any]] = None) -> List[str]:
        return await self._get_symbols_for_sheet_impl(page or sheet or sheet_name or "", limit=limit, body=body)

    # ---------------------------------------------------------------------
    # quote APIs
    # ---------------------------------------------------------------------
    async def _fetch_patch(self, provider: str, symbol: str) -> Tuple[str, Optional[Dict[str, Any]], float, Optional[str]]:
        start = time.time()
        async with self._sem:
            module, stats = await self._registry.get_provider(provider)
            if getattr(stats, "is_circuit_open", False):
                return provider, None, 0.0, "circuit_open"
            if module is None:
                err = getattr(stats, "last_import_error", "provider module missing")
                await self._registry.record_failure(provider, err)
                return provider, None, (time.time() - start) * 1000.0, err
            fn = _pick_provider_callable(module, provider)
            if fn is None:
                err = f"no callable fetch function for provider '{provider}'"
                await self._registry.record_failure(provider, err)
                return provider, None, (time.time() - start) * 1000.0, err
            call_variants = [
                ((symbol,), {}),
                ((), {"symbol": symbol}),
                ((), {"ticker": symbol}),
                ((), {"requested_symbol": symbol}),
                ((symbol,), {"settings": self.settings}),
                ((), {"symbol": symbol, "settings": self.settings}),
            ]
            result = None
            collected_errs: List[str] = []
            for args, kwargs in call_variants:
                try:
                    async with asyncio.timeout(self.request_timeout):
                        result = await _call_maybe_async(fn, *args, **kwargs)
                    break
                except TypeError:
                    continue
                except Exception as exc:
                    collected_errs.append(f"{type(exc).__name__}: {str(exc)[:120]}")
                    continue
            latency = (time.time() - start) * 1000.0
            patch = _model_to_dict(result)
            if patch and isinstance(patch, dict):
                await self._registry.record_success(provider, latency)
                return provider, patch, latency, None
            err = " | ".join(collected_errs) if collected_errs else "non_dict_or_empty"
            await self._registry.record_failure(provider, err)
            return provider, None, latency, err

    def _providers_for(self, symbol: str) -> List[str]:
        info = get_symbol_info(symbol)
        is_ksa_sym = bool(info.get("is_ksa"))
        providers = list(self.ksa_providers if is_ksa_sym else self.global_providers)
        providers = [p for p in providers if p in self.enabled_providers]
        if is_ksa_sym and self.ksa_disallow_eodhd:
            providers = [p for p in providers if p != "eodhd"]
        if self.primary_provider and self.primary_provider in self.enabled_providers:
            if self.primary_provider in providers:
                providers = [p for p in providers if p != self.primary_provider]
            providers.insert(0, self.primary_provider)
        seen: Set[str] = set()
        out: List[str] = []
        for p in providers:
            if p not in seen:
                seen.add(p)
                out.append(p)
        return out

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
        sorted_patches = sorted(patches, key=lambda item: (PROVIDER_PRIORITIES.get(item[0], 999), -sum(1 for v in item[1].values() if v not in (None, "", [], {}))))
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
        if merged.get("name") is None:
            merged["name"] = norm
        return merged

    def _data_quality(self, row: Dict[str, Any]) -> str:
        if _as_float(row.get("current_price")) is None:
            return QuoteQuality.MISSING.value
        return QuoteQuality.GOOD.value if any(row.get(k) is not None for k in ("overall_score", "forecast_price_3m", "pb_ratio")) else QuoteQuality.FAIR.value

    async def _get_enriched_quote_impl(self, symbol: str, use_cache: bool = True) -> UnifiedQuote:
        info = get_symbol_info(symbol)
        norm = _safe_str(info.get("normalized"))
        if not norm:
            row = {
                "symbol": _safe_str(symbol),
                "symbol_normalized": None,
                "requested_symbol": _safe_str(symbol),
                "data_quality": QuoteQuality.MISSING.value,
                "error": "Invalid symbol",
                "last_updated_utc": _now_utc_iso(),
                "last_updated_riyadh": _now_riyadh_iso(),
            }
            _compute_scores_fallback(row)
            _compute_recommendation(row)
            return UnifiedQuote(**row)
        if use_cache:
            cached = await self._cache.get(symbol=norm)
            if isinstance(cached, dict) and cached:
                return UnifiedQuote(**cached)
        providers = self._providers_for(norm)
        patches_ok: List[Tuple[str, Dict[str, Any], float]] = []
        if providers:
            gathered = await asyncio.gather(*[self._fetch_patch(p, norm) for p in providers[:4]], return_exceptions=True)
            for item in gathered:
                if isinstance(item, tuple) and len(item) == 4:
                    provider, patch, latency, _err = item
                    if patch:
                        patches_ok.append((provider, patch, latency))
        if patches_ok:
            row = self._merge(symbol, norm, patches_ok)
        else:
            row = {
                "symbol": norm,
                "symbol_normalized": norm,
                "requested_symbol": _safe_str(symbol),
                "name": norm,
                "current_price": None,
                "data_sources": [],
                "provider_latency": {},
                "warnings": ["No live provider data available"],
                "last_updated_utc": _now_utc_iso(),
                "last_updated_riyadh": _now_riyadh_iso(),
            }
        _compute_scores_fallback(row)
        _compute_recommendation(row)
        row["data_quality"] = self._data_quality(row)
        row["data_provider"] = row.get("data_provider") or ((row.get("data_sources") or [""])[0] if isinstance(row.get("data_sources"), list) else "")
        q = UnifiedQuote(**row)
        if use_cache:
            await self._cache.set(_model_to_dict(q), symbol=norm)
        return q

    async def get_enriched_quote(self, symbol: str, use_cache: bool = True, *, schema: Any = None) -> UnifiedQuote:
        key = f"quote:{normalize_symbol(symbol)}:{'cache' if use_cache else 'live'}"
        raw_q = await self._singleflight.execute(key, lambda: self._get_enriched_quote_impl(symbol, use_cache))
        if schema is None:
            return raw_q
        row = _model_to_dict(raw_q)
        projected = _normalize_to_schema_keys(_schema_for_sheet(_safe_str(schema))[2], _schema_for_sheet(_safe_str(schema))[1], row) if isinstance(schema, str) else row
        return UnifiedQuote(**projected)

    async def get_enriched_quote_dict(self, symbol: str, use_cache: bool = True, *, schema: Any = None) -> Dict[str, Any]:
        q = await self.get_enriched_quote(symbol, use_cache=use_cache, schema=schema)
        return _model_to_dict(q)

    async def get_enriched_quotes(self, symbols: List[str], *, schema: Any = None) -> List[UnifiedQuote]:
        if not symbols:
            return []
        batch = max(1, min(500, _get_env_int("QUOTE_BATCH_SIZE", 25)))
        out: List[UnifiedQuote] = []
        for i in range(0, len(symbols), batch):
            part = symbols[i:i + batch]
            out.extend(await asyncio.gather(*[self.get_enriched_quote(s, schema=schema) for s in part]))
        return out

    async def get_enriched_quotes_batch(self, symbols: List[str], mode: str = "", *, schema: Any = None) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        norm_syms = _normalize_symbol_list(symbols, limit=len(symbols) + 10)
        quotes = await asyncio.gather(*[self.get_enriched_quote_dict(s, schema=schema) for s in norm_syms])
        for req_sym, qd in zip(norm_syms, quotes):
            out[req_sym] = qd
            norm = _safe_str(qd.get("symbol_normalized") or qd.get("symbol"))
            if norm:
                out[norm] = qd
        for req in symbols:
            req2 = _safe_str(req)
            if req2 and req2 not in out:
                norm = normalize_symbol(req2)
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

    # ---------------------------------------------------------------------
    # builder fallbacks
    # ---------------------------------------------------------------------
    async def _build_data_dictionary_rows(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for sheet_name in _list_sheet_names_best_effort():
            _spec, headers, keys, source = _schema_for_sheet(sheet_name)
            for idx, (header, key) in enumerate(zip(headers, keys), start=1):
                rows.append({
                    "sheet": sheet_name,
                    "group": "Canonical",
                    "header": header,
                    "key": key,
                    "dtype": "string",
                    "fmt": "",
                    "required": idx <= 3,
                    "source": source,
                    "notes": "static/registry contract",
                })
        return rows

    async def _build_insights_rows_fallback(self, body: Optional[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
        body = dict(body or {})
        symbols = _extract_requested_symbols_from_body(body, limit=limit)
        if not symbols:
            for page_name in TOP10_ENGINE_DEFAULT_PAGES:
                symbols.extend(await self.get_sheet_symbols(page_name, limit=max(limit, 10), body=body))
        symbols = _normalize_symbol_list(symbols, limit=max(limit, 20))
        if not symbols:
            symbols = list(EMERGENCY_PAGE_SYMBOLS.get("Market_Leaders", [])[: max(limit, 5)])
        quotes = await self.get_enriched_quotes(symbols, schema=None)
        rows: List[Dict[str, Any]] = []
        for q in quotes[:limit]:
            d = _model_to_dict(q)
            rows.append({
                "symbol": d.get("symbol"),
                "recommendation": d.get("recommendation"),
                "recommendation_reason": d.get("recommendation_reason"),
                "current_price": d.get("current_price"),
                "forecast_price_1m": d.get("forecast_price_1m"),
                "forecast_price_3m": d.get("forecast_price_3m"),
                "forecast_price_12m": d.get("forecast_price_12m"),
                "expected_roi_1m": d.get("expected_roi_1m"),
                "expected_roi_3m": d.get("expected_roi_3m"),
                "expected_roi_12m": d.get("expected_roi_12m"),
                "forecast_confidence": d.get("forecast_confidence"),
                "overall_score": d.get("overall_score"),
                "risk_bucket": d.get("risk_bucket"),
                "horizon_days": d.get("horizon_days") or 90,
                "invest_period_label": d.get("invest_period_label") or "3M",
                "top10_rank": None,
                "selection_reason": None,
                "criteria_snapshot": None,
            })
        return rows

    async def _build_top10_rows_fallback(self, headers: List[str], keys: List[str], body: Optional[Dict[str, Any]], limit: int, mode: str) -> Tuple[List[str], List[str], List[Dict[str, Any]]]:
        body = dict(body or {})
        criteria = _extract_nested_dict(body, "criteria")
        top_n = _safe_int(criteria.get("top_n") or body.get("top_n") or limit, 10, lo=1, hi=50)
        headers, keys = _ensure_top10_contract(headers, keys)
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
        rows = [_model_to_dict(q) for q in quotes]
        if not rows:
            return headers, keys, []
        min_roi = _as_pct_fraction(criteria.get("min_expected_roi"))
        req_risk = _safe_str(criteria.get("risk_level")).upper()
        req_conf = _safe_str(criteria.get("confidence_level")).upper()
        horizon_days = _safe_int(criteria.get("horizon_days") or criteria.get("invest_period_days") or body.get("horizon_days") or body.get("invest_period_days"), 365)
        filtered_rows: List[Dict[str, Any]] = []
        for row in rows:
            if min_roi is not None:
                if horizon_days <= 30:
                    roi = _as_float(row.get("expected_roi_1m")) or _as_float(row.get("expected_roi_3m")) or _as_float(row.get("expected_roi_12m")) or -999.0
                elif horizon_days <= 90:
                    roi = _as_float(row.get("expected_roi_3m")) or _as_float(row.get("expected_roi_12m")) or _as_float(row.get("expected_roi_1m")) or -999.0
                else:
                    roi = _as_float(row.get("expected_roi_12m")) or _as_float(row.get("expected_roi_3m")) or _as_float(row.get("expected_roi_1m")) or -999.0
                if roi < min_roi:
                    continue
            if req_risk and req_risk != "ALL":
                row_risk = _safe_str(row.get("risk_bucket")).upper()
                if row_risk and row_risk != req_risk:
                    continue
            if req_conf and req_conf != "ALL":
                row_conf = _safe_str(row.get("confidence_bucket")).upper()
                if row_conf and row_conf != req_conf:
                    continue
            filtered_rows.append(row)
        if not filtered_rows and rows:
            filtered_rows = rows
        def _sort_key(r: Dict[str, Any]) -> Tuple[float, float, float]:
            op = _as_float(r.get("opportunity_score"))
            ov = _as_float(r.get("overall_score"))
            conf = _as_float(r.get("forecast_confidence"))
            if conf is None:
                conf = _as_float(r.get("confidence_score"))
            if conf is not None and conf > 1.5:
                conf = conf / 100.0
            return (op if op is not None else -1.0, ov if ov is not None else -1.0, conf if conf is not None else -1.0)
        filtered_rows.sort(key=_sort_key, reverse=True)
        filtered_rows = filtered_rows[:top_n]
        _apply_rank_overall(filtered_rows)
        snapshot = _top10_criteria_snapshot(criteria)
        final_rows: List[Dict[str, Any]] = []
        for i, row in enumerate(filtered_rows, start=1):
            row["top10_rank"] = i
            row.setdefault("selection_reason", _top10_selection_reason(row))
            row.setdefault("criteria_snapshot", snapshot)
            projected = _strict_project_row(keys, _normalize_to_schema_keys(keys, headers, row))
            if projected.get("top10_rank") is None:
                projected["top10_rank"] = i
            if not projected.get("selection_reason"):
                projected["selection_reason"] = row.get("selection_reason")
            if not projected.get("criteria_snapshot"):
                projected["criteria_snapshot"] = snapshot
            final_rows.append(projected)
        return headers, keys, final_rows

    # ---------------------------------------------------------------------
    # main sheet/page APIs
    # ---------------------------------------------------------------------
    async def get_page_rows(self, page: Optional[str] = None, *, sheet: Optional[str] = None, sheet_name: Optional[str] = None, limit: int = 2000, offset: int = 0, mode: str = "", body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return await self.get_sheet_rows(page or sheet or sheet_name, limit=limit, offset=offset, mode=mode, body=body)

    async def get_sheet(self, sheet_name: Optional[str] = None, *, sheet: Optional[str] = None, page: Optional[str] = None, limit: int = 2000, offset: int = 0, mode: str = "", body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return await self.get_sheet_rows(sheet_name or sheet or page, limit=limit, offset=offset, mode=mode, body=body)

    async def get_sheet_rows(self, sheet: Optional[str] = None, *, sheet_name: Optional[str] = None, page: Optional[str] = None, limit: int = 2000, offset: int = 0, mode: str = "", body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
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
        recovered_from: Optional[str] = None

        # Data Dictionary
        if target_sheet == "Data_Dictionary":
            if _is_schema_only_body(body):
                return self._finalize_payload(sheet=target_sheet, headers=headers, keys=keys, rows=[], include_matrix=include_matrix, status="success", meta={"schema_source": schema_src, "contract_level": contract_level, "strict_requested": strict_req, "strict_enforced": False, "target_sheet_known": True, "builder": "schema_only_fast_path", "rows": 0, "limit": limit, "offset": offset, "mode": mode})
            rows_all = await self._build_data_dictionary_rows()
            rows_proj = [_strict_project_row(keys, _normalize_to_schema_keys(keys, headers, r)) for r in rows_all]
            payload_full = self._finalize_payload(sheet=target_sheet, headers=headers, keys=keys, rows=rows_proj, include_matrix=include_matrix, status="success", meta={"schema_source": schema_src, "contract_level": contract_level, "strict_requested": strict_req, "strict_enforced": False, "target_sheet_known": True, "builder": "engine.internal_data_dictionary", "rows": len(rows_proj), "limit": limit, "offset": offset, "mode": mode})
            self._store_sheet_snapshot(target_sheet, payload_full)
            rows_page = rows_proj[offset: offset + limit]
            return self._finalize_payload(sheet=target_sheet, headers=headers, keys=keys, rows=rows_page, include_matrix=include_matrix, status="success", meta={**payload_full.get("meta", {}), "rows": len(rows_page)})

        # Insights Analysis
        if target_sheet == "Insights_Analysis":
            if _is_schema_only_body(body):
                return self._finalize_payload(sheet=target_sheet, headers=headers, keys=keys, rows=[], include_matrix=include_matrix, status="success", meta={"schema_source": schema_src, "contract_level": contract_level, "strict_requested": strict_req, "strict_enforced": False, "target_sheet_known": True, "builder": "schema_only_fast_path", "rows": 0, "limit": limit, "offset": offset, "mode": mode})
            rows0: List[Dict[str, Any]] = []
            builder_name = "core.analysis.insights_builder"
            try:
                from core.analysis.insights_builder import build_insights_analysis_rows  # type: ignore
                crit = body.get("criteria") if isinstance(body.get("criteria"), dict) else None
                universes = body.get("universes") if isinstance(body.get("universes"), dict) else None
                symbols = body.get("symbols") if isinstance(body.get("symbols"), list) else None
                payload = await build_insights_analysis_rows(engine=self, criteria=crit, universes=universes, symbols=symbols, mode=mode or "")
                rows0 = _coerce_rows_list(payload)
            except Exception as exc:
                builder_name = f"fallback:insights_builder_failed:{type(exc).__name__}"
                rows0 = []
            if not rows0:
                rows0 = await self._build_insights_rows_fallback(body, limit=max(limit + offset, 10))
                builder_name = "fallback:engine_insights_rows"
            rows_proj = [_strict_project_row(keys, _normalize_to_schema_keys(keys, headers, r)) for r in rows0]
            payload_full = self._finalize_payload(sheet=target_sheet, headers=headers, keys=keys, rows=rows_proj, include_matrix=include_matrix, status="success" if rows_proj else "warn", meta={"schema_source": schema_src, "contract_level": contract_level, "strict_requested": strict_req, "strict_enforced": False, "target_sheet_known": True, "builder": builder_name, "rows": len(rows_proj), "limit": limit, "offset": offset, "mode": mode})
            self._store_sheet_snapshot(target_sheet, payload_full)
            rows_page = rows_proj[offset: offset + limit]
            return self._finalize_payload(sheet=target_sheet, headers=headers, keys=keys, rows=rows_page, include_matrix=include_matrix, status=payload_full.get("status", "success"), meta={**payload_full.get("meta", {}), "rows": len(rows_page)})

        # Top10
        if target_sheet == "Top_10_Investments":
            top10_body, route_warnings = _normalize_top10_body_for_engine(body, limit=max(1, min(limit, 50)))
            if _is_schema_only_body(top10_body):
                headers, keys = _ensure_top10_contract(headers, keys)
                return self._finalize_payload(sheet=target_sheet, headers=headers, keys=keys, rows=[], include_matrix=include_matrix, status="success", meta={"schema_source": schema_src, "contract_level": contract_level, "strict_requested": strict_req, "strict_enforced": False, "target_sheet_known": True, "builder": "schema_only_fast_path", "rows": 0, "limit": limit, "offset": offset, "mode": mode, "warnings": route_warnings})
            rows_proj: List[Dict[str, Any]] = []
            builder_used = "core.analysis.top10_selector"
            status_out = "success"
            try:
                from core.analysis.top10_selector import build_top10_rows  # type: ignore
                criteria = top10_body.get("criteria") if isinstance(top10_body.get("criteria"), dict) else None
                payload = await build_top10_rows(engine=self, settings=self.settings, criteria=criteria, body=dict(top10_body or {}), limit=int(top10_body.get("limit") or top10_body.get("top_n") or min(limit, 10) or 10), mode=mode or "")
                rows0 = _coerce_rows_list(payload)
                if rows0:
                    rows_proj = [_strict_project_row(keys, _normalize_to_schema_keys(keys, headers, r)) for r in rows0]
                status_out = _safe_str(payload.get("status"), "success") if isinstance(payload, dict) else "success"
            except Exception as exc:
                builder_used = f"fallback:top10_selector_failed:{type(exc).__name__}"
                status_out = "warn"
            if not rows_proj:
                headers, keys, rows_proj = await self._build_top10_rows_fallback(headers, keys, top10_body, limit=max(limit + offset, 10), mode=mode)
                builder_used = "fallback:live_ranker"
                if rows_proj:
                    status_out = "warn"
            payload_full = self._finalize_payload(sheet=target_sheet, headers=headers, keys=keys, rows=rows_proj, include_matrix=include_matrix, status=status_out if rows_proj else "warn", meta={"schema_source": schema_src, "contract_level": contract_level, "strict_requested": strict_req, "strict_enforced": False, "target_sheet_known": True, "builder": builder_used, "rows": len(rows_proj), "limit": limit, "offset": offset, "mode": mode, "warnings": route_warnings})
            if rows_proj:
                self._store_sheet_snapshot(target_sheet, payload_full)
            rows_page = rows_proj[offset: offset + limit]
            return self._finalize_payload(sheet=target_sheet, headers=headers, keys=keys, rows=rows_page, include_matrix=include_matrix, status=payload_full.get("status", "warn"), meta={**payload_full.get("meta", {}), "rows": len(rows_page)})

        # Contract recovery for instrument pages
        if contract_level != "canonical":
            cached_snap = self.get_cached_sheet_snapshot(sheet=target_sheet)
            recovered = False
            if isinstance(cached_snap, dict):
                c_headers = cached_snap.get("headers") or cached_snap.get("display_headers")
                c_keys = cached_snap.get("keys") or cached_snap.get("fields")
                if c_headers or c_keys:
                    ch, ck = _complete_schema_contract(c_headers or [], c_keys or [])
                    if _usable_contract(ch, ck, target_sheet):
                        headers, keys = ch, ck
                        schema_src = "recovered_from_cache_contract"
                        contract_level = "recovered"
                        recovered_from = "cache_contract"
                        recovered = True
            if not recovered and target_sheet in STATIC_CANONICAL_SHEET_CONTRACTS:
                c = STATIC_CANONICAL_SHEET_CONTRACTS[target_sheet]
                headers, keys = _complete_schema_contract(c["headers"], c["keys"])
                schema_src = "static_canonical_contract_recovery"
                contract_level = "recovered"
                recovered_from = "static_contract"
                recovered = True
            if not recovered and target_sheet in INSTRUMENT_SHEETS:
                headers, keys = list(INSTRUMENT_CANONICAL_HEADERS), list(INSTRUMENT_CANONICAL_KEYS)
                schema_src = "fallback_union"
                contract_level = "union_fallback"
                recovered_from = "union_fallback"

        final_status = "success"
        schema_warning: Optional[str] = None
        if contract_level == "union_fallback" and target_sheet_known:
            final_status = "warn"
            schema_warning = "canonical_schema_unusable_used_union_schema"
        elif not target_sheet_known and not strict_req:
            final_status = "warn"
            schema_warning = "unknown_sheet_non_strict_mode"

        base_meta = {
            "schema_source": schema_src,
            "contract_level": contract_level,
            "strict_requested": strict_req,
            "strict_enforced": False,
            "target_sheet_known": target_sheet_known,
        }
        if recovered_from:
            base_meta["recovered_from"] = recovered_from
        if schema_warning:
            base_meta["schema_warning"] = schema_warning

        if _is_schema_only_body(body):
            return self._finalize_payload(sheet=target_sheet, headers=headers, keys=keys, rows=[], include_matrix=include_matrix, status=final_status, meta={**base_meta, "rows": 0, "limit": limit, "offset": offset, "mode": mode, "built_from": "schema_only_fast_path"})

        requested_symbols = _extract_requested_symbols_from_body(body, limit=limit + offset)
        built_from = "body_symbols" if requested_symbols else "live_quotes"
        if requested_symbols:
            self._set_sheet_symbols_meta(target_sheet, "body_symbols", len(requested_symbols))
        if not requested_symbols and target_sheet in INSTRUMENT_SHEETS:
            requested_symbols = await self.get_sheet_symbols(target_sheet, limit=limit + offset, body=body)
            built_from = self._get_sheet_symbols_meta(target_sheet).get("source") or ("auto_sheet_symbols" if requested_symbols else "empty")

        out_headers = list(headers)
        out_keys = list(keys)

        # Prefer external rows reader when present
        if target_sheet in INSTRUMENT_SHEETS:
            ext_rows = await self._get_rows_from_external_reader(target_sheet, limit + offset)
            if ext_rows:
                enriched_rows: List[Dict[str, Any]] = []
                symbols = _extract_symbols_from_rows(ext_rows, limit=limit + offset)
                quote_map: Dict[str, Dict[str, Any]] = {}
                if self.rows_hydrate_external and symbols:
                    for q in await self.get_enriched_quotes(symbols, schema=None):
                        d = _model_to_dict(q)
                        sym = normalize_symbol(_safe_str(d.get("symbol")))
                        if sym:
                            quote_map[sym] = d
                for row in ext_rows:
                    merged = dict(row)
                    sym = normalize_symbol(self._extract_row_symbol(row))
                    if sym and sym in quote_map:
                        for k, v in quote_map[sym].items():
                            if merged.get(k) in (None, "", [], {}):
                                merged[k] = v
                    _compute_scores_fallback(merged)
                    _compute_recommendation(merged)
                    enriched_rows.append(_strict_project_row(out_keys, _normalize_to_schema_keys(out_keys, out_headers, merged)))
                _apply_rank_overall(enriched_rows)
                payload_full = self._finalize_payload(sheet=target_sheet, headers=out_headers, keys=out_keys, rows=enriched_rows, include_matrix=include_matrix, status=final_status, meta={**base_meta, "rows": len(enriched_rows), "limit": limit, "offset": offset, "mode": mode, "built_from": "external_rows_reader", "rows_reader_source": self._rows_reader_source, "symbols_reader_source": self._symbols_reader_source, "symbol_resolution_meta": self._get_sheet_symbols_meta(target_sheet)})
                self._store_sheet_snapshot(target_sheet, payload_full)
                rows_page = enriched_rows[offset: offset + limit]
                return self._finalize_payload(sheet=target_sheet, headers=out_headers, keys=out_keys, rows=rows_page, include_matrix=include_matrix, status=final_status, meta={**payload_full.get("meta", {}), "rows": len(rows_page)})

        if not requested_symbols:
            cached_snap = self.get_cached_sheet_snapshot(sheet=target_sheet)
            cached_rows = _coerce_rows_list(cached_snap)
            if cached_rows:
                proj_rows = [_strict_project_row(out_keys, _normalize_to_schema_keys(out_keys, out_headers, row)) for row in cached_rows]
                rows_page = proj_rows[offset: offset + limit]
                return self._finalize_payload(sheet=target_sheet, headers=out_headers, keys=out_keys, rows=rows_page, include_matrix=include_matrix, status=final_status, meta={**base_meta, "rows": len(rows_page), "limit": limit, "offset": offset, "mode": mode, "built_from": "cached_snapshot", "symbols_reader_source": self._symbols_reader_source, "symbol_resolution_meta": self._get_sheet_symbols_meta(target_sheet)})

        rows_full: List[Dict[str, Any]] = []
        if requested_symbols:
            quotes = await self.get_enriched_quotes(requested_symbols, schema=None)
            for q in quotes:
                row = _model_to_dict(q)
                rows_full.append(_strict_project_row(out_keys, _normalize_to_schema_keys(out_keys, out_headers, row)))
            _apply_rank_overall(rows_full)

        if rows_full:
            payload_full = self._finalize_payload(sheet=target_sheet, headers=out_headers, keys=out_keys, rows=rows_full, include_matrix=include_matrix, status=final_status, meta={**base_meta, "rows": len(rows_full), "limit": limit, "offset": offset, "mode": mode, "built_from": built_from, "resolved_symbols_count": len(requested_symbols), "symbols_reader_source": self._symbols_reader_source, "symbol_resolution_meta": self._get_sheet_symbols_meta(target_sheet)})
            self._store_sheet_snapshot(target_sheet, payload_full)
            rows_page = rows_full[offset: offset + limit]
            return self._finalize_payload(sheet=target_sheet, headers=out_headers, keys=out_keys, rows=rows_page, include_matrix=include_matrix, status=final_status, meta={**payload_full.get("meta", {}), "rows": len(rows_page)})

        return self._finalize_payload(sheet=target_sheet, headers=out_headers, keys=out_keys, rows=[], include_matrix=include_matrix, status=final_status, meta={**base_meta, "rows": 0, "limit": limit, "offset": offset, "mode": mode, "built_from": built_from, "resolved_symbols_count": len(requested_symbols), "symbols_reader_source": self._symbols_reader_source, "symbol_resolution_meta": self._get_sheet_symbols_meta(target_sheet)})

    async def sheet_rows(self, *args, **kwargs) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    async def build_sheet_rows(self, *args, **kwargs) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    # ---------------------------------------------------------------------
    # health / stats
    # ---------------------------------------------------------------------
    async def health(self) -> Dict[str, Any]:
        return {
            "status": "ok",
            "version": self.version,
            "schema_available": True,
            "static_contract_sheets": sorted(list(STATIC_CANONICAL_SHEET_CONTRACTS.keys())),
            "snapshot_sheets": len(self._sheet_snapshots),
            "rows_reader_source": self._rows_reader_source,
            "symbols_reader_source": self._symbols_reader_source,
        }

    async def get_health(self) -> Dict[str, Any]:
        return await self.health()

    async def health_check(self) -> Dict[str, Any]:
        return await self.health()

    async def get_stats(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "primary_provider": self.primary_provider,
            "enabled_providers": list(self.enabled_providers),
            "ksa_providers": list(self.ksa_providers),
            "global_providers": list(self.global_providers),
            "ksa_disallow_eodhd": bool(self.ksa_disallow_eodhd),
            "flags": dict(self.flags),
            "provider_stats": await self._registry.get_stats(),
            "schema_available": True,
            "schema_strict_sheet_rows": bool(self.schema_strict_sheet_rows),
            "top10_force_full_schema": bool(self.top10_force_full_schema),
            "rows_hydrate_external": bool(self.rows_hydrate_external),
            "symbols_reader_source": self._symbols_reader_source,
            "rows_reader_source": self._rows_reader_source,
            "snapshot_sheets": sorted(list(self._sheet_snapshots.keys())),
            "sheet_symbol_resolution_meta": dict(self._sheet_symbol_resolution_meta),
        }


_ENGINE_INSTANCE: Optional[DataEngineV5] = None
ENGINE: Optional[DataEngineV5] = None
engine: Optional[DataEngineV5] = None
_ENGINE: Optional[DataEngineV5] = None
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
    if _ENGINE_INSTANCE is not None:
        await _ENGINE_INSTANCE.aclose()
    _ENGINE_INSTANCE = None
    ENGINE = None
    engine = None
    _ENGINE = None


def get_engine_if_ready() -> Optional[DataEngineV5]:
    return _ENGINE_INSTANCE


def peek_engine() -> Optional[DataEngineV5]:
    return _ENGINE_INSTANCE


def get_cache() -> Any:
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
    "UnifiedQuote",
    "__version__",
    "get_sheet_spec",
]
