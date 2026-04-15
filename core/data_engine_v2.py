#!/usr/bin/env python3
# core/data_engine_v2.py
"""
================================================================================
Data Engine V2 -- GLOBAL-FIRST ORCHESTRATOR -- v5.49.0
================================================================================

WHY v5.47.2
-----------
- FIX: makes provider priority page-aware so non-KSA pages like
       Global_Markets, Commodities_FX, and Mutual_Funds prefer EODHD first
       while KSA pages keep their protected local-first routing.
- FIX: makes quote cache / singleflight keys provider-profile aware so a row
       cached for one page context does not silently override another page
       that should use a different primary provider.
- FIX: threads page context through enriched quote batch builders so
       Global_Markets, Commodities_FX, and Mutual_Funds consistently use the
       intended provider order during page builds and fallback hydration.
- FIX: public engine entrypoints now tolerate extra kwargs from route wrappers
       instead of failing on TypeError before building a canonical envelope.
- FIX: merges request/query/body dicts into one normalized body so GET and POST
       variants produce the same schema-first response contract.
- FIX: adds schema/contract helper aliases expected by diagnostics/wrappers and
       improves single-symbol extraction from direct request payloads.
- FIX: keeps rows / rows_matrix strictly matrix-aligned to keys while
       row_objects / items / records / data / quotes stay dict-row payloads.
- FIX: hardens canonical sheet-name resolution by consulting page catalog
       aliases/functions before falling back to static contracts.
- FIX: strips fully blank schema pairs instead of fabricating ghost columns,
       preventing false leading-column drift from partial specs.
- FIX: prevents EODHD from being re-inserted for KSA symbols when
       KSA_DISALLOW_EODHD=true even if it is the global primary provider.
- FIX: enriches fallback Insights rows with market summary, risk-bucket counts,
       leaderboard items, and portfolio KPI style signals.
- FIX: improves fallback scoring with valuation_score, richer quality/risk logic,
       and more stable opportunity/recommendation support.
- FIX: preserves aligned snapshots and route compatibility aliases expected by
       advisor / advanced / enriched wrappers and direct router calls.
- FIX: adds commodity/FX self-recovery from chart/history payloads when live
       quote payloads are sparse or unavailable.
- FIX: applies symbol-aware page defaults so Commodities_FX rows keep useful
       identity/context fields even during provider degradation.
- FIX: adds a native Top 10 engine fallback ranker so the engine does not
       depend on an external selector to build Top_10_Investments rows.
- FIX: exposes normalize_row_to_schema and batch-analysis aliases expected by
       downstream analysis/advisor routers.
- FIX: adds snapshot-assisted row backfill for sparse live quote rows so
       previously cached richer rows can safely fill missing schema fields.
- FIX: exposes display-header object payloads alongside canonical key-based
       row objects to make diagnostics and route wrappers easier to validate.
- FIX: bridges EODHD-style aliases used by the new global provider revision,
       including forward_pe -> pe_forward, day_open -> open_price,
       fcf_ttm -> free_cash_flow_ttm, d_e_ratio -> debt_to_equity,
       and avg_vol_* -> avg_volume_*.
- FIX: strengthens page-aware backfill for Mutual_Funds and Commodities_FX so
       identity/context fields stay populated without fabricating missing
       equity-only fundamentals.
- FIX: expands global-provider alias bridges for fundamentals / margins /
       market-cap style fields commonly returned under alternative names.
- FIX: adds cross-snapshot symbol backfill so richer rows built on one page can
       safely fill sparse rows on Top_10_Investments, My_Portfolio, and other
       dependent pages.
- FIX: improves ETF / fund context defaults so non-KSA pages keep better
       identity metadata when provider payloads are thin.

Design goals
------------
- Never fail import because an optional module is missing.
- Never return an empty schema for a known page.
- Prefer live or external rows when available.
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
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal
from enum import Enum
from importlib import import_module
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

try:
    from pydantic import BaseModel, ConfigDict
except Exception:  # pragma: no cover
    class BaseModel:  # type: ignore
        def __init__(self, **data: Any) -> None:
            self.__dict__.update(data)

        def model_dump(self, mode: str = "python") -> Dict[str, Any]:
            return dict(self.__dict__)

    def ConfigDict(**kwargs: Any) -> Dict[str, Any]:  # type: ignore
        return dict(kwargs)

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

__version__ = "5.49.0"

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
    "Section",
    "Item",
    "Metric",
    "Value",
    "Notes",
    "Source",
    "Sort Order",
]
INSIGHTS_KEYS: List[str] = [
    "section",
    "item",
    "metric",
    "value",
    "notes",
    "source",
    "sort_order",
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
NON_KSA_EODHD_PRIMARY_PAGES = {"Global_Markets", "Commodities_FX", "Mutual_Funds"}
PAGE_PRIMARY_PROVIDER_DEFAULTS = {page: "eodhd" for page in NON_KSA_EODHD_PRIMARY_PAGES}
PROVIDER_PRIORITIES = {
    "tadawul": 10,
    "argaam": 20,
    "eodhd": 30,
    "yahoo": 40,
    "finnhub": 50,
    "yahoo_chart": 60,
}


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


def _as_pct_points(x: Any) -> Optional[float]:
    v = _as_float(x)
    if v is None:
        return None
    return v * 100.0 if abs(v) <= 1.5 else v


def _dedupe_keep_order(items: Sequence[Any]) -> List[Any]:
    out: List[Any] = []
    seen: Set[Any] = set()
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _page_catalog_candidates() -> List[Any]:
    # FIX v5.49.0: added "page_catalog" repo-root fallback (3rd candidate)
    modules: List[Any] = []
    for mod_path in ("core.sheets.page_catalog", "sheets.page_catalog", "page_catalog"):
        try:
            modules.append(import_module(mod_path))
        except Exception:
            continue
    return modules


def _page_catalog_canonical_name(name: str) -> str:
    raw = _safe_str(name)
    if not raw:
        return ""

    for mod in _page_catalog_candidates():
        for fn_name in ("canonicalize_page_name", "normalize_page_name", "get_canonical_page_name", "canonical_page_name"):
            fn = getattr(mod, fn_name, None)
            if callable(fn):
                for args, kwargs in (((raw,), {}), ((), {"page": raw}), ((), {"name": raw}), ((), {"sheet": raw})):
                    try:
                        val = fn(*args, **kwargs)
                    except TypeError:
                        continue
                    except Exception:
                        continue
                    text = _safe_str(val)
                    if text:
                        return text

        for attr_name in ("PAGE_ALIASES", "SHEET_ALIASES", "ALIASES", "PAGE_NAME_ALIASES"):
            mapping = getattr(mod, attr_name, None)
            if isinstance(mapping, dict):
                for cand in (raw, raw.replace(" ", "_"), raw.replace("-", "_"), _norm_key(raw), _norm_key_loose(raw)):
                    for key, val in mapping.items():
                        if cand in {_safe_str(key), _norm_key(_safe_str(key)), _norm_key_loose(_safe_str(key))}:
                            text = _safe_str(val)
                            if text:
                                return text
    return ""


def _canonicalize_sheet_name(name: str) -> str:
    raw = _safe_str(name)
    if not raw:
        return ""

    candidates = [raw, raw.replace(" ", "_"), raw.replace("-", "_"), _norm_key(raw)]
    known = {k: k for k in STATIC_CANONICAL_SHEET_CONTRACTS.keys()}
    by_norm = {_norm_key(k): k for k in STATIC_CANONICAL_SHEET_CONTRACTS.keys()}
    by_loose = {_norm_key_loose(k): k for k in STATIC_CANONICAL_SHEET_CONTRACTS.keys()}

    for cand in candidates:
        if cand in known:
            return known[cand]
        nk = _norm_key(cand)
        if nk in by_norm:
            return by_norm[nk]
        nkl = _norm_key_loose(cand)
        if nkl in by_loose:
            return by_loose[nkl]

    page_catalog_name = _page_catalog_canonical_name(raw)
    if page_catalog_name:
        pc_candidates = [page_catalog_name, page_catalog_name.replace(" ", "_"), _norm_key(page_catalog_name), _norm_key_loose(page_catalog_name)]
        for cand in pc_candidates:
            if cand in known:
                return known[cand]
            if _norm_key(cand) in by_norm:
                return by_norm[_norm_key(cand)]
            if _norm_key_loose(cand) in by_loose:
                return by_loose[_norm_key_loose(cand)]
        return page_catalog_name.replace(" ", "_")

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
        "symbol",
        "ticker",
        "code",
        "requested_symbol",
    ):
        raw.extend(_split_symbols(body.get(key)))
    criteria = body.get("criteria")
    if isinstance(criteria, dict):
        for key in ("symbols", "tickers", "selected_symbols", "direct_symbols", "codes", "symbol", "ticker", "code"):
            raw.extend(_split_symbols(criteria.get(key)))
    return _normalize_symbol_list(raw, limit=limit)


def _merge_route_body_dicts(*parts: Any) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    for part in parts:
        if part is None:
            continue
        if isinstance(part, Mapping):
            for k, v in part.items():
                key = _safe_str(k)
                if not key:
                    continue
                merged[key] = v
            continue
        try:
            if hasattr(part, "multi_items") and callable(getattr(part, "multi_items")):
                for k, v in part.multi_items():
                    key = _safe_str(k)
                    if key:
                        merged[key] = v
                continue
        except Exception:
            pass
        try:
            if hasattr(part, "items") and callable(getattr(part, "items")):
                for k, v in part.items():
                    key = _safe_str(k)
                    if key:
                        merged[key] = v
                continue
        except Exception:
            pass
        try:
            d = _model_to_dict(part)
            if isinstance(d, dict) and d:
                for k, v in d.items():
                    key = _safe_str(k)
                    if key:
                        merged[key] = v
        except Exception:
            continue
    return merged


def _extract_request_route_parts(request: Any) -> Dict[str, Any]:
    if request is None:
        return {}
    out: Dict[str, Any] = {}
    for attr in ("query_params", "path_params"):
        try:
            part = getattr(request, attr, None)
        except Exception:
            part = None
        if part is not None:
            out.update(_merge_route_body_dicts(part))
    try:
        state = getattr(request, "state", None)
        if state is not None:
            for attr in ("payload", "body", "json", "data", "params"):
                val = getattr(state, attr, None)
                if isinstance(val, Mapping):
                    out.update(_merge_route_body_dicts(val))
    except Exception:
        pass
    return out


def _normalize_route_call_inputs(
    *,
    page: Optional[str] = None,
    sheet: Optional[str] = None,
    sheet_name: Optional[str] = None,
    limit: int = 2000,
    offset: int = 0,
    mode: str = "",
    body: Optional[Dict[str, Any]] = None,
    extras: Optional[Dict[str, Any]] = None,
) -> Tuple[str, int, int, str, Dict[str, Any], Dict[str, Any]]:
    extras = dict(extras or {})
    request_parts = _extract_request_route_parts(extras.get("request"))

    merged_body = _merge_route_body_dicts(
        request_parts,
        extras.get("params"),
        extras.get("query"),
        extras.get("query_params"),
        extras.get("payload"),
        extras.get("data"),
        extras.get("json"),
        extras.get("body"),
        body,
        extras,
    )

    target_raw = (
        page
        or sheet
        or sheet_name
        or _safe_str(merged_body.get("page"))
        or _safe_str(merged_body.get("sheet"))
        or _safe_str(merged_body.get("sheet_name"))
        or _safe_str(merged_body.get("page_name"))
        or _safe_str(merged_body.get("name"))
        or _safe_str(merged_body.get("tab"))
        or _safe_str(merged_body.get("worksheet"))
        or _safe_str(merged_body.get("sheetName"))
        or _safe_str(merged_body.get("pageName"))
        or _safe_str(merged_body.get("worksheet_name"))
        or "Market_Leaders"
    )

    effective_limit = _safe_int(
        merged_body.get("limit", limit),
        default=limit,
        lo=1,
        hi=5000,
    )
    if effective_limit <= 0:
        effective_limit = max(1, min(5000, int(limit or 2000)))

    effective_offset = _safe_int(merged_body.get("offset", offset), default=offset, lo=0)
    effective_mode = _safe_str(merged_body.get("mode") or mode)

    passthrough = {
        k: v for k, v in merged_body.items()
        if k not in {"request", "params", "query", "query_params", "payload", "data", "json", "body"}
    }
    return _canonicalize_sheet_name(target_raw) or "Market_Leaders", effective_limit, effective_offset, effective_mode, passthrough, request_parts


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


def _complete_schema_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    raw_headers = list(headers or [])
    raw_keys = list(keys or [])
    max_len = max(len(raw_headers), len(raw_keys))
    hdrs: List[str] = []
    ks: List[str] = []

    for i in range(max_len):
        h = _safe_str(raw_headers[i]) if i < len(raw_headers) else ""
        k = _safe_str(raw_keys[i]) if i < len(raw_keys) else ""

        if not h and not k:
            # Drop fully blank pairs instead of fabricating ghost columns.
            continue
        if h and not k:
            k = _norm_key(h)
        elif k and not h:
            h = k.replace("_", " ").title()

        if not h and k:
            h = k.replace("_", " ").title()
        if h and not k:
            k = _norm_key(h)

        if h and k:
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
        if not ({"section", "item", "metric", "value"} <= keyset):
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
    if is_dataclass(value):
        try:
            return {str(k): _json_safe(v) for k, v in asdict(value).items()}
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


def _rows_from_matrix_payload(matrix: Any, cols: Sequence[Any]) -> List[Dict[str, Any]]:
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


def _coerce_rows_list(out: Any) -> List[Dict[str, Any]]:
    """
    Prefer explicit dict-row collections first:
    row_objects / records / items / data / quotes
    Then accept rows if they are dict rows.
    Finally fall back to matrix + keys or to explicit single row dicts.
    """
    if out is None:
        return []

    if isinstance(out, list):
        if not out:
            return []
        if isinstance(out[0], dict):
            return [dict(r) for r in out if isinstance(r, dict)]
        if isinstance(out[0], (list, tuple)):
            return []
        return [_model_to_dict(r) for r in out if _model_to_dict(r)]

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

        for key in ("row_objects", "records", "items", "data", "quotes", "rows"):
            val = out.get(key)
            if isinstance(val, list):
                if val and isinstance(val[0], dict):
                    return [dict(r) for r in val if isinstance(r, dict)]
                if val and isinstance(val[0], (list, tuple)):
                    cols = out.get("keys") or out.get("headers") or out.get("columns") or []
                    if isinstance(cols, list) and cols:
                        return _rows_from_matrix_payload(val, cols)
            if isinstance(val, dict):
                nested_rows = _coerce_rows_list(val)
                if nested_rows:
                    return nested_rows

        rows_matrix = out.get("rows_matrix") or out.get("matrix")
        if isinstance(rows_matrix, list):
            cols = out.get("keys") or out.get("headers") or out.get("columns") or []
            if isinstance(cols, list) and cols:
                return _rows_from_matrix_payload(rows_matrix, cols)

        if _looks_like_explicit_row_dict(out):
            return [dict(out)]

        for key in ("payload", "result", "response", "output"):
            nested = out.get(key)
            nested_rows = _coerce_rows_list(nested)
            if nested_rows:
                return nested_rows

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


_NULL_STRINGS: Set[str] = {"", "null", "none", "n/a", "na", "nan", "-", "--"}

_CANONICAL_FIELD_ALIASES: Dict[str, Tuple[str, ...]] = {
    "symbol": ("symbol", "ticker", "code", "requested_symbol", "regularMarketSymbol"),
    "name": ("name", "shortName", "longName", "displayName", "companyName", "fundName", "description"),
    "asset_class": ("asset_class", "assetClass", "quoteType", "assetType", "instrumentType", "securityType", "type"),
    "exchange": ("exchange", "exchangeName", "fullExchangeName", "market", "marketName", "mic", "exchangeCode"),
    "currency": ("currency", "financialCurrency", "reportingCurrency", "quoteCurrency", "baseCurrency"),
    "country": ("country", "countryName", "country_code", "countryCode", "localeCountry"),
    "sector": ("sector", "sectorDisp", "gicsSector", "industryGroup", "sectorName", "gics_sector", "Sector", "General.Sector"),
    "industry": ("industry", "industryDisp", "gicsIndustry", "category", "industryName", "Industry", "General.Industry", "industry_group"),
    "current_price": ("current_price", "currentPrice", "price", "last", "lastPrice", "latestPrice", "regularMarketPrice", "nav", "close", "adjusted_close", "adjclose", "closePrice", "last_trade_price", "regular_market_price", "price_close"),
    "previous_close": ("previous_close", "previousClose", "regularMarketPreviousClose", "prevClose", "priorClose", "close_yesterday", "previous_close_price"),
    "open_price": ("open_price", "day_open", "dayOpen", "open", "openPrice", "regularMarketOpen", "open_price_day", "dailyOpen", "sessionOpen"),
    "day_high": ("day_high", "high", "dayHigh", "regularMarketDayHigh", "sessionHigh", "highPrice", "intradayHigh", "dailyHigh"),
    "day_low": ("day_low", "low", "dayLow", "regularMarketDayLow", "sessionLow", "lowPrice", "intradayLow", "dailyLow"),
    "week_52_high": ("week_52_high", "52WeekHigh", "fiftyTwoWeekHigh", "yearHigh", "week52High"),
    "week_52_low": ("week_52_low", "52WeekLow", "fiftyTwoWeekLow", "yearLow", "week52Low"),
    "price_change": ("price_change", "change", "priceChange", "regularMarketChange", "netChange"),
    "percent_change": ("percent_change", "changePercent", "percentChange", "regularMarketChangePercent", "pctChange", "change_pct"),
    "volume": ("volume", "regularMarketVolume", "sharesTraded", "tradeVolume", "Volume", "vol", "trade_count_volume"),
    "avg_volume_10d": ("avg_volume_10d", "avg_vol_10d", "averageVolume10days", "avgVolume10Day", "avgVol10d", "averageVolume10Day", "avg_volume_10_day"),
    "avg_volume_30d": ("avg_volume_30d", "avg_vol_30d", "averageVolume", "averageDailyVolume3Month", "avgVolume3Month", "avgVol30d", "averageVolume30Day", "avg_volume_30_day"),
    "market_cap": ("market_cap", "marketCap", "marketCapitalization", "MarketCapitalization", "capitalization", "Capitalization", "market_capitalization"),
    "float_shares": ("float_shares", "floatShares", "sharesFloat", "FloatShares", "SharesFloat", "sharesOutstanding", "SharesOutstanding"),
    "beta_5y": ("beta_5y", "beta", "beta5Y", "Beta", "beta5Year"),
    "pe_ttm": ("pe_ttm", "trailingPE", "peRatio", "priceEarningsTTM", "pe", "PERatio", "PriceEarningsTTM", "peTTM"),
    "pe_forward": ("pe_forward", "forward_pe", "forwardPE", "forwardPe", "ForwardPE", "ForwardPERatio", "forwardPERatio"),
    "eps_ttm": ("eps_ttm", "trailingEps", "eps", "earningsPerShare", "epsTTM", "EarningsShare", "epsTtm", "DilutedEPSTTM"),
    "dividend_yield": ("dividend_yield", "dividendYield", "trailingAnnualDividendYield", "distributionYield", "DividendYield", "forwardAnnualDividendYield", "Yield"),
    "payout_ratio": ("payout_ratio", "payoutRatio", "PayoutRatio", "payout", "PayoutRatioTTM"),
    "revenue_ttm": ("revenue_ttm", "totalRevenue", "revenueTTM", "revenue", "RevenueTTM", "TotalRevenueTTM", "Revenue", "SalesTTM"),
    "revenue_growth_yoy": ("revenue_growth_yoy", "revenueGrowth", "revenueGrowthYoY", "revenue_yoy_growth", "RevenueGrowthYOY", "QuarterlyRevenueGrowthYOY", "revenueGrowthYoy"),
    "gross_margin": ("gross_margin", "grossMargins", "grossMargin", "GrossMargin", "GrossProfitMargin", "grossMarginTTM"),
    "operating_margin": ("operating_margin", "operatingMargins", "operatingMargin", "OperatingMargin", "OperatingMarginTTM", "operatingMarginTTM"),
    "profit_margin": ("profit_margin", "profitMargins", "profitMargin", "netMargin", "ProfitMargin", "NetProfitMargin", "profitMarginTTM"),
    "debt_to_equity": ("debt_to_equity", "d_e_ratio", "debtToEquity", "deRatio", "DebtToEquity", "TotalDebtEquity"),
    "free_cash_flow_ttm": ("free_cash_flow_ttm", "fcf_ttm", "freeCashflow", "freeCashFlow", "fcf", "FreeCashFlow", "FreeCashFlowTTM"),
    "rsi_14": ("rsi_14", "rsi", "rsi14"),
    "volatility_30d": ("volatility_30d", "volatility30d", "vol30d"),
    "volatility_90d": ("volatility_90d", "volatility90d", "vol90d"),
    "max_drawdown_1y": ("max_drawdown_1y", "maxDrawdown1y", "drawdown1y"),
    "var_95_1d": ("var_95_1d", "var95_1d", "valueAtRisk95_1d"),
    "sharpe_1y": ("sharpe_1y", "sharpe1y", "sharpeRatio"),
    "risk_score": ("risk_score",),
    "risk_bucket": ("risk_bucket",),
    "pb_ratio": ("pb_ratio", "priceToBook", "pb"),
    "ps_ratio": ("ps_ratio", "priceToSalesTrailing12Months", "ps"),
    "ev_ebitda": ("ev_ebitda", "enterpriseToEbitda", "evToEbitda"),
    "peg_ratio": ("peg_ratio", "peg", "pegRatio"),
    "intrinsic_value": ("intrinsic_value", "fairValue", "dcf", "dcfValue", "intrinsicValue"),
    "valuation_score": ("valuation_score",),
    "forecast_price_1m": ("forecast_price_1m", "targetPrice1m", "priceTarget1m"),
    "forecast_price_3m": ("forecast_price_3m", "targetPrice3m", "priceTarget3m", "targetPrice", "targetMeanPrice"),
    "forecast_price_12m": ("forecast_price_12m", "targetPrice12m", "priceTarget12m", "targetMedianPrice", "targetHighPrice"),
    "expected_roi_1m": ("expected_roi_1m", "expectedReturn1m", "roi1m"),
    "expected_roi_3m": ("expected_roi_3m", "expectedReturn3m", "roi3m"),
    "expected_roi_12m": ("expected_roi_12m", "expectedReturn12m", "roi12m"),
    "forecast_confidence": ("forecast_confidence", "confidence", "confidencePct", "modelConfidence"),
    "confidence_score": ("confidence_score", "modelConfidenceScore"),
    "confidence_bucket": ("confidence_bucket",),
    "value_score": ("value_score",),
    "quality_score": ("quality_score",),
    "momentum_score": ("momentum_score",),
    "growth_score": ("growth_score",),
    "overall_score": ("overall_score", "score", "compositeScore"),
    "opportunity_score": ("opportunity_score",),
    "rank_overall": ("rank_overall", "rank", "overallRank"),
    "recommendation": ("recommendation", "rating", "action", "reco", "consensus"),
    "recommendation_reason": ("recommendation_reason", "reason", "summary", "thesis", "analysis"),
    "horizon_days": ("horizon_days", "horizon", "days"),
    "invest_period_label": ("invest_period_label", "periodLabel", "horizonLabel"),
    "position_qty": ("position_qty", "positionQty", "qty", "quantity", "shares", "holdingQty"),
    "avg_cost": ("avg_cost", "avgCost", "averageCost", "costBasisPerShare"),
    "position_cost": ("position_cost", "positionCost", "costBasis", "totalCost"),
    "position_value": ("position_value", "marketValue", "positionValue", "holdingValue"),
    "unrealized_pl": ("unrealized_pl", "unrealizedPnL", "unrealizedPL", "profitLoss"),
    "unrealized_pl_pct": ("unrealized_pl_pct", "unrealizedPnLPct", "unrealizedPLPct"),
    "data_provider": ("data_provider", "provider", "source", "dataProvider"),
    "last_updated_utc": ("last_updated_utc", "lastUpdated", "updatedAt", "timestamp", "asOf"),
    "last_updated_riyadh": ("last_updated_riyadh",),
    "warnings": ("warnings", "warning", "messages", "errors"),
}

_COMMODITY_SYMBOL_HINTS: Tuple[str, ...] = ("GC=F", "SI=F", "BZ=F", "CL=F", "NG=F", "HG=F")
_ETF_SYMBOL_HINTS: Tuple[str, ...] = ("SPY", "QQQ", "VTI", "VOO", "IWM", "DIA", "IVV", "EFA", "EEM", "ARKK")
_ETF_DISPLAY_NAMES: Dict[str, str] = {
    "SPY": "SPDR S&P 500 ETF",
    "QQQ": "Invesco QQQ Trust",
    "VTI": "Vanguard Total Stock Market ETF",
    "VOO": "Vanguard S&P 500 ETF",
    "IWM": "iShares Russell 2000 ETF",
    "DIA": "SPDR Dow Jones Industrial Average ETF",
    "IVV": "iShares Core S&P 500 ETF",
    "EFA": "iShares MSCI EAFE ETF",
    "EEM": "iShares MSCI Emerging Markets ETF",
    "ARKK": "ARK Innovation ETF",
}
_COMMODITY_DISPLAY_NAMES: Dict[str, str] = {
    "GC=F": "Gold Futures",
    "SI=F": "Silver Futures",
    "BZ=F": "Brent Crude Futures",
    "CL=F": "WTI Crude Futures",
    "NG=F": "Natural Gas Futures",
    "HG=F": "Copper Futures",
}
_COMMODITY_INDUSTRY_HINTS: Dict[str, str] = {
    "GC=F": "Precious Metals",
    "SI=F": "Precious Metals",
    "HG=F": "Industrial Metals",
    "BZ=F": "Energy",
    "CL=F": "Energy",
    "NG=F": "Energy",
}


def _is_blank_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().lower() in _NULL_STRINGS
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0
    return False


def _to_scalar(value: Any) -> Any:
    if isinstance(value, (list, tuple, set)):
        seq = [v for v in value if not _is_blank_value(v)]
        if not seq:
            return None
        if all(not isinstance(v, (dict, list, tuple, set)) for v in seq):
            if len(seq) == 1:
                return seq[0]
            return "; ".join(_safe_str(v) for v in seq if _safe_str(v))
        return None
    return value


def _flatten_scalar_fields(obj: Any, out: Optional[Dict[str, Any]] = None, prefix: str = "", depth: int = 0, max_depth: int = 4) -> Dict[str, Any]:
    if out is None:
        out = {}
    if depth > max_depth or obj is None:
        return out
    if isinstance(obj, Mapping):
        for k, v in obj.items():
            key = _safe_str(k)
            if not key:
                continue
            full = f"{prefix}.{key}" if prefix else key
            if isinstance(v, Mapping):
                _flatten_scalar_fields(v, out=out, prefix=full, depth=depth + 1, max_depth=max_depth)
                continue
            if isinstance(v, (list, tuple, set)) and v and isinstance(next(iter(v)), Mapping):
                continue
            scalar = _to_scalar(v)
            if scalar is None:
                continue
            out.setdefault(key, scalar)
            out.setdefault(full, scalar)
    return out


def _lookup_alias_value(src: Mapping[str, Any], flat: Mapping[str, Any], alias: str) -> Any:
    if not alias:
        return None
    candidates = [
        alias,
        alias.lower(),
        _norm_key(alias),
        _norm_key_loose(alias),
        alias.replace("_", " "),
        alias.replace("_", "-"),
    ]
    src_ci = {str(k).strip().lower(): v for k, v in src.items()}
    src_loose = {_norm_key_loose(k): v for k, v in src.items()}
    flat_ci = {str(k).strip().lower(): v for k, v in flat.items()}
    flat_loose = {_norm_key_loose(k): v for k, v in flat.items()}
    for cand in candidates:
        if cand in src and not _is_blank_value(src.get(cand)):
            return src.get(cand)
        if cand in flat and not _is_blank_value(flat.get(cand)):
            return flat.get(cand)
        lower = cand.lower()
        if lower in src_ci and not _is_blank_value(src_ci.get(lower)):
            return src_ci.get(lower)
        if lower in flat_ci and not _is_blank_value(flat_ci.get(lower)):
            return flat_ci.get(lower)
        loose = _norm_key_loose(cand)
        if loose in src_loose and not _is_blank_value(src_loose.get(loose)):
            return src_loose.get(loose)
        if loose in flat_loose and not _is_blank_value(flat_loose.get(loose)):
            return flat_loose.get(loose)
    return None


def _infer_asset_class_from_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if not s:
        return ""
    if s.endswith(".SR") or re.match(r"^[0-9]{4}$", s):
        return "Equity"
    if s.endswith("=X"):
        return "FX"
    if s.endswith("=F") or s in _COMMODITY_SYMBOL_HINTS:
        return "Commodity"
    if s in {"SPY", "QQQ", "VTI", "VOO", "IWM", "DIA"}:
        return "ETF"
    return "Equity"


def _infer_exchange_from_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if s.endswith(".SR") or re.match(r"^[0-9]{4}$", s):
        return "Tadawul"
    if s.endswith("=X"):
        return "FX"
    if s.endswith("=F"):
        return "Futures"
    return "NASDAQ/NYSE"


def _infer_currency_from_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if s.endswith(".SR") or re.match(r"^[0-9]{4}$", s):
        return "SAR"
    if s.endswith("=X"):
        pair = s[:-2]
        if len(pair) >= 6:
            return pair[-3:]
        if pair:
            return pair
        return "FX"
    if s.endswith("=F"):
        return "USD"
    return "USD"


def _infer_country_from_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if s.endswith(".SR") or re.match(r"^[0-9]{4}$", s):
        return "Saudi Arabia"
    if s.endswith("=X") or s.endswith("=F"):
        return "Global"
    return "USA"


def _infer_sector_from_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if s.endswith("=X"):
        return "Currencies"
    if s.endswith("=F") or s in _COMMODITY_SYMBOL_HINTS:
        return "Commodities"
    if s in _ETF_SYMBOL_HINTS:
        return "Broad Market"
    if s.endswith(".SR") or re.match(r"^[0-9]{4}$", s):
        return "Saudi Market"
    return ""


def _infer_industry_from_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if s in _COMMODITY_INDUSTRY_HINTS:
        return _COMMODITY_INDUSTRY_HINTS[s]
    if s.endswith("=X"):
        return "Foreign Exchange"
    if s.endswith("=F"):
        return "Commodity Futures"
    if s in _ETF_SYMBOL_HINTS:
        return "ETF"
    if s.endswith(".SR") or re.match(r"^[0-9]{4}$", s):
        return "Listed Equities"
    return ""


def _infer_display_name_from_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if not s:
        return ""
    if s in _COMMODITY_DISPLAY_NAMES:
        return _COMMODITY_DISPLAY_NAMES[s]
    if s in _ETF_DISPLAY_NAMES:
        return _ETF_DISPLAY_NAMES[s]
    if s.endswith("=X"):
        pair = s[:-2]
        if len(pair) >= 6:
            return f"{pair[:3]}/{pair[3:6]}"
        return f"{pair} FX" if pair else s
    if s.endswith("=F"):
        return _safe_str(s.replace("=F", "")).strip() or s
    return s


def _apply_symbol_context_defaults(row: Dict[str, Any], symbol: str = "", page: str = "") -> Dict[str, Any]:
    out = dict(row or {})
    sym = normalize_symbol(symbol or _safe_str(out.get("symbol") or out.get("ticker") or out.get("requested_symbol")))
    if not sym:
        return out

    page = _canonicalize_sheet_name(page) if page else ""

    if not out.get("symbol"):
        out["symbol"] = sym
    if not out.get("requested_symbol"):
        out["requested_symbol"] = sym
    if not out.get("symbol_normalized"):
        out["symbol_normalized"] = sym

    if page == "Commodities_FX" or sym.endswith("=F") or sym.endswith("=X"):
        out.setdefault("asset_class", _infer_asset_class_from_symbol(sym))
        out.setdefault("exchange", _infer_exchange_from_symbol(sym))
        out.setdefault("currency", _infer_currency_from_symbol(sym))
        out.setdefault("country", _infer_country_from_symbol(sym))
        out.setdefault("sector", _infer_sector_from_symbol(sym))
        out.setdefault("industry", _infer_industry_from_symbol(sym))

        current_name = _safe_str(out.get("name"))
        inferred_name = _infer_display_name_from_symbol(sym)
        if not current_name or current_name == sym:
            out["name"] = inferred_name or sym

        if sym.endswith("=X"):
            out.setdefault("market_cap", None)
            out.setdefault("float_shares", None)
            out.setdefault("beta_5y", None)
        if sym.endswith("=F"):
            out.setdefault("market_cap", None)
            out.setdefault("float_shares", None)

        if out.get("invest_period_label") in (None, ""):
            out["invest_period_label"] = "1Y"
        if out.get("horizon_days") in (None, ""):
            out["horizon_days"] = 365

    return out


def _coerce_datetime_like(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        try:
            return value.isoformat()
        except Exception:
            return _safe_str(value)
    return _safe_str(value) or None


def _canonicalize_provider_row(row: Dict[str, Any], requested_symbol: str = "", normalized_symbol: str = "", provider: str = "") -> Dict[str, Any]:
    src = dict(row or {})
    flat = _flatten_scalar_fields(src)
    symbol = normalized_symbol or normalize_symbol(_safe_str(_lookup_alias_value(src, flat, "symbol") or requested_symbol))
    out: Dict[str, Any] = {
        "symbol": symbol or requested_symbol,
        "symbol_normalized": symbol or requested_symbol,
        "requested_symbol": requested_symbol or symbol,
    }
    for field, aliases in _CANONICAL_FIELD_ALIASES.items():
        for alias in (field,) + tuple(aliases):
            val = _lookup_alias_value(src, flat, alias)
            if not _is_blank_value(val):
                out[field] = _json_safe(_to_scalar(val))
                break

    inferred_symbol = out.get("symbol") or normalized_symbol or requested_symbol
    inferred_name = _infer_display_name_from_symbol(inferred_symbol)
    if not out.get("name") or _safe_str(out.get("name")) == _safe_str(inferred_symbol):
        out["name"] = inferred_name or out.get("symbol") or normalized_symbol or requested_symbol
    if not out.get("asset_class"):
        out["asset_class"] = _infer_asset_class_from_symbol(inferred_symbol)
    if not out.get("exchange"):
        out["exchange"] = _infer_exchange_from_symbol(inferred_symbol)
    if not out.get("currency"):
        out["currency"] = _infer_currency_from_symbol(inferred_symbol)
    if not out.get("country"):
        out["country"] = _infer_country_from_symbol(inferred_symbol)
    if not out.get("sector"):
        out["sector"] = _infer_sector_from_symbol(inferred_symbol)
    if not out.get("industry"):
        out["industry"] = _infer_industry_from_symbol(inferred_symbol)

    if provider and not out.get("data_provider"):
        out["data_provider"] = provider

    if not out.get("last_updated_utc"):
        out["last_updated_utc"] = _coerce_datetime_like(_lookup_alias_value(src, flat, "last_updated_utc")) or _now_utc_iso()
    if not out.get("last_updated_riyadh"):
        out["last_updated_riyadh"] = _now_riyadh_iso()

    warnings = out.get("warnings")
    if isinstance(warnings, (list, tuple, set)):
        out["warnings"] = "; ".join(_safe_str(v) for v in warnings if _safe_str(v))

    price = _as_float(out.get("current_price")) or _as_float(out.get("price"))
    prev = _as_float(out.get("previous_close"))
    change = _as_float(out.get("price_change"))
    pct = _as_float(out.get("percent_change"))
    if price is None:
        price = _as_float(out.get("close"))
        if price is not None:
            out["current_price"] = price
    if prev is None and price is not None and change is not None:
        prev = price - change
        out["previous_close"] = prev
    if change is None and price is not None and prev is not None:
        change = price - prev
        out["price_change"] = round(change, 6)
    if pct is None and price is not None and prev not in (None, 0):
        pct = ((price - prev) / prev) * 100.0
        out["percent_change"] = round(pct, 6)
    elif pct is not None and abs(pct) <= 1.5:
        out["percent_change"] = round(pct * 100.0, 6)

    high52 = _as_float(out.get("week_52_high"))
    low52 = _as_float(out.get("week_52_low"))
    # FIX v5.48.0/v5.49.0: normalize week_52_position_pct to pct-points.
    # Providers (eodhd, history patch) supply fractions (0-1). Convert here so
    # TFB_PERCENT_MODE=points displays 91.48 not 0.91.
    pos52 = _as_float(out.get("week_52_position_pct"))
    if pos52 is not None:
        if abs(pos52) <= 1.5:
            out["week_52_position_pct"] = round(pos52 * 100.0, 6)
    elif price is not None and high52 is not None and low52 is not None and high52 > low52:
        out["week_52_position_pct"] = round(((price - low52) / (high52 - low52)) * 100.0, 6)

    qty = _as_float(out.get("position_qty"))
    avg_cost = _as_float(out.get("avg_cost"))
    if qty is not None and price is not None and out.get("position_value") is None:
        out["position_value"] = round(qty * price, 6)
    if qty is not None and avg_cost is not None and out.get("position_cost") is None:
        out["position_cost"] = round(qty * avg_cost, 6)
    pos_val = _as_float(out.get("position_value"))
    pos_cost = _as_float(out.get("position_cost"))
    if pos_val is not None and pos_cost is not None and out.get("unrealized_pl") is None:
        out["unrealized_pl"] = round(pos_val - pos_cost, 6)
    upl = _as_float(out.get("unrealized_pl"))
    if upl is not None and pos_cost not in (None, 0) and out.get("unrealized_pl_pct") is None:
        # FIX v5.49.0: store as FRACTION (dtype=pct schema contract).
        # 0.0142 = 1.42% unrealized P/L, NOT 1.42.
        out["unrealized_pl_pct"] = round(upl / pos_cost, 6)

    out = _apply_symbol_context_defaults(out, symbol=inferred_symbol)
    if _as_float(out.get("current_price")) is not None and _safe_str(out.get("warnings")).lower() == "no live provider data available":
        out["warnings"] = "Recovered from history/chart fallback"

    return out


def _normalize_to_schema_keys(keys: Sequence[str], headers: Sequence[str], row: Dict[str, Any]) -> Dict[str, Any]:
    src = _canonicalize_provider_row(dict(row or {}), requested_symbol=_safe_str((row or {}).get("requested_symbol")), normalized_symbol=normalize_symbol(_safe_str((row or {}).get("symbol") or (row or {}).get("ticker"))), provider=_safe_str((row or {}).get("data_provider") or (row or {}).get("provider")))
    flat = _flatten_scalar_fields(src)

    out: Dict[str, Any] = {}
    for idx, key in enumerate(keys or []):
        header = headers[idx] if idx < len(headers) else key
        aliases = [key, header, _norm_key(key), _norm_key(header), key.lower(), header.lower(), key.replace("_", " ")]
        aliases.extend(_CANONICAL_FIELD_ALIASES.get(key, ()))
        val = None
        found = False
        for alias in aliases:
            val = _lookup_alias_value(src, flat, alias)
            if not _is_blank_value(val):
                found = True
                break
        out[key] = _json_safe(_to_scalar(val)) if found else None
    return out


def _apply_page_row_backfill(sheet: str, row: Dict[str, Any]) -> Dict[str, Any]:
    target = _canonicalize_sheet_name(sheet)
    out = _apply_symbol_context_defaults(dict(row or {}), page=target)
    sym = normalize_symbol(_safe_str(out.get("symbol") or out.get("requested_symbol")))

    if out.get("invest_period_label") in (None, ""):
        out["invest_period_label"] = "1Y"
    if out.get("horizon_days") in (None, ""):
        out["horizon_days"] = 365

    if out.get("data_provider") in (None, ""):
        sources = out.get("data_sources")
        if isinstance(sources, list) and sources:
            out["data_provider"] = _safe_str(sources[0])

    conf = _as_float(out.get("confidence_score"))
    if conf is None:
        conf_fraction = _as_float(out.get("forecast_confidence"))
        if conf_fraction is not None:
            conf = conf_fraction * 100.0 if conf_fraction <= 1.5 else conf_fraction
            out.setdefault("confidence_score", round(_clamp(conf, 0.0, 100.0), 2))
    if conf is not None and out.get("confidence_bucket") in (None, ""):
        out["confidence_bucket"] = "HIGH" if conf >= 75 else "MODERATE" if conf >= 55 else "LOW"

    if target == "Commodities_FX" or sym.endswith("=F") or sym.endswith("=X"):
        out.setdefault("data_provider", _safe_str(out.get("data_provider"), "history_or_fallback"))
        if out.get("forecast_confidence") in (None, ""):
            out["forecast_confidence"] = 0.55
        if out.get("confidence_score") in (None, ""):
            out["confidence_score"] = 55.0
        if out.get("forecast_confidence") not in (None, "") and out.get("confidence_bucket") in (None, ""):
            conf = _as_float(out.get("confidence_score")) or ((_as_float(out.get("forecast_confidence")) or 0.55) * 100.0)
            out["confidence_bucket"] = "HIGH" if conf >= 75 else "MODERATE" if conf >= 55 else "LOW"
        if out.get("warnings") in (None, "") and _as_float(out.get("current_price")) is None:
            out["warnings"] = "Live quote sparse; chart/history fallback unavailable"

    if target == "Mutual_Funds":
        if out.get("asset_class") in (None, ""):
            out["asset_class"] = "Fund"
        if out.get("sector") in (None, ""):
            out["sector"] = "Diversified"
        if out.get("industry") in (None, ""):
            out["industry"] = "Mutual Funds"
        if out.get("country") in (None, ""):
            out["country"] = _infer_country_from_symbol(sym)
        if out.get("exchange") in (None, ""):
            out["exchange"] = _infer_exchange_from_symbol(sym)
        if out.get("currency") in (None, ""):
            out["currency"] = _infer_currency_from_symbol(sym)
        if out.get("invest_period_label") in (None, ""):
            out["invest_period_label"] = "1Y"
        if out.get("horizon_days") in (None, ""):
            out["horizon_days"] = 365

    if target in {"Global_Markets", "Market_Leaders", "My_Portfolio", "Top_10_Investments"}:
        asset_class = _safe_str(out.get("asset_class"))
        if sym in _ETF_SYMBOL_HINTS or asset_class.upper() == "ETF":
            out.setdefault("asset_class", "ETF")
            out.setdefault("sector", "Broad Market")
            out.setdefault("industry", "ETF")
            inferred_name = _infer_display_name_from_symbol(sym)
            if inferred_name and (_safe_str(out.get("name")) in ("", sym)):
                out["name"] = inferred_name

    return out


def _strict_project_row(keys: Sequence[str], row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: _json_safe(row.get(k)) for k in keys}


def _strict_project_row_display(headers: Sequence[str], keys: Sequence[str], row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for idx, key in enumerate(keys or []):
        header = headers[idx] if idx < len(headers or []) else key
        out[header] = _json_safe(row.get(key))
    return out


def _rows_display_objects_from_rows(rows: List[Dict[str, Any]], headers: List[str], keys: List[str]) -> List[Dict[str, Any]]:
    return [_strict_project_row_display(headers, keys, row) for row in (rows or [])]


def _merge_missing_fields(base_row: Dict[str, Any], template_row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out = dict(base_row or {})
    if not isinstance(template_row, dict):
        return out
    for k, v in template_row.items():
        if out.get(k) in (None, "", [], {}) and v not in (None, "", [], {}):
            out[k] = _json_safe(v)
    return out


def _rows_matrix_from_rows(rows: List[Dict[str, Any]], keys: List[str]) -> List[List[Any]]:
    return [[_json_safe(row.get(k)) for k in keys] for row in rows or []]


def _compute_scores_fallback(row: Dict[str, Any]) -> None:
    price = _as_float(row.get("current_price")) or _as_float(row.get("price"))
    pe = _as_float(row.get("pe_ttm"))
    pb = _as_float(row.get("pb_ratio"))
    ps = _as_float(row.get("ps_ratio"))
    ev_ebitda = _as_float(row.get("ev_ebitda"))
    intrinsic = _as_float(row.get("intrinsic_value"))
    beta = _as_float(row.get("beta_5y"))
    debt_to_equity = _as_float(row.get("debt_to_equity"))

    div_yield_pct = _as_pct_points(row.get("dividend_yield")) or 0.0
    gross_margin_pct = _as_pct_points(row.get("gross_margin")) or 0.0
    operating_margin_pct = _as_pct_points(row.get("operating_margin")) or 0.0
    profit_margin_pct = _as_pct_points(row.get("profit_margin")) or 0.0
    revenue_growth_pct = _as_pct_points(row.get("revenue_growth_yoy")) or 0.0

    seed_roi_1m = _as_pct_points(row.get("expected_roi_1m"))
    seed_roi_3m = _as_pct_points(row.get("expected_roi_3m"))
    seed_roi_12m = _as_pct_points(row.get("expected_roi_12m"))
    seed_best_roi = next((v for v in (seed_roi_3m, seed_roi_12m, seed_roi_1m) if v is not None), 0.0)

    if row.get("value_score") is None:
        value_score = 55.0
        if pe is not None and pe > 0:
            value_score += max(0.0, 22.0 - min(pe, 22.0))
        if pb is not None and pb > 0:
            value_score += max(0.0, 12.0 - min(pb * 3.0, 12.0))
        if ps is not None and ps > 0:
            value_score += max(0.0, 10.0 - min(ps * 2.0, 10.0))
        value_score += min(max(div_yield_pct, 0.0), 12.0)
        row["value_score"] = round(_clamp(float(value_score), 0.0, 100.0), 2)

    if row.get("valuation_score") is None:
        valuation_score = 50.0
        if intrinsic is not None and price not in (None, 0):
            upside_pct = ((intrinsic - price) / price) * 100.0
            valuation_score += _clamp(upside_pct, -20.0, 25.0)
        if ev_ebitda is not None and ev_ebitda > 0:
            valuation_score += max(0.0, 12.0 - min(ev_ebitda, 12.0))
        if pe is not None and pe > 0:
            valuation_score += max(0.0, 15.0 - min(pe, 15.0))
        row["valuation_score"] = round(_clamp(float(valuation_score), 0.0, 100.0), 2)

    if row.get("quality_score") is None:
        quality_score = 45.0
        quality_score += min(max(gross_margin_pct, 0.0), 20.0) * 0.6
        quality_score += min(max(operating_margin_pct, 0.0), 18.0) * 0.7
        quality_score += min(max(profit_margin_pct, 0.0), 15.0) * 0.7
        if debt_to_equity is not None:
            quality_score += max(0.0, 15.0 - min(max(debt_to_equity, 0.0), 15.0))
        row["quality_score"] = round(_clamp(float(quality_score), 0.0, 100.0), 2)

    if row.get("momentum_score") is None:
        # FIX v5.48.0/v5.49.0: percent_change is pct-points after _canonicalize_provider_row.
        # Using _as_pct_points doubled values <= 1.5 (e.g. 1.42 → 142) → score = 100 for all.
        pct = _as_float(row.get("percent_change")) or _as_float(row.get("change_cpt")) or _as_float(row.get("change_pct")) or 0.0
        row["momentum_score"] = round(_clamp(50.0 + pct, 0.0, 100.0), 2)

    if row.get("growth_score") is None:
        growth_score = 50.0 + _clamp(revenue_growth_pct, -25.0, 35.0)
        eps = _as_float(row.get("eps_ttm"))
        if eps is not None and eps > 0:
            growth_score += 3.0
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

    if row.get("risk_score") is None:
        vol = _as_pct_points(row.get("volatility_90d"))
        drawdown = _as_pct_points(row.get("max_drawdown_1y"))
        var95 = _as_pct_points(row.get("var_95_1d"))
        risk_score = 30.0
        if vol is not None:
            risk_score += min(max(vol, 0.0), 35.0)
        if drawdown is not None:
            risk_score += min(max(drawdown, 0.0), 20.0) * 0.6
        if var95 is not None:
            risk_score += min(max(var95, 0.0), 12.0)
        if beta is not None:
            risk_score += min(max(beta * 8.0, 0.0), 15.0)
        row["risk_score"] = round(_clamp(float(risk_score), 0.0, 100.0), 2)

    if row.get("overall_score") is None:
        vals = [
            _as_float(row.get("value_score")),
            _as_float(row.get("valuation_score")),
            _as_float(row.get("quality_score")),
            _as_float(row.get("momentum_score")),
            _as_float(row.get("growth_score")),
        ]
        vals2 = [v for v in vals if v is not None]
        overall = sum(vals2) / len(vals2) if vals2 else 50.0
        row["overall_score"] = round(_clamp(float(overall), 0.0, 100.0), 2)

    if price is not None and row.get("forecast_price_1m") is None:
        drift = max(0.5, min(4.0, seed_best_roi if seed_best_roi else 1.0))
        row["forecast_price_1m"] = round(price * (1.0 + drift / 300.0), 4)
    if price is not None and row.get("forecast_price_3m") is None:
        drift = max(1.0, min(8.0, seed_best_roi if seed_best_roi else 3.0))
        row["forecast_price_3m"] = round(price * (1.0 + drift / 100.0), 4)
    if price is not None and row.get("forecast_price_12m") is None:
        drift = max(3.0, min(18.0, (seed_roi_12m if seed_roi_12m is not None else seed_best_roi) or 8.0))
        row["forecast_price_12m"] = round(price * (1.0 + drift / 100.0), 4)

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

    final_roi_1m = _as_pct_points(row.get("expected_roi_1m"))
    final_roi_3m = _as_pct_points(row.get("expected_roi_3m"))
    final_roi_12m = _as_pct_points(row.get("expected_roi_12m"))
    final_best_roi = next((v for v in (final_roi_3m, final_roi_12m, final_roi_1m) if v is not None), 0.0)

    if row.get("opportunity_score") is None:
        base = _as_float(row.get("overall_score")) or 50.0
        confidence_boost = ((_as_float(row.get("confidence_score")) or 50.0) - 50.0) * 0.20
        risk_penalty = ((_as_float(row.get("risk_score")) or 50.0) - 50.0) * 0.25
        roi_boost = _clamp(final_best_roi, -25.0, 35.0) * 0.35
        row["opportunity_score"] = round(_clamp(base + confidence_boost + roi_boost - risk_penalty, 0.0, 100.0), 2)

    if not row.get("risk_bucket"):
        rs = _as_float(row.get("risk_score")) or 50.0
        row["risk_bucket"] = "LOW" if rs < 40 else "MODERATE" if rs < 70 else "HIGH"

    if not row.get("confidence_bucket"):
        cs = _as_float(row.get("confidence_score")) or 55.0
        row["confidence_bucket"] = "HIGH" if cs >= 75 else "MODERATE" if cs >= 55 else "LOW"


def _compute_recommendation(row: Dict[str, Any]) -> None:
    _CANONICAL_RECO_MAP: Dict[str, str] = {
        "ACCUMULATE": "BUY", "ADD": "BUY", "OUTPERFORM": "BUY", "OVERWEIGHT": "BUY",
        "STRONG BUY": "STRONG_BUY", "STRONGBUY": "STRONG_BUY", "STRONG_BUY": "STRONG_BUY",
        "BUY": "BUY", "HOLD": "HOLD", "NEUTRAL": "HOLD", "MARKET PERFORM": "HOLD",
        "REDUCE": "REDUCE", "UNDERPERFORM": "REDUCE", "UNDERWEIGHT": "REDUCE", "AVOID": "REDUCE",
        "SELL": "SELL", "STRONG SELL": "SELL", "STRONG_SELL": "SELL",
    }
    if row.get("recommendation"):
        return
    overall = _as_float(row.get("overall_score")) or 50.0
    conf = _as_float(row.get("confidence_score")) or 55.0
    risk = _as_float(row.get("risk_score")) or 50.0
    if overall >= 75 and conf >= 65 and risk <= 60:
        rec = "BUY"
    elif overall >= 60 and conf >= 55:
        rec = "BUY"          # FIX v5.48.0/v5.49.0: ACCUMULATE was non-canonical, now BUY
    elif overall <= 35 or risk >= 85:
        rec = "REDUCE"
    else:
        rec = "HOLD"
    row["recommendation"] = rec
    row.setdefault(
        "recommendation_reason",
        f"overall={round(overall,1)} confidence={round(conf,1)} risk={round(risk,1)}",
    )


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
    for key, label in (
        ("overall_score", "overall"),
        ("opportunity_score", "opportunity"),
        ("confidence_score", "confidence"),
        ("risk_score", "risk"),
    ):
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


async def _call_maybe_async(fn: Any, *args: Any, **kwargs: Any) -> Any:
    if inspect.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)

    result = await asyncio.to_thread(fn, *args, **kwargs)
    return await result if inspect.isawaitable(result) else result


# =============================================================================
# Schema registry helpers
# =============================================================================
# FIX v5.49.0: multi-path import, same pattern as data_dictionary.py v3.3.0
_RAW_SCHEMA_REGISTRY: dict = {}
_RAW_GET_SHEET_SPEC = None
_SCHEMA_AVAILABLE = False
for _sreg_path in ("core.sheets.schema_registry", "core.schema_registry", "schema_registry"):
    try:
        _sreg_mod = import_module(_sreg_path)
        _cand_reg = getattr(_sreg_mod, "SCHEMA_REGISTRY", None)
        _cand_fn  = getattr(_sreg_mod, "get_sheet_spec", None)
        if isinstance(_cand_reg, dict) or callable(_cand_fn):
            _RAW_SCHEMA_REGISTRY = _cand_reg if isinstance(_cand_reg, dict) else {}
            _RAW_GET_SHEET_SPEC   = _cand_fn if callable(_cand_fn) else None
            _SCHEMA_AVAILABLE     = True
            break
    except Exception:
        continue
del _sreg_path

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
    static_contract = STATIC_CANONICAL_SHEET_CONTRACTS.get(canon)
    if static_contract:
        return dict(static_contract)
    raise KeyError(f"Unknown sheet spec: {sheet}")


def _schema_for_sheet(sheet: str) -> Tuple[Any, List[str], List[str], str]:
    canon = _canonicalize_sheet_name(sheet)

    try:
        spec = get_sheet_spec(canon)
        h, k = _schema_keys_headers_from_spec(spec)
        if canon == "Top_10_Investments":
            h, k = _ensure_top10_contract(h, k)
        if _usable_contract(h, k, canon):
            return spec, h, k, "schema_registry"
    except Exception:
        pass

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
# Async utilities
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
    def __init__(self, name: str, l1_ttl: int = 60, max_l1_size: int = 5000) -> None:
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
        stats = type(
            "ProviderStats",
            (),
            {"is_circuit_open": False, "last_import_error": "" if module is not None else "provider module missing"},
        )()
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
    for name in ("get_quote", "fetch_quote", "fetch_enriched_quote", "get_enriched_quote", "quote"):
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
    def __init__(self, settings: Any = None) -> None:
        self.settings = settings if settings is not None else _try_get_settings()
        self.flags = _feature_flags(self.settings)
        self.version = __version__

        self.primary_provider = (
            _safe_str(getattr(self.settings, "primary_provider", "eodhd") if self.settings is not None else _safe_env("PRIMARY_PROVIDER", "eodhd")).lower()
            or "eodhd"
        )
        self.enabled_providers = _get_env_list("ENABLED_PROVIDERS", DEFAULT_PROVIDERS)
        self.ksa_providers = _get_env_list("KSA_PROVIDERS", DEFAULT_KSA_PROVIDERS)
        self.global_providers = _get_env_list("GLOBAL_PROVIDERS", DEFAULT_GLOBAL_PROVIDERS)
        self.non_ksa_primary_provider = (
            _safe_str(
                getattr(self.settings, "non_ksa_primary_provider", None) if self.settings is not None else None,
                _safe_env("NON_KSA_PRIMARY_PROVIDER", "eodhd"),
            ).lower()
            or "eodhd"
        )
        configured_non_ksa_pages = [
            _canonicalize_sheet_name(p)
            for p in _get_env_list("NON_KSA_PRIMARY_PAGES", list(NON_KSA_EODHD_PRIMARY_PAGES))
            if _safe_str(p)
        ]
        self.page_primary_providers = {
            page: self.non_ksa_primary_provider
            for page in configured_non_ksa_pages
            if page
        }
        self.history_fallback_providers = _get_env_list(
            "HISTORY_FALLBACK_PROVIDERS",
            ["yahoo_chart", "yahoo", "eodhd", "finnhub", "tadawul", "argaam"],
        )
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
            max_l1_size=_get_env_int("CACHE_L1_MAX", 5000),
        )
        self._symbols_cache = MultiLevelCache(
            name="sheet_symbols",
            l1_ttl=_get_env_int("SHEET_SYMBOLS_L1_TTL", 300),
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

    # ------------------------------------------------------------------
    # compatibility aliases
    # ------------------------------------------------------------------
    async def execute_sheet_rows(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    async def run_sheet_rows(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    async def build_analysis_sheet_rows(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    async def run_analysis_sheet_rows(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    async def get_rows_for_sheet(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    async def get_rows_for_page(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_page_rows(*args, **kwargs)

    async def get_analysis_rows_batch(self, symbols: List[str], mode: str = "", *, schema: Any = None) -> Dict[str, Dict[str, Any]]:
        return await self.get_enriched_quotes_batch(symbols, mode=mode, schema=schema)

    async def get_analysis_row_dict(self, symbol: str, use_cache: bool = True, *, schema: Any = None) -> Dict[str, Any]:
        return await self.get_enriched_quote_dict(symbol, use_cache=use_cache, schema=schema)

    def get_page_snapshot(self, *args: Any, **kwargs: Any) -> Optional[Dict[str, Any]]:
        return self.get_cached_sheet_snapshot(*args, **kwargs)

    # ------------------------------------------------------------------
    # snapshot helpers
    # ------------------------------------------------------------------
    def _store_sheet_snapshot(self, sheet: str, payload: Dict[str, Any]) -> None:
        target = _canonicalize_sheet_name(sheet)
        if not target or not isinstance(payload, dict):
            return
        self._sheet_snapshots[target] = dict(payload)

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
        return dict(snap) if isinstance(snap, dict) else None

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

    # ------------------------------------------------------------------
    # symbol resolution meta
    # ------------------------------------------------------------------
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

    def _get_cached_snapshot_symbol_map(self, sheet: str) -> Dict[str, Dict[str, Any]]:
        target = _canonicalize_sheet_name(sheet)
        snap = self.get_cached_sheet_snapshot(sheet=target)
        rows = _coerce_rows_list(snap)
        out: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            sym = normalize_symbol(self._extract_row_symbol(row))
            if sym and sym not in out:
                out[sym] = dict(row)
        return out

    @staticmethod
    def _non_empty_field_count(row: Optional[Dict[str, Any]]) -> int:
        if not isinstance(row, dict):
            return 0
        return sum(1 for v in row.values() if v not in (None, "", [], {}))

    def _get_best_cached_snapshot_row_for_symbol(self, symbol: str, prefer_sheet: str = "") -> Optional[Dict[str, Any]]:
        sym = normalize_symbol(symbol)
        if not sym:
            return None
        preferred = _canonicalize_sheet_name(prefer_sheet) if prefer_sheet else ""
        best_row: Optional[Dict[str, Any]] = None
        best_score: float = -1.0
        for sheet_name, snap in self._sheet_snapshots.items():
            rows = _coerce_rows_list(snap)
            if not rows:
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                row_sym = normalize_symbol(self._extract_row_symbol(row))
                if row_sym != sym:
                    continue
                score = float(self._non_empty_field_count(row))
                if preferred and _canonicalize_sheet_name(sheet_name) == preferred:
                    score += 1000.0
                if score > best_score:
                    best_score = score
                    best_row = dict(row)
        return best_row

    # ------------------------------------------------------------------
    # final payload
    # ------------------------------------------------------------------
    def _finalize_payload(
        self,
        *,
        sheet: str,
        headers: List[str],
        keys: List[str],
        row_objects: List[Dict[str, Any]],
        include_matrix: bool,
        status: str = "success",
        meta: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> Dict[str, Any]:
        headers, keys = _complete_schema_contract(headers, keys)
        dict_rows = [_strict_project_row(keys, r) for r in (row_objects or [])]
        display_row_objects = _rows_display_objects_from_rows(dict_rows, headers, keys)
        matrix_rows = _rows_matrix_from_rows(dict_rows, keys) if include_matrix else []

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
            "rows": matrix_rows,
            "rows_matrix": matrix_rows,
            "row_objects": dict_rows,
            "items": dict_rows,
            "records": dict_rows,
            "data": dict_rows,
            "quotes": dict_rows,
            "display_row_objects": display_row_objects,
            "display_items": display_row_objects,
            "display_records": display_row_objects,
            "rows_dict_display": display_row_objects,
            "count": len(dict_rows),
            "meta": dict(meta or {}),
            "version": self.version,
        }
        if error is not None:
            payload["error"] = error
        return _json_safe(payload)

    # ------------------------------------------------------------------
    # rows reader discovery
    # ------------------------------------------------------------------
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

                if any(
                    callable(getattr(mod, nm, None))
                    for nm in ("get_rows_for_sheet", "read_rows_for_sheet", "get_sheet_rows", "fetch_sheet_rows", "sheet_rows", "get_rows")
                ):
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
        obj, _src = await self._init_rows_reader()
        if obj is None:
            return []
        return await self._call_rows_reader(obj, sheet, limit)

    # ------------------------------------------------------------------
    # symbols reader discovery
    # ------------------------------------------------------------------
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

        for name in (
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
        ):
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

        for attr_name in (
            f"{sheet.lower()}_symbols",
            f"{sheet.lower()}_tickers",
            f"{sheet.lower()}_codes",
            "default_symbols",
            "page_symbols",
            "sheet_symbols",
        ):
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
                for args, kwargs in [
                    ((sheet,), {"limit": limit}),
                    ((sheet,), {}),
                    ((), {"page": sheet, "limit": limit}),
                    ((), {"sheet": sheet, "limit": limit}),
                ]:
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

    async def get_sheet_symbols(
        self,
        sheet: Optional[str] = None,
        *,
        sheet_name: Optional[str] = None,
        page: Optional[str] = None,
        limit: int = 5000,
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> List[str]:
        target_sheet, effective_limit, _offset, _mode, normalized_body, _request_parts = _normalize_route_call_inputs(
            page=page,
            sheet=sheet,
            sheet_name=sheet_name,
            limit=limit,
            offset=0,
            mode="",
            body=body,
            extras=kwargs,
        )
        return await self._get_symbols_for_sheet_impl(target_sheet, limit=effective_limit, body=normalized_body)

    async def get_page_symbols(
        self,
        page: Optional[str] = None,
        *,
        sheet: Optional[str] = None,
        sheet_name: Optional[str] = None,
        limit: int = 5000,
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> List[str]:
        target_sheet, effective_limit, _offset, _mode, normalized_body, _request_parts = _normalize_route_call_inputs(
            page=page,
            sheet=sheet,
            sheet_name=sheet_name,
            limit=limit,
            offset=0,
            mode="",
            body=body,
            extras=kwargs,
        )
        return await self._get_symbols_for_sheet_impl(target_sheet, limit=effective_limit, body=normalized_body)

    async def list_symbols_for_page(self, page: str, *, limit: int = 5000, body: Optional[Dict[str, Any]] = None, **kwargs: Any) -> List[str]:
        target_sheet, effective_limit, _offset, _mode, normalized_body, _request_parts = _normalize_route_call_inputs(
            page=page,
            limit=limit,
            offset=0,
            mode="",
            body=body,
            extras=kwargs,
        )
        return await self._get_symbols_for_sheet_impl(target_sheet, limit=effective_limit, body=normalized_body)

    async def list_symbols(
        self,
        sheet: Optional[str] = None,
        *,
        page: Optional[str] = None,
        sheet_name: Optional[str] = None,
        limit: int = 5000,
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> List[str]:
        target_sheet, effective_limit, _offset, _mode, normalized_body, _request_parts = _normalize_route_call_inputs(
            page=page,
            sheet=sheet,
            sheet_name=sheet_name,
            limit=limit,
            offset=0,
            mode="",
            body=body,
            extras=kwargs,
        )
        return await self._get_symbols_for_sheet_impl(target_sheet, limit=effective_limit, body=normalized_body)

    async def get_symbols(
        self,
        sheet: Optional[str] = None,
        *,
        page: Optional[str] = None,
        sheet_name: Optional[str] = None,
        limit: int = 5000,
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> List[str]:
        target_sheet, effective_limit, _offset, _mode, normalized_body, _request_parts = _normalize_route_call_inputs(
            page=page,
            sheet=sheet,
            sheet_name=sheet_name,
            limit=limit,
            offset=0,
            mode="",
            body=body,
            extras=kwargs,
        )
        return await self._get_symbols_for_sheet_impl(target_sheet, limit=effective_limit, body=normalized_body)

    # ------------------------------------------------------------------
    # quote context / provider preference helpers
    # ------------------------------------------------------------------
    def _resolve_quote_page_context(
        self,
        *,
        page: Optional[str] = None,
        sheet: Optional[str] = None,
        body: Optional[Dict[str, Any]] = None,
        schema: Any = None,
        extras: Optional[Dict[str, Any]] = None,
    ) -> str:
        merged = _merge_route_body_dicts(body, extras or {})
        target_raw = (
            page
            or sheet
            or _safe_str(merged.get("page"))
            or _safe_str(merged.get("sheet"))
            or _safe_str(merged.get("sheet_name"))
            or _safe_str(merged.get("page_name"))
            or _safe_str(merged.get("name"))
            or _safe_str(merged.get("tab"))
            or _safe_str(merged.get("worksheet"))
        )
        if not target_raw and isinstance(schema, str):
            target_raw = schema
        return _canonicalize_sheet_name(target_raw) if target_raw else ""

    def _page_primary_provider_for(self, symbol: str, page: str = "") -> str:
        info = get_symbol_info(symbol)
        is_ksa_sym = bool(info.get("is_ksa"))
        page_ctx = _canonicalize_sheet_name(page) if page else ""
        if (
            not is_ksa_sym
            and page_ctx
            and page_ctx in self.page_primary_providers
        ):
            candidate = _safe_str(self.page_primary_providers.get(page_ctx), self.non_ksa_primary_provider).lower()
            if candidate:
                return candidate
        return self.primary_provider

    def _provider_profile_key(self, symbol: str, page: str = "") -> str:
        info = get_symbol_info(symbol)
        page_ctx = _canonicalize_sheet_name(page) if page else ""
        primary = self._page_primary_provider_for(symbol, page_ctx)
        market = "ksa" if bool(info.get("is_ksa")) else "global"
        return f"{market}|{page_ctx or 'default'}|{primary or 'none'}"

    # ------------------------------------------------------------------
    # quote APIs
    # ------------------------------------------------------------------
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

    def _providers_for(self, symbol: str, page: str = "") -> List[str]:
        info = get_symbol_info(symbol)
        is_ksa_sym = bool(info.get("is_ksa"))
        page_ctx = _canonicalize_sheet_name(page) if page else ""

        def _provider_allowed(provider: str) -> bool:
            provider = _safe_str(provider).lower()
            if not provider or provider not in self.enabled_providers:
                return False
            if is_ksa_sym and self.ksa_disallow_eodhd and provider == "eodhd":
                return False
            return True

        providers = [p for p in (self.ksa_providers if is_ksa_sym else self.global_providers) if _provider_allowed(p)]

        primary_provider = self._page_primary_provider_for(symbol, page_ctx)
        if primary_provider and _provider_allowed(primary_provider):
            if primary_provider in providers:
                providers = [p for p in providers if p != primary_provider]
            providers.insert(0, primary_provider)

        # Preserve paid EODHD priority for configured non-KSA/global pages even
        # when settings or env put another global provider first.
        if (not is_ksa_sym) and page_ctx and page_ctx in self.page_primary_providers and _provider_allowed("eodhd"):
            providers = [p for p in providers if p != "eodhd"]
            providers.insert(0, "eodhd")

        seen: Set[str] = set()
        out: List[str] = []
        for p in providers:
            p2 = _safe_str(p).lower()
            if not p2 or p2 in seen:
                continue
            seen.add(p2)
            out.append(p2)
        return out

    def _rows_from_parallel_series(
        self,
        timestamps: Sequence[Any],
        opens: Optional[Sequence[Any]] = None,
        highs: Optional[Sequence[Any]] = None,
        lows: Optional[Sequence[Any]] = None,
        closes: Optional[Sequence[Any]] = None,
        volumes: Optional[Sequence[Any]] = None,
        adjcloses: Optional[Sequence[Any]] = None,
    ) -> List[Dict[str, Any]]:
        ts_list = list(timestamps or [])
        if not ts_list:
            return []

        rows: List[Dict[str, Any]] = []
        for idx, ts in enumerate(ts_list):
            row = {
                "timestamp": ts,
                "open": opens[idx] if opens is not None and idx < len(opens) else None,
                "high": highs[idx] if highs is not None and idx < len(highs) else None,
                "low": lows[idx] if lows is not None and idx < len(lows) else None,
                "close": closes[idx] if closes is not None and idx < len(closes) else None,
                "volume": volumes[idx] if volumes is not None and idx < len(volumes) else None,
                "adjclose": adjcloses[idx] if adjcloses is not None and idx < len(adjcloses) else None,
            }
            if any(v is not None for k, v in row.items() if k != "timestamp"):
                rows.append(row)
        return rows

    def _coerce_history_rows(self, result: Any) -> List[Dict[str, Any]]:
        if result is None:
            return []
        if hasattr(result, "to_dict") and callable(getattr(result, "to_dict")):
            try:
                rows = result.to_dict("records")
                if isinstance(rows, list):
                    return [dict(r) for r in rows if isinstance(r, dict)]
            except Exception:
                pass
        if isinstance(result, list):
            out: List[Dict[str, Any]] = []
            for item in result:
                if isinstance(item, dict):
                    if {"open", "high", "low", "close"} & set(item.keys()) and not isinstance(item.get("open"), list):
                        out.append(dict(item))
                    else:
                        nested = self._coerce_history_rows(item)
                        if nested:
                            out.extend(nested)
                        else:
                            out.append(dict(item))
                elif isinstance(item, (list, tuple)) and len(item) >= 5:
                    out.append({
                        "timestamp": item[0],
                        "open": item[1],
                        "high": item[2],
                        "low": item[3],
                        "close": item[4],
                        "volume": item[5] if len(item) > 5 else None,
                    })
            return out
        if isinstance(result, dict):
            # Yahoo chart-style payloads
            if isinstance(result.get("chart"), Mapping):
                chart = result.get("chart") or {}
                nested = self._coerce_history_rows(chart.get("result"))
                if nested:
                    return nested

            if isinstance(result.get("result"), list):
                for item in result.get("result") or []:
                    nested = self._coerce_history_rows(item)
                    if nested:
                        return nested

            timestamps = result.get("timestamp") or result.get("timestamps") or result.get("time")
            if isinstance(timestamps, list) and timestamps:
                indicators = result.get("indicators") if isinstance(result.get("indicators"), Mapping) else {}
                quote = None
                if isinstance(indicators.get("quote"), list) and indicators.get("quote"):
                    quote = indicators.get("quote")[0]
                adj = None
                if isinstance(indicators.get("adjclose"), list) and indicators.get("adjclose"):
                    adj = indicators.get("adjclose")[0]
                if isinstance(quote, Mapping):
                    rows = self._rows_from_parallel_series(
                        timestamps=timestamps,
                        opens=list(quote.get("open") or []),
                        highs=list(quote.get("high") or []),
                        lows=list(quote.get("low") or []),
                        closes=list(quote.get("close") or []),
                        volumes=list(quote.get("volume") or []),
                        adjcloses=list(adj.get("adjclose") or []) if isinstance(adj, Mapping) else None,
                    )
                    if rows:
                        return rows

            # Generic parallel-array payloads
            if any(isinstance(result.get(k), list) for k in ("close", "open", "high", "low", "volume")):
                ts = result.get("timestamp") or list(range(len(result.get("close") or result.get("price") or [])))
                rows = self._rows_from_parallel_series(
                    timestamps=list(ts or []),
                    opens=list(result.get("open") or []),
                    highs=list(result.get("high") or []),
                    lows=list(result.get("low") or []),
                    closes=list(result.get("close") or result.get("adjclose") or result.get("price") or result.get("value") or []),
                    volumes=list(result.get("volume") or []),
                    adjcloses=list(result.get("adjclose") or []),
                )
                if rows:
                    return rows

            # AlphaVantage-style keyed time series
            for key in ("Time Series (Daily)", "time_series", "series"):
                series = result.get(key)
                if isinstance(series, Mapping):
                    rows: List[Dict[str, Any]] = []
                    for ts, entry in series.items():
                        if not isinstance(entry, Mapping):
                            continue
                        rows.append({
                            "timestamp": ts,
                            "open": entry.get("1. open") or entry.get("open"),
                            "high": entry.get("2. high") or entry.get("high"),
                            "low": entry.get("3. low") or entry.get("low"),
                            "close": entry.get("4. close") or entry.get("close"),
                            "volume": entry.get("5. volume") or entry.get("volume"),
                        })
                    if rows:
                        rows.sort(key=lambda r: _safe_str(r.get("timestamp")))
                        return rows

            for key in ("history", "bars", "candles", "prices", "data", "items", "rows", "chart"):
                if key in result:
                    nested = self._coerce_history_rows(result.get(key))
                    if nested:
                        return nested
            if {"open", "high", "low", "close"} & set(result.keys()):
                return [dict(result)]
        return []

    def _safe_mean(self, values: List[float]) -> float:
        return sum(values) / len(values) if values else 0.0

    def _safe_std(self, values: List[float]) -> float:
        if len(values) < 2:
            return 0.0
        mean = self._safe_mean(values)
        var = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
        return math.sqrt(max(var, 0.0))

    def _quantile(self, values: List[float], q: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        pos = max(0.0, min(1.0, q)) * (len(ordered) - 1)
        lo = int(math.floor(pos))
        hi = int(math.ceil(pos))
        if lo == hi:
            return ordered[lo]
        frac = pos - lo
        return ordered[lo] + (ordered[hi] - ordered[lo]) * frac

    def _compute_history_patch_from_rows(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        closes: List[float] = []
        highs: List[float] = []
        lows: List[float] = []
        volumes: List[float] = []
        opens: List[float] = []
        for row in rows or []:
            close = _as_float(row.get("close") or row.get("adjclose") or row.get("price") or row.get("value"))
            high = _as_float(row.get("high") or row.get("day_high"))
            low = _as_float(row.get("low") or row.get("day_low"))
            vol = _as_float(row.get("volume"))
            opn = _as_float(row.get("open") or row.get("open_price"))
            if close is not None:
                closes.append(close)
            if high is not None:
                highs.append(high)
            if low is not None:
                lows.append(low)
            if vol is not None:
                volumes.append(vol)
            if opn is not None:
                opens.append(opn)
        if len(closes) < 2:
            return {}
        returns = []
        for prev, cur in zip(closes[:-1], closes[1:]):
            if prev not in (None, 0):
                returns.append((cur / prev) - 1.0)
        if not returns:
            return {}
        recent14 = closes[-15:]
        gains: List[float] = []
        losses: List[float] = []
        for prev, cur in zip(recent14[:-1], recent14[1:]):
            delta = cur - prev
            if delta >= 0:
                gains.append(delta)
                losses.append(0.0)
            else:
                gains.append(0.0)
                losses.append(abs(delta))
        avg_gain = self._safe_mean(gains)
        avg_loss = self._safe_mean(losses)
        rsi = None
        if gains and losses:
            if avg_loss == 0:
                rsi = 100.0
            else:
                rs = avg_gain / avg_loss
                rsi = 100.0 - (100.0 / (1.0 + rs))
        last30 = returns[-30:] if len(returns) >= 30 else returns
        last90 = returns[-90:] if len(returns) >= 90 else returns
        vol30 = self._safe_std(last30) * math.sqrt(252.0) if last30 else None
        vol90 = self._safe_std(last90) * math.sqrt(252.0) if last90 else None
        mean_daily = self._safe_mean(last90)
        std_daily = self._safe_std(last90)
        sharpe = (mean_daily / std_daily) * math.sqrt(252.0) if std_daily not in (None, 0.0) else None
        var95 = abs(self._quantile(last90, 0.05)) if last90 else None
        peak = closes[0]
        max_dd = 0.0
        for price in closes:
            peak = max(peak, price)
            if peak > 0:
                dd = (price / peak) - 1.0
                max_dd = min(max_dd, dd)
        patch: Dict[str, Any] = {
            "current_price": closes[-1],
            "previous_close": closes[-2],
            "open_price": opens[-1] if opens else None,
            "day_high": highs[-1] if highs else None,
            "day_low": lows[-1] if lows else None,
            "week_52_high": max(highs) if highs else max(closes),
            "week_52_low": min(lows) if lows else min(closes),
            "avg_volume_10d": self._safe_mean(volumes[-10:]) if volumes else None,
            "avg_volume_30d": self._safe_mean(volumes[-30:]) if volumes else None,
            "volatility_30d": vol30,
            "volatility_90d": vol90,
            "max_drawdown_1y": abs(max_dd) * 100.0,
            "var_95_1d": var95 * 100.0 if var95 is not None else None,
            "sharpe_1y": sharpe,
            "rsi_14": rsi,
            "price_change": closes[-1] - closes[-2],
            # FIX v5.48.0/v5.49.0: store as FRACTION (dtype=pct schema contract).
            # _canonicalize_provider_row will convert to percent-points.
            "percent_change": ((closes[-1] - closes[-2]) / closes[-2]) if closes[-2] not in (None, 0) else None,
            "volume": volumes[-1] if volumes else None,
        }
        if patch.get("current_price") is not None and patch.get("week_52_high") is not None and patch.get("week_52_low") is not None:
            hi = _as_float(patch.get("week_52_high"))
            lo = _as_float(patch.get("week_52_low"))
            cp = _as_float(patch.get("current_price"))
            if hi is not None and lo is not None and cp is not None and hi > lo:
                # FIX v5.48.0/v5.49.0: store as fraction (0-1). _canonicalize_provider_row converts to pct-points.
                patch["week_52_position_pct"] = (cp - lo) / (hi - lo)
        return {k: v for k, v in patch.items() if v is not None}

    async def _fetch_history_patch(self, provider: str, symbol: str) -> Dict[str, Any]:
        module, _stats = await self._registry.get_provider(provider)
        if module is None:
            return {}
        callables = []
        for name in (
            "get_history",
            "fetch_history",
            "get_price_history",
            "fetch_price_history",
            "history",
            "get_chart",
            "fetch_chart",
            "get_chart_history",
            "fetch_chart_history",
            "get_historical_data",
            "fetch_historical_data",
            "get_history_rows",
            "fetch_history_rows",
            "get_timeseries",
            "fetch_timeseries",
            "get_series",
            "fetch_series",
            "get_ohlcv",
            "fetch_ohlcv",
        ):
            fn = getattr(module, name, None)
            if callable(fn):
                callables.append(fn)
        if not callables:
            return {}
        variants = [
            ((symbol,), {"period": "1y", "interval": "1d"}),
            ((symbol,), {"range": "1y", "interval": "1d"}),
            ((symbol,), {"lookback": "1y", "interval": "1d"}),
            ((), {"symbol": symbol, "period": "1y", "interval": "1d"}),
            ((), {"ticker": symbol, "period": "1y", "interval": "1d"}),
            ((), {"code": symbol, "period": "1y", "interval": "1d"}),
            ((), {"symbol": symbol, "range": "1y", "interval": "1d"}),
            ((), {"ticker": symbol, "range": "1y", "interval": "1d"}),
            ((symbol,), {}),
        ]
        for fn in callables:
            for args, kwargs in variants:
                try:
                    async with asyncio.timeout(max(5.0, self.request_timeout)):
                        result = await _call_maybe_async(fn, *args, **kwargs)
                    rows = self._coerce_history_rows(result)
                    patch = self._compute_history_patch_from_rows(rows)
                    if patch:
                        patch["data_provider"] = provider
                        return patch
                except TypeError:
                    continue
                except Exception:
                    continue
        return {}

    async def _get_history_patch_best_effort(self, symbol: str, providers: Sequence[str], page: str = "") -> Dict[str, Any]:
        candidates: List[str] = []
        for provider in list(providers or []):
            if provider and provider not in candidates:
                candidates.append(provider)

        page_ctx = _canonicalize_sheet_name(page) if page else ""
        preferred_history = list(self.history_fallback_providers or [])
        primary_provider = self._page_primary_provider_for(symbol, page_ctx)

        if (not get_symbol_info(symbol).get("is_ksa")) and page_ctx and page_ctx in self.page_primary_providers:
            preferred_history = [primary_provider, "yahoo_chart", "yahoo", "eodhd", "finnhub"] + preferred_history
        elif symbol.endswith("=F") or symbol.endswith("=X"):
            preferred_history = ["yahoo_chart", "yahoo", "eodhd", "finnhub"] + preferred_history

        for provider in preferred_history:
            provider = _safe_str(provider).lower()
            if provider and provider not in candidates and (provider in self.enabled_providers or provider in preferred_history):
                candidates.append(provider)

        for provider in candidates:
            patch = await self._fetch_history_patch(provider, symbol)
            if patch:
                return patch
        return {}

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
        normalized_patches: List[Tuple[str, Dict[str, Any], float]] = []
        for prov, patch, latency in patches:
            canonical = _canonicalize_provider_row(patch, requested_symbol=requested_symbol, normalized_symbol=norm, provider=prov)
            normalized_patches.append((prov, canonical, latency))

        sorted_patches = sorted(
            normalized_patches,
            key=lambda item: (
                PROVIDER_PRIORITIES.get(item[0], 999),
                -sum(1 for v in item[1].values() if v not in (None, "", [], {})),
            ),
        )

        for prov, patch, latency in sorted_patches:
            merged["data_sources"].append(prov)
            merged["provider_latency"][prov] = round(float(latency or 0.0), 2)
            for k, v in patch.items():
                if k in protected or v is None:
                    continue
                if k not in merged or merged.get(k) in (None, "", [], {}):
                    merged[k] = v

        merged = _canonicalize_provider_row(merged, requested_symbol=requested_symbol, normalized_symbol=norm, provider=_safe_str((merged.get("data_sources") or [""])[0] if isinstance(merged.get("data_sources"), list) else ""))
        return merged


    def _data_quality(self, row: Dict[str, Any]) -> str:
        if _as_float(row.get("current_price")) is None:
            return QuoteQuality.MISSING.value
        return QuoteQuality.GOOD.value if any(row.get(k) is not None for k in ("overall_score", "forecast_price_3m", "pb_ratio")) else QuoteQuality.FAIR.value

    async def _get_enriched_quote_impl(self, symbol: str, use_cache: bool = True, *, page: str = "", sheet: str = "", body: Optional[Dict[str, Any]] = None, **kwargs: Any) -> UnifiedQuote:
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
            row = _apply_symbol_context_defaults(row, symbol=_safe_str(symbol))
            _compute_scores_fallback(row)
            _compute_recommendation(row)
            return UnifiedQuote(**row)

        page_context = self._resolve_quote_page_context(page=page, sheet=sheet, body=body, extras=kwargs)
        provider_profile = self._provider_profile_key(norm, page_context)

        if use_cache:
            cached = await self._cache.get(symbol=norm, provider_profile=provider_profile)
            if isinstance(cached, dict) and cached:
                return UnifiedQuote(**cached)

        providers = self._providers_for(norm, page=page_context)
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
                "name": _infer_display_name_from_symbol(norm) or norm,
                "current_price": None,
                "data_sources": [],
                "provider_latency": {},
                "warnings": "No live provider data available",
                "last_updated_utc": _now_utc_iso(),
                "last_updated_riyadh": _now_riyadh_iso(),
            }

        row = _apply_symbol_context_defaults(row, symbol=norm)

        cached_best_row = self._get_best_cached_snapshot_row_for_symbol(norm, prefer_sheet=page_context)
        if cached_best_row:
            row = _merge_missing_fields(row, cached_best_row)
            row = _apply_page_row_backfill(page_context or sheet or "", row)

        missing_history_fields = [
            "current_price",
            "previous_close",
            "day_high",
            "day_low",
            "week_52_high",
            "week_52_low",
            "avg_volume_10d",
            "avg_volume_30d",
            "volatility_30d",
            "volatility_90d",
            "max_drawdown_1y",
            "var_95_1d",
            "sharpe_1y",
            "rsi_14",
        ]
        if any(row.get(k) in (None, "", [], {}) for k in missing_history_fields):
            hist_patch = await self._get_history_patch_best_effort(norm, providers, page=page_context)
            if hist_patch:
                row = self._merge(symbol, norm, patches_ok + [(hist_patch.get("data_provider") or "history", hist_patch, 0.0)])
                row = _apply_symbol_context_defaults(row, symbol=norm)

        if cached_best_row:
            row = _merge_missing_fields(row, cached_best_row)
            row = _apply_page_row_backfill(page_context or sheet or "", row)

        if _as_float(row.get("current_price")) is not None and _safe_str(row.get("warnings")).lower() == "no live provider data available":
            row["warnings"] = "Recovered from history/chart fallback"
        elif _as_float(row.get("current_price")) is None and not row.get("warnings"):
            row["warnings"] = "No live quote payload and no usable history fallback"

        _compute_scores_fallback(row)
        _compute_recommendation(row)
        row["data_quality"] = self._data_quality(row)
        row["data_provider"] = row.get("data_provider") or ((row.get("data_sources") or [""])[0] if isinstance(row.get("data_sources"), list) else "")
        q = UnifiedQuote(**row)

        if use_cache:
            await self._cache.set(_model_to_dict(q), symbol=norm, provider_profile=provider_profile)

        return q

    async def get_enriched_quote(
        self,
        symbol: str,
        use_cache: bool = True,
        *,
        schema: Any = None,
        page: str = "",
        sheet: str = "",
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> UnifiedQuote:
        page_context = self._resolve_quote_page_context(page=page, sheet=sheet, body=body, schema=schema, extras=kwargs)
        provider_profile = self._provider_profile_key(normalize_symbol(symbol), page_context)
        key = f"quote:{normalize_symbol(symbol)}:{provider_profile}:{'cache' if use_cache else 'live'}"
        raw_q = await self._singleflight.execute(
            key,
            lambda: self._get_enriched_quote_impl(symbol, use_cache, page=page_context, body=body, schema=schema, **kwargs),
        )
        if schema is None:
            return raw_q
        row = _model_to_dict(raw_q)
        if isinstance(schema, str):
            _spec, hdrs, keys, _src = _schema_for_sheet(_safe_str(schema))
            projected = _normalize_to_schema_keys(keys, hdrs, row)
            return UnifiedQuote(**projected)
        return raw_q

    async def get_enriched_quote_dict(
        self,
        symbol: str,
        use_cache: bool = True,
        *,
        schema: Any = None,
        page: str = "",
        sheet: str = "",
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        q = await self.get_enriched_quote(symbol, use_cache=use_cache, schema=schema, page=page, sheet=sheet, body=body, **kwargs)
        return _model_to_dict(q)

    async def get_enriched_quotes(
        self,
        symbols: List[str],
        *,
        schema: Any = None,
        page: str = "",
        sheet: str = "",
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> List[UnifiedQuote]:
        if not symbols:
            return []
        batch = max(1, min(500, _get_env_int("QUOTE_BATCH_SIZE", 25)))
        out: List[UnifiedQuote] = []
        for i in range(0, len(symbols), batch):
            part = symbols[i:i + batch]
            out.extend(
                await asyncio.gather(*[
                    self.get_enriched_quote(s, schema=schema, page=page, sheet=sheet, body=body, **kwargs)
                    for s in part
                ])
            )
        return out

    async def get_enriched_quotes_batch(
        self,
        symbols: List[str],
        mode: str = "",
        *,
        schema: Any = None,
        page: str = "",
        sheet: str = "",
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        norm_syms = _normalize_symbol_list(symbols, limit=len(symbols) + 10)
        quotes = await asyncio.gather(*[self.get_enriched_quote_dict(s, schema=schema, page=page, sheet=sheet, body=body, **kwargs) for s in norm_syms])
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

    # ------------------------------------------------------------------
    # builder fallbacks
    # ------------------------------------------------------------------
    async def _build_data_dictionary_rows(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for sheet_name in _list_sheet_names_best_effort():
            spec, headers, keys, source = _schema_for_sheet(sheet_name)
            columns = _schema_columns_from_any(spec)
            for idx, (header, key) in enumerate(zip(headers, keys), start=1):
                col_meta = columns[idx - 1] if idx - 1 < len(columns) else {}
                if not isinstance(col_meta, Mapping):
                    col_meta = _model_to_dict(col_meta)
                dtype = _safe_str(col_meta.get("dtype") or col_meta.get("type") or col_meta.get("data_type") or "string")
                fmt = _safe_str(col_meta.get("format") or col_meta.get("fmt") or col_meta.get("number_format") or "")
                required = bool(col_meta.get("required")) if "required" in col_meta else idx <= 3
                source_hint = _safe_str(col_meta.get("source") or source)
                notes = _safe_str(col_meta.get("notes") or col_meta.get("description") or "static/registry contract")

                group = "Canonical"
                if key in TOP10_REQUIRED_FIELDS:
                    group = "Top10"
                elif sheet_name == "Insights_Analysis":
                    group = "Insights"
                elif sheet_name == "Data_Dictionary":
                    group = "Metadata"
                elif idx <= 8:
                    group = "Identity"
                elif idx <= 18:
                    group = "Price"
                elif idx <= 24:
                    group = "Liquidity"
                elif idx <= 36:
                    group = "Fundamentals"
                elif idx <= 44:
                    group = "Risk"
                elif idx <= 50:
                    group = "Valuation"
                elif idx <= 69:
                    group = "Forecast & Scoring"
                else:
                    group = "Portfolio & Provenance"

                rows.append(
                    {
                        "sheet": sheet_name,
                        "group": group,
                        "header": header,
                        "key": key,
                        "dtype": dtype,
                        "fmt": fmt,
                        "required": required,
                        "source": source_hint,
                        "notes": notes,
                    }
                )
        return rows

    async def _build_insights_rows_fallback(self, body: Optional[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
        body = dict(body or {})
        symbols = _extract_requested_symbols_from_body(body, limit=max(limit * 2, 10))
        if not symbols:
            for page_name in TOP10_ENGINE_DEFAULT_PAGES:
                symbols.extend(await self.get_sheet_symbols(page_name, limit=max(limit * 2, 10), body=body))
        symbols = _normalize_symbol_list(symbols, limit=max(limit * 3, 30))
        if not symbols:
            symbols = list(EMERGENCY_PAGE_SYMBOLS.get("Market_Leaders", [])[: max(limit, 6)])

        quotes = await self.get_enriched_quotes(symbols, schema=None, page="Insights_Analysis", body=body)
        quote_rows = [_model_to_dict(q) for q in quotes]
        quote_rows = [r for r in quote_rows if isinstance(r, dict)]
        quote_rows.sort(
            key=lambda r: (
                _as_float(r.get("opportunity_score")) or _as_float(r.get("overall_score")) or 0.0,
                _as_float(r.get("confidence_score")) or 0.0,
            ),
            reverse=True,
        )

        def _avg(values: List[Optional[float]]) -> Optional[float]:
            nums = [v for v in values if v is not None]
            return round(sum(nums) / len(nums), 4) if nums else None

        total = len(quote_rows)
        avg_overall = _avg([_as_float(r.get("overall_score")) for r in quote_rows])
        avg_roi_3m = _avg([_as_float(r.get("expected_roi_3m")) for r in quote_rows])

        risk_counts = {"LOW": 0, "MODERATE": 0, "HIGH": 0}
        for row in quote_rows:
            bucket = _safe_str(row.get("risk_bucket")).upper()
            if bucket in risk_counts:
                risk_counts[bucket] += 1

        rows: List[Dict[str, Any]] = []
        rows.append(
            {
                "section": "Market Summary",
                "item": "Universe",
                "metric": "Symbols Analyzed",
                "value": total,
                "notes": f"fallback summary from {len(symbols)} requested symbols",
                "source": "engine_fallback",
                "sort_order": 1,
            }
        )
        rows.append(
            {
                "section": "Market Summary",
                "item": "Universe",
                "metric": "Average Overall Score",
                "value": avg_overall,
                "notes": "mean overall score across analyzed instruments",
                "source": "engine_fallback",
                "sort_order": 2,
            }
        )
        rows.append(
            {
                "section": "Market Summary",
                "item": "Universe",
                "metric": "Average Expected ROI 3M",
                "value": avg_roi_3m,
                "notes": "fractional ROI where available",
                "source": "engine_fallback",
                "sort_order": 3,
            }
        )

        sort_order = 10
        for bucket in ("LOW", "MODERATE", "HIGH"):
            rows.append(
                {
                    "section": "Risk Distribution",
                    "item": bucket,
                    "metric": "Count",
                    "value": risk_counts[bucket],
                    "notes": "fallback risk bucket summary",
                    "source": "engine_fallback",
                    "sort_order": sort_order,
                }
            )
            sort_order += 1

        top_quotes = quote_rows[: max(3, min(7, limit))]
        for idx, d in enumerate(top_quotes, start=1):
            rows.append(
                {
                    "section": "Top Ideas",
                    "item": d.get("symbol"),
                    "metric": "Recommendation",
                    "value": d.get("recommendation"),
                    "notes": d.get("recommendation_reason") or f"overall={d.get('overall_score')} opportunity={d.get('opportunity_score')}",
                    "source": "engine_fallback",
                    "sort_order": 100 + idx,
                }
            )
            rows.append(
                {
                    "section": "Top Ideas",
                    "item": d.get("symbol"),
                    "metric": "Expected ROI 3M",
                    "value": d.get("expected_roi_3m"),
                    "notes": f"confidence={d.get('confidence_score')} risk={d.get('risk_bucket')}",
                    "source": "engine_fallback",
                    "sort_order": 120 + idx,
                }
            )
            if _as_float(d.get("position_value")) is not None or _as_float(d.get("unrealized_pl")) is not None:
                rows.append(
                    {
                        "section": "Portfolio Signals",
                        "item": d.get("symbol"),
                        "metric": "Unrealized P/L",
                        "value": d.get("unrealized_pl"),
                        "notes": f"value={d.get('position_value')} cost={d.get('position_cost')}",
                        "source": "engine_fallback",
                        "sort_order": 140 + idx,
                    }
                )

        return rows[:limit]

    def _top10_sort_key(self, row: Dict[str, Any]) -> Tuple[float, ...]:
        return (
            _as_float(row.get("opportunity_score")) or float("-inf"),
            _as_float(row.get("overall_score")) or float("-inf"),
            _as_float(row.get("confidence_score")) or float("-inf"),
            _as_float(row.get("expected_roi_3m")) or float("-inf"),
            _as_float(row.get("expected_roi_12m")) or float("-inf"),
            _as_float(row.get("value_score")) or float("-inf"),
            _as_float(row.get("quality_score")) or float("-inf"),
            _as_float(row.get("momentum_score")) or float("-inf"),
            _as_float(row.get("growth_score")) or float("-inf"),
            _as_float(row.get("current_price")) or float("-inf"),
        )

    async def _build_top10_rows_fallback(
        self,
        headers: Sequence[str],
        keys: Sequence[str],
        body: Optional[Dict[str, Any]],
        limit: int,
        mode: str = "",
    ) -> Tuple[List[str], List[str], List[Dict[str, Any]]]:
        body = dict(body or {})
        criteria = dict(body.get("criteria") or {}) if isinstance(body.get("criteria"), dict) else {}
        out_headers, out_keys = _ensure_top10_contract(headers, keys)

        top_n = max(1, min(int(criteria.get("top_n") or body.get("top_n") or 10), max(1, limit)))
        requested_pages = _extract_top10_pages_from_body(body) or list(TOP10_ENGINE_DEFAULT_PAGES)
        requested_symbols = _extract_requested_symbols_from_body(body, limit=max(limit * 10, 200))

        for page_name in requested_pages:
            if len(requested_symbols) >= max(limit * 10, 200):
                break
            syms = await self.get_sheet_symbols(page_name, limit=max(limit * 2, 25), body=body)
            if syms:
                requested_symbols.extend(syms)

        if not requested_symbols:
            requested_symbols = list(EMERGENCY_PAGE_SYMBOLS.get("Top_10_Investments") or [])

        requested_symbols = _normalize_symbol_list(requested_symbols, limit=max(limit * 10, 200))
        if not requested_symbols:
            return out_headers, out_keys, []

        quotes = await self.get_enriched_quotes(requested_symbols, schema=None, page="Top_10_Investments", body=body)
        rows: List[Dict[str, Any]] = []
        for q in quotes:
            row = _model_to_dict(q)
            row = _apply_page_row_backfill("Top_10_Investments", row)
            _compute_scores_fallback(row)
            _compute_recommendation(row)
            rows.append(row)

        if not rows:
            return out_headers, out_keys, []

        _apply_rank_overall(rows)
        rows.sort(key=self._top10_sort_key, reverse=True)

        criteria_snapshot = _top10_criteria_snapshot({
            **criteria,
            "pages_selected": requested_pages,
            "direct_symbols": requested_symbols,
            "top_n": top_n,
        })

        selected: List[Dict[str, Any]] = []
        seen: Set[str] = set()
        for row in rows:
            sym = normalize_symbol(_safe_str(row.get("symbol") or row.get("ticker") or row.get("requested_symbol")))
            dedupe_key = sym or _safe_str(row.get("name")) or f"row_{len(selected)+1}"
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            row["top10_rank"] = len(selected) + 1
            row["selection_reason"] = row.get("selection_reason") or _top10_selection_reason(row)
            row["criteria_snapshot"] = row.get("criteria_snapshot") or criteria_snapshot
            projected = _normalize_to_schema_keys(out_keys, out_headers, row)
            projected["top10_rank"] = row["top10_rank"]
            projected["selection_reason"] = row["selection_reason"]
            projected["criteria_snapshot"] = row["criteria_snapshot"]
            selected.append(_strict_project_row(out_keys, projected))
            if len(selected) >= top_n:
                break

        return out_headers, out_keys, selected

    # ------------------------------------------------------------------
    # main sheet/page APIs
    # ------------------------------------------------------------------
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
        **kwargs: Any,
    ) -> Dict[str, Any]:
        return await self.get_sheet_rows(
            page or sheet or sheet_name,
            limit=limit,
            offset=offset,
            mode=mode,
            body=body,
            page=page,
            sheet=sheet,
            sheet_name=sheet_name,
            **kwargs,
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
        **kwargs: Any,
    ) -> Dict[str, Any]:
        return await self.get_sheet_rows(
            sheet_name or sheet or page,
            limit=limit,
            offset=offset,
            mode=mode,
            body=body,
            page=page,
            sheet=sheet,
            sheet_name=sheet_name,
            **kwargs,
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
        **kwargs: Any,
    ) -> Dict[str, Any]:
        target_sheet, limit, offset, mode, body, request_parts = _normalize_route_call_inputs(
            page=page,
            sheet=sheet,
            sheet_name=sheet_name,
            limit=limit,
            offset=offset,
            mode=mode,
            body=body,
            extras=kwargs,
        )
        include_matrix = _safe_bool(body.get("include_matrix"), True)

        spec, headers, keys, schema_src = _schema_for_sheet(target_sheet)
        headers, keys = _complete_schema_contract(headers, keys)

        if target_sheet == "Top_10_Investments" and self.top10_force_full_schema:
            static_contract = STATIC_CANONICAL_SHEET_CONTRACTS.get("Top_10_Investments", {})
            static_headers, static_keys = _complete_schema_contract(static_contract.get("headers", []), static_contract.get("keys", []))
            if len(static_keys) >= len(keys):
                headers, keys = _ensure_top10_contract(static_headers, static_keys)
                schema_src = f"{schema_src}|top10_force_full_schema"

        target_sheet_known = target_sheet in INSTRUMENT_SHEETS or target_sheet in SPECIAL_SHEETS or bool(spec)
        strict_req = bool(self.schema_strict_sheet_rows)
        contract_level = "canonical" if _usable_contract(headers, keys, target_sheet) else "partial"
        recovered_from: Optional[str] = None

        # Data Dictionary
        if target_sheet == "Data_Dictionary":
            if _is_schema_only_body(body):
                return self._finalize_payload(
                    sheet=target_sheet,
                    headers=headers,
                    keys=keys,
                    row_objects=[],
                    include_matrix=include_matrix,
                    status="success",
                    meta={
                        "schema_source": schema_src,
                        "contract_level": contract_level,
                        "strict_requested": strict_req,
                        "strict_enforced": False,
                        "target_sheet_known": True,
                        "builder": "schema_only_fast_path",
                        "rows": 0,
                        "limit": limit,
                        "offset": offset,
                        "mode": mode,
                    },
                )

            rows_all = await self._build_data_dictionary_rows()
            rows_proj = [_strict_project_row(keys, _normalize_to_schema_keys(keys, headers, r)) for r in rows_all]
            payload_full = self._finalize_payload(
                sheet=target_sheet,
                headers=headers,
                keys=keys,
                row_objects=rows_proj,
                include_matrix=include_matrix,
                status="success",
                meta={
                    "schema_source": schema_src,
                    "contract_level": contract_level,
                    "strict_requested": strict_req,
                    "strict_enforced": False,
                    "target_sheet_known": True,
                    "builder": "engine.internal_data_dictionary",
                    "rows": len(rows_proj),
                    "limit": limit,
                    "offset": offset,
                    "mode": mode,
                },
            )
            self._store_sheet_snapshot(target_sheet, payload_full)
            rows_page = rows_proj[offset : offset + limit]
            return self._finalize_payload(
                sheet=target_sheet,
                headers=headers,
                keys=keys,
                row_objects=rows_page,
                include_matrix=include_matrix,
                status="success",
                meta={**payload_full.get("meta", {}), "rows": len(rows_page)},
            )

        # Insights Analysis
        if target_sheet == "Insights_Analysis":
            if _is_schema_only_body(body):
                return self._finalize_payload(
                    sheet=target_sheet,
                    headers=headers,
                    keys=keys,
                    row_objects=[],
                    include_matrix=include_matrix,
                    status="success",
                    meta={
                        "schema_source": schema_src,
                        "contract_level": contract_level,
                        "strict_requested": strict_req,
                        "strict_enforced": False,
                        "target_sheet_known": True,
                        "builder": "schema_only_fast_path",
                        "rows": 0,
                        "limit": limit,
                        "offset": offset,
                        "mode": mode,
                    },
                )

            rows0: List[Dict[str, Any]] = []
            builder_name = "core.analysis.insights_builder"
            try:
                from core.analysis.insights_builder import build_insights_analysis_rows  # type: ignore
                crit = body.get("criteria") if isinstance(body.get("criteria"), dict) else None
                universes = body.get("universes") if isinstance(body.get("universes"), dict) else None
                symbols = body.get("symbols") if isinstance(body.get("symbols"), list) else None
                payload = await build_insights_analysis_rows(
                    engine=self,
                    criteria=crit,
                    universes=universes,
                    symbols=symbols,
                    mode=mode or "",
                )
                rows0 = _coerce_rows_list(payload)
            except Exception as exc:
                builder_name = f"fallback:insights_builder_failed:{type(exc).__name__}"
                rows0 = []

            if not rows0:
                rows0 = await self._build_insights_rows_fallback(body, limit=max(limit + offset, 10))
                builder_name = "fallback:engine_insights_rows"

            rows_proj = [_strict_project_row(keys, _normalize_to_schema_keys(keys, headers, r)) for r in rows0]
            payload_full = self._finalize_payload(
                sheet=target_sheet,
                headers=headers,
                keys=keys,
                row_objects=rows_proj,
                include_matrix=include_matrix,
                status="success" if rows_proj else "warn",
                meta={
                    "schema_source": schema_src,
                    "contract_level": contract_level,
                    "strict_requested": strict_req,
                    "strict_enforced": False,
                    "target_sheet_known": True,
                    "builder": builder_name,
                    "rows": len(rows_proj),
                    "limit": limit,
                    "offset": offset,
                    "mode": mode,
                },
            )
            self._store_sheet_snapshot(target_sheet, payload_full)
            rows_page = rows_proj[offset : offset + limit]
            return self._finalize_payload(
                sheet=target_sheet,
                headers=headers,
                keys=keys,
                row_objects=rows_page,
                include_matrix=include_matrix,
                status=payload_full.get("status", "success"),
                meta={**payload_full.get("meta", {}), "rows": len(rows_page)},
            )

        # Top10
        if target_sheet == "Top_10_Investments":
            top10_body, route_warnings = _normalize_top10_body_for_engine(body, limit=max(1, min(limit, 50)))
            if _is_schema_only_body(top10_body):
                headers, keys = _ensure_top10_contract(headers, keys)
                return self._finalize_payload(
                    sheet=target_sheet,
                    headers=headers,
                    keys=keys,
                    row_objects=[],
                    include_matrix=include_matrix,
                    status="success",
                    meta={
                        "schema_source": schema_src,
                        "contract_level": contract_level,
                        "strict_requested": strict_req,
                        "strict_enforced": False,
                        "target_sheet_known": True,
                        "builder": "schema_only_fast_path",
                        "rows": 0,
                        "limit": limit,
                        "offset": offset,
                        "mode": mode,
                        "warnings": route_warnings,
                    },
                )

            rows_proj: List[Dict[str, Any]] = []
            builder_used = "core.analysis.top10_selector"
            status_out = "success"

            try:
                from core.analysis.top10_selector import build_top10_rows  # type: ignore
                criteria = top10_body.get("criteria") if isinstance(top10_body.get("criteria"), dict) else None
                payload = await build_top10_rows(
                    engine=self,
                    settings=self.settings,
                    criteria=criteria,
                    body=dict(top10_body or {}),
                    limit=int(top10_body.get("limit") or top10_body.get("top_n") or min(limit, 10) or 10),
                    mode=mode or "",
                )
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

            payload_full = self._finalize_payload(
                sheet=target_sheet,
                headers=headers,
                keys=keys,
                row_objects=rows_proj,
                include_matrix=include_matrix,
                status=status_out if rows_proj else "warn",
                meta={
                    "schema_source": schema_src,
                    "contract_level": contract_level,
                    "strict_requested": strict_req,
                    "strict_enforced": False,
                    "target_sheet_known": True,
                    "builder": builder_used,
                    "rows": len(rows_proj),
                    "limit": limit,
                    "offset": offset,
                    "mode": mode,
                    "warnings": route_warnings,
                },
            )
            if rows_proj:
                self._store_sheet_snapshot(target_sheet, payload_full)

            rows_page = rows_proj[offset : offset + limit]
            return self._finalize_payload(
                sheet=target_sheet,
                headers=headers,
                keys=keys,
                row_objects=rows_page,
                include_matrix=include_matrix,
                status=payload_full.get("status", "warn"),
                meta={**payload_full.get("meta", {}), "rows": len(rows_page)},
            )

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
            "route_input_keys": sorted([str(k) for k in body.keys()]) if isinstance(body, dict) else [],
            "request_input_keys": sorted([str(k) for k in request_parts.keys()]) if isinstance(request_parts, dict) else [],
        }
        if recovered_from:
            base_meta["recovered_from"] = recovered_from
        if schema_warning:
            base_meta["schema_warning"] = schema_warning

        if _is_schema_only_body(body):
            return self._finalize_payload(
                sheet=target_sheet,
                headers=headers,
                keys=keys,
                row_objects=[],
                include_matrix=include_matrix,
                status=final_status,
                meta={**base_meta, "rows": 0, "limit": limit, "offset": offset, "mode": mode, "built_from": "schema_only_fast_path"},
            )

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
                    for q in await self.get_enriched_quotes(symbols, schema=None, page=target_sheet, body=body):
                        d = _model_to_dict(q)
                        sym = normalize_symbol(_safe_str(d.get("symbol")))
                        if sym:
                            quote_map[sym] = d

                snapshot_map = self._get_cached_snapshot_symbol_map(target_sheet)

                for row in ext_rows:
                    merged = dict(row)
                    sym = normalize_symbol(self._extract_row_symbol(row))
                    if sym and sym in snapshot_map:
                        merged = _merge_missing_fields(merged, snapshot_map[sym])
                    best_snapshot_row = self._get_best_cached_snapshot_row_for_symbol(sym, prefer_sheet=target_sheet) if sym else None
                    if best_snapshot_row:
                        merged = _merge_missing_fields(merged, best_snapshot_row)
                    if sym and sym in quote_map:
                        merged = _merge_missing_fields(merged, quote_map[sym])
                    merged = _apply_page_row_backfill(target_sheet, merged)
                    _compute_scores_fallback(merged)
                    _compute_recommendation(merged)
                    enriched_rows.append(_strict_project_row(out_keys, _normalize_to_schema_keys(out_keys, out_headers, merged)))

                _apply_rank_overall(enriched_rows)

                payload_full = self._finalize_payload(
                    sheet=target_sheet,
                    headers=out_headers,
                    keys=out_keys,
                    row_objects=enriched_rows,
                    include_matrix=include_matrix,
                    status=final_status,
                    meta={
                        **base_meta,
                        "rows": len(enriched_rows),
                        "limit": limit,
                        "offset": offset,
                        "mode": mode,
                        "built_from": "external_rows_reader",
                        "rows_reader_source": self._rows_reader_source,
                        "symbols_reader_source": self._symbols_reader_source,
                        "symbol_resolution_meta": self._get_sheet_symbols_meta(target_sheet),
                    },
                )
                self._store_sheet_snapshot(target_sheet, payload_full)
                rows_page = enriched_rows[offset : offset + limit]
                return self._finalize_payload(
                    sheet=target_sheet,
                    headers=out_headers,
                    keys=out_keys,
                    row_objects=rows_page,
                    include_matrix=include_matrix,
                    status=final_status,
                    meta={**payload_full.get("meta", {}), "rows": len(rows_page)},
                )

        if not requested_symbols:
            cached_snap = self.get_cached_sheet_snapshot(sheet=target_sheet)
            cached_rows = _coerce_rows_list(cached_snap)
            if cached_rows:
                proj_rows = [_strict_project_row(out_keys, _normalize_to_schema_keys(out_keys, out_headers, row)) for row in cached_rows]
                rows_page = proj_rows[offset : offset + limit]
                return self._finalize_payload(
                    sheet=target_sheet,
                    headers=out_headers,
                    keys=out_keys,
                    row_objects=rows_page,
                    include_matrix=include_matrix,
                    status=final_status,
                    meta={
                        **base_meta,
                        "rows": len(rows_page),
                        "limit": limit,
                        "offset": offset,
                        "mode": mode,
                        "built_from": "cached_snapshot",
                        "symbols_reader_source": self._symbols_reader_source,
                        "symbol_resolution_meta": self._get_sheet_symbols_meta(target_sheet),
                    },
                )

        rows_full: List[Dict[str, Any]] = []
        if requested_symbols:
            snapshot_map = self._get_cached_snapshot_symbol_map(target_sheet)
            quotes = await self.get_enriched_quotes(requested_symbols, schema=None, page=target_sheet, body=body)
            # FIX v5.48.0/v5.49.0: deduplicate by normalized symbol before assembling rows.
            _seen_row_symbols: Set[str] = set()
            for q in quotes:
                row = _model_to_dict(q)
                sym = normalize_symbol(_safe_str(row.get("symbol") or row.get("requested_symbol")))
                if sym and sym in _seen_row_symbols:
                    logger.debug("data_engine_v2: skipping duplicate symbol %s for sheet %s", sym, target_sheet)
                    continue
                if sym:
                    _seen_row_symbols.add(sym)
                if sym and sym in snapshot_map:
                    row = _merge_missing_fields(row, snapshot_map[sym])
                best_snapshot_row = self._get_best_cached_snapshot_row_for_symbol(sym, prefer_sheet=target_sheet) if sym else None
                if best_snapshot_row:
                    row = _merge_missing_fields(row, best_snapshot_row)
                row = _apply_page_row_backfill(target_sheet, row)
                _compute_scores_fallback(row)
                _compute_recommendation(row)
                rows_full.append(_strict_project_row(out_keys, _normalize_to_schema_keys(out_keys, out_headers, row)))
            _apply_rank_overall(rows_full)

        if rows_full:
            payload_full = self._finalize_payload(
                sheet=target_sheet,
                headers=out_headers,
                keys=out_keys,
                row_objects=rows_full,
                include_matrix=include_matrix,
                status=final_status,
                meta={
                    **base_meta,
                    "rows": len(rows_full),
                    "limit": limit,
                    "offset": offset,
                    "mode": mode,
                    "built_from": built_from,
                    "resolved_symbols_count": len(requested_symbols),
                    "symbols_reader_source": self._symbols_reader_source,
                    "symbol_resolution_meta": self._get_sheet_symbols_meta(target_sheet),
                },
            )
            self._store_sheet_snapshot(target_sheet, payload_full)
            rows_page = rows_full[offset : offset + limit]
            return self._finalize_payload(
                sheet=target_sheet,
                headers=out_headers,
                keys=out_keys,
                row_objects=rows_page,
                include_matrix=include_matrix,
                status=final_status,
                meta={**payload_full.get("meta", {}), "rows": len(rows_page)},
            )

        return self._finalize_payload(
            sheet=target_sheet,
            headers=out_headers,
            keys=out_keys,
            row_objects=[],
            include_matrix=include_matrix,
            status=final_status,
            meta={
                **base_meta,
                "rows": 0,
                "limit": limit,
                "offset": offset,
                "mode": mode,
                "built_from": built_from,
                "resolved_symbols_count": len(requested_symbols),
                "symbols_reader_source": self._symbols_reader_source,
                "symbol_resolution_meta": self._get_sheet_symbols_meta(target_sheet),
            },
        )

    async def sheet_rows(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    async def build_sheet_rows(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    def get_sheet_contract(self, sheet: str) -> Dict[str, Any]:
        target = _canonicalize_sheet_name(sheet) or sheet or "Market_Leaders"
        _spec, headers, keys, source = _schema_for_sheet(target)
        if target == "Top_10_Investments":
            headers, keys = _ensure_top10_contract(headers, keys)
        return {
            "sheet": target,
            "page": target,
            "sheet_name": target,
            "headers": headers,
            "display_headers": headers,
            "keys": keys,
            "fields": keys,
            "source": source,
            "count": len(keys),
        }

    def get_page_contract(self, page: str) -> Dict[str, Any]:
        return self.get_sheet_contract(page)

    def get_sheet_schema(self, sheet: str) -> Dict[str, Any]:
        return self.get_sheet_contract(sheet)

    def get_page_schema(self, page: str) -> Dict[str, Any]:
        return self.get_sheet_contract(page)

    def get_headers_for_sheet(self, sheet: str) -> List[str]:
        return list(self.get_sheet_contract(sheet).get("headers") or [])

    def get_keys_for_sheet(self, sheet: str) -> List[str]:
        return list(self.get_sheet_contract(sheet).get("keys") or [])

    # ------------------------------------------------------------------
    # health / stats
    # ------------------------------------------------------------------
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
            "non_ksa_primary_provider": self.non_ksa_primary_provider,
            "page_primary_providers": dict(self.page_primary_providers),
            "history_fallback_providers": list(self.history_fallback_providers),
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



def normalize_row_to_schema(sheet: str, row: Dict[str, Any], keep_extras: bool = False) -> Dict[str, Any]:
    target = _canonicalize_sheet_name(sheet) or sheet or "Market_Leaders"
    _spec, headers, keys, _src = _schema_for_sheet(target)
    if target == "Top_10_Investments":
        headers, keys = _ensure_top10_contract(headers, keys)
    normalized = _normalize_to_schema_keys(keys, headers, dict(row or {}))
    normalized = _apply_page_row_backfill(target, normalized)
    if keep_extras and isinstance(row, dict):
        for k, v in row.items():
            if k not in normalized:
                normalized[k] = _json_safe(v)
    return normalized

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
    "STATIC_CANONICAL_SHEET_CONTRACTS",
    "get_sheet_spec",
    "normalize_row_to_schema",
]
