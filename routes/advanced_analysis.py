#!/usr/bin/env python3
# routes/advanced_analysis.py
"""
================================================================================
Advanced Analysis Root Owner — v4.3.0
================================================================================
ROOT SHEET-ROWS OWNER • SCHEMA-FIRST • FAIL-SOFT • STABLE ENVELOPE • JSON-SAFE
GET+POST MERGED • HEADERS-ONLY / SCHEMA-ONLY • CANONICAL WIDTHS • OWNER-ALIGNED
HYphen + UNDERSCORE ALIASES • NON-EMPTY FALLBACKS • TOP10 SAFE CONTRACT

Purpose
-------
Owns the canonical root paths:
- /sheet-rows
- /sheet_rows
- /schema
- /schema/sheet-spec
- /schema/sheet_spec
- /schema/pages
- /schema/data-dictionary
- /schema/data_dictionary
and their /v1/schema aliases.

Why this revision
-----------------
- FIX: aligns fallback schema widths with current validator expectations:
       My_Portfolio=45, Insights_Analysis=10, Top_10_Investments=83, Data_Dictionary=9.
- FIX: adds underscore aliases for sheet_spec / data_dictionary / sheet_rows.
- FIX: supports both object rows and rows_matrix-style upstream payloads.
- FIX: keeps root /sheet-rows authoritative with a stable envelope even when
       upstream builders degrade.
- FIX: never emits empty-success payloads for special pages.
- FIX: supports schema_only / headers_only in both GET and POST flows.
- ENHANCE: provides schema-safe local fallbacks for Top_10_Investments,
           Insights_Analysis, Data_Dictionary, My_Portfolio, and instrument pages.
================================================================================
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import math
import os
import re
import time
import uuid
from dataclasses import is_dataclass
from datetime import date, datetime, time as dt_time
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, status

logger = logging.getLogger("routes.advanced_analysis")
logger.addHandler(logging.NullHandler())

ADVANCED_ANALYSIS_VERSION = "4.3.0"
router = APIRouter(tags=["schema", "root-sheet-rows"])

_TOP10_PAGE = "Top_10_Investments"
_INSIGHTS_PAGE = "Insights_Analysis"
_DICTIONARY_PAGE = "Data_Dictionary"
_SPECIAL_PAGES = {_TOP10_PAGE, _INSIGHTS_PAGE, _DICTIONARY_PAGE}

_EXPECTED_SHEET_LENGTHS: Dict[str, int] = {
    "Market_Leaders": 80,
    "Global_Markets": 80,
    "Commodities_FX": 80,
    "Mutual_Funds": 80,
    "My_Portfolio": 45,
    "My_Investments": 80,
    _TOP10_PAGE: 83,
    _INSIGHTS_PAGE: 10,
    _DICTIONARY_PAGE: 9,
}

_TOP10_REQUIRED_FIELDS: Tuple[str, ...] = (
    "top10_rank",
    "selection_reason",
    "criteria_snapshot",
)

_TOP10_REQUIRED_HEADERS: Dict[str, str] = {
    "top10_rank": "Top10 Rank",
    "selection_reason": "Selection Reason",
    "criteria_snapshot": "Criteria Snapshot",
}

# =============================================================================
# Imports / fallbacks
# =============================================================================
get_sheet_headers = None  # type: ignore
get_sheet_keys = None     # type: ignore
get_sheet_len = None      # type: ignore
get_sheet_spec = None     # type: ignore

for _sreg_path in ("core.sheets.schema_registry", "core.schema_registry", "schema_registry"):
    try:
        import importlib as _il
        _sreg = _il.import_module(_sreg_path)
        _fh = getattr(_sreg, "get_sheet_headers", None)
        _fk = getattr(_sreg, "get_sheet_keys", None)
        if callable(_fh) and callable(_fk):
            get_sheet_headers = _fh
            get_sheet_keys = _fk
            get_sheet_len = getattr(_sreg, "get_sheet_len", None)
            get_sheet_spec = getattr(_sreg, "get_sheet_spec", None)
            break
    except Exception:
        continue
del _sreg_path

CANONICAL_PAGES: List[str] = []
FORBIDDEN_PAGES: set = {"KSA_Tadawul", "Advisor_Criteria"}


def allowed_pages() -> List[str]:
    return list(CANONICAL_PAGES) if CANONICAL_PAGES else []


def normalize_page_name(name: str, allow_output_pages: bool = True) -> str:
    return (name or "").strip().replace(" ", "_")


for _pcat_path in ("core.sheets.page_catalog", "core.page_catalog", "page_catalog"):
    try:
        import importlib as _il2
        _pcat = _il2.import_module(_pcat_path)
        _ap = getattr(_pcat, "allowed_pages", None)
        _np = getattr(_pcat, "normalize_page_name", None)
        if callable(_ap):
            _cp = getattr(_pcat, "CANONICAL_PAGES", None)
            _fp = getattr(_pcat, "FORBIDDEN_PAGES", None)
            if _cp is not None:
                CANONICAL_PAGES[:] = list(_cp)
            if _fp is not None:
                FORBIDDEN_PAGES.clear()
                FORBIDDEN_PAGES.update(_fp)
            allowed_pages = _ap
            normalize_page_name = _np or normalize_page_name
            break
    except Exception:
        continue
del _pcat_path

try:
    from core.config import auth_ok, get_settings_cached, is_open_mode  # type: ignore
except Exception:
    auth_ok = None  # type: ignore
    is_open_mode = None  # type: ignore

    def get_settings_cached(*args: Any, **kwargs: Any) -> Any:  # type: ignore
        return None


_CORE_GET_ROWS_SOURCE = "unavailable"
try:
    from core.data_engine import get_sheet_rows as core_get_sheet_rows  # type: ignore
    _CORE_GET_ROWS_SOURCE = "core.data_engine.get_sheet_rows"
except Exception:
    try:
        from core.data_engine_v2 import get_sheet_rows as core_get_sheet_rows  # type: ignore
        _CORE_GET_ROWS_SOURCE = "core.data_engine_v2.get_sheet_rows"
    except Exception:
        core_get_sheet_rows = None  # type: ignore


# =============================================================================
# Canonical fallback schemas
# =============================================================================
_CANONICAL_80_HEADERS: List[str] = [
    "Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country", "Sector", "Industry",
    "Current Price", "Previous Close", "Open", "Day High", "Day Low", "52W High", "52W Low",
    "Price Change", "Percent Change", "52W Position %", "Volume", "Avg Volume 10D", "Avg Volume 30D",
    "Market Cap", "Float Shares", "Beta (5Y)", "P/E (TTM)", "P/E (Forward)", "EPS (TTM)",
    "Dividend Yield", "Payout Ratio", "Revenue (TTM)", "Revenue Growth YoY", "Gross Margin",
    "Operating Margin", "Profit Margin", "Debt/Equity", "Free Cash Flow (TTM)", "RSI (14)",
    "Volatility 30D", "Volatility 90D", "Max Drawdown 1Y", "VaR 95% (1D)", "Sharpe (1Y)",
    "Risk Score", "Risk Bucket", "P/B", "P/S", "EV/EBITDA", "PEG", "Intrinsic Value",
    "Valuation Score", "Forecast Price 1M", "Forecast Price 3M", "Forecast Price 12M",
    "Expected ROI 1M", "Expected ROI 3M", "Expected ROI 12M", "Forecast Confidence",
    "Confidence Score", "Confidence Bucket", "Value Score", "Quality Score", "Momentum Score",
    "Growth Score", "Overall Score", "Opportunity Score", "Rank (Overall)", "Recommendation",
    "Recommendation Reason", "Horizon Days", "Invest Period Label", "Position Qty", "Avg Cost",
    "Position Cost", "Position Value", "Unrealized P/L", "Unrealized P/L %", "Data Provider",
    "Last Updated (UTC)", "Last Updated (Riyadh)", "Warnings",
]

_CANONICAL_80_KEYS: List[str] = [
    "symbol", "name", "asset_class", "exchange", "currency", "country", "sector", "industry",
    "current_price", "previous_close", "open_price", "day_high", "day_low", "week_52_high",
    "week_52_low", "price_change", "percent_change", "week_52_position_pct", "volume",
    "avg_volume_10d", "avg_volume_30d", "market_cap", "float_shares", "beta_5y", "pe_ttm",
    "pe_forward", "eps_ttm", "dividend_yield", "payout_ratio", "revenue_ttm", "revenue_growth_yoy",
    "gross_margin", "operating_margin", "profit_margin", "debt_to_equity", "free_cash_flow_ttm",
    "rsi_14", "volatility_30d", "volatility_90d", "max_drawdown_1y", "var_95_1d", "sharpe_1y",
    "risk_score", "risk_bucket", "pb_ratio", "ps_ratio", "ev_ebitda", "peg_ratio",
    "intrinsic_value", "valuation_score", "forecast_price_1m", "forecast_price_3m",
    "forecast_price_12m", "expected_roi_1m", "expected_roi_3m", "expected_roi_12m",
    "forecast_confidence", "confidence_score", "confidence_bucket", "value_score", "quality_score",
    "momentum_score", "growth_score", "overall_score", "opportunity_score", "rank_overall",
    "recommendation", "recommendation_reason", "horizon_days", "invest_period_label", "position_qty",
    "avg_cost", "position_cost", "position_value", "unrealized_pl", "unrealized_pl_pct",
    "data_provider", "last_updated_utc", "last_updated_riyadh", "warnings",
]

_MY_PORTFOLIO_45_HEADERS: List[str] = [
    "Symbol", "Name", "Current Price", "Previous Close", "Price Change", "Percent Change",
    "Position Qty", "Avg Cost", "Position Cost", "Position Value", "Unrealized P/L", "Unrealized P/L %",
    "Market Cap", "Volume", "Exchange", "Currency", "Country", "Sector", "Industry",
    "Beta (5Y)", "P/E (TTM)", "EPS (TTM)", "Dividend Yield", "Revenue (TTM)", "Gross Margin",
    "Operating Margin", "Profit Margin", "Debt/Equity", "RSI (14)", "Volatility 30D",
    "Risk Score", "Risk Bucket", "P/B", "P/S", "EV/EBITDA", "PEG", "Intrinsic Value",
    "Forecast Price 1M", "Forecast Price 3M", "Forecast Price 12M",
    "Expected ROI 1M", "Expected ROI 3M", "Expected ROI 12M", "Recommendation",
    "Last Updated (Riyadh)",
]

_MY_PORTFOLIO_45_KEYS: List[str] = [
    "symbol", "name", "current_price", "previous_close", "price_change", "percent_change",
    "position_qty", "avg_cost", "position_cost", "position_value", "unrealized_pl", "unrealized_pl_pct",
    "market_cap", "volume", "exchange", "currency", "country", "sector", "industry",
    "beta_5y", "pe_ttm", "eps_ttm", "dividend_yield", "revenue_ttm", "gross_margin",
    "operating_margin", "profit_margin", "debt_to_equity", "rsi_14", "volatility_30d",
    "risk_score", "risk_bucket", "pb_ratio", "ps_ratio", "ev_ebitda", "peg_ratio", "intrinsic_value",
    "forecast_price_1m", "forecast_price_3m", "forecast_price_12m",
    "expected_roi_1m", "expected_roi_3m", "expected_roi_12m", "recommendation",
    "last_updated_riyadh",
]

_INSIGHTS_HEADERS: List[str] = [
    "Section", "Item", "Symbol", "Metric", "Value",
    "Recommendation", "Confidence", "Notes", "Source", "Last Updated (Riyadh)",
]

_INSIGHTS_KEYS: List[str] = [
    "section", "item", "symbol", "metric", "value",
    "recommendation", "confidence", "notes", "source", "last_updated_riyadh",
]

_DICTIONARY_HEADERS: List[str] = [
    "Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes",
]

_DICTIONARY_KEYS: List[str] = [
    "sheet", "group", "header", "key", "dtype", "fmt", "required", "source", "notes",
]

EMERGENCY_PAGE_SYMBOLS: Dict[str, List[str]] = {
    "Market_Leaders": ["2222.SR", "1120.SR", "2010.SR", "7010.SR", "AAPL", "MSFT", "NVDA", "GOOGL"],
    "Global_Markets": ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AVGO"],
    "Commodities_FX": ["GC=F", "BZ=F", "SI=F", "EURUSD=X", "GBPUSD=X", "JPY=X", "SAR=X", "CL=F"],
    "Mutual_Funds": ["SPY", "QQQ", "VTI", "VOO", "IWM"],
    "My_Portfolio": ["2222.SR", "AAPL", "MSFT", "QQQ", "GC=F"],
    "My_Investments": ["2222.SR", "AAPL", "MSFT"],
    _INSIGHTS_PAGE: ["2222.SR", "AAPL", "GC=F"],
    _TOP10_PAGE: ["2222.SR", "1120.SR", "AAPL", "MSFT", "NVDA"],
}

_FIELD_ALIAS_HINTS: Dict[str, List[str]] = {
    "symbol": ["ticker", "code", "instrument", "requested_symbol"],
    "name": ["short_name", "long_name", "instrument_name"],
    "current_price": ["price", "last_price", "last", "close", "market_price", "value", "nav"],
    "open_price": ["open"],
    "week_52_high": ["fiftyTwoWeekHigh", "high_52w", "52_week_high"],
    "week_52_low": ["fiftyTwoWeekLow", "low_52w", "52_week_low"],
    "percent_change": ["pct_change", "change_pct", "percentChange"],
    "top10_rank": ["rank", "top_rank"],
    "selection_reason": ["reason", "selection_notes"],
    "criteria_snapshot": ["criteria", "snapshot", "criteria_json"],
    "position_qty": ["qty", "quantity"],
    "avg_cost": ["average_cost", "cost_basis"],
    "position_value": ["market_value", "current_value"],
    "unrealized_pl": ["upl", "unrealized_profit_loss"],
}

_DEFAULT_UPSTREAM_TIMEOUT_SEC = 10.0
_DEFAULT_SPECIAL_TIMEOUT_SEC = 8.0
_DEFAULT_INSIGHTS_TIMEOUT_SEC = 6.0
_DEFAULT_TOP10_TIMEOUT_SEC = 8.0


# =============================================================================
# Generic helpers
# =============================================================================
def _strip(v: Any) -> str:
    try:
        s = str(v).strip()
        return "" if s.lower() in {"none", "null"} else s
    except Exception:
        return ""


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return None if (math.isnan(value) or math.isinf(value)) else value
    if isinstance(value, Decimal):
        try:
            f = float(value)
            return None if (math.isnan(f) or math.isinf(f)) else f
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
            return _json_safe(value.model_dump(mode="python"))  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        if hasattr(value, "dict") and callable(getattr(value, "dict")):
            return _json_safe(value.dict())  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        if is_dataclass(value):
            return _json_safe(getattr(value, "__dict__", {}))
    except Exception:
        pass
    try:
        return _json_safe(vars(value))
    except Exception:
        return str(value)


async def _maybe_await(x: Any) -> Any:
    try:
        if inspect.isawaitable(x):
            return await x
    except Exception:
        pass
    return x


def _as_list(v: Any) -> List[Any]:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, tuple):
        return list(v)
    if isinstance(v, set):
        return list(v)
    if isinstance(v, str):
        return [v]
    if isinstance(v, Iterable) and not isinstance(v, Mapping):
        try:
            return list(v)
        except Exception:
            return [v]
    return [v]


def _maybe_bool(v: Any, default: bool) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        try:
            return bool(int(v))
        except Exception:
            return default
    s = _strip(v).lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _maybe_int(v: Any, default: int) -> int:
    try:
        if v is None or isinstance(v, bool):
            return default
        if isinstance(v, int):
            return v
        if isinstance(v, float):
            return int(v)
        s = _strip(v)
        return default if not s else int(float(s))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    raw = _strip(os.getenv(name))
    if not raw:
        return default
    try:
        value = float(raw)
        return value if value > 0 else default
    except Exception:
        return default


def _page_upstream_timeout(page: str) -> float:
    page_key = _strip(page).upper()
    specific_name = f"ADVANCED_ANALYSIS_TIMEOUT_{page_key}_SEC"
    if _strip(os.getenv(specific_name)):
        return _env_float(specific_name, _DEFAULT_SPECIAL_TIMEOUT_SEC)
    if page == _INSIGHTS_PAGE:
        return _env_float("ADVANCED_ANALYSIS_INSIGHTS_TIMEOUT_SEC", _DEFAULT_INSIGHTS_TIMEOUT_SEC)
    if page == _TOP10_PAGE:
        return _env_float("ADVANCED_ANALYSIS_TOP10_TIMEOUT_SEC", _DEFAULT_TOP10_TIMEOUT_SEC)
    if page in _SPECIAL_PAGES:
        return _env_float("ADVANCED_ANALYSIS_SPECIAL_TIMEOUT_SEC", _DEFAULT_SPECIAL_TIMEOUT_SEC)
    return _env_float("ADVANCED_ANALYSIS_UPSTREAM_TIMEOUT_SEC", _DEFAULT_UPSTREAM_TIMEOUT_SEC)


def _split_symbols_string(v: str) -> List[str]:
    raw = (v or "").replace(";", ",").replace("\n", ",").replace("\t", ",").replace(" ", ",")
    out: List[str] = []
    seen = set()
    for p in [x.strip() for x in raw.split(",") if x.strip()]:
        if p not in seen:
            seen.add(p)
            out.append(p)
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


def _get_list(body: Mapping[str, Any], *keys: str) -> List[str]:
    for k in keys:
        v = body.get(k)
        if isinstance(v, list):
            out: List[str] = []
            seen = set()
            for item in v:
                s = _normalize_symbol_token(item) if "symbol" in k or "ticker" in k or k in {"code", "requested_symbol"} else _strip(item)
                if s and s not in seen:
                    seen.add(s)
                    out.append(s)
            if out:
                return out
        if isinstance(v, str) and v.strip():
            parts = _split_symbols_string(v)
            if "symbol" in k or "ticker" in k or k in {"code", "requested_symbol"}:
                parts = [_normalize_symbol_token(x) for x in parts if _normalize_symbol_token(x)]
            if parts:
                return parts
    return []


def _extract_requested_symbols(body: Mapping[str, Any], limit: int) -> List[str]:
    symbols: List[str] = []
    for key in (
        "symbols", "tickers", "tickers_list", "selected_symbols", "selected_tickers", "direct_symbols",
        "symbol", "ticker", "code", "requested_symbol",
    ):
        symbols.extend(_get_list(body, key))
    out: List[str] = []
    seen = set()
    for sym in symbols:
        s = _normalize_symbol_token(sym)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
        if len(out) >= limit:
            break
    return out


def _pick_page_from_body(body: Mapping[str, Any]) -> str:
    for k in ("sheet", "page", "sheet_name", "sheetName", "page_name", "pageName", "worksheet", "name", "tab"):
        s = _strip(body.get(k))
        if s:
            return s
    return ""


def _collect_get_body(request: Request) -> Dict[str, Any]:
    qp = request.query_params
    body: Dict[str, Any] = {}
    for key in ("sheet", "page", "sheet_name", "sheetName", "page_name", "pageName", "worksheet", "name", "tab"):
        v = _strip(qp.get(key))
        if v:
            body[key] = v
    for key in ("symbols", "tickers", "tickers_list", "selected_symbols", "selected_tickers", "direct_symbols", "symbol", "ticker", "code", "requested_symbol"):
        vals = qp.getlist(key)
        if vals:
            body[key] = _split_symbols_string(vals[0]) if len(vals) == 1 else [s.strip() for s in vals if _strip(s)]
    for key in ("limit", "offset", "top_n", "include_matrix", "schema_only", "headers_only"):
        v = qp.get(key)
        if v is not None:
            body[key] = v
    return body


def _merge_body_with_query(body: Optional[Dict[str, Any]], request: Request) -> Dict[str, Any]:
    out = dict(body or {})
    for k, v in _collect_get_body(request).items():
        if k not in out or out.get(k) in (None, "", []):
            out[k] = v
    return out


def _allow_query_token(settings: Any, request: Request) -> bool:
    try:
        if settings is not None:
            return bool(getattr(settings, "ALLOW_QUERY_TOKEN", False) or getattr(settings, "allow_query_token", False))
    except Exception:
        pass
    if (os.getenv("ALLOW_QUERY_TOKEN", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}:
        return True
    try:
        if _strip(request.headers.get("X-Allow-Query-Token")).lower() in {"1", "true", "yes"}:
            return True
    except Exception:
        pass
    return False


def _extract_auth_token(
    *,
    token_query: Optional[str],
    x_app_token: Optional[str],
    x_api_key: Optional[str],
    authorization: Optional[str],
    settings: Any,
    request: Request,
) -> str:
    auth_token = _strip(x_app_token) or _strip(x_api_key)
    if authorization and authorization.strip().lower().startswith("bearer "):
        auth_token = authorization.strip().split(" ", 1)[1].strip()
    if token_query and not auth_token and _allow_query_token(settings, request):
        auth_token = _strip(token_query)
    return auth_token


def _auth_passed(*, request: Request, settings: Any, auth_token: str, authorization: Optional[str]) -> bool:
    if auth_ok is None:
        return True
    try:
        if callable(is_open_mode) and bool(is_open_mode()):
            return True
    except Exception:
        pass

    headers_dict = dict(request.headers)
    path = str(getattr(getattr(request, "url", None), "path", "") or "")
    attempts = [
        {"token": auth_token, "authorization": authorization, "headers": headers_dict, "path": path, "request": request, "settings": settings},
        {"token": auth_token, "authorization": authorization, "headers": headers_dict, "path": path, "request": request},
        {"token": auth_token, "authorization": authorization, "headers": headers_dict, "path": path},
        {"token": auth_token, "authorization": authorization, "headers": headers_dict},
        {"token": auth_token, "authorization": authorization},
        {"token": auth_token},
    ]
    for kwargs in attempts:
        try:
            return bool(auth_ok(**kwargs))
        except TypeError:
            continue
        except Exception:
            return False
    return False


def _normalize_page_flexible(page_raw: str) -> str:
    raw = _strip(page_raw)
    if not raw:
        return "Market_Leaders"
    for kwargs in ({"allow_output_pages": True}, {}):
        try:
            value = normalize_page_name(raw, **kwargs)  # type: ignore[misc]
            normalized = _strip(value)
            if normalized:
                return normalized
        except TypeError:
            continue
        except Exception:
            break
    return raw.replace(" ", "_")


def _safe_allowed_pages() -> List[str]:
    try:
        pages = allowed_pages()
        if isinstance(pages, list):
            return pages
        if isinstance(pages, tuple):
            return list(pages)
    except Exception:
        pass
    return list(CANONICAL_PAGES or [])


def _ensure_page_allowed(page: str) -> None:
    forbidden = set(FORBIDDEN_PAGES or set())
    if page in forbidden:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": f"Forbidden/removed page: {page}"},
        )
    ap = _safe_allowed_pages()
    if ap and page not in set(ap):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": f"Unknown page: {page}", "allowed_pages": ap},
        )


def _normalize_key_name(header: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", _strip(header).lower()).strip("_")


def _complete_schema_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    raw_headers = list(headers or [])
    raw_keys = list(keys or [])
    max_len = max(len(raw_headers), len(raw_keys))
    hdrs: List[str] = []
    ks: List[str] = []
    for i in range(max_len):
        h = _strip(raw_headers[i]) if i < len(raw_headers) else ""
        k = _strip(raw_keys[i]) if i < len(raw_keys) else ""
        if h and not k:
            k = _normalize_key_name(h)
        elif k and not h:
            h = k.replace("_", " ").title()
        elif not h and not k:
            h = f"Column {i + 1}"
            k = f"column_{i + 1}"
        hdrs.append(h)
        ks.append(k)
    return hdrs, ks


def _pad_contract(
    headers: Sequence[str],
    keys: Sequence[str],
    expected_len: int,
    *,
    header_prefix: str = "Column",
    key_prefix: str = "column",
) -> Tuple[List[str], List[str]]:
    hdrs, ks = _complete_schema_contract(headers, keys)
    while len(hdrs) < expected_len:
        i = len(hdrs) + 1
        hdrs.append(f"{header_prefix} {i}")
        ks.append(f"{key_prefix}_{i}")
    return hdrs[:expected_len], ks[:expected_len]


def _ensure_top10_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    hdrs, ks = _complete_schema_contract(headers, keys)
    for field in _TOP10_REQUIRED_FIELDS:
        if field not in ks:
            ks.append(field)
            hdrs.append(_TOP10_REQUIRED_HEADERS[field])
    return _pad_contract(hdrs, ks, 83)


def _static_contract(page: str) -> Tuple[List[str], List[str], str]:
    if page == _TOP10_PAGE:
        h, k = _ensure_top10_contract(_CANONICAL_80_HEADERS, _CANONICAL_80_KEYS)
        return h, k, "static_canonical_top10"
    if page == _INSIGHTS_PAGE:
        h, k = _pad_contract(_INSIGHTS_HEADERS, _INSIGHTS_KEYS, 10)
        return h, k, "static_canonical_insights"
    if page == _DICTIONARY_PAGE:
        h, k = _pad_contract(_DICTIONARY_HEADERS, _DICTIONARY_KEYS, 9)
        return h, k, "static_canonical_dictionary"
    if page == "My_Portfolio":
        h, k = _pad_contract(_MY_PORTFOLIO_45_HEADERS, _MY_PORTFOLIO_45_KEYS, 45)
        return h, k, "static_canonical_my_portfolio"
    h, k = _pad_contract(_CANONICAL_80_HEADERS, _CANONICAL_80_KEYS, _EXPECTED_SHEET_LENGTHS.get(page, 80))
    return h, k, "static_canonical_instrument"


def _expected_len(page: str) -> int:
    if callable(get_sheet_len):
        try:
            n = int(get_sheet_len(page))  # type: ignore[misc]
            if n > 0:
                return n
        except Exception:
            pass
    return _EXPECTED_SHEET_LENGTHS.get(page, 80)


def _extract_headers_keys_from_spec(spec: Any) -> Tuple[List[str], List[str]]:
    headers: List[str] = []
    keys: List[str] = []

    if isinstance(spec, Mapping):
        headers2 = spec.get("headers") or spec.get("display_headers") or spec.get("sheet_headers")
        keys2 = spec.get("keys") or spec.get("fields") or spec.get("columns")
        if isinstance(headers2, list):
            headers = [_strip(x) for x in headers2 if _strip(x)]
        if isinstance(keys2, list):
            keys = [_strip(x) for x in keys2 if _strip(x)]
        if headers or keys:
            return _complete_schema_contract(headers, keys)

        cols = spec.get("columns") or spec.get("fields")
        if isinstance(cols, list):
            for c in cols:
                if isinstance(c, Mapping):
                    h = _strip(c.get("header") or c.get("display_header") or c.get("label") or c.get("title"))
                    k = _strip(c.get("key") or c.get("field") or c.get("name") or c.get("id"))
                    if h or k:
                        headers.append(h or k.replace("_", " ").title())
                        keys.append(k or _normalize_key_name(h))

    return _complete_schema_contract(headers, keys)


def _schema_from_registry(page: str) -> Tuple[List[str], List[str], Any, str]:
    spec = None

    if callable(get_sheet_headers) and callable(get_sheet_keys):
        try:
            headers = [_strip(x) for x in get_sheet_headers(page) if _strip(x)]  # type: ignore[misc]
            keys = [_strip(x) for x in get_sheet_keys(page) if _strip(x)]  # type: ignore[misc]
            if headers and keys:
                if callable(get_sheet_spec):
                    try:
                        spec = get_sheet_spec(page)  # type: ignore[misc]
                    except Exception:
                        spec = None
                ch, ck = _complete_schema_contract(headers, keys)
                return ch, ck, spec, "schema_registry.helpers"
        except Exception:
            pass

    if get_sheet_spec is None:
        return [], [], None, "registry_unavailable"

    try:
        spec = get_sheet_spec(page)  # type: ignore[misc]
    except Exception as e:
        return [], [], None, f"registry_error:{e}"

    headers, keys = _extract_headers_keys_from_spec(spec)
    return headers, keys, spec, "schema_registry.spec"


def _resolve_contract(page: str) -> Tuple[List[str], List[str], Any, str]:
    expected_len = _expected_len(page)
    headers, keys, spec, source = _schema_from_registry(page)

    if headers and keys:
        headers, keys = _complete_schema_contract(headers, keys)
        if page == _TOP10_PAGE:
            headers, keys = _ensure_top10_contract(headers, keys)
        else:
            headers, keys = _pad_contract(headers, keys, expected_len)
        return headers, keys, spec, source

    sh, sk, ssrc = _static_contract(page)
    return sh, sk, {"source": ssrc, "page": page}, ssrc


def _rows_to_matrix(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    return [[_json_safe(r.get(k)) for k in keys] for r in rows]


def _slice(rows: List[Dict[str, Any]], *, limit: int, offset: int) -> List[Dict[str, Any]]:
    start = max(0, int(offset))
    if limit <= 0:
        return rows[start:]
    return rows[start:start + max(0, int(limit))]


def _key_variants(key: str) -> List[str]:
    k = _strip(key)
    if not k:
        return []
    variants = [k, k.lower(), k.upper(), k.replace("_", " "), k.replace("_", "").lower()]
    for alias in _FIELD_ALIAS_HINTS.get(k, []):
        variants.extend([alias, alias.lower(), alias.upper(), alias.replace("_", " "), alias.replace("_", "").lower()])
    seen = set()
    out: List[str] = []
    for v in variants:
        if v and v not in seen:
            seen.add(v)
            out.append(v)
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


def _to_plain_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return dict(obj)
    try:
        if hasattr(obj, "model_dump") and callable(getattr(obj, "model_dump")):
            d = obj.model_dump(mode="python")  # type: ignore[attr-defined]
            return d if isinstance(d, dict) else {}
    except Exception:
        pass
    try:
        if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
            d = obj.dict()  # type: ignore[attr-defined]
            return d if isinstance(d, dict) else {}
    except Exception:
        pass
    try:
        dd = getattr(obj, "__dict__", None)
        if isinstance(dd, dict):
            return dict(dd)
    except Exception:
        pass
    return {}


def _extract_rows_like(payload: Any, depth: int = 0) -> List[Dict[str, Any]]:
    if payload is None or depth > 6:
        return []

    if isinstance(payload, list):
        if payload and isinstance(payload[0], Mapping):
            return [dict(x) for x in payload]
        return []

    if not isinstance(payload, Mapping):
        return []

    for name in ("row_objects", "records", "items", "data", "quotes", "results"):
        value = payload.get(name)
        if isinstance(value, list) and value and isinstance(value[0], Mapping):
            return [dict(x) for x in value]

    rows_value = payload.get("rows")
    if isinstance(rows_value, list) and rows_value and isinstance(rows_value[0], Mapping):
        return [dict(x) for x in rows_value]

    for name in ("payload", "result", "response", "output", "data"):
        nested = payload.get(name)
        if isinstance(nested, Mapping):
            found = _extract_rows_like(nested, depth + 1)
            if found:
                return found

    return []


def _extract_rows_matrix_like(payload: Any, depth: int = 0) -> List[List[Any]]:
    if payload is None or depth > 6:
        return []

    if isinstance(payload, Mapping):
        for name in ("rows_matrix", "matrix"):
            value = payload.get(name)
            if isinstance(value, list) and value and isinstance(value[0], list):
                return [list(x) for x in value]

        rows_value = payload.get("rows")
        if isinstance(rows_value, list) and rows_value and isinstance(rows_value[0], list):
            return [list(x) for x in rows_value]

        for name in ("payload", "result", "response", "output", "data"):
            nested = payload.get(name)
            if isinstance(nested, Mapping):
                found = _extract_rows_matrix_like(nested, depth + 1)
                if found:
                    return found

    return []


def _rows_from_matrix(matrix: Sequence[Sequence[Any]], keys: Sequence[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in matrix or []:
        row_list = list(row or [])
        obj: Dict[str, Any] = {}
        for idx, key in enumerate(keys):
            obj[str(key)] = _json_safe(row_list[idx] if idx < len(row_list) else None)
        out.append(obj)
    return out


def _extract_status_error(payload: Any) -> Tuple[str, Optional[str], Dict[str, Any]]:
    if not isinstance(payload, Mapping):
        return "success", None, {}
    status_out = _strip(payload.get("status")) or "success"
    error_out = payload.get("error") or payload.get("detail") or payload.get("message")
    if isinstance(error_out, Mapping):
        try:
            error_out = json.dumps(_json_safe(error_out), ensure_ascii=False)
        except Exception:
            error_out = str(error_out)
    meta_out = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    return status_out, (str(error_out) if error_out is not None else None), meta_out


def _normalize_to_schema_keys(*, schema_keys: Sequence[str], schema_headers: Sequence[str], raw: Mapping[str, Any]) -> Dict[str, Any]:
    raw = dict(raw or {})
    header_by_key = {str(k): str(h) for k, h in zip(schema_keys, schema_headers)}
    out: Dict[str, Any] = {}

    for k in schema_keys:
        ks = str(k)
        v = _extract_from_raw(raw, _key_variants(ks))
        if v is None:
            h = header_by_key.get(ks, "")
            if h:
                v = _extract_from_raw(raw, [h, h.lower(), h.upper()])
        if ks in {"warnings", "recommendation_reason", "selection_reason"} and isinstance(v, (list, tuple, set)):
            v = "; ".join([_strip(x) for x in v if _strip(x)])
        out[ks] = _json_safe(v)

    return out


def _to_number(value: Any) -> float:
    if value is None:
        return float("-inf")
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        try:
            f = float(value)
            return f if math.isfinite(f) else float("-inf")
        except Exception:
            return float("-inf")
    s = _strip(value)
    if not s:
        return float("-inf")
    s = s.replace("%", "").replace(",", "")
    try:
        f = float(s)
        return f if math.isfinite(f) else float("-inf")
    except Exception:
        return float("-inf")


def _top10_sort_key(row: Mapping[str, Any]) -> Tuple[float, ...]:
    return (
        _to_number(row.get("overall_score")),
        _to_number(row.get("opportunity_score")),
        _to_number(row.get("expected_roi_3m")),
        _to_number(row.get("expected_roi_1m")),
        _to_number(row.get("forecast_confidence")),
        _to_number(row.get("confidence_score")),
    )


def _top10_selection_reason(row: Mapping[str, Any]) -> str:
    parts: List[str] = []
    labels = (
        ("overall_score", "Overall"),
        ("opportunity_score", "Opportunity"),
        ("expected_roi_3m", "Exp ROI 3M"),
        ("forecast_confidence", "Forecast Conf"),
    )
    for key, label in labels:
        value = row.get(key)
        if value in (None, "", [], {}, ()):
            continue
        parts.append(f"{label} {round(value, 2) if isinstance(value, float) else value}")
        if len(parts) >= 3:
            break
    return " | ".join(parts) if parts else "Top10 fallback selection based on strongest available composite signals."


def _top10_criteria_snapshot(row: Mapping[str, Any]) -> str:
    snapshot = {}
    for key in (
        "overall_score", "opportunity_score", "expected_roi_1m", "expected_roi_3m",
        "forecast_confidence", "confidence_score", "risk_bucket", "recommendation", "symbol",
    ):
        value = row.get(key)
        if value not in (None, "", [], {}, ()):
            snapshot[key] = _json_safe(value)
    try:
        return json.dumps(snapshot, ensure_ascii=False, sort_keys=True)
    except Exception:
        return str(snapshot)


def _ensure_top10_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    requested_symbols: Sequence[str],
    top_n: int,
    schema_keys: Sequence[str],
    schema_headers: Sequence[str],
) -> List[Dict[str, Any]]:
    normalized_rows = [
        _normalize_to_schema_keys(schema_keys=schema_keys, schema_headers=schema_headers, raw=(r or {}))
        for r in rows or []
    ]

    deduped: List[Dict[str, Any]] = []
    seen = set()
    for row in sorted(normalized_rows, key=_top10_sort_key, reverse=True):
        sym = _strip(row.get("symbol"))
        name = _strip(row.get("name"))
        key = sym or name or f"row_{len(deduped)+1}"
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    final_rows = deduped[:max(1, int(top_n))]
    for idx, row in enumerate(final_rows, start=1):
        row["top10_rank"] = idx
        if not _strip(row.get("selection_reason")):
            row["selection_reason"] = _top10_selection_reason(row)
        if not _strip(row.get("criteria_snapshot")):
            row["criteria_snapshot"] = _top10_criteria_snapshot(row)
    return final_rows


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
        return "advanced_analysis.placeholder_fallback"
    if kk in {"last_updated_utc", "last_updated_riyadh"}:
        return datetime.utcnow().isoformat()
    if kk == "recommendation":
        return "HOLD" if row_index > 3 else "BUY"
    if kk == "recommendation_reason":
        return "Placeholder fallback because live engine returned no usable rows."
    if kk in {"top10_rank", "rank_overall"}:
        return row_index
    if kk == "selection_reason":
        return "Placeholder fallback because upstream builders returned no usable rows."
    if kk == "criteria_snapshot":
        return json.dumps({"symbol": symbol, "row_index": row_index, "source": "placeholder"}, ensure_ascii=False)
    if kk in {"warnings", "notes"}:
        return "placeholder"
    if kk in {"recommendation", "source"}:
        return "advanced_analysis.local_fallback"
    if kk in {"current_price", "previous_close", "open_price", "day_high", "day_low", "forecast_price_1m", "forecast_price_3m", "forecast_price_12m", "avg_cost", "position_cost", "position_value", "unrealized_pl", "intrinsic_value"}:
        base = 100.0 + float(row_index)
        return round(base, 2)
    if kk in {"percent_change", "expected_roi_1m", "expected_roi_3m", "expected_roi_12m", "forecast_confidence", "confidence_score", "confidence", "overall_score", "opportunity_score"}:
        return round(max(1.0, 100.0 - float(row_index * 3)), 2)
    if kk in {"risk_bucket", "confidence_bucket"}:
        return "Moderate" if row_index > 3 else "High Confidence"
    if kk == "invest_period_label":
        return "3M"
    if kk == "horizon_days":
        return 90
    if kk == "value":
        return row_index
    return None


def _build_placeholder_rows(
    *,
    page: str,
    keys: Sequence[str],
    requested_symbols: Sequence[str],
    limit: int,
    offset: int,
) -> List[Dict[str, Any]]:
    symbols = [_normalize_symbol_token(x) for x in requested_symbols if _normalize_symbol_token(x)]
    if not symbols:
        symbols = [_normalize_symbol_token(x) for x in EMERGENCY_PAGE_SYMBOLS.get(page, []) if _normalize_symbol_token(x)]
    symbols = symbols[offset: offset + limit] if (offset or len(symbols) > limit) else symbols[:limit]

    rows: List[Dict[str, Any]] = []
    for idx, sym in enumerate(symbols, start=offset + 1):
        row = {str(k): _placeholder_value_for_key(page, str(k), sym, idx) for k in keys}
        rows.append(row)

    if page == _TOP10_PAGE:
        for idx, row in enumerate(rows, start=offset + 1):
            row["top10_rank"] = idx
            row.setdefault("selection_reason", "Placeholder fallback because upstream builders returned no usable rows.")
            row.setdefault("criteria_snapshot", "{}")

    return rows


def _build_dictionary_fallback_rows(*, page: str, headers: Sequence[str], keys: Sequence[str], limit: int, offset: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for idx, (header, key) in enumerate(zip(headers, keys), start=1):
        rows.append({
            "sheet": page,
            "group": "Core Contract",
            "header": header,
            "key": key,
            "dtype": "number" if any(token in key for token in ("price", "score", "roi", "qty", "value", "cap", "volume", "margin")) else "text",
            "fmt": "" if "score" not in key and "roi" not in key else "0.00",
            "required": key in {"sheet", "header", "key", "symbol", "name", "current_price"},
            "source": "advanced_analysis.local_dictionary_fallback",
            "notes": f"Auto-generated fallback row {idx} from schema contract",
        })
    return _slice(rows, limit=limit, offset=offset)


def _build_insights_fallback_rows(*, requested_symbols: Sequence[str], limit: int, offset: int) -> List[Dict[str, Any]]:
    symbols = [_normalize_symbol_token(x) for x in requested_symbols if _normalize_symbol_token(x)]
    if not symbols:
        symbols = [_normalize_symbol_token(x) for x in EMERGENCY_PAGE_SYMBOLS.get(_INSIGHTS_PAGE, []) if _normalize_symbol_token(x)]

    stamp = datetime.utcnow().isoformat()
    rows: List[Dict[str, Any]] = [
        {
            "section": "Coverage",
            "item": "Requested symbols",
            "symbol": "",
            "metric": "count",
            "value": len(symbols),
            "recommendation": "",
            "confidence": 100,
            "notes": "Local insights fallback summary",
            "source": "advanced_analysis.local_fallback",
            "last_updated_riyadh": stamp,
        },
        {
            "section": "Coverage",
            "item": "Universe sample",
            "symbol": "",
            "metric": "symbols",
            "value": ", ".join(symbols[:5]),
            "recommendation": "",
            "confidence": 100,
            "notes": "Sample of the symbols used by fallback mode",
            "source": "advanced_analysis.local_fallback",
            "last_updated_riyadh": stamp,
        },
    ]
    for idx, sym in enumerate(symbols[: max(1, limit + offset)], start=1):
        rows.append({
            "section": "Signals",
            "item": f"Fallback signal {idx}",
            "symbol": sym,
            "metric": "recommendation",
            "value": "HOLD" if idx > 2 else "BUY",
            "recommendation": "HOLD" if idx > 2 else "BUY",
            "confidence": round(max(30, 95 - idx * 7), 2),
            "notes": "Generated locally because upstream insights payload was unavailable",
            "source": "advanced_analysis.local_fallback",
            "last_updated_riyadh": stamp,
        })
    return _slice(rows, limit=limit, offset=offset)


def _build_nonempty_failsoft_rows(
    *,
    page: str,
    headers: Sequence[str],
    keys: Sequence[str],
    requested_symbols: Sequence[str],
    limit: int,
    offset: int,
    top_n: int,
) -> List[Dict[str, Any]]:
    if page == _DICTIONARY_PAGE:
        return _build_dictionary_fallback_rows(page=page, headers=headers, keys=keys, limit=limit, offset=offset)
    if page == _INSIGHTS_PAGE:
        return _build_insights_fallback_rows(requested_symbols=requested_symbols, limit=limit, offset=offset)
    if page == _TOP10_PAGE:
        rows = _build_placeholder_rows(
            page=page,
            keys=keys,
            requested_symbols=requested_symbols or EMERGENCY_PAGE_SYMBOLS.get(page, []),
            limit=max(limit, top_n),
            offset=0,
        )
        rows = _ensure_top10_rows(
            rows,
            requested_symbols=requested_symbols,
            top_n=top_n,
            schema_keys=keys,
            schema_headers=headers,
        )
        return _slice(rows, limit=limit, offset=offset)
    return _build_placeholder_rows(
        page=page,
        keys=keys,
        requested_symbols=requested_symbols or EMERGENCY_PAGE_SYMBOLS.get(page, []),
        limit=limit,
        offset=offset,
    )


def _payload_envelope(
    *,
    page: str,
    headers: Sequence[str],
    keys: Sequence[str],
    row_objects: Sequence[Mapping[str, Any]],
    include_matrix: bool,
    request_id: str,
    started_at: float,
    mode: str,
    status_out: str,
    error_out: Optional[str],
    meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    hdrs = list(headers or [])
    ks = list(keys or [])
    rows_dict = [{str(k): _json_safe(dict(r).get(k)) for k in ks} for r in (row_objects or [])]
    matrix = _rows_to_matrix(rows_dict, ks) if include_matrix else []

    return _json_safe({
        "status": status_out,
        "page": page,
        "sheet": page,
        "sheet_name": page,
        "route_family": "root_schema",
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
        "count": len(rows_dict),
        "detail": error_out or "",
        "error": error_out,
        "version": ADVANCED_ANALYSIS_VERSION,
        "request_id": request_id,
        "meta": {
            "duration_ms": round((time.time() - started_at) * 1000.0, 3),
            "mode": mode,
            "count": len(rows_dict),
            "dispatch": "advanced_analysis_root",
            "source": _CORE_GET_ROWS_SOURCE,
            **(meta or {}),
        },
    })


# =============================================================================
# Core adapter
# =============================================================================
async def _call_core_maybe_async(fn: Any, *args: Any, **kwargs: Any) -> Any:
    if inspect.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)
    result = fn(*args, **kwargs)
    return await _maybe_await(result)


async def _call_core_sheet_rows_best_effort(
    *,
    page: str,
    limit: int,
    offset: int,
    mode: str,
    body: Dict[str, Any],
    timeout_sec: Optional[float] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if core_get_sheet_rows is None:
        return None, None

    timeout_value = timeout_sec or _page_upstream_timeout(page)

    candidates = [
        ((), {"sheet": page, "limit": limit, "offset": offset, "mode": mode, "body": body}),
        ((), {"sheet": page, "limit": limit, "offset": offset, "mode": mode}),
        ((), {"sheet": page, "limit": limit, "offset": offset}),
        ((page,), {"limit": limit, "offset": offset, "mode": mode, "body": body}),
        ((page,), {"limit": limit, "offset": offset, "mode": mode}),
        ((page,), {"limit": limit, "offset": offset}),
        ((page,), {}),
    ]

    last_err: Optional[Exception] = None

    for args, kwargs in candidates:
        try:
            res = await asyncio.wait_for(
                _call_core_maybe_async(core_get_sheet_rows, *args, **kwargs),
                timeout=timeout_value,
            )
            if isinstance(res, dict):
                return res, _CORE_GET_ROWS_SOURCE
            if isinstance(res, list):
                return {"row_objects": res}, _CORE_GET_ROWS_SOURCE
        except TypeError as e:
            last_err = e
            continue
        except Exception as e:
            last_err = e
            break

    if last_err is not None:
        return {"status": "error", "error": str(last_err), "row_objects": []}, _CORE_GET_ROWS_SOURCE
    return None, None


def _normalize_external_payload(
    *,
    external_payload: Mapping[str, Any],
    page: str,
    headers: Sequence[str],
    keys: Sequence[str],
    include_matrix: bool,
    request_id: str,
    started_at: float,
    mode: str,
    limit: int = 2000,
    offset: int = 0,
    top_n: int = 2000,
    requested_symbols: Optional[Sequence[str]] = None,
    meta_extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    ext = dict(external_payload or {})
    hdrs = list(headers or [])
    ks = list(keys or [])

    rows = _extract_rows_like(ext)
    if not rows:
        matrix_rows = _extract_rows_matrix_like(ext)
        if matrix_rows:
            rows = _rows_from_matrix(matrix_rows, ks)

    normalized_rows = [
        _normalize_to_schema_keys(schema_keys=ks, schema_headers=hdrs, raw=(r or {}))
        for r in rows
    ]

    if page == _TOP10_PAGE:
        normalized_rows = _ensure_top10_rows(
            normalized_rows,
            requested_symbols=requested_symbols or [],
            top_n=top_n,
            schema_keys=ks,
            schema_headers=hdrs,
        )

    normalized_rows = _slice(normalized_rows, limit=limit, offset=offset)
    status_out, error_out, ext_meta = _extract_status_error(ext)

    if not normalized_rows:
        status_out = "partial"
        error_out = error_out or "No usable rows returned"

    final_meta = dict(ext_meta or {})
    if meta_extra:
        final_meta.update(meta_extra)

    return _payload_envelope(
        page=page,
        headers=hdrs,
        keys=ks,
        row_objects=normalized_rows,
        include_matrix=include_matrix,
        request_id=request_id,
        started_at=started_at,
        mode=mode,
        status_out=status_out or ("success" if normalized_rows else "partial"),
        error_out=error_out,
        meta=final_meta,
    )


# =============================================================================
# Main implementation
# =============================================================================
async def _run_advanced_sheet_rows_impl(
    request: Request,
    body: Dict[str, Any],
    mode: str = "",
    include_matrix_q: Optional[bool] = None,
    token: Optional[str] = None,
    x_app_token: Optional[str] = None,
    x_api_key: Optional[str] = None,
    authorization: Optional[str] = None,
    x_request_id: Optional[str] = None,
) -> Dict[str, Any]:
    start = time.time()
    request_id = _strip(x_request_id) or str(uuid.uuid4())[:12]

    try:
        settings = get_settings_cached()
    except Exception:
        settings = None

    auth_token = _extract_auth_token(
        token_query=token,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
        settings=settings,
        request=request,
    )
    if not _auth_passed(request=request, settings=settings, auth_token=auth_token, authorization=authorization):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    merged_body = _merge_body_with_query(body, request)
    page = _normalize_page_flexible(_pick_page_from_body(merged_body) or "Market_Leaders")
    _ensure_page_allowed(page)

    include_matrix = _maybe_bool(
        merged_body.get("include_matrix"),
        include_matrix_q if include_matrix_q is not None else True,
    )
    limit = max(1, min(5000, _maybe_int(merged_body.get("limit"), 2000)))
    offset = max(0, _maybe_int(merged_body.get("offset"), 0))
    top_n = max(1, min(5000, _maybe_int(merged_body.get("top_n"), limit)))
    schema_only = _maybe_bool(merged_body.get("schema_only"), False)
    headers_only = _maybe_bool(merged_body.get("headers_only"), False)
    requested_symbols = _extract_requested_symbols(merged_body, max(top_n, limit + offset, 50))

    headers, keys, spec, schema_source = _resolve_contract(page)

    if schema_only or headers_only:
        return _payload_envelope(
            page=page,
            headers=headers,
            keys=keys,
            row_objects=[],
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=start,
            mode=mode,
            status_out="success",
            error_out=None,
            meta={
                "dispatch": "schema_only",
                "schema_source": schema_source,
                "headers_only": headers_only,
                "schema_only": schema_only,
            },
        )

    payload, source = await _call_core_sheet_rows_best_effort(
        page=page,
        limit=max(limit + offset, top_n),
        offset=0,
        mode=mode or "",
        body=merged_body,
        timeout_sec=_page_upstream_timeout(page),
    )

    if isinstance(payload, dict):
        normalized = _normalize_external_payload(
            external_payload=payload,
            page=page,
            headers=headers,
            keys=keys,
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=start,
            mode=mode,
            limit=limit,
            offset=offset,
            top_n=top_n,
            requested_symbols=requested_symbols,
            meta_extra={
                "schema_source": schema_source,
                "source": source or _CORE_GET_ROWS_SOURCE,
                "upstream_timeout_sec": _page_upstream_timeout(page),
            },
        )
        if normalized.get("count", 0):
            return normalized

    fallback_rows = _build_nonempty_failsoft_rows(
        page=page,
        headers=headers,
        keys=keys,
        requested_symbols=requested_symbols,
        limit=limit,
        offset=offset,
        top_n=top_n,
    )
    fallback_status = "partial" if fallback_rows else "error"
    fallback_error = (
        "Local non-empty fallback emitted after upstream degradation"
        if fallback_rows
        else "No usable rows returned; schema-shaped fallback emitted"
    )

    return _payload_envelope(
        page=page,
        headers=headers,
        keys=keys,
        row_objects=fallback_rows,
        include_matrix=include_matrix,
        request_id=request_id,
        started_at=start,
        mode=mode,
        status_out=fallback_status,
        error_out=fallback_error,
        meta={
            "dispatch": "advanced_analysis_fail_soft_nonempty" if fallback_rows else "advanced_analysis_fail_soft",
            "schema_source": schema_source,
            "source": source or _CORE_GET_ROWS_SOURCE,
            "upstream_timeout_sec": _page_upstream_timeout(page),
        },
    )


# =============================================================================
# Health / schema routes
# =============================================================================
@router.get("/health")
@router.get("/v1/schema/health")
async def advanced_analysis_health(request: Request) -> Dict[str, Any]:
    return _json_safe({
        "status": "ok",
        "service": "advanced_analysis",
        "version": ADVANCED_ANALYSIS_VERSION,
        "schema_registry_available": bool(get_sheet_spec is not None),
        "adapter_available": bool(core_get_sheet_rows is not None),
        "allowed_pages_count": len(_safe_allowed_pages()),
        "path": str(getattr(getattr(request, "url", None), "path", "")),
        "core_source": _CORE_GET_ROWS_SOURCE,
    })


@router.get("/schema")
@router.get("/v1/schema")
async def schema_root() -> Dict[str, Any]:
    return _json_safe({
        "status": "success",
        "version": ADVANCED_ANALYSIS_VERSION,
        "pages": _safe_allowed_pages() or list(_EXPECTED_SHEET_LENGTHS.keys()),
        "aliases": {
            "sheet_rows": ["/sheet-rows", "/sheet_rows"],
            "sheet_spec": ["/schema/sheet-spec", "/schema/sheet_spec", "/v1/schema/sheet-spec", "/v1/schema/sheet_spec"],
            "data_dictionary": ["/schema/data-dictionary", "/schema/data_dictionary", "/v1/schema/data-dictionary", "/v1/schema/data_dictionary"],
        },
    })


@router.get("/schema/pages")
@router.get("/v1/schema/pages")
async def schema_pages() -> Dict[str, Any]:
    pages = _safe_allowed_pages() or list(_EXPECTED_SHEET_LENGTHS.keys())
    return _json_safe({
        "status": "success",
        "pages": pages,
        "count": len(pages),
        "version": ADVANCED_ANALYSIS_VERSION,
    })


def _schema_spec_payload(page: str) -> Dict[str, Any]:
    headers, keys, spec, schema_source = _resolve_contract(page)
    columns = [{"header": h, "key": k} for h, k in zip(headers, keys)]
    return _json_safe({
        "status": "success",
        "page": page,
        "sheet": page,
        "sheet_name": page,
        "headers": headers,
        "display_headers": headers,
        "sheet_headers": headers,
        "column_headers": headers,
        "keys": keys,
        "fields": keys,
        "columns": columns,
        "count": len(keys),
        "meta": {
            "schema_source": schema_source,
            "version": ADVANCED_ANALYSIS_VERSION,
        },
    })


@router.get("/schema/sheet-spec")
@router.get("/schema/sheet_spec")
@router.get("/v1/schema/sheet-spec")
@router.get("/v1/schema/sheet_spec")
async def schema_sheet_spec_get(
    request: Request,
    page: str = Query(default=""),
    sheet: str = Query(default=""),
    sheet_name: str = Query(default=""),
    name: str = Query(default=""),
    tab: str = Query(default=""),
) -> Dict[str, Any]:
    page_name = _normalize_page_flexible(page or sheet or sheet_name or name or tab or "Market_Leaders")
    _ensure_page_allowed(page_name)
    return _schema_spec_payload(page_name)


@router.post("/schema/sheet-spec")
@router.post("/schema/sheet_spec")
@router.post("/v1/schema/sheet-spec")
@router.post("/v1/schema/sheet_spec")
async def schema_sheet_spec_post(body: Dict[str, Any] = Body(default_factory=dict)) -> Dict[str, Any]:
    page_name = _normalize_page_flexible(_pick_page_from_body(body) or "Market_Leaders")
    _ensure_page_allowed(page_name)
    return _schema_spec_payload(page_name)


@router.get("/schema/data-dictionary")
@router.get("/schema/data_dictionary")
@router.get("/v1/schema/data-dictionary")
@router.get("/v1/schema/data_dictionary")
async def schema_data_dictionary() -> Dict[str, Any]:
    payload = _schema_spec_payload(_DICTIONARY_PAGE)
    payload["page"] = _DICTIONARY_PAGE
    payload["sheet"] = _DICTIONARY_PAGE
    payload["sheet_name"] = _DICTIONARY_PAGE
    return payload


# =============================================================================
# Root sheet rows routes
# =============================================================================
@router.get("/sheet-rows")
@router.get("/sheet_rows")
async def root_sheet_rows_get(
    request: Request,
    page: str = Query(default=""),
    sheet: str = Query(default=""),
    sheet_name: str = Query(default=""),
    name: str = Query(default=""),
    tab: str = Query(default=""),
    symbols: str = Query(default=""),
    tickers: str = Query(default=""),
    direct_symbols: str = Query(default=""),
    symbol: str = Query(default=""),
    ticker: str = Query(default=""),
    code: str = Query(default=""),
    requested_symbol: str = Query(default=""),
    limit: Optional[int] = Query(default=None),
    offset: Optional[int] = Query(default=None),
    top_n: Optional[int] = Query(default=None),
    mode: str = Query(default=""),
    include_matrix_q: Optional[bool] = Query(default=None, alias="include_matrix"),
    schema_only: Optional[bool] = Query(default=None),
    headers_only: Optional[bool] = Query(default=None),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    body: Dict[str, Any] = {}
    for k, v in {
        "page": page,
        "sheet": sheet,
        "sheet_name": sheet_name,
        "name": name,
        "tab": tab,
        "symbols": symbols,
        "tickers": tickers,
        "direct_symbols": direct_symbols,
        "symbol": symbol,
        "ticker": ticker,
        "code": code,
        "requested_symbol": requested_symbol,
        "limit": limit,
        "offset": offset,
        "top_n": top_n,
        "schema_only": schema_only,
        "headers_only": headers_only,
    }.items():
        if v not in (None, ""):
            body[k] = v

    return await _run_advanced_sheet_rows_impl(
        request=request,
        body=body,
        mode=mode,
        include_matrix_q=include_matrix_q,
        token=token,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
        x_request_id=x_request_id,
    )


@router.post("/sheet-rows")
@router.post("/sheet_rows")
async def root_sheet_rows_post(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    mode: str = Query(default=""),
    include_matrix_q: Optional[bool] = Query(default=None, alias="include_matrix"),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    return await _run_advanced_sheet_rows_impl(
        request=request,
        body=body,
        mode=mode,
        include_matrix_q=include_matrix_q,
        token=token,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
        x_request_id=x_request_id,
    )


__all__ = ["router", "ADVANCED_ANALYSIS_VERSION", "_run_advanced_sheet_rows_impl"]
