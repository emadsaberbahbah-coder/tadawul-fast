#!/usr/bin/env python3
# routes/analysis_sheet_rows.py
"""
================================================================================
Analysis Sheet-Rows Router — v3.3.0
================================================================================
CANONICAL • REGISTRY-FIRST • FIXED-WIDTH CONTRACTS • FAIL-SAFE • V2-ENGINE FIRST
SPECIAL-PAGE SAFE • GET+POST SAFE • OBJECT + MATRIX RESPONSE SAFE • JSON-SAFE
TOP10 / INSIGHTS / DICTIONARY HARDENED • NO WEAK 5-COL FALLBACKS • ROOT-FALLBACK BRIDGED

What this revision improves
--------------------------
- FIX: contract resolution is now registry-first and exact-width always.
       Weak payloads can no longer redefine the response shape.
- FIX: static fallbacks now preserve canonical widths:
       standard pages=80, Top_10_Investments=83, Insights_Analysis=7,
       Data_Dictionary=9.
- FIX: schema extraction prefers schema_registry helpers directly
       (get_sheet_headers/get_sheet_keys/get_sheet_len) before looser introspection.
- FIX: empty-success payloads are downgraded to stable partial schema-shaped responses
       instead of drifting into inconsistent contracts.
- FIX: builder/engine failures degrade gracefully without backend 5xx from this router.
- FIX: Top 10 always guarantees top10_rank / selection_reason / criteria_snapshot.
- FIX: Top 10 now falls back to engine/root sheet-rows payloads and symbol-driven scoring when the builder returns zero rows.
- FIX: canonical key extraction now searches nested payloads and common provider aliases for price / range / score fields.
================================================================================
"""

from __future__ import annotations

import importlib
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

logger = logging.getLogger("routes.analysis_sheet_rows")

# -----------------------------------------------------------------------------
# Optional imports
# -----------------------------------------------------------------------------
try:
    from core.sheets.schema_registry import (  # type: ignore
        get_sheet_headers,
        get_sheet_keys,
        get_sheet_len,
        get_sheet_spec,
    )
except Exception as e:  # pragma: no cover
    get_sheet_headers = None  # type: ignore
    get_sheet_keys = None  # type: ignore
    get_sheet_len = None  # type: ignore
    get_sheet_spec = None  # type: ignore
    _SCHEMA_IMPORT_ERROR = repr(e)
else:
    _SCHEMA_IMPORT_ERROR = None

try:
    from core.sheets.page_catalog import (  # type: ignore
        CANONICAL_PAGES,
        FORBIDDEN_PAGES,
        allowed_pages,
        get_route_family,
        normalize_page_name,
    )
except Exception:  # pragma: no cover
    CANONICAL_PAGES = []  # type: ignore
    FORBIDDEN_PAGES = {"KSA_Tadawul", "Advisor_Criteria"}  # type: ignore

    def allowed_pages() -> List[str]:  # type: ignore
        return list(CANONICAL_PAGES) if CANONICAL_PAGES else []

    def normalize_page_name(name: str, allow_output_pages: bool = True) -> str:  # type: ignore
        return (name or "").strip().replace(" ", "_")

    def get_route_family(name: str) -> str:  # type: ignore
        if name == "Top_10_Investments":
            return "top10"
        if name == "Insights_Analysis":
            return "insights"
        if name == "Data_Dictionary":
            return "dictionary"
        return "instrument"

try:
    from core.config import auth_ok, get_settings_cached, mask_settings  # type: ignore
except Exception:  # pragma: no cover
    auth_ok = None  # type: ignore
    mask_settings = None  # type: ignore

    def get_settings_cached(*args, **kwargs):  # type: ignore
        return None


ANALYSIS_SHEET_ROWS_VERSION = "3.3.0"
router = APIRouter(prefix="/v1/analysis", tags=["Analysis Sheet Rows"])

_TOP10_PAGE = "Top_10_Investments"
_INSIGHTS_PAGE = "Insights_Analysis"
_DICTIONARY_PAGE = "Data_Dictionary"

_EXPECTED_SHEET_LENGTHS: Dict[str, int] = {
    "Market_Leaders": 80,
    "Global_Markets": 80,
    "Commodities_FX": 80,
    "Mutual_Funds": 80,
    "My_Portfolio": 80,
    _TOP10_PAGE: 83,
    _INSIGHTS_PAGE: 7,
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

_FIELD_ALIAS_HINTS: Dict[str, List[str]] = {
    "symbol": ["ticker", "code", "instrument", "security", "requested_symbol", "symbol_code"],
    "ticker": ["symbol", "code", "instrument", "security", "requested_symbol", "symbol_code"],
    "name": ["short_name", "long_name", "display_name", "instrument_name", "security_name"],
    "asset_class": ["asset_type", "assetType", "quote_type", "quoteType", "instrument_type", "security_type", "type"],
    "exchange": ["exchange_name", "full_exchange_name", "market", "market_name", "mic"],
    "currency": ["currency_code", "ccy", "fx_currency"],
    "country": ["country_name", "region_country", "domicile_country"],
    "sector": ["sector_name", "gics_sector"],
    "industry": ["industry_name", "gics_industry"],
    "current_price": ["price", "last_price", "last", "close", "market_price", "nav", "spot", "currentPrice", "regularMarketPrice", "value"],
    "previous_close": ["prev_close", "previousClose", "prior_close", "regularMarketPreviousClose"],
    "open_price": ["open", "openPrice", "regularMarketOpen"],
    "day_high": ["high", "dayHigh", "regularMarketDayHigh", "session_high"],
    "day_low": ["low", "dayLow", "regularMarketDayLow", "session_low"],
    "week_52_high": ["fiftyTwoWeekHigh", "fifty_two_week_high", "year_high", "52_week_high"],
    "week_52_low": ["fiftyTwoWeekLow", "fifty_two_week_low", "year_low", "52_week_low"],
    "price_change": ["change", "net_change", "regularMarketChange"],
    "percent_change": ["pct_change", "change_pct", "changePercent", "regularMarketChangePercent", "percentChange"],
    "volume": ["regularMarketVolume", "trade_volume", "traded_volume", "volume_traded"],
    "avg_volume_10d": ["averageDailyVolume10Day", "avg10_volume", "ten_day_avg_volume", "average_volume_10d"],
    "avg_volume_30d": ["averageDailyVolume3Month", "averageVolume3Month", "avg30_volume", "thirty_day_avg_volume"],
    "market_cap": ["marketCap", "market_capitalization"],
    "float_shares": ["floatShares", "sharesFloat", "free_float_shares"],
    "overall_score": ["score", "composite_score", "total_score"],
    "opportunity_score": ["opportunity", "opportunity_rank_score", "conviction_score"],
    "forecast_confidence": ["confidence", "confidence_pct", "forecastConfidence"],
    "confidence_score": ["confidence", "confidence_pct"],
    "expected_roi_1m": ["roi_1m", "expected_return_1m", "target_return_1m"],
    "expected_roi_3m": ["roi_3m", "expected_return_3m", "target_return_3m"],
    "expected_roi_12m": ["roi_12m", "expected_return_12m", "target_return_12m"],
    "forecast_price_1m": ["target_price_1m", "projected_price_1m"],
    "forecast_price_3m": ["target_price_3m", "projected_price_3m"],
    "forecast_price_12m": ["target_price_12m", "projected_price_12m"],
    "recommendation": ["signal", "rating", "action"],
    "recommendation_reason": ["rationale", "reasoning", "signal_reason"],
    "data_provider": ["provider", "source_provider", "primary_provider"],
    "last_updated_utc": ["updated_at", "timestamp_utc", "as_of_utc", "last_updated", "last_update_utc"],
    "last_updated_riyadh": ["timestamp_riyadh", "as_of_riyadh", "last_update_riyadh"],
    "warnings": ["warning", "messages", "errors", "issues"],
    "top10_rank": ["rank", "top_rank", "position_rank"],
    "selection_reason": ["reason", "selection_notes", "selector_reason"],
    "criteria_snapshot": ["criteria", "criteria_json", "snapshot"],
    "group": ["section"],
    "fmt": ["format"],
    "dtype": ["type", "data_type"],
    "notes": ["description", "commentary"],
}

_TOP10_MODULE_CANDIDATES = (
    "core.analysis.top10_selector",
    "routes.top10_investments",
    "routes.investment_advisor",
)

_TOP10_FUNCTION_CANDIDATES = (
    "build_top10_investments_rows",
    "build_top10_rows",
    "build_top10_output_rows",
    "build_top_10_investments_rows",
    "build_top_10_rows",
    "select_top10_rows",
    "select_top10",
    "get_top10_rows",
    "build_rows",
)

_INSIGHTS_MODULE_CANDIDATES = (
    "core.analysis.insights_builder",
    "routes.ai_analysis",
)

_INSIGHTS_FUNCTION_CANDIDATES = (
    "build_insights_analysis_rows",
    "build_insights_rows",
    "build_insights_output_rows",
    "build_insights_analysis",
    "get_insights_rows",
    "build_rows",
)

_ENRICHED_MODULE_CANDIDATES = (
    "core.enriched_quote",
)

_ENRICHED_FUNCTION_CANDIDATES = (
    "build_enriched_sheet_rows",
    "build_enriched_quote_rows",
    "build_sheet_rows",
    "build_page_rows",
    "build_enriched_rows",
    "get_page_rows",
    "get_sheet_rows",
    "run_enriched_quote",
    "build_rows",
)

# -----------------------------------------------------------------------------
# Canonical static contracts (exact widths; used only when registry is weak)
# -----------------------------------------------------------------------------
_CANONICAL_80_HEADERS: List[str] = [
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

_CANONICAL_80_KEYS: List[str] = [
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

_INSIGHTS_HEADERS: List[str] = [
    "Section",
    "Item",
    "Symbol",
    "Metric",
    "Value",
    "Notes",
    "Last Updated (Riyadh)",
]
_INSIGHTS_KEYS: List[str] = [
    "section",
    "item",
    "symbol",
    "metric",
    "value",
    "notes",
    "last_updated_riyadh",
]

_DICTIONARY_HEADERS: List[str] = [
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
_DICTIONARY_KEYS: List[str] = [
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
        return _json_safe(vars(value))
    except Exception:
        return str(value)


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
        if is_dataclass(obj):
            dd = getattr(obj, "__dict__", None)
            if isinstance(dd, dict):
                return {k: v for k, v in dd.items() if not str(k).startswith("_")}
    except Exception:
        pass
    try:
        dd = getattr(obj, "__dict__", None)
        if isinstance(dd, dict):
            return dict(dd)
    except Exception:
        pass
    return {}


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


def _request_id(request: Request, x_request_id: Optional[str]) -> str:
    if x_request_id and _strip(x_request_id):
        return _strip(x_request_id)
    try:
        rid = _strip(getattr(request.state, "request_id", ""))
        if rid:
            return rid
    except Exception:
        pass
    return str(uuid.uuid4())[:12]


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


def _maybe_bool(v: Any, default: bool) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
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


def _split_symbols_string(v: str) -> List[str]:
    raw = (v or "").replace(";", ",").replace("\n", ",").replace("\t", ",").replace(" ", ",")
    out: List[str] = []
    seen = set()
    for p in [x.strip() for x in raw.split(",") if x.strip()]:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def _get_list(body: Mapping[str, Any], *keys: str) -> List[str]:
    for k in keys:
        v = body.get(k)
        if isinstance(v, list):
            out: List[str] = []
            seen = set()
            for item in v:
                s = _strip(item)
                if s and s not in seen:
                    seen.add(s)
                    out.append(s)
            return out
        if isinstance(v, str) and v.strip():
            return _split_symbols_string(v)
    return []


def _pick_page_from_body(body: Mapping[str, Any]) -> str:
    for k in ("sheet", "page", "sheet_name", "sheetName", "name", "tab"):
        s = _strip(body.get(k))
        if s:
            return s
    return ""


def _collect_get_body(request: Request) -> Dict[str, Any]:
    qp = request.query_params
    body: Dict[str, Any] = {}
    for key in ("sheet", "page", "sheet_name", "sheetName", "name", "tab"):
        v = _strip(qp.get(key))
        if v:
            body[key] = v
    for key in ("symbols", "tickers", "tickers_list"):
        vals = qp.getlist(key)
        if vals:
            body[key] = _split_symbols_string(vals[0]) if len(vals) == 1 else [s.strip() for s in vals if _strip(s)]
            break
    for key in (
        "limit",
        "offset",
        "top_n",
        "include_matrix",
        "risk_level",
        "risk_profile",
        "confidence_level",
        "investment_period_days",
        "horizon_days",
        "min_expected_roi",
        "min_roi",
        "min_confidence",
    ):
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


# =============================================================================
# Auth
# =============================================================================
def _is_public_path(settings: Any, path: str) -> bool:
    p = _strip(path)
    if p in {"/", "/health", "/readyz", "/livez", "/meta"} or p.endswith("/health"):
        return True
    public_paths_env = os.getenv("PUBLIC_PATHS", "") or os.getenv("AUTH_PUBLIC_PATHS", "")
    for candidate in [x.strip() for x in public_paths_env.split(",") if x.strip()]:
        if candidate.endswith("*") and p.startswith(candidate[:-1]):
            return True
        if p == candidate:
            return True
    if settings is None:
        return False
    for attr in ("public_paths", "PUBLIC_PATHS", "auth_public_paths", "AUTH_PUBLIC_PATHS"):
        try:
            value = getattr(settings, attr, None)
            for candidate in _as_list(value):
                c = _strip(candidate)
                if not c:
                    continue
                if c.endswith("*") and p.startswith(c[:-1]):
                    return True
                if p == c:
                    return True
        except Exception:
            continue
    return False


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


def _extract_auth_token(*, token_query: Optional[str], x_app_token: Optional[str], authorization: Optional[str], settings: Any, request: Request) -> str:
    auth_token = _strip(x_app_token)
    if authorization and authorization.strip().lower().startswith("bearer "):
        auth_token = authorization.strip().split(" ", 1)[1].strip()
    if token_query and not auth_token and _allow_query_token(settings, request):
        auth_token = _strip(token_query)
    return auth_token


def _auth_passed(*, request: Request, settings: Any, auth_token: str, authorization: Optional[str]) -> bool:
    if auth_ok is None:
        return True
    path = str(getattr(getattr(request, "url", None), "path", "") or "")
    if _is_public_path(settings, path):
        return True
    headers_dict = dict(request.headers)
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


# =============================================================================
# Schema / contract helpers
# =============================================================================
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


def _route_family_flexible(page: str) -> str:
    try:
        family = _strip(get_route_family(page))
        if family:
            return family
    except Exception:
        pass
    if page == _TOP10_PAGE:
        return "top10"
    if page == _INSIGHTS_PAGE:
        return "insights"
    if page == _DICTIONARY_PAGE:
        return "dictionary"
    return "instrument"


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
            detail={"error": f"Forbidden/removed page: {page}", "forbidden_pages": sorted(list(forbidden))},
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


def _pad_contract(headers: Sequence[str], keys: Sequence[str], expected_len: int, *, header_prefix: str = "Column", key_prefix: str = "column") -> Tuple[List[str], List[str]]:
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
        return _ensure_top10_contract(_CANONICAL_80_HEADERS, _CANONICAL_80_KEYS)[0], _ensure_top10_contract(_CANONICAL_80_HEADERS, _CANONICAL_80_KEYS)[1], "static_canonical_top10"
    if page == _INSIGHTS_PAGE:
        h, k = _pad_contract(_INSIGHTS_HEADERS, _INSIGHTS_KEYS, 7)
        return h, k, "static_canonical_insights"
    if page == _DICTIONARY_PAGE:
        h, k = _pad_contract(_DICTIONARY_HEADERS, _DICTIONARY_KEYS, 9)
        return h, k, "static_canonical_dictionary"
    h, k = _pad_contract(_CANONICAL_80_HEADERS, _CANONICAL_80_KEYS, 80)
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
    if isinstance(cols, tuple) and cols:
        return list(cols)
    fields = getattr(spec, "fields", None)
    if isinstance(fields, list) and fields:
        return fields
    if isinstance(fields, tuple) and fields:
        return list(fields)
    if isinstance(spec, Mapping):
        cols2 = spec.get("columns") or spec.get("fields")
        if isinstance(cols2, list) and cols2:
            return cols2
        if isinstance(cols2, tuple) and cols2:
            return list(cols2)
    try:
        d = getattr(spec, "__dict__", None)
        if isinstance(d, dict):
            cols3 = d.get("columns") or d.get("fields")
            if isinstance(cols3, list) and cols3:
                return cols3
            if isinstance(cols3, tuple) and cols3:
                return list(cols3)
    except Exception:
        pass
    return []


def _extract_headers_keys_from_spec(spec: Any) -> Tuple[List[str], List[str]]:
    headers: List[str] = []
    keys: List[str] = []
    for c in _schema_columns_from_any(spec):
        if isinstance(c, Mapping):
            h = _strip(c.get("header") or c.get("display_header") or c.get("displayHeader") or c.get("label") or c.get("title"))
            k = _strip(c.get("key") or c.get("field") or c.get("name") or c.get("id"))
        else:
            h = _strip(getattr(c, "header", getattr(c, "display_header", getattr(c, "displayHeader", getattr(c, "label", getattr(c, "title", None))))))
            k = _strip(getattr(c, "key", getattr(c, "field", getattr(c, "name", getattr(c, "id", None)))))
        if h or k:
            headers.append(h or k.replace("_", " ").title())
            keys.append(k or _normalize_key_name(h))
    if not headers and not keys and isinstance(spec, Mapping):
        headers2 = spec.get("headers") or spec.get("display_headers") or spec.get("sheet_headers")
        keys2 = spec.get("keys") or spec.get("fields") or spec.get("columns")
        if isinstance(headers2, list):
            headers = [_strip(x) for x in headers2 if _strip(x)]
        if isinstance(keys2, list):
            keys = [_strip(x) for x in keys2 if _strip(x)]
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
                return _complete_schema_contract(headers, keys)[0], _complete_schema_contract(headers, keys)[1], spec, "schema_registry.helpers"
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
        if len(headers) == expected_len and len(keys) == expected_len:
            return headers, keys, spec, source
    sh, sk, ssrc = _static_contract(page)
    return sh, sk, {"source": ssrc, "page": page}, ssrc


# =============================================================================
# Payload extraction / normalization helpers
# =============================================================================
def _rows_to_matrix(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    return [[_json_safe(r.get(k)) for k in keys] for r in rows]


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


def _extract_from_nested_raw(raw: Any, candidates: Sequence[str], depth: int = 0) -> Any:
    if raw is None or depth > 2:
        return None
    if isinstance(raw, Mapping):
        direct = _extract_from_raw(dict(raw), candidates)
        if direct is not None:
            return direct
        preferred_nested_keys = (
            "quote",
            "analysis",
            "fundamentals",
            "forecast",
            "scores",
            "metrics",
            "summary",
            "snapshot",
            "payload",
            "data",
            "item",
            "record",
            "row",
            "meta",
            "stats",
            "price",
            "market_data",
        )
        for nk in preferred_nested_keys:
            nv = raw.get(nk)
            if isinstance(nv, Mapping):
                found = _extract_from_nested_raw(nv, candidates, depth + 1)
                if found is not None:
                    return found
        for nv in raw.values():
            if isinstance(nv, Mapping):
                found = _extract_from_nested_raw(nv, candidates, depth + 1)
                if found is not None:
                    return found
    return None


def _normalize_to_schema_keys(*, schema_keys: Sequence[str], schema_headers: Sequence[str], raw: Mapping[str, Any]) -> Dict[str, Any]:
    raw = dict(raw or {})
    header_by_key = {str(k): str(h) for k, h in zip(schema_keys, schema_headers)}
    out: Dict[str, Any] = {}
    for k in schema_keys:
        ks = str(k)
        v = _extract_from_nested_raw(raw, _key_variants(ks))
        if v is None:
            h = header_by_key.get(ks, "")
            if h:
                v = _extract_from_nested_raw(raw, [h, h.lower(), h.upper()])
        if ks in {"warnings", "recommendation_reason", "selection_reason"} and isinstance(v, (list, tuple, set)):
            v = "; ".join([_strip(x) for x in v if _strip(x)])
        out[ks] = _json_safe(v)
    if "symbol" in out and not out.get("symbol"):
        out["symbol"] = _extract_from_nested_raw(raw, _key_variants("symbol"))
    if "current_price" in out and out.get("current_price") is None:
        out["current_price"] = _extract_from_nested_raw(raw, _key_variants("current_price"))
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


def _row_has_any_signal(row: Mapping[str, Any]) -> bool:
    if not isinstance(row, Mapping):
        return False
    signal_keys = (
        "symbol",
        "name",
        "current_price",
        "overall_score",
        "opportunity_score",
        "expected_roi_3m",
        "expected_roi_1m",
        "recommendation",
        "selection_reason",
    )
    for key in signal_keys:
        value = row.get(key)
        if value not in (None, "", [], {}, ()):
            return True
    return False


def _top10_sort_key(row: Mapping[str, Any]) -> Tuple[float, ...]:
    return (
        _to_number(row.get("overall_score")),
        _to_number(row.get("opportunity_score")),
        _to_number(row.get("expected_roi_3m")),
        _to_number(row.get("expected_roi_1m")),
        _to_number(row.get("forecast_confidence")),
        _to_number(row.get("confidence_score")),
        _to_number(row.get("value_score")),
        _to_number(row.get("quality_score")),
        _to_number(row.get("momentum_score")),
        _to_number(row.get("growth_score")),
        _to_number(row.get("current_price")),
    )


def _top10_selection_reason(row: Mapping[str, Any]) -> str:
    parts: List[str] = []
    labels = (
        ("overall_score", "Overall"),
        ("opportunity_score", "Opportunity"),
        ("expected_roi_3m", "Exp ROI 3M"),
        ("forecast_confidence", "Forecast Conf"),
        ("confidence_score", "Confidence"),
        ("recommendation", "Reco"),
    )
    for key, label in labels:
        value = row.get(key)
        if value in (None, "", [], {}, ()):
            continue
        if isinstance(value, float):
            parts.append(f"{label} {round(value, 2)}")
        else:
            parts.append(f"{label} {value}")
        if len(parts) >= 3:
            break
    return " | ".join(parts) if parts else "Top10 fallback selection based on strongest available composite signals."


def _top10_criteria_snapshot(row: Mapping[str, Any]) -> str:
    snapshot = {}
    for key in (
        "overall_score",
        "opportunity_score",
        "expected_roi_1m",
        "expected_roi_3m",
        "expected_roi_12m",
        "forecast_confidence",
        "confidence_score",
        "risk_bucket",
        "recommendation",
        "symbol",
    ):
        value = row.get(key)
        if value not in (None, "", [], {}, ()):
            snapshot[key] = _json_safe(value)
    try:
        return json.dumps(snapshot, ensure_ascii=False, sort_keys=True)
    except Exception:
        return str(snapshot)


def _ensure_top10_rows(rows: Sequence[Mapping[str, Any]], *, requested_symbols: Sequence[str], top_n: int, schema_keys: Sequence[str], schema_headers: Sequence[str]) -> List[Dict[str, Any]]:
    normalized_rows: List[Dict[str, Any]] = []
    for raw_row in rows or []:
        normalized = _normalize_to_schema_keys(schema_keys=schema_keys, schema_headers=schema_headers, raw=(raw_row or {}))
        if _row_has_any_signal(normalized):
            normalized_rows.append(normalized)

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

    if requested_symbols:
        requested = {_strip(s): idx for idx, s in enumerate(requested_symbols) if _strip(s)}
        deduped.sort(
            key=lambda r: (
                0 if _strip(r.get("symbol")) in requested else 1,
                requested.get(_strip(r.get("symbol")), 10**6),
                -_top10_sort_key(r)[0],
                -_top10_sort_key(r)[1],
            )
        )
        deduped = sorted(deduped, key=_top10_sort_key, reverse=True)

    final_rows = deduped[: max(1, int(top_n))]
    for idx, row in enumerate(final_rows, start=1):
        row["top10_rank"] = idx
        if not _strip(row.get("selection_reason")):
            row["selection_reason"] = _top10_selection_reason(row)
        if not _strip(row.get("criteria_snapshot")):
            row["criteria_snapshot"] = _top10_criteria_snapshot(row)
    return final_rows



def _looks_like_symbol_token(x: Any) -> bool:
    s = _strip(x).upper()
    if not s or " " in s or len(s) > 24:
        return False
    return bool(re.fullmatch(r"[A-Z0-9\.\=\-\^:_/]{1,24}", s))


def _looks_like_explicit_row_dict(d: Any) -> bool:
    if not isinstance(d, Mapping) or not d:
        return False
    keyset = {str(k) for k in d.keys()}
    if keyset & {"symbol", "ticker", "code", "requested_symbol"}:
        return True
    if {"sheet", "header", "key"}.issubset(keyset):
        return True
    if {"section", "item"}.issubset(keyset):
        return True
    if {"top10_rank", "selection_reason"}.issubset(keyset):
        return True
    return False


def _rows_from_matrix(rows_matrix: Any, cols: Sequence[str]) -> List[Dict[str, Any]]:
    if not isinstance(rows_matrix, list) or not cols:
        return []
    keys = [_strip(c) for c in cols if _strip(c)]
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
            return [_to_plain_dict(r) for r in payload]
        if payload and isinstance(payload[0], (list, tuple)):
            return []
        out: List[Dict[str, Any]] = []
        for item in payload:
            s = _strip(item)
            if s:
                out.append({"symbol": s})
        return out
    if not isinstance(payload, Mapping):
        d = _to_plain_dict(payload)
        return [d] if _looks_like_explicit_row_dict(d) else []
    if _looks_like_explicit_row_dict(payload):
        return [dict(payload)]
    maybe_symbol_map = True
    rows_from_symbol_map: List[Dict[str, Any]] = []
    for k, v in payload.items():
        if not isinstance(v, Mapping) or not _looks_like_symbol_token(k):
            maybe_symbol_map = False
            break
        row = dict(v)
        if not row.get("symbol"):
            row["symbol"] = _strip(k)
        rows_from_symbol_map.append(row)
    if maybe_symbol_map and rows_from_symbol_map:
        return rows_from_symbol_map
    for name in ("rows", "data", "items", "records", "quotes", "recommendations", "results"):
        value = payload.get(name)
        if isinstance(value, list):
            rows = _extract_rows_like(value, depth + 1)
            if rows:
                return rows
        if isinstance(value, Mapping):
            rows = _extract_rows_like(value, depth + 1)
            if rows:
                return rows
    return []


def _extract_matrix_like(payload: Any, depth: int = 0) -> Optional[List[List[Any]]]:
    if depth > 8:
        return None
    if isinstance(payload, dict):
        for name in ("rows_matrix", "matrix", "values"):
            value = payload.get(name)
            if isinstance(value, list):
                return [list(r) if isinstance(r, (list, tuple)) else [r] for r in value]
        rows_value = payload.get("rows")
        if isinstance(rows_value, list) and rows_value and isinstance(rows_value[0], (list, tuple)):
            return [list(r) if isinstance(r, (list, tuple)) else [r] for r in rows_value]
        for name in ("data", "payload", "result", "response", "output"):
            nested = payload.get(name)
            if isinstance(nested, dict):
                mx = _extract_matrix_like(nested, depth + 1)
                if mx is not None:
                    return mx
    return None


def _extract_status_error(payload: Any) -> Tuple[str, Optional[str], Dict[str, Any]]:
    if not isinstance(payload, dict):
        return "success", None, {}
    status_out = _strip(payload.get("status")) or "success"
    error_out = payload.get("error") or payload.get("detail") or payload.get("message")
    meta_out = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    return status_out, (str(error_out) if error_out is not None else None), meta_out


def _payload_has_real_rows(payload: Any) -> bool:
    return bool(_extract_rows_like(payload) or _extract_matrix_like(payload))


def _payload_quality_score(payload: Any, page: str = "") -> int:
    if payload is None:
        return -10
    if isinstance(payload, list):
        return 100 if payload else 0
    if not isinstance(payload, dict):
        return 0
    score = 0
    rows_like = _extract_rows_like(payload)
    matrix_like = _extract_matrix_like(payload)
    if rows_like:
        score += 100 + min(25, len(rows_like))
    if matrix_like:
        score += 85 + min(15, len(matrix_like))
    if page == _TOP10_PAGE and rows_like:
        for field in _TOP10_REQUIRED_FIELDS:
            if any(isinstance(r, Mapping) and r.get(field) not in (None, "", [], {}) for r in rows_like):
                score += 10
    status_out, error_out, meta_in = _extract_status_error(payload)
    if _strip(status_out).lower() == "success":
        score += 4
    elif _strip(status_out).lower() == "partial":
        score += 2
    elif _strip(status_out).lower() == "error":
        score -= 3
    if isinstance(meta_in, dict) and meta_in.get("known_sheets"):
        score += 1
    if _strip(error_out):
        score -= 6
    return score


def _slice(rows: List[Dict[str, Any]], *, limit: int, offset: int) -> List[Dict[str, Any]]:
    start = max(0, int(offset))
    if limit <= 0:
        return rows[start:]
    return rows[start:start + max(0, int(limit))]


def _empty_schema_row(keys: Sequence[str], *, symbol: str = "") -> Dict[str, Any]:
    row = {k: None for k in keys}
    if symbol:
        if "symbol" in row and not row.get("symbol"):
            row["symbol"] = symbol
    return row


def _payload(*, page: str, route_family: str, headers: Sequence[str], keys: Sequence[str], rows: Sequence[Mapping[str, Any]], include_matrix: bool, request_id: str, started_at: float, mode: str, status_out: str = "success", error_out: Optional[str] = None, meta_extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    rows_list = [dict(r) for r in rows]
    hdrs = list(headers)
    ks = list(keys)
    meta = {
        "duration_ms": round((time.time() - started_at) * 1000.0, 3),
        "mode": mode,
        "count": len(rows_list),
    }
    if meta_extra:
        meta.update(meta_extra)
    return _json_safe({
        "status": status_out,
        "page": page,
        "sheet": page,
        "sheet_name": page,
        "route_family": route_family,
        "headers": hdrs,
        "display_headers": hdrs,
        "sheet_headers": hdrs,
        "column_headers": hdrs,
        "keys": ks,
        "columns": ks,
        "fields": ks,
        "rows": rows_list,
        "items": rows_list,
        "data": rows_list,
        "quotes": rows_list,
        "records": rows_list,
        "rows_matrix": _rows_to_matrix(rows_list, ks) if include_matrix else [],
        "error": error_out,
        "version": ANALYSIS_SHEET_ROWS_VERSION,
        "request_id": request_id,
        "meta": meta,
    })


def _emergency_payload(*, page: str, request_id: str, started_at: float, mode: str, route_family: Optional[str] = None, error_out: str, meta_extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    headers, keys, _spec, schema_source = _resolve_contract(page)
    meta = dict(meta_extra or {})
    meta.setdefault("dispatch", "analysis_sheet_rows_emergency_fallback")
    meta.setdefault("schema_source", schema_source)
    return _payload(
        page=page,
        route_family=route_family or _route_family_flexible(page),
        headers=headers,
        keys=keys,
        rows=[],
        include_matrix=True,
        request_id=request_id,
        started_at=started_at,
        mode=mode,
        status_out="partial",
        error_out=error_out,
        meta_extra=meta,
    )


# =============================================================================
# Engine / builder access
# =============================================================================
async def _get_engine(request: Request) -> Tuple[Optional[Any], str]:
    try:
        st = getattr(request.app, "state", None)
        if st:
            for attr in ("engine", "data_engine", "quote_engine", "cache_engine"):
                value = getattr(st, attr, None)
                if value is not None:
                    return value, f"app.state.{attr}"
    except Exception:
        pass
    for modpath in ("core.data_engine_v2", "core.data_engine"):
        try:
            mod = __import__(modpath, fromlist=["get_engine"])
            get_engine = getattr(mod, "get_engine", None)
            if callable(get_engine):
                eng = get_engine()
                eng = await _maybe_await(eng)
                if eng is not None:
                    return eng, f"{modpath}.get_engine"
        except Exception:
            continue
    return None, "unavailable"


async def _call_engine(fn: Any, *args: Any, **kwargs: Any) -> Any:
    return await _maybe_await(fn(*args, **kwargs))


def _dict_is_symbol_map(d: Dict[str, Any], symbols: Sequence[str]) -> bool:
    if not isinstance(d, dict) or not symbols:
        return False
    symset = set(symbols)
    keys = [k for k in d.keys() if isinstance(k, str)]
    if not keys:
        return False
    hit = sum(1 for k in keys if k in symset)
    return hit == len(symset) if len(symset) > 0 else False


async def _fetch_analysis_rows(engine: Any, symbols: List[str], *, mode: str, settings: Any, schema: Any) -> Dict[str, Any]:
    if not symbols or engine is None:
        return {}
    computations_enabled = bool(getattr(settings, "computations_enabled", True)) if settings is not None else True
    forecasting_enabled = bool(getattr(settings, "forecasting_enabled", True)) if settings is not None else True
    scoring_enabled = bool(getattr(settings, "scoring_enabled", True)) if settings is not None else True
    want_analysis = computations_enabled and (forecasting_enabled or scoring_enabled)
    preferred: List[str] = []
    if want_analysis:
        preferred += ["get_analysis_rows_batch", "get_analysis_quotes_batch", "get_enriched_quotes_batch"]
    else:
        preferred += ["get_enriched_quotes_batch"]
    preferred += ["get_quotes_batch", "quotes_batch", "get_enriched_quotes", "get_quotes"]
    for method in preferred:
        fn = getattr(engine, method, None)
        if not callable(fn):
            continue
        try:
            try:
                res = await _call_engine(fn, symbols, mode=mode, schema=schema)
            except TypeError:
                try:
                    res = await _call_engine(fn, symbols, schema=schema)
                except TypeError:
                    try:
                        res = await _call_engine(fn, symbols, mode=mode)
                    except TypeError:
                        res = await _call_engine(fn, symbols)
            if isinstance(res, dict):
                if _dict_is_symbol_map(res, symbols):
                    return res
                data = res.get("data") or res.get("rows") or res.get("items") or res.get("quotes")
                if isinstance(data, dict) and _dict_is_symbol_map(data, symbols):
                    return data
                if isinstance(data, list):
                    return {s: r for s, r in zip(symbols, data)}
            elif isinstance(res, list):
                return {s: r for s, r in zip(symbols, res)}
        except Exception:
            continue
    out: Dict[str, Any] = {}
    per_dict_fn = getattr(engine, "get_enriched_quote_dict", None) or getattr(engine, "get_analysis_row_dict", None) or getattr(engine, "get_quote_dict", None)
    per_fn = getattr(engine, "get_enriched_quote", None) or getattr(engine, "get_analysis_row", None) or getattr(engine, "get_quote", None)
    for s in symbols:
        try:
            if callable(per_dict_fn):
                try:
                    out[s] = await _call_engine(per_dict_fn, s, mode=mode, schema=schema)
                except TypeError:
                    try:
                        out[s] = await _call_engine(per_dict_fn, s, schema=schema)
                    except TypeError:
                        try:
                            out[s] = await _call_engine(per_dict_fn, s, mode=mode)
                        except TypeError:
                            out[s] = await _call_engine(per_dict_fn, s)
            elif callable(per_fn):
                try:
                    out[s] = await _call_engine(per_fn, s, mode=mode, schema=schema)
                except TypeError:
                    try:
                        out[s] = await _call_engine(per_fn, s, schema=schema)
                    except TypeError:
                        try:
                            out[s] = await _call_engine(per_fn, s, mode=mode)
                        except TypeError:
                            out[s] = await _call_engine(per_fn, s)
            else:
                out[s] = {"symbol": s, "error": "engine_missing_quote_method"}
        except Exception as e:
            out[s] = {"symbol": s, "error": str(e)}
    return out


def _import_first_available(mod_names: Sequence[str]):
    last_err: Optional[Exception] = None
    for mn in mod_names:
        try:
            return importlib.import_module(mn)
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(str(last_err) if last_err else "import failed")


async def _call_function_flexible(fn: Any, call_specs: Sequence[Tuple[Tuple[Any, ...], Dict[str, Any]]]) -> Any:
    last_err: Optional[Exception] = None
    for args, kwargs in call_specs:
        try:
            return await _maybe_await(fn(*args, **kwargs))
        except TypeError as e:
            last_err = e
            continue
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(str(last_err) if last_err else "call failed")


async def _call_builder_best_effort(*, module_names: Sequence[str], function_names: Sequence[str], request: Request, settings: Any, engine: Any, mode: str, body: Dict[str, Any], schema_keys: Sequence[str], schema_headers: Sequence[str], friendly_name: str, page: str, limit: int, offset: int, schema: Any) -> Tuple[List[Dict[str, Any]], str, Optional[str], Dict[str, Any], Any]:
    try:
        mod = _import_first_available(module_names)
    except Exception as e:
        return [], "partial", f"{friendly_name} builder import failed: {e}", {"builder_import_failed": True}, None
    fn = None
    chosen_name = ""
    for name in function_names:
        cand = getattr(mod, name, None)
        if callable(cand):
            fn = cand
            chosen_name = name
            break
    if fn is None:
        return [], "partial", f"{friendly_name} builder missing callable function", {"builder_missing": True}, None

    requested_top_n = max(1, _maybe_int(body.get("top_n"), limit))
    requested_symbols = _get_list(body, "symbols", "tickers", "tickers_list")

    call_specs: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = [
        ((), {"request": request, "settings": settings, "engine": engine, "data_engine": engine, "quote_engine": engine, "cache_engine": engine, "mode": mode, "body": body, "payload": body, "page": page, "sheet": page, "sheet_name": page, "limit": limit, "top_n": requested_top_n, "offset": offset, "schema": schema, "schema_keys": list(schema_keys), "schema_headers": list(schema_headers), "symbols": requested_symbols, "tickers": requested_symbols, "route_family": _route_family_flexible(page)}),
        ((), {"request": request, "settings": settings, "engine": engine, "mode": mode, "body": body, "page": page, "limit": limit, "top_n": requested_top_n, "offset": offset, "schema": schema, "symbols": requested_symbols, "tickers": requested_symbols}),
        ((body,), {}),
        ((engine, body), {}),
        ((engine,), {}),
        ((), {}),
    ]
    try:
        out = await _call_function_flexible(fn, call_specs)
    except Exception as e:
        return [], "partial", f"{friendly_name} builder call failed: {e}", {"builder_call_failed": True}, None
    rows = _extract_rows_like(out)
    matrix = _extract_matrix_like(out)
    if not rows and matrix:
        rows = _rows_from_matrix(matrix, schema_keys)
    status_out, error_out, meta_out = _extract_status_error(out)
    normalized = [_normalize_to_schema_keys(schema_keys=schema_keys, schema_headers=schema_headers, raw=(r or {})) for r in rows]
    meta_out = dict(meta_out or {})
    meta_out["builder_function"] = chosen_name
    meta_out["raw_payload_quality"] = _payload_quality_score(out, page=page)
    return normalized, status_out, error_out, meta_out, out



async def _build_top10_from_requested_symbols(*, engine: Any, settings: Any, symbols: Sequence[str], page: str, mode: str, schema_keys: Sequence[str], schema_headers: Sequence[str], schema: Any, top_n: int) -> List[Dict[str, Any]]:
    if engine is None or not symbols:
        return []
    requested_symbols = [_strip(s) for s in symbols if _strip(s)]
    if not requested_symbols:
        return []
    data_map = await _fetch_analysis_rows(engine, requested_symbols, mode=mode, settings=settings, schema=schema)
    raw_rows: List[Dict[str, Any]] = []
    for sym in requested_symbols:
        raw = _to_plain_dict(data_map.get(sym))
        if not raw:
            raw = {"symbol": sym}
        if not raw.get("symbol"):
            raw["symbol"] = sym
        raw_rows.append(raw)
    return _ensure_top10_rows(
        raw_rows,
        requested_symbols=requested_symbols,
        top_n=top_n,
        schema_keys=schema_keys,
        schema_headers=schema_headers,
    )


def _build_data_dictionary_rows_to_schema(*, schema_keys: Sequence[str], schema_headers: Sequence[str]) -> Tuple[List[Dict[str, Any]], Optional[str], Any]:
    try:
        from core.sheets.data_dictionary import build_data_dictionary_rows  # type: ignore
    except Exception as e:
        return [], f"Data_Dictionary generator import failed: {e}", None
    try:
        raw_rows = build_data_dictionary_rows(include_meta_sheet=True)
    except TypeError:
        try:
            raw_rows = build_data_dictionary_rows()
        except Exception as e:
            return [], f"Data_Dictionary generator failed: {e}", None
    except Exception as e:
        return [], f"Data_Dictionary generator failed: {e}", None
    out: List[Dict[str, Any]] = []
    for r in _as_list(raw_rows):
        out.append(_normalize_to_schema_keys(schema_keys=schema_keys, schema_headers=schema_headers, raw=_to_plain_dict(r)))
    return out, None, raw_rows


async def _call_sheet_rows_payload_best_effort(*, engine: Any, page: str, limit: int, offset: int, mode: str, body: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    candidates: List[Any] = []
    if engine is not None:
        for name in ("get_sheet_rows", "sheet_rows", "build_sheet_rows", "execute_sheet_rows", "run_sheet_rows", "get_page_rows", "get_rows", "get_rows_for_sheet", "get_rows_for_page", "get_sheet", "get_cached_sheet_rows", "get_sheet_snapshot", "get_cached_sheet_snapshot"):
            fn = getattr(engine, name, None)
            if callable(fn):
                candidates.append(fn)
    for modpath in ("core.data_engine_v2", "core.data_engine"):
        try:
            mod = importlib.import_module(modpath)
            for name in ("get_sheet_rows", "sheet_rows", "build_sheet_rows", "get_page_rows"):
                fn = getattr(mod, name, None)
                if callable(fn):
                    candidates.append(fn)
        except Exception:
            continue
    best_payload: Optional[Dict[str, Any]] = None
    best_score = -9999
    for fn in candidates:
        call_specs = [
            ((), {"sheet": page, "limit": limit, "offset": offset, "mode": mode, "body": body}),
            ((), {"page": page, "limit": limit, "offset": offset, "mode": mode, "body": body}),
            ((), {"sheet_name": page, "limit": limit, "offset": offset, "mode": mode, "body": body}),
            ((), {"sheet": page, "limit": limit, "offset": offset, "mode": mode}),
            ((), {"page": page, "limit": limit, "offset": offset, "mode": mode}),
            ((), {"sheet": page, "limit": limit, "offset": offset}),
            ((), {"page": page, "limit": limit, "offset": offset}),
            ((page,), {"limit": limit, "offset": offset, "mode": mode, "body": body}),
            ((page,), {"limit": limit, "offset": offset, "mode": mode}),
            ((page,), {"limit": limit, "offset": offset}),
            ((page,), {}),
        ]
        try:
            res = await _call_function_flexible(fn, call_specs)
            payload = res if isinstance(res, dict) else {"status": "success", "rows": res if isinstance(res, list) else []}
            score = _payload_quality_score(payload, page=page)
            if score > best_score:
                best_score = score
                best_payload = payload
            if _payload_has_real_rows(payload):
                return payload
        except Exception:
            continue
    return best_payload


async def _call_enriched_page_best_effort(*, request: Request, settings: Any, engine: Any, page: str, limit: int, offset: int, mode: str, body: Dict[str, Any], schema_keys: Sequence[str], schema_headers: Sequence[str], schema: Any) -> Tuple[List[Dict[str, Any]], str, Optional[str], Dict[str, Any], Any]:
    builder_body = dict(body or {})
    builder_body.update({"page": page, "sheet": page, "sheet_name": page, "limit": limit, "offset": offset})
    return await _call_builder_best_effort(
        module_names=_ENRICHED_MODULE_CANDIDATES,
        function_names=_ENRICHED_FUNCTION_CANDIDATES,
        request=request,
        settings=settings,
        engine=engine,
        mode=mode,
        body=builder_body,
        schema_keys=schema_keys,
        schema_headers=schema_headers,
        friendly_name=page,
        page=page,
        limit=limit,
        offset=offset,
        schema=schema,
    )


# =============================================================================
# Health
# =============================================================================
@router.get("/health")
async def analysis_sheet_rows_health(request: Request) -> Dict[str, Any]:
    settings = None
    try:
        settings = get_settings_cached()
    except Exception:
        settings = None
    auth_summary = None
    try:
        if callable(mask_settings) and settings is not None:
            masked = mask_settings(settings)
            auth_summary = {"open_mode_effective": masked.get("open_mode_effective"), "token_count": masked.get("token_count")}
    except Exception:
        auth_summary = None
    return _json_safe({
        "status": "ok",
        "service": "analysis_sheet_rows",
        "version": ANALYSIS_SHEET_ROWS_VERSION,
        "schema_registry_available": bool(get_sheet_spec is not None),
        "schema_import_error": _SCHEMA_IMPORT_ERROR,
        "allowed_pages_count": len(_safe_allowed_pages()),
        "auth": auth_summary,
        "path": str(getattr(getattr(request, "url", None), "path", "")),
    })


# =============================================================================
# Internal implementation
# =============================================================================
async def _analysis_sheet_rows_impl_core(request: Request, body: Dict[str, Any], mode: str, include_matrix_q: Optional[bool], token: Optional[str], x_app_token: Optional[str], authorization: Optional[str], x_request_id: Optional[str]) -> Dict[str, Any]:
    start = time.time()
    request_id = _request_id(request, x_request_id)
    try:
        settings = get_settings_cached()
    except Exception:
        settings = None
    auth_token = _extract_auth_token(token_query=token, x_app_token=x_app_token, authorization=authorization, settings=settings, request=request)
    if not _auth_passed(request=request, settings=settings, auth_token=auth_token, authorization=authorization):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    merged_body = _merge_body_with_query(body, request)
    page = _normalize_page_flexible(_pick_page_from_body(merged_body) or "Market_Leaders")
    _ensure_page_allowed(page)
    route_family = _route_family_flexible(page)
    include_matrix = _maybe_bool(merged_body.get("include_matrix"), include_matrix_q if include_matrix_q is not None else True)
    limit = max(1, min(5000, _maybe_int(merged_body.get("limit"), 2000)))
    offset = max(0, _maybe_int(merged_body.get("offset"), 0))
    top_n = max(1, min(5000, _maybe_int(merged_body.get("top_n"), limit)))
    symbols = _get_list(merged_body, "symbols", "tickers", "tickers_list")
    if symbols:
        symbols = symbols[:top_n]

    headers, keys, spec, schema_source = _resolve_contract(page)
    engine, engine_source = await _get_engine(request)

    if route_family == "dictionary":
        rows, error_out, _raw_payload = _build_data_dictionary_rows_to_schema(schema_keys=keys, schema_headers=headers)
        rows = _slice(rows, limit=limit, offset=offset)
        return _payload(
            page=page,
            route_family=route_family,
            headers=headers,
            keys=keys,
            rows=rows,
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=start,
            mode=mode,
            status_out="success" if rows else "partial",
            error_out=error_out,
            meta_extra={"dispatch": "data_dictionary", "builder": "core.sheets.data_dictionary.build_data_dictionary_rows", "schema_source": schema_source, "engine_source": engine_source},
        )

    if route_family == "insights":
        rows, status_out, error_out, meta_out, _raw_payload = await _call_builder_best_effort(
            module_names=_INSIGHTS_MODULE_CANDIDATES,
            function_names=_INSIGHTS_FUNCTION_CANDIDATES,
            request=request,
            settings=settings,
            engine=engine,
            mode=(mode or ""),
            body=merged_body,
            schema_keys=keys,
            schema_headers=headers,
            friendly_name="Insights_Analysis",
            page=page,
            limit=limit,
            offset=offset,
            schema=spec,
        )
        rows = _slice(rows, limit=limit, offset=offset)
        return _payload(
            page=page,
            route_family=route_family,
            headers=headers,
            keys=keys,
            rows=rows,
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=start,
            mode=mode,
            status_out=status_out or ("success" if rows else "partial"),
            error_out=error_out,
            meta_extra={"dispatch": "insights_builder", "builder": "insights_builder", "schema_source": schema_source, "engine_source": engine_source, **dict(meta_out or {})},
        )

    if route_family == "top10":
        headers, keys = _ensure_top10_contract(headers, keys)
        requested_symbols = _get_list(merged_body, "symbols", "tickers", "tickers_list")
        requested_top_n = max(1, min(5000, _maybe_int(merged_body.get("top_n"), top_n)))

        rows, status_out, error_out, meta_out, raw_builder_payload = await _call_builder_best_effort(
            module_names=_TOP10_MODULE_CANDIDATES,
            function_names=_TOP10_FUNCTION_CANDIDATES,
            request=request,
            settings=settings,
            engine=engine,
            mode=(mode or ""),
            body=merged_body,
            schema_keys=keys,
            schema_headers=headers,
            friendly_name="Top_10_Investments",
            page=page,
            limit=limit,
            offset=offset,
            schema=spec,
        )

        rows = _ensure_top10_rows(
            rows,
            requested_symbols=requested_symbols,
            top_n=requested_top_n,
            schema_keys=keys,
            schema_headers=headers,
        )

        dispatch = "top10_builder"
        raw_quality = _payload_quality_score(raw_builder_payload, page=page)

        if not rows:
            engine_payload = await _call_sheet_rows_payload_best_effort(
                engine=engine,
                page=page,
                limit=max(limit, requested_top_n),
                offset=0,
                mode=(mode or ""),
                body=merged_body,
            )
            if _payload_has_real_rows(engine_payload):
                payload_rows = _extract_rows_like(engine_payload)
                payload_matrix = _extract_matrix_like(engine_payload)
                if not payload_rows and payload_matrix:
                    payload_rows = _rows_from_matrix(payload_matrix, keys)
                rows = _ensure_top10_rows(
                    payload_rows,
                    requested_symbols=requested_symbols,
                    top_n=requested_top_n,
                    schema_keys=keys,
                    schema_headers=headers,
                )
                if rows:
                    dispatch = "top10_engine_sheet_rows_fallback"
                    status_from_engine, error_from_engine, payload_meta = _extract_status_error(engine_payload if isinstance(engine_payload, dict) else {})
                    status_out = status_from_engine or status_out
                    error_out = error_from_engine or error_out
                    meta_out = {**dict(meta_out or {}), **dict(payload_meta or {})}
                    meta_out["engine_payload_quality"] = _payload_quality_score(engine_payload, page=page)

        if not rows and requested_symbols:
            symbol_rows = await _build_top10_from_requested_symbols(
                engine=engine,
                settings=settings,
                symbols=requested_symbols,
                page=page,
                mode=(mode or ""),
                schema_keys=keys,
                schema_headers=headers,
                schema=spec,
                top_n=requested_top_n,
            )
            if symbol_rows:
                rows = symbol_rows
                dispatch = "top10_symbol_rank_fallback"
                if not error_out:
                    error_out = "Top10 builder returned no rows; generated fallback ranking from requested symbols."
                status_out = "partial"

        rows = _slice(rows, limit=limit, offset=offset)
        return _payload(
            page=page,
            route_family=route_family,
            headers=headers,
            keys=keys,
            rows=rows,
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=start,
            mode=mode,
            status_out=status_out or ("success" if rows else "partial"),
            error_out=error_out,
            meta_extra={
                "dispatch": dispatch,
                "builder": "top10_selector",
                "schema_source": schema_source,
                "engine_source": engine_source,
                "requested_symbols": len(requested_symbols),
                "requested_top_n": requested_top_n,
                "builder_payload_quality": raw_quality,
                **dict(meta_out or {}),
            },
        )

    if symbols:
        if engine is None:
            fallback_rows = [_empty_schema_row(keys, symbol=s) for s in symbols]
            return _payload(
                page=page,
                route_family=route_family,
                headers=headers,
                keys=keys,
                rows=fallback_rows,
                include_matrix=include_matrix,
                request_id=request_id,
                started_at=start,
                mode=mode,
                status_out="partial",
                error_out="Data engine unavailable",
                meta_extra={"requested": len(symbols), "errors": len(symbols), "dispatch": "instrument_mode_no_engine", "builder": "schema_only", "schema_source": schema_source, "engine_source": engine_source},
            )
        data_map = await _fetch_analysis_rows(engine, symbols, mode=(mode or ""), settings=settings, schema=spec)
        normalized_rows: List[Dict[str, Any]] = []
        errors = 0
        normalize_fn = None
        try:
            from core.data_engine_v2 import normalize_row_to_schema as _normalize_row_to_schema  # type: ignore
            normalize_fn = _normalize_row_to_schema
        except Exception:
            normalize_fn = None
        for sym in symbols:
            raw = _to_plain_dict(data_map.get(sym))
            if not raw:
                raw = {"symbol": sym, "error": "missing_row"}
                errors += 1
            elif isinstance(raw, dict) and raw.get("error"):
                errors += 1
            if callable(normalize_fn):
                try:
                    row = normalize_fn(page, raw, keep_extras=True)
                    row = row if isinstance(row, dict) else raw
                except Exception:
                    row = raw
            else:
                row = raw
            normalized = _normalize_to_schema_keys(schema_keys=keys, schema_headers=headers, raw=row)
            if "symbol" in keys and not normalized.get("symbol"):
                normalized["symbol"] = sym
            normalized_rows.append(normalized)
        status_out = "success" if errors == 0 else ("partial" if errors < len(symbols) else "error")
        return _payload(
            page=page,
            route_family=route_family,
            headers=headers,
            keys=keys,
            rows=normalized_rows,
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=start,
            mode=mode,
            status_out=status_out,
            error_out=f"{errors} errors" if errors else None,
            meta_extra={"requested": len(symbols), "errors": errors, "dispatch": "instrument_mode", "builder": "engine.batch_quote_or_analysis", "schema_source": schema_source, "engine_source": engine_source},
        )

    engine_payload = await _call_sheet_rows_payload_best_effort(engine=engine, page=page, limit=limit, offset=offset, mode=(mode or ""), body=merged_body)
    rows: List[Dict[str, Any]] = []
    status_out = "partial"
    error_out: Optional[str] = None
    meta_out: Dict[str, Any] = {"dispatch": "engine_sheet_rows", "builder": "engine_or_enriched_fallback", "schema_source": schema_source, "engine_source": engine_source}

    if _payload_has_real_rows(engine_payload):
        payload_rows = _extract_rows_like(engine_payload)
        payload_matrix = _extract_matrix_like(engine_payload)
        if not payload_rows and payload_matrix:
            payload_rows = _rows_from_matrix(payload_matrix, keys)
        rows = [_normalize_to_schema_keys(schema_keys=keys, schema_headers=headers, raw=(r or {})) for r in payload_rows]
        status_out, error_out, payload_meta = _extract_status_error(engine_payload if isinstance(engine_payload, dict) else {})
        meta_out.update(payload_meta or {})
        meta_out["engine_payload_quality"] = _payload_quality_score(engine_payload, page=page)
    else:
        enriched_rows, enriched_status, enriched_error, enriched_meta, _enriched_raw_payload = await _call_enriched_page_best_effort(
            request=request,
            settings=settings,
            engine=engine,
            page=page,
            limit=limit,
            offset=offset,
            mode=(mode or ""),
            body=merged_body,
            schema_keys=keys,
            schema_headers=headers,
            schema=spec,
        )
        if enriched_rows:
            rows = enriched_rows
            status_out = enriched_status or "partial"
            error_out = enriched_error
            meta_out.update(enriched_meta or {})
            meta_out["dispatch"] = "enriched_fallback"
        else:
            if engine_payload is not None and isinstance(engine_payload, dict):
                _, engine_error, engine_meta = _extract_status_error(engine_payload)
                error_out = engine_error
                meta_out.update(engine_meta or {})
            if not error_out:
                error_out = "No usable rows returned; schema-shaped fallback emitted"
            rows = []

    rows = _slice(rows, limit=limit, offset=offset)
    return _payload(
        page=page,
        route_family=route_family,
        headers=headers,
        keys=keys,
        rows=rows,
        include_matrix=include_matrix,
        request_id=request_id,
        started_at=start,
        mode=mode,
        status_out=status_out or ("success" if rows else "partial"),
        error_out=error_out,
        meta_extra=meta_out,
    )


async def _analysis_sheet_rows_impl(request: Request, body: Dict[str, Any], mode: str, include_matrix_q: Optional[bool], token: Optional[str], x_app_token: Optional[str], authorization: Optional[str], x_request_id: Optional[str]) -> Dict[str, Any]:
    start = time.time()
    request_id = _request_id(request, x_request_id)
    merged_body = _merge_body_with_query(body, request)
    fallback_page = _normalize_page_flexible(_pick_page_from_body(merged_body) or "Market_Leaders")
    try:
        return await _analysis_sheet_rows_impl_core(request=request, body=body, mode=mode, include_matrix_q=include_matrix_q, token=token, x_app_token=x_app_token, authorization=authorization, x_request_id=x_request_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("analysis_sheet_rows_impl_unhandled", extra={"page": fallback_page})
        return _emergency_payload(page=fallback_page, request_id=request_id, started_at=start, mode=mode, error_out=f"analysis_sheet_rows runtime fallback: {e}", meta_extra={"exception_type": type(e).__name__})


# =============================================================================
# Routes
# =============================================================================
@router.get("/sheet-rows")
async def analysis_sheet_rows_get(
    request: Request,
    page: str = Query(default="", description="sheet/page name"),
    sheet: str = Query(default="", description="sheet/page name"),
    sheet_name: str = Query(default="", description="sheet/page name"),
    name: str = Query(default="", description="sheet/page name"),
    tab: str = Query(default="", description="sheet/page name"),
    symbols: str = Query(default="", description="comma-separated symbols"),
    tickers: str = Query(default="", description="comma-separated tickers"),
    limit: Optional[int] = Query(default=None),
    offset: Optional[int] = Query(default=None),
    top_n: Optional[int] = Query(default=None),
    mode: str = Query(default="", description="Optional mode hint for engine/provider"),
    include_matrix_q: Optional[bool] = Query(default=None, alias="include_matrix"),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    body: Dict[str, Any] = {}
    for k, v in {"page": page, "sheet": sheet, "sheet_name": sheet_name, "name": name, "tab": tab, "symbols": symbols, "tickers": tickers, "limit": limit, "offset": offset, "top_n": top_n}.items():
        if v not in (None, ""):
            body[k] = v
    return await _analysis_sheet_rows_impl(request=request, body=body, mode=mode, include_matrix_q=include_matrix_q, token=token, x_app_token=x_app_token, authorization=authorization, x_request_id=x_request_id)


@router.post("/sheet-rows")
async def analysis_sheet_rows(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    mode: str = Query(default="", description="Optional mode hint for engine/provider"),
    include_matrix_q: Optional[bool] = Query(default=None, alias="include_matrix"),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    return await _analysis_sheet_rows_impl(request=request, body=body, mode=mode, include_matrix_q=include_matrix_q, token=token, x_app_token=x_app_token, authorization=authorization, x_request_id=x_request_id)


__all__ = ["router", "ANALYSIS_SHEET_ROWS_VERSION"]
