#!/usr/bin/env python3
"""
routes/investment_advisor.py
================================================================================
ADVANCED INVESTMENT ADVISOR ROUTER — v2.5.0
================================================================================
TOP10-HARDENED • PAGE-AWARE • STAGE-TIMEOUT SAFE • CONTRACT-PROJECTED •
BRIDGE-AWARE • JSON-SAFE • STARTUP-SAFE • REQUEST-ID SAFE • AUTH-TOLERANT •
DERIVED-PAGE QUALITY GATES • ENGINE / RUNNER / BUILDER TOLERANT

Why this revision
-----------------
- FIX: Top_10_Investments no longer waits too long on weak or empty advanced
       execution paths. It now uses stage-aware timeouts and controlled degrade.
- FIX: weak fallback-only Top10 payloads are rejected instead of being treated
       as valid success.
- FIX: outputs are projected to canonical contracts whenever headers can be
       resolved, improving width alignment and completeness.
- FIX: source pages prefer sheet-rows bridge / engine page routes instead of
       unnecessary advisor-style Top10 resolution.
- FIX: derived pages use safer execution order:
       Top10 builder -> advanced runner -> bridge -> engine fallback
       Insights     -> bridge -> runner -> engine fallback
       Data Dict    -> bridge -> engine fallback
- FIX: returns schema-aligned partial payloads instead of hanging or producing
       ambiguous timeout behavior.
- SAFE: no import-time network work.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import math
import os
import time
import uuid
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal
from enum import Enum
from importlib import import_module
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, Response, status
from fastapi.encoders import jsonable_encoder

logger = logging.getLogger("routes.investment_advisor")
logger.addHandler(logging.NullHandler())

INVESTMENT_ADVISOR_VERSION = "2.5.0"

TOP10_PAGE_NAME = "Top_10_Investments"
INSIGHTS_PAGE_NAME = "Insights_Analysis"
DATA_DICTIONARY_PAGE_NAME = "Data_Dictionary"

BASE_SOURCE_PAGES = {
    "Market_Leaders",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
    "My_Portfolio",
}

DERIVED_PAGES = {
    TOP10_PAGE_NAME,
    INSIGHTS_PAGE_NAME,
}

KNOWN_CANONICAL_HEADER_COUNTS: Dict[str, int] = {
    "Market_Leaders": 80,
    "Global_Markets": 80,
    "Commodities_FX": 80,
    "Mutual_Funds": 80,
    "My_Portfolio": 80,
    "Insights_Analysis": 7,
    "Top_10_Investments": 83,
    "Data_Dictionary": 9,
}

TOP10_SPECIAL_FIELDS: Tuple[str, ...] = (
    "top10_rank",
    "selection_reason",
    "criteria_snapshot",
)

TOP10_BUSINESS_SIGNAL_FIELDS: Tuple[str, ...] = (
    "symbol",
    "name",
    "asset_class",
    "exchange",
    "currency",
    "country",
    "sector",
    "industry",
    "current_price",
    "price_change",
    "percent_change",
    "risk_score",
    "valuation_score",
    "overall_score",
    "opportunity_score",
    "risk_bucket",
    "confidence_bucket",
    "recommendation",
)

PAGE_ALIAS_MAP: Dict[str, str] = {
    "top10": TOP10_PAGE_NAME,
    "top_10": TOP10_PAGE_NAME,
    "top10investments": TOP10_PAGE_NAME,
    "top_10_investments": TOP10_PAGE_NAME,
    "top-10-investments": TOP10_PAGE_NAME,
    "top 10 investments": TOP10_PAGE_NAME,
    "investment_advisor": TOP10_PAGE_NAME,
    "investment-advisor": TOP10_PAGE_NAME,
    "advisor": TOP10_PAGE_NAME,
    "insights": INSIGHTS_PAGE_NAME,
    "insight": INSIGHTS_PAGE_NAME,
    "insights_analysis": INSIGHTS_PAGE_NAME,
    "insights-analysis": INSIGHTS_PAGE_NAME,
    "data_dictionary": DATA_DICTIONARY_PAGE_NAME,
    "data-dictionary": DATA_DICTIONARY_PAGE_NAME,
    "dictionary": DATA_DICTIONARY_PAGE_NAME,
}

TOP10_MODULE_CANDIDATES: Tuple[str, ...] = (
    "core.analysis.top10_selector",
    "core.analysis.top10_builder",
    "core.investment_advisor",
    "core.investment_advisor_engine",
    "core.analysis.investment_advisor",
    "core.analysis.investment_advisor_engine",
)

RUNNER_MODULE_CANDIDATES: Tuple[str, ...] = (
    "core.investment_advisor",
    "core.investment_advisor_engine",
    "core.analysis.investment_advisor",
    "core.analysis.investment_advisor_engine",
    "core.data_engine_v2",
    "core.data_engine",
)

BRIDGE_MODULE_CANDIDATES: Tuple[str, ...] = (
    "routes.analysis_sheet_rows",
    "routes.advanced_analysis",
)

TOP10_FUNCTION_CANDIDATES: Tuple[str, ...] = (
    "build_top10_output_rows",
    "build_top10_rows",
    "build_top10_investments_rows",
    "build_top10_investments_output_rows",
    "build_top10_investments",
    "build_top10",
    "get_top10_rows",
    "select_top10",
    "select_top10_symbols",
)

RUNNER_FUNCTION_CANDIDATES: Tuple[str, ...] = (
    "run_investment_advisor_engine",
    "run_investment_advisor",
    "run_advisor",
    "execute_investment_advisor",
    "execute_advisor",
    "get_sheet_rows",
    "get_page_rows",
    "build_top10_rows",
    "build_top10_output_rows",
    "build_top10_investments_rows",
    "select_top10",
    "select_top10_symbols",
)

BRIDGE_FUNCTION_CANDIDATES: Tuple[str, ...] = (
    "_run_analysis_sheet_rows_impl",
    "run_analysis_sheet_rows_impl",
    "_run_advanced_sheet_rows_impl",
    "run_advanced_sheet_rows_impl",
    "_run_sheet_rows_impl",
    "run_sheet_rows_impl",
    "_sheet_rows_impl",
)

CONTAINER_NAME_CANDIDATES: Tuple[str, ...] = (
    "top10_selector",
    "top10_builder",
    "advisor_service",
    "investment_advisor_service",
    "advisor_engine",
    "investment_advisor_engine",
    "advisor_runner",
    "investment_advisor_runner",
    "engine",
    "service",
    "create_top10_selector",
    "create_top10_builder",
    "get_top10_selector",
    "get_top10_builder",
    "create_investment_advisor",
    "get_investment_advisor",
    "build_investment_advisor",
)

STATE_ATTR_CANDIDATES: Tuple[str, ...] = (
    "top10_builder",
    "top10_selector",
    "investment_advisor_runner",
    "advisor_runner",
    "build_top10_rows",
    "select_top10",
    "select_top10_symbols",
    "run_investment_advisor",
    "run_investment_advisor_engine",
    "run_advisor",
    "investment_advisor_service",
    "advisor_service",
    "engine",
)

ENGINE_METHOD_CANDIDATES: Tuple[str, ...] = (
    "build_top10_rows",
    "build_top10_output_rows",
    "build_top10_investments_rows",
    "select_top10",
    "select_top10_symbols",
    "get_top10_rows",
    "run_investment_advisor",
    "run_investment_advisor_engine",
    "run_advisor",
    "execute_investment_advisor",
    "execute_advisor",
    "get_sheet_rows",
    "get_page_rows",
    "sheet_rows",
    "fetch_sheet_rows",
    "build_sheet_rows",
    "read_sheet_rows",
    "run_sheet_rows",
)

SCHEMA_MODULE_CANDIDATES: Tuple[str, ...] = (
    "core.sheets.schema_registry",
    "core.schema_registry",
    "core.sheets.page_catalog",
)

SCHEMA_MAP_CANDIDATES: Tuple[str, ...] = (
    "SCHEMA_REGISTRY",
    "SHEET_SPEC",
    "SHEET_SPECS",
    "PAGE_SPECS",
    "SPECS",
    "SHEET_CONTRACTS",
    "STATIC_CANONICAL_SHEET_CONTRACTS",
)

SCHEMA_FUNCTION_CANDIDATES: Tuple[str, ...] = (
    "get_sheet_spec",
    "get_schema_for_sheet",
    "get_schema",
    "resolve_sheet_spec",
    "resolve_sheet_schema",
    "get_contract_for_sheet",
)

router = APIRouter(prefix="/v1/advanced", tags=["advanced"])

# -----------------------------------------------------------------------------
# Optional Prometheus
# -----------------------------------------------------------------------------
try:
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest  # type: ignore

    PROMETHEUS_AVAILABLE = True
except Exception:
    CONTENT_TYPE_LATEST = "text/plain"
    generate_latest = None  # type: ignore
    PROMETHEUS_AVAILABLE = False

# -----------------------------------------------------------------------------
# Optional config/auth hooks
# -----------------------------------------------------------------------------
try:
    from core.config import auth_ok, get_settings_cached, is_open_mode  # type: ignore
except Exception:
    auth_ok = None  # type: ignore
    is_open_mode = None  # type: ignore

    def get_settings_cached(*args: Any, **kwargs: Any) -> Any:  # type: ignore
        return None


# -----------------------------------------------------------------------------
# Canonical fallback schemas
# -----------------------------------------------------------------------------
_CANONICAL_TOP10_SCHEMA_FALLBACK: List[Tuple[str, str]] = [
    ("symbol", "Symbol"),
    ("name", "Name"),
    ("asset_class", "Asset Class"),
    ("exchange", "Exchange"),
    ("currency", "Currency"),
    ("country", "Country"),
    ("sector", "Sector"),
    ("industry", "Industry"),
    ("current_price", "Current Price"),
    ("previous_close", "Previous Close"),
    ("open", "Open"),
    ("day_high", "Day High"),
    ("day_low", "Day Low"),
    ("high_52w", "52W High"),
    ("low_52w", "52W Low"),
    ("price_change", "Price Change"),
    ("percent_change", "Percent Change"),
    ("position_52w_pct", "52W Position %"),
    ("volume", "Volume"),
    ("avg_volume_10d", "Avg Volume 10D"),
    ("avg_volume_30d", "Avg Volume 30D"),
    ("market_cap", "Market Cap"),
    ("float_shares", "Float Shares"),
    ("beta_5y", "Beta 5Y"),
    ("pe_ttm", "P/E TTM"),
    ("pe_forward", "P/E Forward"),
    ("eps_ttm", "EPS TTM"),
    ("dividend_yield", "Dividend Yield"),
    ("payout_ratio", "Payout Ratio"),
    ("revenue_ttm", "Revenue TTM"),
    ("revenue_growth_yoy", "Revenue YoY Growth"),
    ("gross_margin", "Gross Margin"),
    ("operating_margin", "Operating Margin"),
    ("profit_margin", "Profit Margin"),
    ("debt_to_equity", "Debt/Equity"),
    ("free_cash_flow_ttm", "FCF TTM"),
    ("rsi_14", "RSI 14"),
    ("volatility_30d", "Volatility 30D"),
    ("volatility_90d", "Volatility 90D"),
    ("max_drawdown_1y", "Max Drawdown 1Y"),
    ("var_95_1d", "VaR 95% 1D"),
    ("sharpe_1y", "Sharpe 1Y"),
    ("risk_score", "Risk Score"),
    ("risk_bucket", "Risk Bucket"),
    ("pb_ratio", "P/B"),
    ("ps_ratio", "P/S"),
    ("ev_ebitda", "EV/EBITDA"),
    ("peg_ratio", "PEG"),
    ("intrinsic_value", "Intrinsic Value"),
    ("valuation_score", "Valuation Score"),
    ("forecast_price_1m", "Forecast Price 1M"),
    ("expected_roi_1m", "Expected ROI 1M"),
    ("forecast_price_3m", "Forecast Price 3M"),
    ("expected_roi_3m", "Expected ROI 3M"),
    ("forecast_price_12m", "Forecast Price 12M"),
    ("expected_roi_12m", "Expected ROI 12M"),
    ("forecast_confidence", "Forecast Confidence"),
    ("analyst_target_price", "Analyst Target Price"),
    ("analyst_upside_pct", "Analyst Upside %"),
    ("recommendation", "Recommendation"),
    ("value_score", "Value Score"),
    ("quality_score", "Quality Score"),
    ("momentum_score", "Momentum Score"),
    ("growth_score", "Growth Score"),
    ("overall_score", "Overall Score"),
    ("opportunity_score", "Opportunity Score"),
    ("rank_overall", "Rank Overall"),
    ("confidence_bucket", "Confidence Bucket"),
    ("source", "Source"),
    ("as_of_utc", "As Of UTC"),
    ("last_updated_riyadh", "Last Updated (Riyadh)"),
    ("notes", "Notes"),
    ("primary_provider", "Primary Provider"),
    ("classification_source", "Classification Source"),
    ("history_source", "History Source"),
    ("fundamentals_source", "Fundamentals Source"),
    ("forecast_source", "Forecast Source"),
    ("source_page", "Source Page"),
    ("row_status", "Row Status"),
    ("degraded", "Degraded"),
    ("top10_rank", "Top10 Rank"),
    ("selection_reason", "Selection Reason"),
    ("criteria_snapshot", "Criteria Snapshot"),
]

_CANONICAL_INSTRUMENT_SCHEMA_FALLBACK: List[Tuple[str, str]] = _CANONICAL_TOP10_SCHEMA_FALLBACK[:-3]

_CANONICAL_INSIGHTS_SCHEMA_FALLBACK: List[Tuple[str, str]] = [
    ("section", "Section"),
    ("item", "Item"),
    ("metric", "Metric"),
    ("value", "Value"),
    ("confidence", "Confidence"),
    ("notes", "Notes"),
    ("as_of_utc", "As Of UTC"),
]

_CANONICAL_DATA_DICTIONARY_SCHEMA_FALLBACK: List[Tuple[str, str]] = [
    ("sheet_name", "Sheet Name"),
    ("column_key", "Column Key"),
    ("header", "Header"),
    ("section", "Section"),
    ("data_type", "Data Type"),
    ("required", "Required"),
    ("description", "Description"),
    ("example", "Example"),
    ("order_index", "Order Index"),
]

_SCHEMA_HEADERS_CACHE: Dict[str, List[str]] = {}


# -----------------------------------------------------------------------------
# Generic helpers
# -----------------------------------------------------------------------------
def _s(v: Any) -> str:
    if v is None:
        return ""
    try:
        s = str(v).strip()
    except Exception:
        return ""
    return "" if s.lower() in {"none", "null", "nil", "undefined"} else s


def _safe_dict(v: Any) -> Dict[str, Any]:
    return dict(v) if isinstance(v, Mapping) else {}


def _safe_int(v: Any, default: int) -> int:
    try:
        if v is None or isinstance(v, bool):
            return default
        return int(float(v))
    except Exception:
        return default


def _safe_float(v: Any, default: float) -> float:
    try:
        if v is None or isinstance(v, bool):
            return default
        out = float(v)
        if math.isnan(out) or math.isinf(out):
            return default
        return out
    except Exception:
        return default


def _boolish(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    s = _s(v).lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _split_csv(text: str) -> List[str]:
    raw = (text or "").replace(";", ",").replace("\n", ",").replace("\t", ",")
    out: List[str] = []
    seen = set()
    for part in raw.split(","):
        s = _s(part)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _normalize_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return _split_csv(value.replace(" ", ","))
    if isinstance(value, (list, tuple, set)):
        out: List[str] = []
        seen = set()
        for item in value:
            s = _s(item)
            if s and s not in seen:
                seen.add(s)
                out.append(s)
        return out
    s = _s(value)
    return [s] if s else []


def _source_pages_only(values: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in values:
        page = _normalize_page_name(item)
        if page and page in BASE_SOURCE_PAGES and page not in seen:
            seen.add(page)
            out.append(page)
    return out


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _request_id(request: Optional[Request], x_request_id: Optional[str]) -> str:
    rid = _s(x_request_id)
    if rid:
        return rid
    try:
        if request is not None:
            state_rid = _s(getattr(request.state, "request_id", ""))
            if state_rid:
                return state_rid
            hdr = _s(request.headers.get("X-Request-ID"))
            if hdr:
                return hdr
    except Exception:
        pass
    return uuid.uuid4().hex[:12]


def _json_safe(value: Any) -> Any:
    def _clean(obj: Any) -> Any:
        if obj is None:
            return None
        if isinstance(obj, (str, int, bool)):
            return obj
        if isinstance(obj, float):
            return None if math.isnan(obj) or math.isinf(obj) else obj
        if isinstance(obj, Decimal):
            try:
                f = float(obj)
                return None if math.isnan(f) or math.isinf(f) else f
            except Exception:
                return _s(obj)
        if isinstance(obj, (datetime, date, dt_time)):
            try:
                return obj.isoformat()
            except Exception:
                return _s(obj)
        if isinstance(obj, Enum):
            return _clean(obj.value)
        if is_dataclass(obj):
            try:
                return _clean(asdict(obj))
            except Exception:
                return _s(obj)
        if isinstance(obj, Mapping):
            return {str(k): _clean(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, set)):
            return [_clean(x) for x in obj]
        if hasattr(obj, "model_dump") and callable(getattr(obj, "model_dump")):
            try:
                return _clean(obj.model_dump(mode="python"))
            except Exception:
                pass
        if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
            try:
                return _clean(obj.dict())
            except Exception:
                pass
        try:
            return jsonable_encoder(obj)
        except Exception:
            return _s(obj)

    return _clean(value)


def _json_compact(value: Any) -> str:
    try:
        return json.dumps(_json_safe(value), ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return _s(value)


def _normalize_page_name(value: Any) -> str:
    raw = _s(value)
    if not raw:
        return TOP10_PAGE_NAME

    try:
        from core.sheets.page_catalog import normalize_page_name as normalize_from_catalog  # type: ignore

        normalized = _s(normalize_from_catalog(raw))
        if normalized:
            return normalized
    except Exception:
        pass

    try:
        from core.sheets.page_catalog import resolve_page_name as resolve_from_catalog  # type: ignore

        normalized = _s(resolve_from_catalog(raw))
        if normalized:
            return normalized
    except Exception:
        pass

    compact = raw.strip().lower().replace(" ", "_")
    return PAGE_ALIAS_MAP.get(compact, raw)


def _page_family(page: str) -> str:
    normalized = _normalize_page_name(page)
    if normalized == TOP10_PAGE_NAME:
        return "top10"
    if normalized == INSIGHTS_PAGE_NAME:
        return "insights"
    if normalized == DATA_DICTIONARY_PAGE_NAME:
        return "data_dictionary"
    if normalized in BASE_SOURCE_PAGES:
        return "source"
    return "advanced"


def _timeout_seconds(env_name: str, default: float) -> float:
    return max(0.1, _safe_float(os.getenv(env_name), default))


def _resolver_timeout(stage: str, *, page: str) -> float:
    page = _normalize_page_name(page)
    defaults = {
        "builder": 14.0 if page == TOP10_PAGE_NAME else 10.0,
        "runner": 12.0 if page == TOP10_PAGE_NAME else 16.0,
        "bridge": 10.0 if page in BASE_SOURCE_PAGES or page == DATA_DICTIONARY_PAGE_NAME else 14.0,
        "engine": 12.0 if page in DERIVED_PAGES else 16.0,
    }
    env_map = {
        "builder": "TFB_ADV_BUILDER_TIMEOUT_SEC",
        "runner": "TFB_ADV_RUNNER_TIMEOUT_SEC",
        "bridge": "TFB_ADV_BRIDGE_TIMEOUT_SEC",
        "engine": "TFB_ADV_ENGINE_TIMEOUT_SEC",
    }
    return _timeout_seconds(env_map[stage], defaults[stage])


def _row_count(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (list, tuple, set)):
        return len(value)
    if isinstance(value, Mapping):
        return len(value)
    return 1


def _nonempty(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


# -----------------------------------------------------------------------------
# Auth helpers
# -----------------------------------------------------------------------------
def _is_public_path(path: str) -> bool:
    p = _s(path)
    if not p:
        return False
    if p in {
        "/v1/advanced/health",
        "/v1/advanced/meta",
        "/v1/advanced/metrics",
    }:
        return True

    env_paths = os.getenv("PUBLIC_PATHS", "") or os.getenv("AUTH_PUBLIC_PATHS", "")
    for raw in env_paths.split(","):
        candidate = raw.strip()
        if not candidate:
            continue
        if candidate.endswith("*") and p.startswith(candidate[:-1]):
            return True
        if p == candidate:
            return True
    return False


def _extract_auth_token(
    *,
    token_query: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
) -> str:
    token = _s(x_app_token)

    authz = _s(authorization)
    if authz.lower().startswith("bearer "):
        token = authz.split(" ", 1)[1].strip()

    if token_query and not token:
        allow_query = False
        try:
            settings = get_settings_cached()
            allow_query = bool(getattr(settings, "allow_query_token", False))
        except Exception:
            allow_query = False
        if allow_query:
            token = _s(token_query)

    return token


def _is_open_mode_enabled() -> bool:
    try:
        if callable(is_open_mode):
            result = is_open_mode()
            if inspect.isawaitable(result):
                return False
            return bool(result)
    except Exception:
        pass

    for name in ("OPEN_MODE", "TFB_OPEN_MODE", "AUTH_DISABLED"):
        env_v = _s(os.getenv(name))
        if env_v:
            return _boolish(env_v, False)
    return False


def _auth_passed(
    *,
    request: Request,
    token_query: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
) -> bool:
    if _is_open_mode_enabled():
        return True

    try:
        path = str(getattr(getattr(request, "url", None), "path", "") or "")
    except Exception:
        path = ""

    if _is_public_path(path):
        return True

    if auth_ok is None:
        return True

    auth_token = _extract_auth_token(
        token_query=token_query,
        x_app_token=x_app_token,
        authorization=authorization,
    )
    headers_dict = dict(request.headers)

    settings = None
    try:
        settings = get_settings_cached()
    except Exception:
        settings = None

    attempts = [
        {
            "token": auth_token or None,
            "authorization": authorization,
            "headers": headers_dict,
            "path": path,
            "request": request,
            "settings": settings,
        },
        {
            "token": auth_token or None,
            "authorization": authorization,
            "headers": headers_dict,
            "path": path,
            "request": request,
        },
        {
            "token": auth_token or None,
            "authorization": authorization,
            "headers": headers_dict,
            "path": path,
        },
        {
            "token": auth_token or None,
            "authorization": authorization,
            "headers": headers_dict,
        },
        {"token": auth_token or None, "authorization": authorization},
        {"token": auth_token or None},
        {},
    ]

    for kwargs in attempts:
        try:
            return bool(auth_ok(**kwargs))
        except TypeError:
            continue
        except Exception:
            return False
    return False


def _require_auth_or_401(
    *,
    request: Request,
    token_query: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
) -> None:
    if not _auth_passed(
        request=request,
        token_query=token_query,
        x_app_token=x_app_token,
        authorization=authorization,
    ):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")


# -----------------------------------------------------------------------------
# Engine accessor
# -----------------------------------------------------------------------------
async def _get_engine(request: Request) -> Optional[Any]:
    try:
        st = getattr(request.app, "state", None)
        if st and getattr(st, "engine", None):
            return st.engine
        if st and getattr(st, "data_engine", None):
            return st.data_engine
    except Exception:
        pass

    for modpath in ("core.data_engine_v2", "core.data_engine", "core.investment_advisor_engine"):
        try:
            mod = import_module(modpath)
        except Exception:
            continue

        for attr in ("get_engine", "get_data_engine", "engine", "data_engine", "ENGINE", "DATA_ENGINE"):
            candidate = getattr(mod, attr, None)
            if candidate is None:
                continue
            try:
                if callable(candidate):
                    eng = candidate()
                    if inspect.isawaitable(eng):
                        eng = await eng
                    if eng is not None:
                        return eng
                elif candidate is not None:
                    return candidate
            except Exception:
                continue
    return None


# -----------------------------------------------------------------------------
# Contract / schema helpers
# -----------------------------------------------------------------------------
def _schema_fallback_for_page(page: str) -> Tuple[List[str], List[str]]:
    family = _page_family(page)
    if family == "top10":
        schema = _CANONICAL_TOP10_SCHEMA_FALLBACK
    elif family == "insights":
        schema = _CANONICAL_INSIGHTS_SCHEMA_FALLBACK
    elif family == "data_dictionary":
        schema = _CANONICAL_DATA_DICTIONARY_SCHEMA_FALLBACK
    else:
        schema = _CANONICAL_INSTRUMENT_SCHEMA_FALLBACK
    return [h for _, h in schema], [k for k, _ in schema]


def _append_missing_headers(headers: List[str], fields: Sequence[str]) -> List[str]:
    out = list(headers)
    for field in fields:
        if field not in out:
            out.append(field)
    return out


def _append_missing_keys(keys: List[str], fields: Sequence[str]) -> List[str]:
    out = list(keys)
    for field in fields:
        if field not in out:
            out.append(field)
    return out


def _extract_headers_from_spec(spec: Any) -> List[str]:
    if spec is None:
        return []

    if isinstance(spec, Mapping):
        for key in ("headers", "display_headers", "keys", "columns"):
            value = spec.get(key)
            if isinstance(value, list) and value:
                out = [_s(x) for x in value if _s(x)]
                if out:
                    return out

        for key in ("sheet", "spec", "schema", "contract", "definition"):
            nested = spec.get(key)
            headers = _extract_headers_from_spec(nested)
            if headers:
                return headers
    return []


def _load_schema_defaults(page: str) -> Tuple[List[str], List[str]]:
    page = _normalize_page_name(page)
    if page in _SCHEMA_HEADERS_CACHE:
        headers = list(_SCHEMA_HEADERS_CACHE[page])
        if page == TOP10_PAGE_NAME:
            headers = _append_missing_headers(headers, TOP10_SPECIAL_FIELDS)
        keys = [_s(h) for h in headers]
        return headers, keys

    try:
        from core.sheets.schema_registry import get_sheet_spec  # type: ignore

        spec = get_sheet_spec(page)
        cols = getattr(spec, "columns", None) or []
        if cols:
            headers: List[str] = []
            keys: List[str] = []
            for idx, col in enumerate(cols):
                key = _s(getattr(col, "key", ""))
                header = _s(getattr(col, "header", ""))
                if not key and not header:
                    continue
                if not key and header:
                    key = f"column_{idx + 1}"
                if key and not header:
                    header = key.replace("_", " ").title()
                keys.append(key)
                headers.append(header)
            if headers and keys:
                if page == TOP10_PAGE_NAME:
                    headers = _append_missing_headers(headers, TOP10_SPECIAL_FIELDS)
                    keys = _append_missing_keys(keys, TOP10_SPECIAL_FIELDS)
                _SCHEMA_HEADERS_CACHE[page] = list(headers)
                return headers, keys
    except Exception:
        pass

    for module_name in SCHEMA_MODULE_CANDIDATES:
        module = _import_module_safely(module_name)
        if module is None:
            continue

        for map_name in SCHEMA_MAP_CANDIDATES:
            spec_map = getattr(module, map_name, None)
            if not isinstance(spec_map, Mapping):
                continue
            try:
                hit = spec_map.get(page) or spec_map.get(page.lower()) or spec_map.get(page.upper())
            except Exception:
                hit = None
            headers = _extract_headers_from_spec(hit)
            if headers:
                _SCHEMA_HEADERS_CACHE[page] = list(headers)
                keys = [_s(h) for h in headers]
                if page == TOP10_PAGE_NAME:
                    headers = _append_missing_headers(headers, TOP10_SPECIAL_FIELDS)
                    keys = _append_missing_keys(keys, TOP10_SPECIAL_FIELDS)
                return headers, keys

        for fn_name in SCHEMA_FUNCTION_CANDIDATES:
            fn = getattr(module, fn_name, None)
            if not callable(fn):
                continue
            for kwargs in (
                {"sheet": page},
                {"page": page},
                {"sheet_name": page},
                {"name": page},
                {},
            ):
                try:
                    out = fn(**kwargs) if kwargs else fn(page)
                except TypeError:
                    continue
                except Exception:
                    out = None
                headers = _extract_headers_from_spec(out)
                if headers:
                    _SCHEMA_HEADERS_CACHE[page] = list(headers)
                    keys = [_s(h) for h in headers]
                    if page == TOP10_PAGE_NAME:
                        headers = _append_missing_headers(headers, TOP10_SPECIAL_FIELDS)
                        keys = _append_missing_keys(keys, TOP10_SPECIAL_FIELDS)
                    return headers, keys

    headers, keys = _schema_fallback_for_page(page)
    if page == TOP10_PAGE_NAME:
        headers = _append_missing_headers(headers, TOP10_SPECIAL_FIELDS)
        keys = _append_missing_keys(keys, TOP10_SPECIAL_FIELDS)
    _SCHEMA_HEADERS_CACHE[page] = list(headers)
    return headers, keys


# -----------------------------------------------------------------------------
# Result normalization helpers
# -----------------------------------------------------------------------------
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
    if is_dataclass(obj):
        try:
            return asdict(obj)
        except Exception:
            return {}
    try:
        if hasattr(obj, "model_dump") and callable(getattr(obj, "model_dump")):
            dumped = obj.model_dump(mode="python")
            if isinstance(dumped, dict):
                return dumped
    except Exception:
        pass
    try:
        if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
            dumped = obj.dict()
            if isinstance(dumped, dict):
                return dumped
    except Exception:
        pass
    try:
        if hasattr(obj, "__dict__"):
            d = vars(obj)
            if isinstance(d, dict):
                return dict(d)
    except Exception:
        pass
    return {"result": obj}


def _extract_headers_and_keys(payload_dict: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    headers = (
        payload_dict.get("display_headers")
        or payload_dict.get("sheet_headers")
        or payload_dict.get("column_headers")
        or payload_dict.get("headers")
        or []
    )
    keys = payload_dict.get("keys") or payload_dict.get("fields") or payload_dict.get("columns") or []

    if not isinstance(headers, list):
        headers = []
    if not isinstance(keys, list):
        keys = []

    headers = [_s(x) for x in headers if _s(x)]
    keys = [_s(x) for x in keys if _s(x)]
    return headers, keys


def _rows_to_matrix(rows: List[Dict[str, Any]], keys: List[str]) -> List[List[Any]]:
    return [[row.get(k) for k in keys] for row in rows]


def _dicts_from_matrix(matrix: Any, keys: List[str]) -> List[Dict[str, Any]]:
    if not isinstance(matrix, list) or not keys:
        return []
    out: List[Dict[str, Any]] = []
    for row in matrix:
        if isinstance(row, dict):
            out.append(dict(row))
            continue
        if not isinstance(row, list):
            continue
        out.append({k: (row[idx] if idx < len(row) else None) for idx, k in enumerate(keys)})
    return out


def _extract_rows_candidate(payload: Any, *, keys_hint: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    keys_hint = list(keys_hint or [])

    if isinstance(payload, list):
        if payload and isinstance(payload[0], dict):
            return [dict(x) for x in payload if isinstance(x, dict)]
        if payload and isinstance(payload[0], list) and keys_hint:
            return _dicts_from_matrix(payload, keys_hint)
        return []

    if not isinstance(payload, dict):
        payload = _model_to_dict(payload)
        if not isinstance(payload, dict):
            return []

    local_headers, local_keys = _extract_headers_and_keys(payload)
    effective_keys = list(keys_hint or local_keys or [])

    for key in ("row_objects", "records", "items", "data", "quotes", "rows", "recommendations", "results"):
        value = payload.get(key)
        if isinstance(value, list):
            if value and isinstance(value[0], dict):
                return [dict(x) for x in value if isinstance(x, dict)]
            if value and isinstance(value[0], list) and effective_keys:
                return _dicts_from_matrix(value, effective_keys)
            if not value:
                return []

    for key in ("rows_matrix", "matrix"):
        value = payload.get(key)
        if isinstance(value, list) and effective_keys:
            return _dicts_from_matrix(value, effective_keys)

    result = payload.get("result")
    if isinstance(result, list):
        if result and isinstance(result[0], dict):
            return [dict(x) for x in result if isinstance(x, dict)]
        if result and isinstance(result[0], list) and effective_keys:
            return _dicts_from_matrix(result, effective_keys)

    nested = payload.get("payload")
    if isinstance(nested, dict):
        nested_headers, nested_keys = _extract_headers_and_keys(nested)
        return _extract_rows_candidate(nested, keys_hint=effective_keys or nested_keys or local_keys or nested_headers)

    return []


def _canonical_selection_reason(row: Dict[str, Any]) -> Optional[str]:
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
        ("valuation", "valuation_score"),
    ):
        val = row.get(key)
        if isinstance(val, (int, float)):
            score_parts.append(f"{label}={round(float(val), 2)}")

    roi_parts: List[str] = []
    for label, key in (("1M", "expected_roi_1m"), ("3M", "expected_roi_3m"), ("12M", "expected_roi_12m")):
        val = row.get(key)
        if isinstance(val, (int, float)):
            roi_parts.append(f"{label} ROI={round(float(val) * 100, 2)}%")

    reason_parts: List[str] = []
    if recommendation:
        reason_parts.append(f"Recommendation={recommendation}")
    if confidence_bucket:
        reason_parts.append(f"Confidence={confidence_bucket}")
    if risk_bucket:
        reason_parts.append(f"Risk={risk_bucket}")
    if score_parts:
        reason_parts.append(", ".join(score_parts[:3]))
    if roi_parts:
        reason_parts.append(", ".join(roi_parts[:2]))

    if not reason_parts:
        return None
    return " | ".join(reason_parts)


def _rank_rows_in_order(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows, start=1):
        r = dict(row)
        if not _nonempty(r.get("top10_rank")):
            r["top10_rank"] = idx
        if not _nonempty(r.get("rank_overall")):
            r["rank_overall"] = idx
        out.append(r)
    return out


def _apply_top10_field_backfill(rows: List[Dict[str, Any]], *, keys: List[str], criteria: Dict[str, Any]) -> List[Dict[str, Any]]:
    criteria_snapshot = _json_compact(criteria) if criteria else None
    out: List[Dict[str, Any]] = []

    for idx, row in enumerate(rows, start=1):
        r = dict(row)
        if "top10_rank" in keys and not _nonempty(r.get("top10_rank")):
            r["top10_rank"] = idx
        if "selection_reason" in keys and not _nonempty(r.get("selection_reason")):
            r["selection_reason"] = _canonical_selection_reason(r)
        if "criteria_snapshot" in keys and not _nonempty(r.get("criteria_snapshot")) and criteria_snapshot is not None:
            r["criteria_snapshot"] = criteria_snapshot
        if "source_page" in keys and not _nonempty(r.get("source_page")):
            src_page = _normalize_page_name(row.get("source_page") or row.get("page") or row.get("sheet"))
            if src_page and src_page != TOP10_PAGE_NAME:
                r["source_page"] = src_page
        out.append(r)
    return out


def _ensure_schema_projection(rows: List[Dict[str, Any]], keys: List[str]) -> List[Dict[str, Any]]:
    if not keys:
        return [dict(r) for r in rows if isinstance(r, dict)]
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        normalized.append({k: row.get(k, None) for k in keys})
    return normalized


def _normalize_page_payload(
    payload: Any,
    *,
    target_page: str,
    criteria_used: Dict[str, Any],
    eff_limit: int,
    forced_dispatch: str = "",
) -> Tuple[List[str], List[str], List[Dict[str, Any]], Dict[str, Any], str]:
    if isinstance(payload, dict):
        payload_dict = dict(payload)
    elif isinstance(payload, list):
        payload_dict = {"rows": payload}
    else:
        payload_dict = _model_to_dict(payload)

    headers, keys = _extract_headers_and_keys(payload_dict)
    if not headers or not keys:
        schema_headers, schema_keys = _load_schema_defaults(target_page)
        if not headers:
            headers = schema_headers
        if not keys:
            keys = schema_keys

    if _normalize_page_name(target_page) == TOP10_PAGE_NAME:
        headers = _append_missing_headers(list(headers), TOP10_SPECIAL_FIELDS)
        keys = _append_missing_keys(list(keys), TOP10_SPECIAL_FIELDS)

    rows = _extract_rows_candidate(payload_dict, keys_hint=keys)
    if not rows and isinstance(payload_dict.get("payload"), dict):
        rows = _extract_rows_candidate(payload_dict.get("payload"), keys_hint=keys)

    status_out = _s(payload_dict.get("status")) or ("success" if rows else "partial")
    dict_rows = [dict(r) for r in rows if isinstance(r, dict)]

    if _normalize_page_name(target_page) == TOP10_PAGE_NAME:
        dict_rows = _apply_top10_field_backfill(dict_rows, keys=keys, criteria=criteria_used)
        dict_rows = _rank_rows_in_order(dict_rows)

    norm_rows = _ensure_schema_projection(dict_rows, keys)
    norm_rows = norm_rows[:eff_limit]

    meta = payload_dict.get("meta") if isinstance(payload_dict.get("meta"), dict) else {}
    if forced_dispatch and not _s(meta.get("dispatch")):
        meta["dispatch"] = forced_dispatch
    return headers, keys, norm_rows, dict(meta), status_out


def _looks_like_fallback_only_top10(rows: List[Dict[str, Any]], meta: Mapping[str, Any]) -> bool:
    if not rows:
        return False

    fallback_flag = _boolish(meta.get("fallback"), False)
    reason = _s(meta.get("reason")).lower()

    fallback_selection_hits = 0
    low_signal_rows = 0

    for row in rows:
        selection_reason = _s(row.get("selection_reason")).lower()
        if "fallback candidate" in selection_reason:
            fallback_selection_hits += 1

        informative = 0
        for field in TOP10_BUSINESS_SIGNAL_FIELDS:
            if _nonempty(row.get(field)):
                informative += 1

        if informative <= 2:
            low_signal_rows += 1

    mostly_fallback_selection = fallback_selection_hits >= max(1, math.ceil(len(rows) * 0.6))
    mostly_low_signal = low_signal_rows >= max(1, math.ceil(len(rows) * 0.6))

    if fallback_flag and (mostly_fallback_selection or mostly_low_signal):
        return True
    if "engine_unavailable_or_empty" in reason and mostly_low_signal:
        return True
    return False


def _has_usable_payload(
    *,
    page: str,
    headers: List[str],
    rows: List[Dict[str, Any]],
    meta: Mapping[str, Any],
    schema_only: bool,
) -> bool:
    if schema_only:
        return bool(headers)

    if page == DATA_DICTIONARY_PAGE_NAME:
        return bool(rows) or bool(headers)

    if not rows:
        return False

    if page == TOP10_PAGE_NAME and _looks_like_fallback_only_top10(rows, meta):
        return False

    return True


def _schema_only_payload(
    *,
    request_id: str,
    target_page: str,
    headers: List[str],
    keys: List[str],
    include_matrix: bool,
    meta: Dict[str, Any],
) -> Dict[str, Any]:
    rows_matrix = [] if (include_matrix and keys) else None
    return {
        "status": "partial",
        "page": target_page,
        "sheet": target_page,
        "sheet_name": target_page,
        "route_family": _page_family(target_page),
        "headers": headers,
        "display_headers": headers,
        "keys": keys,
        "rows": [],
        "row_objects": [],
        "items": [],
        "data": [],
        "rows_matrix": rows_matrix,
        "version": INVESTMENT_ADVISOR_VERSION,
        "request_id": request_id,
        "meta": meta,
    }


# -----------------------------------------------------------------------------
# Criteria / payload preparation
# -----------------------------------------------------------------------------
def _flatten_criteria(body: Dict[str, Any]) -> Dict[str, Any]:
    crit: Dict[str, Any] = {}
    if isinstance(body.get("criteria"), dict):
        crit.update(body["criteria"])
    if isinstance(body.get("filters"), dict):
        crit.update(body["filters"])
    settings = body.get("settings")
    if isinstance(settings, dict) and isinstance(settings.get("criteria"), dict):
        crit.update(settings["criteria"])

    for k in (
        "page",
        "sheet",
        "sheet_name",
        "name",
        "tab",
        "pages_selected",
        "pages",
        "selected_pages",
        "direct_symbols",
        "symbols",
        "tickers",
        "limit",
        "top_n",
        "invest_period_days",
        "investment_period_days",
        "horizon_days",
        "invest_period_label",
        "min_expected_roi",
        "min_roi",
        "max_risk_score",
        "risk_level",
        "risk_profile",
        "min_confidence",
        "min_ai_confidence",
        "confidence_level",
        "confidence_bucket",
        "min_volume",
        "use_liquidity_tiebreak",
        "enforce_risk_confidence",
        "include_positions",
        "enrich_final",
        "preview",
        "schema_only",
        "mode",
        "include_matrix",
        "offset",
    ):
        if k in body and body.get(k) is not None:
            crit[k] = body.get(k)
    return crit


def _effective_limit(body: Dict[str, Any], limit_q: Optional[int]) -> int:
    max_limit = max(1, min(200, _safe_int(os.getenv("ADV_TOP10_MAX_LIMIT"), 50)))
    default_limit = max(1, min(max_limit, _safe_int(os.getenv("ADV_TOP10_DEFAULT_LIMIT"), 10)))
    if isinstance(limit_q, int):
        eff = limit_q
    else:
        eff = _safe_int(
            body.get("limit") or body.get("top_n") or _safe_dict(body.get("criteria")).get("top_n") or default_limit,
            default_limit,
        )
    return max(1, min(max_limit, int(eff)))


def _prepare_effective_criteria(body: Dict[str, Any], eff_limit: int) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    crit = _flatten_criteria(body or {})

    target_page = _normalize_page_name(
        crit.get("page") or crit.get("sheet") or crit.get("sheet_name") or crit.get("name") or crit.get("tab") or TOP10_PAGE_NAME
    )
    crit["page"] = target_page
    crit["sheet"] = target_page
    crit["sheet_name"] = target_page

    pages = _normalize_list(crit.get("pages_selected") or crit.get("pages") or crit.get("selected_pages"))
    pages = _source_pages_only(pages)

    direct_symbols = _normalize_list(crit.get("direct_symbols") or crit.get("symbols") or crit.get("tickers"))

    request_unconstrained = (not pages) and (not direct_symbols) and target_page == TOP10_PAGE_NAME
    pages_explicit = bool(pages)

    if request_unconstrained:
        default_pages = _normalize_list(os.getenv("ADV_TOP10_DEFAULT_PAGES", "Market_Leaders,Global_Markets"))
        pages = _source_pages_only(default_pages)
        crit["pages_selected"] = pages

    max_pages = max(1, min(20, _safe_int(os.getenv("ADV_TOP10_MAX_PAGES"), 5)))
    pages_trimmed = False
    if pages and len(pages) > max_pages:
        pages = pages[:max_pages]
        crit["pages_selected"] = pages
        pages_trimmed = True

    if direct_symbols:
        crit["direct_symbols"] = direct_symbols
        crit["symbols"] = direct_symbols
        crit["tickers"] = direct_symbols

    risk_level = _s(crit.get("risk_level")) or _s(crit.get("risk_profile")) or "moderate"
    crit["risk_level"] = risk_level
    crit["risk_profile"] = risk_level

    if crit.get("invest_period_days") is None:
        if crit.get("investment_period_days") is not None:
            crit["invest_period_days"] = crit.get("investment_period_days")
        elif crit.get("horizon_days") is not None:
            crit["invest_period_days"] = crit.get("horizon_days")
        else:
            crit["invest_period_days"] = 90

    if crit.get("horizon_days") is None and crit.get("invest_period_days") is not None:
        crit["horizon_days"] = crit.get("invest_period_days")

    if crit.get("min_roi") is None and crit.get("min_expected_roi") is not None:
        crit["min_roi"] = crit.get("min_expected_roi")
    if crit.get("min_expected_roi") is None and crit.get("min_roi") is not None:
        crit["min_expected_roi"] = crit.get("min_roi")

    crit["top_n"] = eff_limit
    crit["limit"] = eff_limit
    crit["offset"] = max(0, _safe_int(crit.get("offset"), 0))

    if "enrich_final" not in crit:
        if target_page == TOP10_PAGE_NAME:
            crit["enrich_final"] = False if request_unconstrained else (len(pages) <= 2 or bool(direct_symbols))
        else:
            crit["enrich_final"] = True

    prep_meta = {
        "request_unconstrained": request_unconstrained,
        "pages_explicit": pages_explicit,
        "pages_effective": list(pages),
        "direct_symbols_count": len(direct_symbols),
        "pages_trimmed": pages_trimmed,
        "allow_row_fallback": True,
        "target_page": target_page,
        "page_family": _page_family(target_page),
    }
    return crit, prep_meta


def _narrow_criteria_for_fallback(criteria: Dict[str, Any], eff_limit: int) -> Dict[str, Any]:
    narrowed = dict(criteria)
    target_page = _normalize_page_name(narrowed.get("page") or TOP10_PAGE_NAME)

    fallback_pages_cap = max(1, min(10, _safe_int(os.getenv("ADV_TOP10_FALLBACK_MAX_PAGES"), 2)))
    fallback_top_n = max(1, min(eff_limit, _safe_int(os.getenv("ADV_TOP10_FALLBACK_TOP_N"), min(3, eff_limit))))

    direct_symbols = _normalize_list(narrowed.get("direct_symbols") or narrowed.get("symbols") or narrowed.get("tickers"))
    pages = _normalize_list(narrowed.get("pages_selected") or narrowed.get("pages") or narrowed.get("selected_pages"))
    pages = _source_pages_only(pages)

    if target_page == TOP10_PAGE_NAME:
        if direct_symbols:
            narrowed["direct_symbols"] = direct_symbols[: max(1, min(len(direct_symbols), eff_limit))]
            narrowed["symbols"] = narrowed["direct_symbols"]
            narrowed["tickers"] = narrowed["direct_symbols"]
            narrowed["top_n"] = min(eff_limit, len(narrowed["direct_symbols"]))
            narrowed["limit"] = narrowed["top_n"]
        else:
            if not pages:
                pages = _source_pages_only(_normalize_list(os.getenv("ADV_TOP10_DEFAULT_PAGES", "Market_Leaders")))
            pages = pages[:fallback_pages_cap]
            narrowed["pages_selected"] = pages
            narrowed["top_n"] = fallback_top_n
            narrowed["limit"] = fallback_top_n
        narrowed["enrich_final"] = False
    else:
        narrowed["top_n"] = eff_limit
        narrowed["limit"] = eff_limit
        narrowed["enrich_final"] = True

    return narrowed


# -----------------------------------------------------------------------------
# Resolver helpers
# -----------------------------------------------------------------------------
def _import_module_safely(name: str) -> Optional[Any]:
    try:
        return import_module(name)
    except Exception:
        return None


def _callable_by_names(container: Any, names: Iterable[str]) -> Optional[Tuple[Callable[..., Any], str]]:
    for name in names:
        try:
            fn = getattr(container, name, None)
        except Exception:
            fn = None
        if callable(fn):
            return fn, name
    return None


async def _materialize_holder(holder: Any) -> Any:
    if holder is None:
        return None
    try:
        if inspect.isclass(holder):
            return holder()
    except Exception:
        return None

    if callable(holder):
        try:
            out = holder()
            if inspect.isawaitable(out):
                out = await out
            return out
        except TypeError:
            return None
        except Exception:
            return None
    return holder


async def _resolve_from_container(container: Any, label: str, names: Sequence[str]) -> Optional[Tuple[Callable[..., Any], str, str]]:
    direct = _callable_by_names(container, names)
    if direct:
        fn, fn_name = direct
        return fn, label, fn_name

    for holder_name in CONTAINER_NAME_CANDIDATES:
        try:
            holder = getattr(container, holder_name, None)
        except Exception:
            holder = None
        if holder is None:
            continue
        obj = await _materialize_holder(holder)
        if obj is None:
            continue
        direct_obj = _callable_by_names(obj, names)
        if direct_obj:
            fn, fn_name = direct_obj
            return fn, label, f"{holder_name}.{fn_name}"
    return None


async def _resolve_callable(
    request: Request,
    engine: Any,
    *,
    modules: Sequence[str],
    names: Sequence[str],
) -> Tuple[Optional[Callable[..., Any]], str, str, List[str]]:
    searched: List[str] = []
    import_errors: List[str] = []

    try:
        st = getattr(request.app, "state", None)
    except Exception:
        st = None

    if st is not None:
        searched.append("app.state")
        direct_state = _callable_by_names(st, list(names) + list(STATE_ATTR_CANDIDATES))
        if direct_state:
            fn, fn_name = direct_state
            return fn, "app.state", fn_name, searched
        resolved_state = await _resolve_from_container(st, "app.state", names)
        if resolved_state:
            fn, src, name = resolved_state
            return fn, src, name, searched

    for mod_name in modules:
        searched.append(mod_name)
        try:
            mod = import_module(mod_name)
        except Exception as e:
            import_errors.append(f"{mod_name}: {type(e).__name__}: {e}")
            continue
        resolved = await _resolve_from_container(mod, mod_name, names)
        if resolved:
            fn, src, name = resolved
            return fn, src, name, searched

    if engine is not None:
        searched.append("engine")
        direct_engine = _callable_by_names(engine, list(names) + list(ENGINE_METHOD_CANDIDATES))
        if direct_engine:
            fn, fn_name = direct_engine
            return fn, "engine", fn_name, searched

    if import_errors:
        logger.warning("Advanced callable import errors: %s", import_errors)
    return None, "", "", searched


# -----------------------------------------------------------------------------
# Callable invocation helpers
# -----------------------------------------------------------------------------
async def _call_maybe_async(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    if inspect.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)
    out = await asyncio.to_thread(fn, *args, **kwargs)
    if inspect.isawaitable(out):
        return await out
    return out


async def _call_runner_with_tolerance(
    runner: Callable[..., Any],
    *,
    request: Request,
    engine: Any,
    criteria: Dict[str, Any],
    eff_limit: int,
    mode: str,
    settings: Any,
    include_matrix: bool,
    schema_only: bool,
) -> Any:
    page = _normalize_page_name(criteria.get("page") or criteria.get("sheet") or criteria.get("sheet_name") or TOP10_PAGE_NAME)
    body_payload = {
        "page": page,
        "sheet": page,
        "sheet_name": page,
        "criteria": criteria,
        "top_n": eff_limit,
        "limit": eff_limit,
        "mode": mode or "",
        "include_matrix": include_matrix,
        "schema_only": schema_only,
    }

    attempts = [
        {
            "engine": engine,
            "criteria": criteria,
            "limit": eff_limit,
            "mode": mode or "",
            "request": request,
            "settings": settings,
            "page": page,
            "sheet": page,
            "sheet_name": page,
            "include_matrix": include_matrix,
            "schema_only": schema_only,
        },
        {
            "engine": engine,
            "criteria": criteria,
            "limit": eff_limit,
            "mode": mode or "",
            "settings": settings,
            "page": page,
            "sheet": page,
            "sheet_name": page,
        },
        {
            "engine": engine,
            "criteria": criteria,
            "limit": eff_limit,
            "mode": mode or "",
            "page": page,
            "sheet": page,
            "sheet_name": page,
        },
        {
            "engine": engine,
            "body": body_payload,
            "limit": eff_limit,
            "mode": mode or "",
            "request": request,
            "settings": settings,
        },
        {"engine": engine, "body": body_payload, "limit": eff_limit, "mode": mode or ""},
        {"engine": engine, "payload": body_payload, "limit": eff_limit, "mode": mode or ""},
        {"request": request, "body": body_payload, "engine": engine},
        {"criteria": criteria, "engine": engine},
        {"body": body_payload},
        {"payload": body_payload},
        {"criteria": criteria},
        {"page": page, "limit": eff_limit},
        {"page": page},
    ]

    for kwargs in attempts:
        try:
            return await _call_maybe_async(runner, **kwargs)
        except TypeError:
            continue

    positional_attempts = [
        (engine, criteria, eff_limit),
        (criteria, engine, eff_limit),
        (criteria, engine),
        (engine, criteria),
        (criteria,),
        (body_payload,),
        (page,),
    ]
    last_error: Optional[Exception] = None
    for args in positional_attempts:
        try:
            return await _call_maybe_async(runner, *args)
        except TypeError as e:
            last_error = e
            continue

    if last_error is not None:
        raise last_error
    raise RuntimeError("No compatible runner signature matched")


async def _run_callable_with_timeout(
    *,
    runner: Callable[..., Any],
    request: Request,
    engine: Any,
    criteria: Dict[str, Any],
    eff_limit: int,
    mode: str,
    timeout_sec: float,
    settings: Any,
    include_matrix: bool,
    schema_only: bool,
) -> Any:
    coro = _call_runner_with_tolerance(
        runner,
        request=request,
        engine=engine,
        criteria=criteria,
        eff_limit=eff_limit,
        mode=mode,
        settings=settings,
        include_matrix=include_matrix,
        schema_only=schema_only,
    )
    return await asyncio.wait_for(coro, timeout=timeout_sec)


async def _resolve_top10_builder(
    request: Request,
    engine: Any,
) -> Tuple[Optional[Callable[..., Any]], str, str, List[str]]:
    return await _resolve_callable(
        request,
        engine,
        modules=TOP10_MODULE_CANDIDATES,
        names=TOP10_FUNCTION_CANDIDATES,
    )


async def _resolve_runner(
    request: Request,
    engine: Any,
) -> Tuple[Optional[Callable[..., Any]], str, str, List[str]]:
    return await _resolve_callable(
        request,
        engine,
        modules=RUNNER_MODULE_CANDIDATES,
        names=RUNNER_FUNCTION_CANDIDATES,
    )


async def _resolve_bridge_impl(
    request: Request,
) -> Tuple[Optional[Callable[..., Any]], str, str]:
    try:
        st = getattr(request.app, "state", None)
    except Exception:
        st = None

    if st is not None:
        for name in BRIDGE_FUNCTION_CANDIDATES:
            candidate = getattr(st, name, None)
            if callable(candidate):
                return candidate, "app.state", name

    for mod_name in BRIDGE_MODULE_CANDIDATES:
        mod = _import_module_safely(mod_name)
        if mod is None:
            continue
        for name in BRIDGE_FUNCTION_CANDIDATES:
            candidate = getattr(mod, name, None)
            if callable(candidate):
                return candidate, mod_name, name

    return None, "", ""


# -----------------------------------------------------------------------------
# Bridge / engine fallback helpers
# -----------------------------------------------------------------------------
async def _delegate_to_bridge_sheet_rows(
    *,
    request: Request,
    body: Dict[str, Any],
    mode: str,
    include_matrix: bool,
    token: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
    x_request_id: Optional[str],
) -> Optional[Dict[str, Any]]:
    impl, src, name = await _resolve_bridge_impl(request)
    if impl is None:
        return None

    try:
        out = await asyncio.wait_for(
            _call_maybe_async(
                impl,
                request=request,
                body=body,
                payload=body,
                mode=mode or "",
                include_matrix_q=include_matrix,
                token=token,
                x_app_token=x_app_token,
                authorization=authorization,
                x_request_id=x_request_id,
                page=body.get("page"),
                sheet=body.get("sheet"),
                sheet_name=body.get("sheet_name"),
                symbols=body.get("symbols") or body.get("direct_symbols"),
                tickers=body.get("tickers") or body.get("direct_symbols"),
                top_n=body.get("top_n"),
                limit=body.get("limit"),
                offset=body.get("offset"),
            ),
            timeout=_resolver_timeout("bridge", page=_normalize_page_name(body.get("page"))),
        )
        if isinstance(out, dict):
            payload = dict(out)
        else:
            payload = _model_to_dict(out)
        meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
        meta.setdefault("dispatch", "advanced_bridge")
        meta.setdefault("bridge_source", src)
        meta.setdefault("bridge_name", name)
        payload["meta"] = meta
        return payload
    except Exception as exc:
        logger.warning("Advanced bridge failed. source=%s name=%s error=%s", src, name, exc, exc_info=True)
        return None


async def _try_engine_page_fallback(
    *,
    engine: Any,
    criteria: Dict[str, Any],
    eff_limit: int,
    mode: str,
    include_matrix: bool,
    schema_only: bool,
) -> Optional[Dict[str, Any]]:
    if engine is None:
        return None

    page = _normalize_page_name(criteria.get("page") or criteria.get("sheet") or criteria.get("sheet_name") or TOP10_PAGE_NAME)
    query_symbols = _normalize_list(criteria.get("direct_symbols") or criteria.get("symbols") or criteria.get("tickers"))
    family = _page_family(page)

    call_plans: List[Tuple[str, Dict[str, Any]]] = []
    if family == "top10":
        call_plans.extend(
            [
                ("build_top10_rows", {"criteria": criteria, "limit": eff_limit, "mode": mode or ""}),
                ("build_top10_output_rows", {"criteria": criteria, "limit": eff_limit, "mode": mode or ""}),
                ("build_top10_investments_rows", {"criteria": criteria, "limit": eff_limit, "mode": mode or ""}),
                ("select_top10", {"criteria": criteria, "limit": eff_limit, "mode": mode or ""}),
                ("select_top10_symbols", {"criteria": criteria, "limit": eff_limit, "mode": mode or ""}),
            ]
        )

    call_plans.extend(
        [
            (
                "run_investment_advisor_engine",
                {
                    "criteria": criteria,
                    "page": page,
                    "sheet": page,
                    "sheet_name": page,
                    "limit": eff_limit,
                    "mode": mode or "",
                },
            ),
            (
                "run_investment_advisor",
                {
                    "criteria": criteria,
                    "page": page,
                    "sheet": page,
                    "sheet_name": page,
                    "limit": eff_limit,
                    "mode": mode or "",
                },
            ),
            (
                "run_advisor",
                {
                    "criteria": criteria,
                    "page": page,
                    "sheet": page,
                    "sheet_name": page,
                    "limit": eff_limit,
                    "mode": mode or "",
                },
            ),
            (
                "get_sheet_rows",
                {
                    "page": page,
                    "sheet": page,
                    "sheet_name": page,
                    "limit": eff_limit,
                    "mode": mode or "",
                    "tickers": query_symbols,
                    "symbols": query_symbols,
                    "include_matrix": include_matrix,
                    "schema_only": schema_only,
                },
            ),
            (
                "get_page_rows",
                {
                    "page": page,
                    "sheet": page,
                    "sheet_name": page,
                    "limit": eff_limit,
                    "mode": mode or "",
                    "tickers": query_symbols,
                    "symbols": query_symbols,
                    "include_matrix": include_matrix,
                    "schema_only": schema_only,
                },
            ),
        ]
    )

    for method_name, kwargs in call_plans:
        fn = getattr(engine, method_name, None)
        if not callable(fn):
            continue
        try:
            out = await asyncio.wait_for(
                _call_maybe_async(fn, **kwargs),
                timeout=_resolver_timeout("engine", page=page),
            )
        except TypeError:
            try:
                out = await asyncio.wait_for(
                    _call_maybe_async(fn, page),
                    timeout=_resolver_timeout("engine", page=page),
                )
            except Exception:
                continue
        except Exception:
            continue

        if isinstance(out, dict):
            payload = dict(out)
        else:
            rows = _extract_rows_candidate(out)
            if not rows:
                continue
            payload = {
                "status": "success",
                "page": page,
                "sheet": page,
                "rows": rows,
                "meta": {"dispatch": f"engine.{method_name}"},
            }

        meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
        meta.setdefault("dispatch", f"engine.{method_name}")
        payload["meta"] = meta
        return payload

    return None


# -----------------------------------------------------------------------------
# Core executor
# -----------------------------------------------------------------------------
async def _execute_advanced_request(
    *,
    request: Request,
    body: Dict[str, Any],
    mode: str,
    include_matrix: Optional[bool],
    limit: Optional[int],
    schema_only: Optional[bool],
    x_request_id: Optional[str],
    token: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
) -> Dict[str, Any]:
    t0 = time.perf_counter()
    stages: Dict[str, float] = {}
    request_id = _request_id(request, x_request_id)

    include_matrix_final = include_matrix if isinstance(include_matrix, bool) else _boolish(body.get("include_matrix"), True)
    schema_only_final = schema_only if isinstance(schema_only, bool) else _boolish(body.get("schema_only"), False)
    eff_limit = _effective_limit(body or {}, limit)

    s0 = time.perf_counter()
    effective_criteria, prep_meta = _prepare_effective_criteria(body or {}, eff_limit)
    if not _s(mode) and _s(effective_criteria.get("mode")):
        mode = _s(effective_criteria.get("mode"))
    target_page = _normalize_page_name(prep_meta.get("target_page") or TOP10_PAGE_NAME)
    page_family = _page_family(target_page)
    stages["criteria_prepare_ms"] = round((time.perf_counter() - s0) * 1000.0, 3)

    schema_headers, schema_keys = _load_schema_defaults(target_page)

    if schema_only_final:
        meta = {
            "route_version": INVESTMENT_ADVISOR_VERSION,
            "request_id": request_id,
            "limit": eff_limit,
            "mode": mode or "",
            "schema_aligned": bool(schema_keys),
            "schema_columns": len(schema_keys),
            "build_status": "SCHEMA_ONLY",
            "dispatch": "advanced_request",
            "page_family": page_family,
            "stage_durations_ms": stages,
            "duration_ms": round((time.perf_counter() - t0) * 1000.0, 3),
        }
        return jsonable_encoder(
            _schema_only_payload(
                request_id=request_id,
                target_page=target_page,
                headers=schema_headers,
                keys=schema_keys,
                include_matrix=include_matrix_final,
                meta=meta,
            )
        )

    s1 = time.perf_counter()
    engine = await _get_engine(request)
    stages["engine_ms"] = round((time.perf_counter() - s1) * 1000.0, 3)

    if engine is None:
        meta = {
            "route_version": INVESTMENT_ADVISOR_VERSION,
            "request_id": request_id,
            "limit": eff_limit,
            "mode": mode or "",
            "schema_aligned": bool(schema_keys),
            "schema_columns": len(schema_keys),
            "build_status": "DEGRADED",
            "dispatch": "advanced_request",
            "warning": "engine_unavailable",
            "page_family": page_family,
            "stage_durations_ms": stages,
            "duration_ms": round((time.perf_counter() - t0) * 1000.0, 3),
        }
        return jsonable_encoder(
            _schema_only_payload(
                request_id=request_id,
                target_page=target_page,
                headers=schema_headers,
                keys=schema_keys,
                include_matrix=include_matrix_final,
                meta=meta,
            )
        )

    s2 = time.perf_counter()
    settings = None
    try:
        settings = get_settings_cached()
    except Exception:
        settings = None
    stages["settings_ms"] = round((time.perf_counter() - s2) * 1000.0, 3)

    s3 = time.perf_counter()
    top10_builder, top10_builder_source, top10_builder_name, top10_builder_search_path = await _resolve_top10_builder(request, engine)
    stages["top10_builder_resolve_ms"] = round((time.perf_counter() - s3) * 1000.0, 3)

    s4 = time.perf_counter()
    runner, runner_source, runner_name, runner_search_path = await _resolve_runner(request, engine)
    stages["runner_resolve_ms"] = round((time.perf_counter() - s4) * 1000.0, 3)

    selected_payload: Optional[Any] = None
    selected_criteria = dict(effective_criteria)
    selected_source = ""
    fallback_used = False
    fallback_reason = ""
    warnings: List[str] = []

    async def _try_callable(
        *,
        fn: Optional[Callable[..., Any]],
        source: str,
        criteria_obj: Dict[str, Any],
        stage_name: str,
    ) -> bool:
        nonlocal selected_payload, selected_source, fallback_used, fallback_reason, warnings, stages
        if fn is None:
            return False

        s = time.perf_counter()
        try:
            payload = await _run_callable_with_timeout(
                runner=fn,
                request=request,
                engine=engine,
                criteria=criteria_obj,
                eff_limit=min(eff_limit, _safe_int(criteria_obj.get("top_n"), eff_limit)),
                mode=mode or "",
                timeout_sec=_resolver_timeout(stage_name, page=target_page),
                settings=settings,
                include_matrix=include_matrix_final,
                schema_only=schema_only_final,
            )
        except asyncio.TimeoutError:
            fallback_used = True
            fallback_reason = (fallback_reason + "; " if fallback_reason else "") + f"{stage_name}_timeout"
            warnings.append(f"{stage_name}_timeout")
            stages[f"{stage_name}_call_ms"] = round((time.perf_counter() - s) * 1000.0, 3)
            return False
        except Exception as exc:
            fallback_used = True
            fallback_reason = (fallback_reason + "; " if fallback_reason else "") + f"{stage_name}_error:{type(exc).__name__}"
            warnings.append(f"{stage_name}_error:{type(exc).__name__}")
            stages[f"{stage_name}_call_ms"] = round((time.perf_counter() - s) * 1000.0, 3)
            return False

        stages[f"{stage_name}_call_ms"] = round((time.perf_counter() - s) * 1000.0, 3)

        headers, keys, norm_rows, meta_in, status_out = _normalize_page_payload(
            payload,
            target_page=target_page,
            criteria_used=criteria_obj,
            eff_limit=eff_limit,
            forced_dispatch="advanced_request",
        )

        if not _has_usable_payload(
            page=target_page,
            headers=headers,
            rows=norm_rows,
            meta=meta_in,
            schema_only=schema_only_final,
        ):
            fallback_used = True
            fallback_reason = (fallback_reason + "; " if fallback_reason else "") + f"{stage_name}_weak_or_empty"
            warnings.append(f"{stage_name}_weak_or_empty")
            return False

        selected_payload = {
            "status": status_out,
            "headers": headers,
            "keys": keys,
            "rows": norm_rows,
            "meta": meta_in,
        }
        selected_source = source
        return True

    # Execution order by page family
    if target_page == TOP10_PAGE_NAME:
        if await _try_callable(
            fn=top10_builder,
            source=top10_builder_source or "top10_builder",
            criteria_obj=effective_criteria,
            stage_name="builder",
        ):
            pass
        elif await _try_callable(
            fn=runner,
            source=runner_source or "runner",
            criteria_obj=effective_criteria,
            stage_name="runner",
        ):
            pass
        else:
            bridge_payload = await _delegate_to_bridge_sheet_rows(
                request=request,
                body={
                    "page": target_page,
                    "sheet": target_page,
                    "sheet_name": target_page,
                    "criteria": effective_criteria,
                    "symbols": effective_criteria.get("symbols") or effective_criteria.get("direct_symbols"),
                    "tickers": effective_criteria.get("tickers") or effective_criteria.get("direct_symbols"),
                    "top_n": eff_limit,
                    "limit": eff_limit,
                    "offset": effective_criteria.get("offset", 0),
                    "mode": mode or "",
                    "include_matrix": include_matrix_final,
                    "schema_only": schema_only_final,
                },
                mode=mode or "",
                include_matrix=include_matrix_final,
                token=token,
                x_app_token=x_app_token,
                authorization=authorization,
                x_request_id=x_request_id,
            )
            if bridge_payload is not None:
                headers, keys, norm_rows, meta_in, status_out = _normalize_page_payload(
                    bridge_payload,
                    target_page=target_page,
                    criteria_used=effective_criteria,
                    eff_limit=eff_limit,
                    forced_dispatch="advanced_bridge",
                )
                if _has_usable_payload(
                    page=target_page,
                    headers=headers,
                    rows=norm_rows,
                    meta=meta_in,
                    schema_only=schema_only_final,
                ):
                    selected_payload = {
                        "status": status_out,
                        "headers": headers,
                        "keys": keys,
                        "rows": norm_rows,
                        "meta": meta_in,
                    }
                    selected_source = "bridge"
                else:
                    fallback_used = True
                    fallback_reason = (fallback_reason + "; " if fallback_reason else "") + "bridge_weak_or_empty"
                    warnings.append("bridge_weak_or_empty")

            if selected_payload is None:
                narrowed_criteria = _narrow_criteria_for_fallback(effective_criteria, eff_limit)
                engine_payload = await _try_engine_page_fallback(
                    engine=engine,
                    criteria=narrowed_criteria,
                    eff_limit=min(eff_limit, _safe_int(narrowed_criteria.get("top_n"), eff_limit)),
                    mode=mode or "",
                    include_matrix=include_matrix_final,
                    schema_only=schema_only_final,
                )
                if engine_payload is not None:
                    headers, keys, norm_rows, meta_in, status_out = _normalize_page_payload(
                        engine_payload,
                        target_page=target_page,
                        criteria_used=narrowed_criteria,
                        eff_limit=eff_limit,
                        forced_dispatch="advanced_engine_fallback",
                    )
                    if _has_usable_payload(
                        page=target_page,
                        headers=headers,
                        rows=norm_rows,
                        meta=meta_in,
                        schema_only=schema_only_final,
                    ):
                        selected_payload = {
                            "status": status_out,
                            "headers": headers,
                            "keys": keys,
                            "rows": norm_rows,
                            "meta": meta_in,
                        }
                        selected_criteria = narrowed_criteria
                        selected_source = "engine_fallback"
                        warnings.append("engine_page_fallback_used")
                        fallback_used = True
                    else:
                        fallback_used = True
                        fallback_reason = (fallback_reason + "; " if fallback_reason else "") + "engine_fallback_weak_or_empty"
                        warnings.append("engine_fallback_weak_or_empty")

    elif target_page == INSIGHTS_PAGE_NAME:
        bridge_payload = await _delegate_to_bridge_sheet_rows(
            request=request,
            body={
                "page": target_page,
                "sheet": target_page,
                "sheet_name": target_page,
                "criteria": effective_criteria,
                "top_n": eff_limit,
                "limit": eff_limit,
                "offset": effective_criteria.get("offset", 0),
                "mode": mode or "",
                "include_matrix": include_matrix_final,
                "schema_only": schema_only_final,
            },
            mode=mode or "",
            include_matrix=include_matrix_final,
            token=token,
            x_app_token=x_app_token,
            authorization=authorization,
            x_request_id=x_request_id,
        )
        if bridge_payload is not None:
            headers, keys, norm_rows, meta_in, status_out = _normalize_page_payload(
                bridge_payload,
                target_page=target_page,
                criteria_used=effective_criteria,
                eff_limit=eff_limit,
                forced_dispatch="advanced_bridge",
            )
            if _has_usable_payload(page=target_page, headers=headers, rows=norm_rows, meta=meta_in, schema_only=schema_only_final):
                selected_payload = {
                    "status": status_out,
                    "headers": headers,
                    "keys": keys,
                    "rows": norm_rows,
                    "meta": meta_in,
                }
                selected_source = "bridge"

        if selected_payload is None:
            if await _try_callable(
                fn=runner,
                source=runner_source or "runner",
                criteria_obj=effective_criteria,
                stage_name="runner",
            ):
                pass
            else:
                engine_payload = await _try_engine_page_fallback(
                    engine=engine,
                    criteria=effective_criteria,
                    eff_limit=eff_limit,
                    mode=mode or "",
                    include_matrix=include_matrix_final,
                    schema_only=schema_only_final,
                )
                if engine_payload is not None:
                    headers, keys, norm_rows, meta_in, status_out = _normalize_page_payload(
                        engine_payload,
                        target_page=target_page,
                        criteria_used=effective_criteria,
                        eff_limit=eff_limit,
                        forced_dispatch="advanced_engine_fallback",
                    )
                    if _has_usable_payload(page=target_page, headers=headers, rows=norm_rows, meta=meta_in, schema_only=schema_only_final):
                        selected_payload = {
                            "status": status_out,
                            "headers": headers,
                            "keys": keys,
                            "rows": norm_rows,
                            "meta": meta_in,
                        }
                        selected_source = "engine_fallback"

    elif target_page == DATA_DICTIONARY_PAGE_NAME or target_page in BASE_SOURCE_PAGES:
        bridge_payload = await _delegate_to_bridge_sheet_rows(
            request=request,
            body={
                "page": target_page,
                "sheet": target_page,
                "sheet_name": target_page,
                "criteria": effective_criteria,
                "symbols": effective_criteria.get("symbols") or effective_criteria.get("direct_symbols"),
                "tickers": effective_criteria.get("tickers") or effective_criteria.get("direct_symbols"),
                "top_n": eff_limit,
                "limit": eff_limit,
                "offset": effective_criteria.get("offset", 0),
                "mode": mode or "",
                "include_matrix": include_matrix_final,
                "schema_only": schema_only_final,
            },
            mode=mode or "",
            include_matrix=include_matrix_final,
            token=token,
            x_app_token=x_app_token,
            authorization=authorization,
            x_request_id=x_request_id,
        )
        if bridge_payload is not None:
            headers, keys, norm_rows, meta_in, status_out = _normalize_page_payload(
                bridge_payload,
                target_page=target_page,
                criteria_used=effective_criteria,
                eff_limit=eff_limit,
                forced_dispatch="advanced_bridge",
            )
            if _has_usable_payload(page=target_page, headers=headers, rows=norm_rows, meta=meta_in, schema_only=schema_only_final):
                selected_payload = {
                    "status": status_out,
                    "headers": headers,
                    "keys": keys,
                    "rows": norm_rows,
                    "meta": meta_in,
                }
                selected_source = "bridge"

        if selected_payload is None:
            engine_payload = await _try_engine_page_fallback(
                engine=engine,
                criteria=effective_criteria,
                eff_limit=eff_limit,
                mode=mode or "",
                include_matrix=include_matrix_final,
                schema_only=schema_only_final,
            )
            if engine_payload is not None:
                headers, keys, norm_rows, meta_in, status_out = _normalize_page_payload(
                    engine_payload,
                    target_page=target_page,
                    criteria_used=effective_criteria,
                    eff_limit=eff_limit,
                    forced_dispatch="advanced_engine_fallback",
                )
                if _has_usable_payload(page=target_page, headers=headers, rows=norm_rows, meta=meta_in, schema_only=schema_only_final):
                    selected_payload = {
                        "status": status_out,
                        "headers": headers,
                        "keys": keys,
                        "rows": norm_rows,
                        "meta": meta_in,
                    }
                    selected_source = "engine_fallback"
    else:
        if await _try_callable(
            fn=runner,
            source=runner_source or "runner",
            criteria_obj=effective_criteria,
            stage_name="runner",
        ):
            pass
        else:
            bridge_payload = await _delegate_to_bridge_sheet_rows(
                request=request,
                body={
                    "page": target_page,
                    "sheet": target_page,
                    "sheet_name": target_page,
                    "criteria": effective_criteria,
                    "symbols": effective_criteria.get("symbols") or effective_criteria.get("direct_symbols"),
                    "tickers": effective_criteria.get("tickers") or effective_criteria.get("direct_symbols"),
                    "top_n": eff_limit,
                    "limit": eff_limit,
                    "offset": effective_criteria.get("offset", 0),
                    "mode": mode or "",
                    "include_matrix": include_matrix_final,
                    "schema_only": schema_only_final,
                },
                mode=mode or "",
                include_matrix=include_matrix_final,
                token=token,
                x_app_token=x_app_token,
                authorization=authorization,
                x_request_id=x_request_id,
            )
            if bridge_payload is not None:
                headers, keys, norm_rows, meta_in, status_out = _normalize_page_payload(
                    bridge_payload,
                    target_page=target_page,
                    criteria_used=effective_criteria,
                    eff_limit=eff_limit,
                    forced_dispatch="advanced_bridge",
                )
                if _has_usable_payload(page=target_page, headers=headers, rows=norm_rows, meta=meta_in, schema_only=schema_only_final):
                    selected_payload = {
                        "status": status_out,
                        "headers": headers,
                        "keys": keys,
                        "rows": norm_rows,
                        "meta": meta_in,
                    }
                    selected_source = "bridge"

            if selected_payload is None:
                engine_payload = await _try_engine_page_fallback(
                    engine=engine,
                    criteria=effective_criteria,
                    eff_limit=eff_limit,
                    mode=mode or "",
                    include_matrix=include_matrix_final,
                    schema_only=schema_only_final,
                )
                if engine_payload is not None:
                    headers, keys, norm_rows, meta_in, status_out = _normalize_page_payload(
                        engine_payload,
                        target_page=target_page,
                        criteria_used=effective_criteria,
                        eff_limit=eff_limit,
                        forced_dispatch="advanced_engine_fallback",
                    )
                    if _has_usable_payload(page=target_page, headers=headers, rows=norm_rows, meta=meta_in, schema_only=schema_only_final):
                        selected_payload = {
                            "status": status_out,
                            "headers": headers,
                            "keys": keys,
                            "rows": norm_rows,
                            "meta": meta_in,
                        }
                        selected_source = "engine_fallback"

    if selected_payload is None:
        meta = {
            "route_version": INVESTMENT_ADVISOR_VERSION,
            "request_id": request_id,
            "limit": eff_limit,
            "mode": mode or "",
            "schema_aligned": bool(schema_keys),
            "schema_columns": len(schema_keys),
            "build_status": "DEGRADED",
            "dispatch": "advanced_request",
            "page_family": page_family,
            "request_unconstrained": prep_meta.get("request_unconstrained", False),
            "pages_explicit": prep_meta.get("pages_explicit", False),
            "pages_effective": prep_meta.get("pages_effective", []),
            "direct_symbols_count": prep_meta.get("direct_symbols_count", 0),
            "pages_trimmed": prep_meta.get("pages_trimmed", False),
            "allow_row_fallback": prep_meta.get("allow_row_fallback", True),
            "target_page": target_page,
            "fallback_used": fallback_used,
            "fallback_reason": fallback_reason,
            "criteria_used": _json_safe(selected_criteria),
            "warnings": warnings,
            "engine_present": True,
            "engine_type": type(engine).__name__,
            "top10_builder_available": bool(top10_builder),
            "top10_builder_source": top10_builder_source,
            "top10_builder_name": top10_builder_name,
            "top10_builder_search_path": top10_builder_search_path,
            "runner_available": bool(runner),
            "runner_source": runner_source,
            "runner_name": runner_name,
            "runner_search_path": runner_search_path,
            "stage_durations_ms": stages,
            "duration_ms": round((time.perf_counter() - t0) * 1000.0, 3),
        }
        return jsonable_encoder(
            _schema_only_payload(
                request_id=request_id,
                target_page=target_page,
                headers=schema_headers,
                keys=schema_keys,
                include_matrix=include_matrix_final,
                meta=meta,
            )
        )

    headers = list(selected_payload.get("headers") or schema_headers)
    keys = list(selected_payload.get("keys") or schema_keys)
    norm_rows = list(selected_payload.get("rows") or [])
    meta_in = selected_payload.get("meta") if isinstance(selected_payload.get("meta"), dict) else {}
    status_out = _s(selected_payload.get("status")) or ("success" if norm_rows else "partial")

    build_status = _s(meta_in.get("build_status"))
    if not build_status:
        build_status = "OK" if norm_rows else "WARN"

    merged_warnings = list(warnings)
    meta_warnings = meta_in.get("warnings")
    if isinstance(meta_warnings, list):
        merged_warnings.extend([_s(x) for x in meta_warnings if _s(x)])
    dedup_warnings: List[str] = []
    seen_warn = set()
    for w in merged_warnings:
        if w and w not in seen_warn:
            seen_warn.add(w)
            dedup_warnings.append(w)

    meta = dict(meta_in)
    meta.update(
        {
            "route_version": INVESTMENT_ADVISOR_VERSION,
            "request_id": request_id,
            "limit": eff_limit,
            "mode": mode or "",
            "duration_ms": round((time.perf_counter() - t0) * 1000.0, 3),
            "schema_aligned": bool(keys),
            "schema_columns": len(keys),
            "page_family": page_family,
            "criteria_used": _json_safe(selected_criteria),
            "request_unconstrained": prep_meta.get("request_unconstrained", False),
            "pages_explicit": prep_meta.get("pages_explicit", False),
            "pages_effective": prep_meta.get("pages_effective", []),
            "direct_symbols_count": prep_meta.get("direct_symbols_count", 0),
            "pages_trimmed": prep_meta.get("pages_trimmed", False),
            "allow_row_fallback": prep_meta.get("allow_row_fallback", True),
            "target_page": target_page,
            "fallback_used": fallback_used,
            "fallback_reason": fallback_reason,
            "engine_present": True,
            "engine_type": type(engine).__name__,
            "top10_builder_available": bool(top10_builder),
            "top10_builder_source": top10_builder_source,
            "top10_builder_name": top10_builder_name,
            "top10_builder_search_path": top10_builder_search_path,
            "runner_available": bool(runner),
            "runner_source": runner_source,
            "runner_name": runner_name,
            "runner_search_path": runner_search_path,
            "selected_source": selected_source,
            "warnings": dedup_warnings,
            "build_status": build_status,
            "dispatch": _s(meta_in.get("dispatch")) or "advanced_request",
            "contract_header_count": KNOWN_CANONICAL_HEADER_COUNTS.get(target_page, len(keys)),
            "actual_header_count": len(keys),
            "stage_durations_ms": stages,
        }
    )

    response = {
        "status": status_out or ("success" if norm_rows else "partial"),
        "page": target_page,
        "sheet": target_page,
        "sheet_name": target_page,
        "route_family": page_family,
        "headers": headers,
        "display_headers": headers,
        "keys": keys,
        "rows": norm_rows,
        "row_objects": norm_rows,
        "data": norm_rows,
        "items": norm_rows,
        "rows_matrix": _rows_to_matrix(norm_rows, keys) if (include_matrix_final and keys) else None,
        "version": INVESTMENT_ADVISOR_VERSION,
        "request_id": request_id,
        "meta": meta,
    }
    return jsonable_encoder(response)


# -----------------------------------------------------------------------------
# Health / Meta / Metrics
# -----------------------------------------------------------------------------
@router.get("/health")
async def advanced_health(request: Request) -> Dict[str, Any]:
    engine = await _get_engine(request)
    top10_builder, top10_builder_source, top10_builder_name, top10_builder_search_path = await _resolve_top10_builder(request, engine)
    runner, runner_source, runner_name, runner_search_path = await _resolve_runner(request, engine)
    bridge_impl, bridge_source, bridge_name = await _resolve_bridge_impl(request)

    return jsonable_encoder(
        {
            "status": "ok" if engine else "degraded",
            "service": "advanced_investment_advisor",
            "version": INVESTMENT_ADVISOR_VERSION,
            "engine_available": bool(engine),
            "engine_type": type(engine).__name__ if engine else "none",
            "top10_builder_available": bool(top10_builder),
            "top10_builder_source": top10_builder_source,
            "top10_builder_name": top10_builder_name,
            "top10_builder_search_path": top10_builder_search_path,
            "runner_available": bool(runner),
            "runner_source": runner_source,
            "runner_name": runner_name,
            "runner_search_path": runner_search_path,
            "bridge_available": bool(bridge_impl),
            "bridge_source": bridge_source,
            "bridge_name": bridge_name,
            "contract_header_counts": dict(KNOWN_CANONICAL_HEADER_COUNTS),
            "timestamp_utc": _now_utc(),
        }
    )


@router.get("/meta")
async def advanced_meta(request: Request) -> Dict[str, Any]:
    engine = await _get_engine(request)
    top10_builder, top10_builder_source, top10_builder_name, top10_builder_search_path = await _resolve_top10_builder(request, engine)
    runner, runner_source, runner_name, runner_search_path = await _resolve_runner(request, engine)
    bridge_impl, bridge_source, bridge_name = await _resolve_bridge_impl(request)

    return jsonable_encoder(
        {
            "status": "success",
            "version": INVESTMENT_ADVISOR_VERSION,
            "route_family": "advanced",
            "engine_present": bool(engine),
            "engine_type": type(engine).__name__ if engine else "none",
            "top10_builder_available": bool(top10_builder),
            "top10_builder_source": top10_builder_source,
            "top10_builder_name": top10_builder_name,
            "top10_builder_search_path": top10_builder_search_path,
            "runner_available": bool(runner),
            "runner_source": runner_source,
            "runner_name": runner_name,
            "runner_search_path": runner_search_path,
            "bridge_available": bool(bridge_impl),
            "bridge_source": bridge_source,
            "bridge_name": bridge_name,
            "contract_header_counts": dict(KNOWN_CANONICAL_HEADER_COUNTS),
            "timestamp_utc": _now_utc(),
        }
    )


@router.get("/metrics")
async def advanced_metrics() -> Response:
    if not PROMETHEUS_AVAILABLE or generate_latest is None:
        return Response(content="Metrics not available", media_type="text/plain", status_code=503)
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


# -----------------------------------------------------------------------------
# POST routes
# -----------------------------------------------------------------------------
@router.post("/top10-investments")
@router.post("/top10")
@router.post("/investment-advisor")
@router.post("/advisor")
@router.post("/run")
@router.post("/sheet-rows")
async def advanced_request_post(
    request: Request,
    response: Response,
    body: Dict[str, Any] = Body(default_factory=dict),
    mode: str = Query(default="", description="Optional mode hint"),
    include_matrix: Optional[bool] = Query(default=None, description="Return rows_matrix"),
    limit: Optional[int] = Query(default=None, ge=1, le=200, description="How many items to return"),
    schema_only: Optional[bool] = Query(default=None, description="Return schema only"),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    _require_auth_or_401(
        request=request,
        token_query=token,
        x_app_token=x_app_token,
        authorization=authorization,
    )

    payload = await _execute_advanced_request(
        request=request,
        body=dict(body or {}),
        mode=mode,
        include_matrix=include_matrix,
        limit=limit,
        schema_only=schema_only,
        x_request_id=x_request_id,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
    )
    response.headers["X-Request-ID"] = payload.get("request_id") or _request_id(request, x_request_id)
    return payload


# -----------------------------------------------------------------------------
# GET aliases
# -----------------------------------------------------------------------------
@router.get("/recommendations")
@router.get("/sheet-rows")
async def advanced_request_get(
    request: Request,
    response: Response,
    page: Optional[str] = Query(default=None),
    sheet: Optional[str] = Query(default=None),
    sheet_name: Optional[str] = Query(default=None),
    name: Optional[str] = Query(default=None),
    tab: Optional[str] = Query(default=None),
    symbols: Optional[str] = Query(default=None, description="Comma/space separated symbols"),
    tickers: Optional[str] = Query(default=None, description="Comma/space separated tickers"),
    pages: Optional[str] = Query(default=None, description="Comma/space separated source pages"),
    sources: Optional[str] = Query(default=None, description="Comma/space separated source pages"),
    risk_level: Optional[str] = Query(default=None),
    risk_profile: Optional[str] = Query(default=None),
    confidence_level: Optional[str] = Query(default=None),
    confidence_bucket: Optional[str] = Query(default=None),
    investment_period_days: Optional[int] = Query(default=None, ge=1, le=3650),
    horizon_days: Optional[int] = Query(default=None, ge=1, le=3650),
    min_expected_roi: Optional[float] = Query(default=None),
    min_roi: Optional[float] = Query(default=None),
    min_confidence: Optional[float] = Query(default=None),
    top_n: Optional[int] = Query(default=None, ge=1, le=200),
    limit: Optional[int] = Query(default=None, ge=1, le=200),
    mode: str = Query(default="", description="Optional mode hint"),
    include_matrix: Optional[bool] = Query(default=None),
    schema_only: Optional[bool] = Query(default=None),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    _require_auth_or_401(
        request=request,
        token_query=token,
        x_app_token=x_app_token,
        authorization=authorization,
    )

    target_page = _normalize_page_name(page or sheet or sheet_name or name or tab or TOP10_PAGE_NAME)
    direct_symbols = _normalize_list(symbols) or _normalize_list(tickers)
    selected_pages = _normalize_list(pages) or _normalize_list(sources)

    body: Dict[str, Any] = {
        "page": target_page,
        "sheet": target_page,
        "sheet_name": target_page,
        "direct_symbols": direct_symbols,
        "symbols": direct_symbols,
        "tickers": direct_symbols,
        "pages_selected": selected_pages,
        "risk_level": _s(risk_level) or _s(risk_profile),
        "risk_profile": _s(risk_profile) or _s(risk_level),
        "confidence_level": _s(confidence_level),
        "confidence_bucket": _s(confidence_bucket),
        "investment_period_days": investment_period_days,
        "horizon_days": horizon_days,
        "min_expected_roi": min_expected_roi,
        "min_roi": min_roi if min_roi is not None else min_expected_roi,
        "min_confidence": min_confidence,
        "top_n": top_n if top_n is not None else limit,
        "limit": limit if limit is not None else top_n,
        "include_matrix": include_matrix,
        "schema_only": schema_only,
    }

    payload = await _execute_advanced_request(
        request=request,
        body=body,
        mode=mode,
        include_matrix=include_matrix,
        limit=limit if limit is not None else top_n,
        schema_only=schema_only,
        x_request_id=x_request_id,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
    )
    response.headers["X-Request-ID"] = payload.get("request_id") or _request_id(request, x_request_id)
    return payload


__all__ = ["router", "INVESTMENT_ADVISOR_VERSION"]
