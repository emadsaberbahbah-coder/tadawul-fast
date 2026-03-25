#!/usr/bin/env python3
"""
routes/investment_advisor.py
--------------------------------------------------------------------------------
ADVANCED INVESTMENT ADVISOR ROUTER — v2.4.0
--------------------------------------------------------------------------------
PAGE-AWARE • LIVE-FIRST • CANONICAL-SCHEMA SAFE • PAGE-CATALOG AWARE
TOP10 / INSIGHTS / DATA-DICTIONARY SAFE • GET+POST ALIAS SAFE
ENGINE / RUNNER / BUILDER TOLERANT • JSON-SAFE • STARTUP-SAFE • REQUEST-ID SAFE

What this revision improves
---------------------------
- FIX: stops treating every advanced request as Top_10_Investments. The router is
  now page-aware and returns the correct target page in the response envelope.
- FIX: keeps Top_10_Investments builder-first, but routes other advanced pages
  through runner/engine fallbacks instead of forcing Top10 shaping.
- FIX: schema loading now works for any requested page and sanitizes blank/ghost
  columns instead of fabricating misaligned leading fields.
- FIX: keeps special pages schema-safe:
      - Top_10_Investments -> 83 columns fallback
      - Insights_Analysis  -> 7 columns fallback
      - Data_Dictionary    -> 9 columns fallback
      - base/source pages  -> canonical 80 columns fallback
- FIX: broader runner / builder discovery across app.state, core modules,
  factories, service objects, and engine methods.
- FIX: GET and POST /v1/advanced/sheet-rows remain stable aliases, while still
  supporting /top10, /advisor, /investment-advisor, /recommendations, and /run.
- SAFE: no network calls at import time.
"""

from __future__ import annotations

import asyncio
import copy
import importlib
import inspect
import json
import logging
import os
import time
import uuid
from dataclasses import asdict, is_dataclass
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, Response, status
from fastapi.encoders import jsonable_encoder

logger = logging.getLogger("routes.investment_advisor")
logger.addHandler(logging.NullHandler())

INVESTMENT_ADVISOR_VERSION = "2.4.0"
TOP10_PAGE_NAME = "Top_10_Investments"
INSIGHTS_PAGE_NAME = "Insights_Analysis"
DATA_DICTIONARY_PAGE_NAME = "Data_Dictionary"

_BASE_SOURCE_PAGES = {
    "Market_Leaders",
    "Global_Markets",
    "Mutual_Funds",
    "Commodities_FX",
    "My_Portfolio",
}

_DERIVED_OR_NON_SOURCE_PAGES = {
    "KSA_TADAWUL",
    "Advisor_Criteria",
    "AI_Opportunity_Report",
    INSIGHTS_PAGE_NAME,
    TOP10_PAGE_NAME,
    DATA_DICTIONARY_PAGE_NAME,
}

_PAGE_ALIAS_MAP = {
    "top10": TOP10_PAGE_NAME,
    "top_10": TOP10_PAGE_NAME,
    "top10investments": TOP10_PAGE_NAME,
    "top_10_investments": TOP10_PAGE_NAME,
    "top-10-investments": TOP10_PAGE_NAME,
    "top_10_investment": TOP10_PAGE_NAME,
    "top-10": TOP10_PAGE_NAME,
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

_BUILDER_MODULE_CANDIDATES = (
    "core.analysis.top10_selector",
    "core.analysis.top10_builder",
    "core.investment_advisor",
    "core.investment_advisor_engine",
    "core.analysis.investment_advisor",
    "core.analysis.investment_advisor_engine",
)

_RUNNER_MODULE_CANDIDATES = (
    "core.investment_advisor",
    "core.investment_advisor_engine",
    "core.analysis.investment_advisor",
    "core.analysis.investment_advisor_engine",
    "core.data_engine_v2",
    "core.data_engine",
)

_TOP10_BUILDER_NAME_CANDIDATES = (
    "build_top10_rows",
    "build_top10_output_rows",
    "build_top10_investments_rows",
    "build_top10_investments_output_rows",
    "build_top10",
    "build_top10_payload",
    "build_top10_result",
    "select_top10",
    "select_top10_symbols",
)

_RUNNER_NAME_CANDIDATES = (
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

_CONTAINER_NAME_CANDIDATES = (
    "top10_selector",
    "top10_builder",
    "advisor_service",
    "investment_advisor_service",
    "advisor_engine",
    "investment_advisor_engine",
    "advisor_runner",
    "investment_advisor_runner",
    "AdvisorService",
    "InvestmentAdvisorService",
    "AdvisorEngine",
    "InvestmentAdvisorEngine",
    "Top10Selector",
    "Top10Builder",
    "create_top10_selector",
    "create_top10_builder",
    "get_top10_selector",
    "get_top10_builder",
    "create_investment_advisor",
    "get_investment_advisor",
    "build_investment_advisor",
)

_STATE_ATTR_CANDIDATES = (
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

_ENGINE_METHOD_CANDIDATES = (
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
)

router = APIRouter(prefix="/v1/advanced", tags=["advanced"])


# =============================================================================
# Optional Prometheus
# =============================================================================
try:
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest  # type: ignore

    _PROMETHEUS_AVAILABLE = True
except Exception:
    CONTENT_TYPE_LATEST = "text/plain"
    generate_latest = None  # type: ignore
    _PROMETHEUS_AVAILABLE = False


# =============================================================================
# Auth (best-effort, aligned with other routers)
# =============================================================================
try:
    from core.config import auth_ok, get_settings_cached, is_open_mode  # type: ignore
except Exception:
    auth_ok = None  # type: ignore
    is_open_mode = None  # type: ignore

    def get_settings_cached(*args: Any, **kwargs: Any) -> Any:  # type: ignore
        return None


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


def _safe_float(v: Any, default: float) -> float:
    try:
        if isinstance(v, bool):
            return default
        return float(v)
    except Exception:
        return default


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


def _env_int(name: str, default: int, *, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    try:
        raw = os.getenv(name, "").strip()
        val = int(raw) if raw else int(default)
    except Exception:
        val = int(default)
    if lo is not None:
        val = max(lo, val)
    if hi is not None:
        val = min(hi, val)
    return val


def _env_float(name: str, default: float, *, lo: Optional[float] = None, hi: Optional[float] = None) -> float:
    try:
        raw = os.getenv(name, "").strip()
        val = float(raw) if raw else float(default)
    except Exception:
        val = float(default)
    if lo is not None:
        val = max(lo, val)
    if hi is not None:
        val = min(hi, val)
    return val


def _env_csv(name: str, default: Sequence[str]) -> List[str]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return [str(x).strip() for x in default if str(x).strip()]
    out: List[str] = []
    seen = set()
    for part in raw.replace(";", ",").split(","):
        item = part.strip()
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


def _normalize_list(value: Any) -> List[str]:
    if value is None:
        return []

    if isinstance(value, str):
        normalized = value.replace(";", ",").replace("\n", ",").replace(" ", ",")
        seq: List[Any] = [x.strip() for x in normalized.split(",") if x.strip()]
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


def _jsonable_snapshot(value: Any) -> Any:
    try:
        return jsonable_encoder(value)
    except Exception:
        try:
            return json.loads(json.dumps(value, default=str))
        except Exception:
            return str(value)


def _json_compact(value: Any) -> str:
    try:
        return json.dumps(_jsonable_snapshot(value), ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return str(value)


def _get_request_id(request: Optional[Request], header_request_id: Optional[str] = None) -> str:
    if _s(header_request_id):
        return _s(header_request_id)
    try:
        if request is not None:
            rid = request.headers.get("X-Request-ID")
            if rid:
                return str(rid).strip()
            state_rid = getattr(getattr(request, "state", None), "request_id", None)
            if state_rid:
                return str(state_rid).strip()
    except Exception:
        pass
    return str(uuid.uuid4())


def _normalize_page_name(value: Any) -> str:
    raw = _s(value)
    if not raw:
        return TOP10_PAGE_NAME

    try:
        from core.sheets.page_catalog import normalize_page_name as catalog_normalize_page_name  # type: ignore

        normalized = catalog_normalize_page_name(raw)
        normalized_s = _s(normalized)
        if normalized_s:
            return normalized_s
    except Exception:
        pass

    try:
        from core.sheets.page_catalog import resolve_page_name as catalog_resolve_page_name  # type: ignore

        normalized = catalog_resolve_page_name(raw)
        normalized_s = _s(normalized)
        if normalized_s:
            return normalized_s
    except Exception:
        pass

    compact = raw.strip().lower().replace(" ", "_")
    return _PAGE_ALIAS_MAP.get(compact, raw)


def _page_family(page: str) -> str:
    normalized = _normalize_page_name(page)
    if normalized == TOP10_PAGE_NAME:
        return "top10"
    if normalized == INSIGHTS_PAGE_NAME:
        return "insights"
    if normalized == DATA_DICTIONARY_PAGE_NAME:
        return "data_dictionary"
    if normalized in _BASE_SOURCE_PAGES:
        return "source"
    return "advanced"


def _safe_source_pages(values: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in values:
        s = _normalize_page_name(item)
        if not s:
            continue
        if s in _DERIVED_OR_NON_SOURCE_PAGES:
            continue
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


# =============================================================================
# Auth helpers
# =============================================================================
def _extract_auth_token(
    *,
    token_query: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
) -> str:
    auth_token = _s(x_app_token)

    authz = _s(authorization)
    if authz.lower().startswith("bearer "):
        auth_token = authz.split(" ", 1)[1].strip()

    if token_query and not auth_token:
        allow_query = False
        try:
            settings = get_settings_cached()
            allow_query = bool(getattr(settings, "allow_query_token", False))
        except Exception:
            allow_query = False
        if allow_query:
            auth_token = _s(token_query)

    return auth_token


def _is_public_path(path: str) -> bool:
    p = _s(path)
    if not p:
        return False
    if p in {"/v1/advanced/health", "/v1/advanced/metrics"}:
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


def _auth_passed(
    *,
    request: Request,
    token_query: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
) -> bool:
    try:
        if callable(is_open_mode) and bool(is_open_mode()):
            return True
    except Exception:
        pass

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

    call_attempts = [
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

    for kwargs in call_attempts:
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


# =============================================================================
# Lazy engine accessor
# =============================================================================
async def _get_engine(request: Request) -> Optional[Any]:
    try:
        st = getattr(request.app, "state", None)
        if st and getattr(st, "engine", None):
            return st.engine
    except Exception:
        pass

    for modpath in ("core.data_engine_v2", "core.data_engine"):
        try:
            mod = importlib.import_module(modpath)
            get_engine = getattr(mod, "get_engine", None)
            if callable(get_engine):
                eng = get_engine()
                if inspect.isawaitable(eng):
                    eng = await eng
                return eng
        except Exception:
            continue
    return None


# =============================================================================
# Canonical fallback schemas
# =============================================================================
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


def _load_schema_defaults(page: str) -> Tuple[List[str], List[str]]:
    normalized_page = _normalize_page_name(page)
    try:
        from core.sheets.schema_registry import get_sheet_spec  # type: ignore

        spec = get_sheet_spec(normalized_page)
        cols = getattr(spec, "columns", None) or []
        if cols:
            keys: List[str] = []
            headers: List[str] = []
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
            if keys and headers:
                if normalized_page == TOP10_PAGE_NAME:
                    keys, headers = _ensure_top10_keys_present(keys, headers)
                return headers, keys
    except Exception:
        pass

    headers, keys = _schema_fallback_for_page(normalized_page)
    if normalized_page == TOP10_PAGE_NAME:
        keys, headers = _ensure_top10_keys_present(keys, headers)
    return headers, keys


# =============================================================================
# Generic row / payload helpers
# =============================================================================
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

    headers = [str(x) for x in headers if _s(x)]
    keys = [str(x) for x in keys if _s(x)]
    return headers, keys


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

    for key in (
        "row_objects",
        "records",
        "items",
        "data",
        "quotes",
        "rows",
        "recommendations",
        "results",
    ):
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


async def _call_maybe_async(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    if inspect.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)
    out = await asyncio.to_thread(fn, *args, **kwargs)
    if inspect.isawaitable(out):
        return await out
    return out


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
        if _is_blank(r.get("top10_rank")):
            r["top10_rank"] = idx
        if _is_blank(r.get("rank_overall")):
            r["rank_overall"] = idx
        out.append(r)
    return out


def _apply_top10_field_backfill(rows: List[Dict[str, Any]], *, keys: List[str], criteria: Dict[str, Any]) -> List[Dict[str, Any]]:
    criteria_snapshot = _json_compact(criteria) if criteria else None
    out: List[Dict[str, Any]] = []

    for idx, row in enumerate(rows, start=1):
        r = dict(row)
        if "top10_rank" in keys and _is_blank(r.get("top10_rank")):
            r["top10_rank"] = idx
        if "selection_reason" in keys and _is_blank(r.get("selection_reason")):
            r["selection_reason"] = _canonical_selection_reason(r)
        if "criteria_snapshot" in keys and _is_blank(r.get("criteria_snapshot")) and criteria_snapshot is not None:
            r["criteria_snapshot"] = criteria_snapshot
        if "source_page" in keys and _is_blank(r.get("source_page")):
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
        "route_family": _page_family(target_page),
        "headers": headers,
        "keys": keys,
        "rows": [],
        "data": [],
        "items": [],
        "rows_matrix": rows_matrix,
        "version": INVESTMENT_ADVISOR_VERSION,
        "request_id": request_id,
        "meta": meta,
    }


# =============================================================================
# Criteria normalization
# =============================================================================
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
    ):
        if k in body and body.get(k) is not None:
            crit[k] = body.get(k)
    return crit


def _effective_limit(body: Dict[str, Any], limit_q: Optional[int]) -> int:
    max_limit = _env_int("ADV_TOP10_MAX_LIMIT", 50, lo=1, hi=200)
    default_limit = _env_int("ADV_TOP10_DEFAULT_LIMIT", 10, lo=1, hi=max_limit)
    if isinstance(limit_q, int):
        eff = limit_q
    else:
        eff = _safe_int(
            body.get("limit") or body.get("top_n") or body.get("criteria", {}).get("top_n") or default_limit,
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
    pages = _safe_source_pages(pages)

    direct_symbols = _normalize_list(crit.get("direct_symbols") or crit.get("symbols") or crit.get("tickers"))

    request_unconstrained = (not pages) and (not direct_symbols) and target_page == TOP10_PAGE_NAME
    pages_explicit = bool(pages)

    if request_unconstrained:
        pages = _env_csv("ADV_TOP10_DEFAULT_PAGES", ["Market_Leaders", "Global_Markets"])
        pages = _safe_source_pages(pages)
        crit["pages_selected"] = pages

    max_pages = _env_int("ADV_TOP10_MAX_PAGES", 5, lo=1, hi=20)
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
    narrowed = copy.deepcopy(criteria)
    target_page = _normalize_page_name(narrowed.get("page") or TOP10_PAGE_NAME)

    fallback_pages_cap = _env_int("ADV_TOP10_FALLBACK_MAX_PAGES", 2, lo=1, hi=10)
    fallback_top_n = _env_int("ADV_TOP10_FALLBACK_TOP_N", min(3, eff_limit), lo=1, hi=eff_limit)

    direct_symbols = _normalize_list(narrowed.get("direct_symbols") or narrowed.get("symbols") or narrowed.get("tickers"))
    pages = _normalize_list(narrowed.get("pages_selected") or narrowed.get("pages") or narrowed.get("selected_pages"))
    pages = _safe_source_pages(pages)

    if target_page == TOP10_PAGE_NAME:
        if direct_symbols:
            narrowed["direct_symbols"] = direct_symbols[: max(1, min(len(direct_symbols), eff_limit))]
            narrowed["symbols"] = narrowed["direct_symbols"]
            narrowed["tickers"] = narrowed["direct_symbols"]
            narrowed["top_n"] = min(eff_limit, len(narrowed["direct_symbols"]))
            narrowed["limit"] = narrowed["top_n"]
        else:
            if not pages:
                pages = _env_csv("ADV_TOP10_DEFAULT_PAGES", ["Market_Leaders"])
            pages = _safe_source_pages(pages)[:fallback_pages_cap]
            narrowed["pages_selected"] = pages
            narrowed["top_n"] = fallback_top_n
            narrowed["limit"] = fallback_top_n
        narrowed["enrich_final"] = False
    else:
        narrowed["top_n"] = eff_limit
        narrowed["limit"] = eff_limit
        narrowed["enrich_final"] = True

    return narrowed


# =============================================================================
# Discovery helpers
# =============================================================================
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
            return await _call_maybe_async(holder)
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

    for holder_name in _CONTAINER_NAME_CANDIDATES:
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
        direct_state = _callable_by_names(st, list(names) + list(_STATE_ATTR_CANDIDATES))
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
            mod = importlib.import_module(mod_name)
        except Exception as e:
            import_errors.append(f"{mod_name}: {type(e).__name__}: {e}")
            continue
        resolved = await _resolve_from_container(mod, mod_name, names)
        if resolved:
            fn, src, name = resolved
            return fn, src, name, searched

    if engine is not None:
        searched.append("engine")
        direct_engine = _callable_by_names(engine, list(names) + list(_ENGINE_METHOD_CANDIDATES))
        if direct_engine:
            fn, fn_name = direct_engine
            return fn, "engine", fn_name, searched

    if import_errors:
        logger.warning("Advanced callable import errors: %s", import_errors)
    return None, "", "", searched


# =============================================================================
# Invocation helpers
# =============================================================================
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
            out = await _call_maybe_async(fn, **kwargs)
        except TypeError:
            try:
                out = await _call_maybe_async(fn, page)
            except Exception:
                continue
        except Exception:
            continue

        if isinstance(out, dict):
            return dict(out)
        rows = _extract_rows_candidate(out)
        if rows:
            return {
                "status": "success",
                "page": page,
                "sheet": page,
                "rows": rows,
                "meta": {"dispatch": f"engine.{method_name}"},
            }
    return None


# =============================================================================
# Payload normalization
# =============================================================================
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
        keys, headers = _ensure_top10_keys_present(list(keys), list(headers))

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


# =============================================================================
# Core executor
# =============================================================================
async def _execute_advanced_request(
    *,
    request: Request,
    body: Dict[str, Any],
    mode: str,
    include_matrix: Optional[bool],
    limit: Optional[int],
    schema_only: Optional[bool],
    x_request_id: Optional[str],
) -> Dict[str, Any]:
    t0 = time.perf_counter()
    stages: Dict[str, float] = {}
    request_id = _get_request_id(request, x_request_id)

    include_matrix_final = include_matrix if isinstance(include_matrix, bool) else _coerce_bool(body.get("include_matrix"), True)
    schema_only_final = schema_only if isinstance(schema_only, bool) else _coerce_bool(body.get("schema_only"), False)
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
    top10_builder, top10_builder_source, top10_builder_name, top10_builder_search_path = await _resolve_callable(
        request,
        engine,
        modules=_BUILDER_MODULE_CANDIDATES,
        names=_TOP10_BUILDER_NAME_CANDIDATES,
    )
    stages["top10_builder_resolve_ms"] = round((time.perf_counter() - s3) * 1000.0, 3)

    s4 = time.perf_counter()
    runner, runner_source, runner_name, runner_search_path = await _resolve_callable(
        request,
        engine,
        modules=_RUNNER_MODULE_CANDIDATES,
        names=_RUNNER_NAME_CANDIDATES,
    )
    stages["runner_resolve_ms"] = round((time.perf_counter() - s4) * 1000.0, 3)

    primary_timeout_sec = _env_float("ADV_TOP10_TIMEOUT_SEC", 45.0, lo=3.0, hi=300.0)
    fallback_timeout_sec = _env_float("ADV_TOP10_FALLBACK_TIMEOUT_SEC", 15.0, lo=2.0, hi=120.0)

    selected_payload: Optional[Any] = None
    selected_criteria = copy.deepcopy(effective_criteria)
    selected_source = ""
    fallback_used = False
    fallback_reason = ""
    warnings: List[str] = []

    primary_callable = top10_builder if page_family == "top10" and top10_builder is not None else runner
    primary_source = top10_builder_source if page_family == "top10" and top10_builder is not None else runner_source

    if primary_callable is not None:
        s5 = time.perf_counter()
        try:
            payload_primary = await _run_callable_with_timeout(
                runner=primary_callable,
                request=request,
                engine=engine,
                criteria=effective_criteria,
                eff_limit=eff_limit,
                mode=mode or "",
                timeout_sec=primary_timeout_sec,
                settings=settings,
                include_matrix=include_matrix_final,
                schema_only=schema_only_final,
            )
            selected_payload = payload_primary
            selected_source = primary_source or "primary"
        except asyncio.TimeoutError:
            fallback_used = True
            fallback_reason = f"primary_timeout_{primary_timeout_sec}s"
            warnings.append("primary_callable_timeout")
        except Exception as e:
            fallback_used = True
            fallback_reason = f"primary_error:{type(e).__name__}"
            warnings.append(f"primary_callable_error:{type(e).__name__}:{e}")
        stages["primary_callable_ms"] = round((time.perf_counter() - s5) * 1000.0, 3)
    else:
        fallback_used = True
        fallback_reason = "primary_callable_unavailable"
        warnings.append("primary_callable_unavailable")

    try:
        normalized_rows_probe = _extract_rows_candidate(selected_payload)
        if page_family == "top10" and prep_meta.get("request_unconstrained") and len(normalized_rows_probe) == 0:
            fallback_used = True
            fallback_reason = fallback_reason or "primary_empty_unconstrained"
            warnings.append("primary_empty_for_unconstrained_request")
            selected_payload = None
    except Exception:
        pass

    if selected_payload is None:
        s6 = time.perf_counter()
        narrowed_criteria = _narrow_criteria_for_fallback(effective_criteria, eff_limit)
        fallback_callable = None
        if page_family == "top10" and runner is not None and runner is not primary_callable:
            fallback_callable = runner
        if fallback_callable is not None:
            try:
                payload_fallback = await _run_callable_with_timeout(
                    runner=fallback_callable,
                    request=request,
                    engine=engine,
                    criteria=narrowed_criteria,
                    eff_limit=min(eff_limit, _safe_int(narrowed_criteria.get("top_n"), eff_limit)),
                    mode=mode or "",
                    timeout_sec=fallback_timeout_sec,
                    settings=settings,
                    include_matrix=include_matrix_final,
                    schema_only=schema_only_final,
                )
                selected_payload = payload_fallback
                selected_criteria = narrowed_criteria
                selected_source = runner_source if fallback_callable is runner else top10_builder_source
            except asyncio.TimeoutError:
                warnings.append("fallback_callable_timeout")
                fallback_reason = (fallback_reason + "; " if fallback_reason else "") + f"fallback_timeout_{fallback_timeout_sec}s"
            except Exception as e:
                warnings.append(f"fallback_callable_error:{type(e).__name__}:{e}")
                fallback_reason = (fallback_reason + "; " if fallback_reason else "") + f"fallback_error:{type(e).__name__}"
        stages["fallback_callable_ms"] = round((time.perf_counter() - s6) * 1000.0, 3)

        if selected_payload is None:
            s7 = time.perf_counter()
            engine_payload = await _try_engine_page_fallback(
                engine=engine,
                criteria=narrowed_criteria,
                eff_limit=min(eff_limit, _safe_int(narrowed_criteria.get("top_n"), eff_limit)),
                mode=mode or "",
                include_matrix=include_matrix_final,
                schema_only=schema_only_final,
            )
            stages["engine_fallback_ms"] = round((time.perf_counter() - s7) * 1000.0, 3)
            if engine_payload is not None:
                selected_payload = engine_payload
                selected_criteria = narrowed_criteria
                selected_source = "engine_fallback"
                warnings.append("engine_page_fallback_used")
                fallback_used = True
                if not fallback_reason:
                    fallback_reason = "engine_page_fallback"

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
            "criteria_used": _jsonable_snapshot(selected_criteria),
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

    s8 = time.perf_counter()
    headers, keys, norm_rows, meta_in, status_out = _normalize_page_payload(
        selected_payload,
        target_page=target_page,
        criteria_used=selected_criteria,
        eff_limit=eff_limit,
        forced_dispatch="advanced_request",
    )
    stages["normalize_ms"] = round((time.perf_counter() - s8) * 1000.0, 3)

    build_status = _s(meta_in.get("build_status"))
    if not build_status:
        build_status = "OK" if norm_rows else "WARN"

    meta_warnings = meta_in.get("warnings")
    merged_warnings: List[str] = []
    if isinstance(meta_warnings, list):
        merged_warnings.extend([_s(x) for x in meta_warnings if _s(x)])
    merged_warnings.extend([_s(x) for x in warnings if _s(x)])
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
            "criteria_used": _jsonable_snapshot(selected_criteria),
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
            "stage_durations_ms": stages,
        }
    )

    response = {
        "status": status_out or ("success" if norm_rows else "partial"),
        "page": target_page,
        "sheet": target_page,
        "route_family": page_family,
        "headers": headers,
        "keys": keys,
        "rows": norm_rows,
        "data": norm_rows,
        "items": norm_rows,
        "rows_matrix": _rows_to_matrix(norm_rows, keys) if (include_matrix_final and keys) else None,
        "version": INVESTMENT_ADVISOR_VERSION,
        "request_id": request_id,
        "meta": meta,
    }
    return jsonable_encoder(response)


# =============================================================================
# Health / Metrics
# =============================================================================
@router.get("/health")
async def advanced_health(request: Request) -> Dict[str, Any]:
    engine = await _get_engine(request)
    top10_builder, top10_builder_source, top10_builder_name, top10_builder_search_path = await _resolve_callable(
        request,
        engine,
        modules=_BUILDER_MODULE_CANDIDATES,
        names=_TOP10_BUILDER_NAME_CANDIDATES,
    )
    runner, runner_source, runner_name, runner_search_path = await _resolve_callable(
        request,
        engine,
        modules=_RUNNER_MODULE_CANDIDATES,
        names=_RUNNER_NAME_CANDIDATES,
    )
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
        }
    )


@router.get("/metrics")
async def advanced_metrics() -> Response:
    if not _PROMETHEUS_AVAILABLE or generate_latest is None:
        return Response(content="Metrics not available", media_type="text/plain", status_code=503)
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


# =============================================================================
# POST routes
# =============================================================================
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
    mode: str = Query(default="", description="Optional mode hint for selector"),
    include_matrix: Optional[bool] = Query(default=None, description="Return rows_matrix"),
    limit: Optional[int] = Query(default=None, ge=1, le=50, description="How many items to return"),
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
    )
    response.headers["X-Request-ID"] = payload.get("request_id") or _get_request_id(request, x_request_id)
    return payload


# =============================================================================
# GET aliases
# =============================================================================
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
    top_n: Optional[int] = Query(default=None, ge=1, le=50),
    limit: Optional[int] = Query(default=None, ge=1, le=50),
    mode: str = Query(default="", description="Optional mode hint for selector"),
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
    )
    response.headers["X-Request-ID"] = payload.get("request_id") or _get_request_id(request, x_request_id)
    return payload


__all__ = ["router", "INVESTMENT_ADVISOR_VERSION"]
