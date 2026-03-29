#!/usr/bin/env python3
"""
routes/advanced_analysis.py
--------------------------------------------------------------------------------
TADAWUL ADVANCED ANALYSIS ROUTER — v7.6.0
(SCHEMA TOP-LEVEL COMPAT / ROOT-SHEET-ROWS LIVE FALLBACK /
 SPECIAL-PAGE HARDENING / DATA-DICTIONARY GUARANTEE / JSON-SAFE / ASYNC-SAFE)

What this revision improves
- FIX: Data_Dictionary always returns a stable 9-column payload from registry and
       known-page fallbacks instead of bubbling internal exceptions as 5xx.
- FIX: root /sheet-rows remains the canonical safe path for special pages
       (Insights_Analysis, Top_10_Investments, Data_Dictionary).
- FIX: special-page builders and engine calls are wrapped with fail-soft logic so
       callers receive a structured partial/degraded payload rather than a 503.
- FIX: schema endpoints keep exposing top-level headers/keys/columns as expected
       by external testers.
- FIX: schema endpoints now accept GET and POST sheet aliases (sheet/page/sheet_name/name/tab/worksheet).
- FIX: instrument pages now have a final non-empty placeholder fallback instead of returning a misleading 200 with zero rows.
- FIX: optional public-read softening for schema metadata and root /sheet-rows via environment flags.
--------------------------------------------------------------------------------
"""
from __future__ import annotations

import asyncio
import inspect
import logging
import math
import os
import re
import time
import uuid
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, time as dt_time
from decimal import Decimal
from enum import Enum
from importlib import import_module
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from fastapi import Body, Header, HTTPException, Query, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.routing import APIRouter

logger = logging.getLogger("routes.advanced_analysis")
logger.addHandler(logging.NullHandler())

ADVANCED_ANALYSIS_VERSION = "7.6.0"

SCHEMA_ROUTE_OWNER = "advanced_analysis"
SCHEMA_ROUTE_FAMILY = "schema"
ROOT_SHEET_ROWS_OWNER = "advanced_analysis"

_root_sheet_rows_router = APIRouter(tags=["advanced-root"])
schema_router_v1 = APIRouter(prefix="/v1/schema", tags=["schema"])
schema_router_compat = APIRouter(prefix="/schema", tags=["schema"])
router = APIRouter(tags=["advanced-analysis"])

try:
    from core.config import auth_ok, get_settings_cached, is_open_mode  # type: ignore
except Exception:
    auth_ok = None  # type: ignore
    is_open_mode = None  # type: ignore
    def get_settings_cached(*args: Any, **kwargs: Any) -> Any:  # type: ignore
        return None


STANDARD_CANONICAL_HEADERS: List[str] = [
    "Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country", "Sector", "Industry",
    "Current Price", "Previous Close", "Open", "Day High", "Day Low", "52W High", "52W Low",
    "Price Change", "Percent Change", "52W Position %", "Volume", "Avg Volume 10D", "Avg Volume 30D",
    "Market Cap", "Float Shares", "Beta 5Y", "P/E TTM", "P/E Forward", "EPS TTM", "Dividend Yield",
    "Payout Ratio", "Revenue TTM", "Revenue YoY Growth", "Gross Margin", "Operating Margin",
    "Profit Margin", "Debt / Equity", "Free Cash Flow TTM", "RSI 14", "Volatility 30D",
    "Volatility 90D", "Max Drawdown 1Y", "VaR 95% 1D", "Sharpe 1Y", "Risk Score", "Risk Bucket",
    "P/B", "P/S", "EV / EBITDA", "PEG", "Intrinsic Value", "Valuation Score", "Forecast Price 1M",
    "Forecast Price 3M", "Forecast Price 12M", "Expected ROI 1M", "Expected ROI 3M", "Expected ROI 12M",
    "Forecast Confidence", "Recommendation", "Recommendation Reason", "Value Score", "Quality Score",
    "Momentum Score", "Growth Score", "Overall Score", "Opportunity Score", "Rank Overall",
    "Confidence Bucket", "Last Updated UTC", "Source",
]
while len(STANDARD_CANONICAL_HEADERS) < 80:
    STANDARD_CANONICAL_HEADERS.append(f"Extra Field {len(STANDARD_CANONICAL_HEADERS) + 1}")
STANDARD_CANONICAL_KEYS: List[str] = [re.sub(r"[^a-z0-9]+", "_", h.lower()).strip("_") for h in STANDARD_CANONICAL_HEADERS]

INSIGHTS_HEADERS = ["Section", "Item", "Symbol", "Metric", "Value", "Notes", "Last Updated Riyadh"]
INSIGHTS_KEYS = ["section", "item", "symbol", "metric", "value", "notes", "last_updated_riyadh"]

DATA_DICTIONARY_HEADERS = ["Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes"]
DATA_DICTIONARY_KEYS = ["sheet", "group", "header", "key", "dtype", "fmt", "required", "source", "notes"]

TOP10_REQUIRED_FIELDS: Tuple[str, ...] = ("top10_rank", "selection_reason", "criteria_snapshot")
TOP10_REQUIRED_HEADERS: Dict[str, str] = {
    "top10_rank": "Top10 Rank",
    "selection_reason": "Selection Reason",
    "criteria_snapshot": "Criteria Snapshot",
}
TOP10_HEADERS = STANDARD_CANONICAL_HEADERS + [TOP10_REQUIRED_HEADERS[k] for k in TOP10_REQUIRED_FIELDS]
TOP10_KEYS = STANDARD_CANONICAL_KEYS + list(TOP10_REQUIRED_FIELDS)

KNOWN_PAGES: Tuple[str, ...] = (
    "Market_Leaders",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
    "My_Portfolio",
    "Insights_Analysis",
    "Top_10_Investments",
    "Data_Dictionary",
)

EMERGENCY_PAGE_SYMBOLS: Dict[str, List[str]] = {
    "Market_Leaders": ["2222.SR", "1120.SR", "2010.SR", "7010.SR", "AAPL", "MSFT", "NVDA", "GOOGL"],
    "Global_Markets": ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AVGO"],
    "Commodities_FX": ["GC=F", "BZ=F", "SI=F", "EURUSD=X", "GBPUSD=X", "JPY=X", "SAR=X", "CL=F"],
    "Mutual_Funds": ["SPY", "QQQ", "VTI", "VOO", "IWM"],
    "My_Portfolio": ["2222.SR", "AAPL", "MSFT", "QQQ", "GC=F"],
    "Insights_Analysis": ["2222.SR", "AAPL", "GC=F"],
    "Top_10_Investments": ["2222.SR", "1120.SR", "AAPL", "MSFT", "NVDA"],
}

TOP10_MODULE_CANDIDATES = (
    "core.analysis.top10_selector",
    "core.analysis.top10_builder",
    "core.analysis.top_10_builder",
    "core.analysis.top10_investments_builder",
    "core.analysis.top_10_investments_builder",
    "routes.investment_advisor",
)
TOP10_FUNCTION_CANDIDATES = (
    "build_top10_rows", "build_top10_output_rows", "build_top10_investments_rows",
    "build_top_10_investments_rows", "get_top10_rows", "select_top10", "select_top10_symbols", "build_rows",
)

INSIGHTS_MODULE_CANDIDATES = (
    "core.analysis.insights_builder",
    "core.analysis.insights_analysis",
    "routes.investment_advisor",
)
INSIGHTS_FUNCTION_CANDIDATES = (
    "build_insights_analysis_rows", "build_insights_rows", "build_insights_output_rows",
    "build_insights_analysis", "get_insights_rows", "build_rows",
)

FIELD_ALIAS_HINTS: Dict[str, List[str]] = {
    "symbol": ["ticker", "code", "instrument", "security", "symbol_code", "requested_symbol"],
    "ticker": ["symbol", "code", "instrument", "security", "requested_symbol"],
    "name": ["company_name", "security_name", "instrument_name", "title", "long_name", "short_name"],
    "current_price": ["price", "last_price", "last", "close", "market_price", "current", "spot", "nav", "value"],
    "recommendation_reason": ["reason", "reco_reason", "recommendation_notes"],
    "horizon_days": ["invest_period_days", "investment_period_days", "period_days"],
    "invest_period_label": ["investment_period_label", "period_label", "horizon_label"],
    "top10_rank": ["rank", "top_rank"],
    "selection_reason": ["selection_notes", "selector_reason"],
    "criteria_snapshot": ["criteria", "criteria_json", "snapshot"],
}


def _strip(v: Any) -> str:
    if v is None:
        return ""
    try:
        s = str(v).strip()
    except Exception:
        return ""
    return "" if s.lower() in {"none", "null", "nil", "undefined"} else s


def _safe_int(v: Any, default: int) -> int:
    try:
        if v is None or isinstance(v, bool):
            return default
        return int(float(v))
    except Exception:
        return default


def _boolish(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    s = _strip(v).lower()
    if not s:
        return default
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _request_id(request: Request, x_request_id: Optional[str]) -> str:
    if _strip(x_request_id):
        return _strip(x_request_id)
    try:
        rid = _strip(getattr(request.state, "request_id", ""))
        if rid:
            return rid
    except Exception:
        pass
    return uuid.uuid4().hex[:12]


def _safe_dict(v: Any) -> Dict[str, Any]:
    return dict(v) if isinstance(v, Mapping) else {}


def _split_csv(text: str) -> List[str]:
    raw = (text or "").replace(";", ",").replace("\n", ",").replace("\t", ",")
    out: List[str] = []
    seen = set()
    for part in raw.split(","):
        s = _strip(part)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _get_list(body: Mapping[str, Any], *keys: str) -> List[str]:
    for k in keys:
        value = body.get(k)
        if isinstance(value, list):
            out: List[str] = []
            seen = set()
            for item in value:
                s = _strip(item)
                if s and s not in seen:
                    seen.add(s)
                    out.append(s)
            if out:
                return out
        if isinstance(value, str) and value.strip():
            vals = _split_csv(value)
            if vals:
                return vals
    return []


def _json_safe(obj: Any) -> Any:
    try:
        return jsonable_encoder(obj)
    except Exception:
        pass

    if obj is None or isinstance(obj, (str, int, bool)):
        return obj
    if isinstance(obj, float):
        return None if math.isnan(obj) or math.isinf(obj) else obj
    if isinstance(obj, Decimal):
        try:
            f = float(obj)
            return None if math.isnan(f) or math.isinf(f) else f
        except Exception:
            return str(obj)
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, (datetime, date, dt_time)):
        try:
            return obj.isoformat()
        except Exception:
            return str(obj)
    if is_dataclass(obj):
        try:
            return {k: _json_safe(v) for k, v in asdict(obj).items()}
        except Exception:
            return str(obj)
    if isinstance(obj, Mapping):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_json_safe(v) for v in obj]
    try:
        return _json_safe(vars(obj))
    except Exception:
        return str(obj)


def _extract_auth_token(*, token_query: Optional[str], x_app_token: Optional[str], x_api_key: Optional[str], authorization: Optional[str]) -> str:
    token = _strip(x_app_token) or _strip(x_api_key)
    authz = _strip(authorization)
    if authz.lower().startswith("bearer "):
        token = _strip(authz.split(" ", 1)[1])
    if token:
        return token
    if token_query:
        allow_query = False
        try:
            settings = get_settings_cached()
            allow_query = bool(getattr(settings, "allow_query_token", False))
        except Exception:
            allow_query = False
        if allow_query:
            return _strip(token_query)
    return ""


def _local_token_match(token: str) -> bool:
    if not token:
        return False
    configured = [_strip(os.getenv(name)) for name in (
        "TFB_TOKEN", "APP_TOKEN", "BACKEND_TOKEN", "X_APP_TOKEN", "AUTH_TOKEN",
        "TOKEN", "TFB_APP_TOKEN", "BACKUP_APP_TOKEN",
    )]
    configured = [x for x in configured if x]
    return bool(configured and token in configured)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = _strip(os.getenv(name))
    if not raw:
        return default
    return _boolish(raw, default)


def _allow_public_schema_reads() -> bool:
    return _env_bool("TFB_ALLOW_PUBLIC_SCHEMA_READS", True) or _env_bool("TFB_ALLOW_PUBLIC_SCHEMA", False)


def _allow_public_root_sheet_rows() -> bool:
    return _env_bool("TFB_ALLOW_PUBLIC_ROOT_SHEET_ROWS", False) or _env_bool("TFB_ALLOW_PUBLIC_SHEET_ROWS", False)


def _extract_sheet_alias_from_mapping(data: Mapping[str, Any]) -> str:
    return _strip(
        data.get("sheet")
        or data.get("sheet_name")
        or data.get("page")
        or data.get("page_name")
        or data.get("name")
        or data.get("tab")
        or data.get("worksheet")
        or ""
    )


def _looks_like_signature_mismatch(exc: TypeError) -> bool:
    msg = _strip(exc).lower()
    return any(x in msg for x in (
        "unexpected keyword argument",
        "required positional argument",
        "missing 1 required positional argument",
        "multiple values for argument",
        "takes",
    ))


def _require_auth_or_401(
    *,
    token_query: Optional[str],
    x_app_token: Optional[str],
    x_api_key: Optional[str],
    authorization: Optional[str],
    allow_public: bool = False,
) -> None:
    if allow_public:
        return
    try:
        if callable(is_open_mode) and bool(is_open_mode()):
            return
    except Exception:
        pass

    token = _extract_auth_token(token_query=token_query, x_app_token=x_app_token, x_api_key=x_api_key, authorization=authorization)
    if _local_token_match(token):
        return

    if auth_ok is None:
        return

    attempts = [
        {"token": token, "authorization": authorization, "headers": {"X-APP-TOKEN": x_app_token, "X-API-Key": x_api_key, "Authorization": authorization}},
        {"token": token, "authorization": authorization},
        {"token": token},
    ]
    last_error: Optional[Exception] = None
    for kwargs in attempts:
        try:
            if bool(auth_ok(**kwargs)):
                return
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
        except TypeError as e:
            if _looks_like_signature_mismatch(e):
                last_error = e
                continue
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=f"Auth check failed: {e}")
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=f"Auth check failed: {e}")

    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=f"Invalid token{f' ({last_error})' if last_error else ''}")


async def _maybe_await(v: Any) -> Any:
    if inspect.isawaitable(v):
        return await v
    return v


async def _call_function_flexible(fn: Any, call_specs: Sequence[Tuple[Tuple[Any, ...], Dict[str, Any]]]) -> Any:
    last_err: Optional[Exception] = None
    for args, kwargs in call_specs:
        try:
            out = fn(*args, **kwargs)
            return await _maybe_await(out)
        except TypeError as e:
            if _looks_like_signature_mismatch(e):
                last_err = e
                continue
            raise
    raise RuntimeError(str(last_err) if last_err else "call failed")


async def _get_engine(request: Request) -> Optional[Any]:
    try:
        st = getattr(request.app, "state", None)
        if st is not None:
            for attr in ("engine", "data_engine", "quote_engine", "cache_engine"):
                value = getattr(st, attr, None)
                if value is not None:
                    return value
    except Exception:
        pass

    for modpath in ("core.data_engine_v2", "core.data_engine"):
        try:
            mod = import_module(modpath)
            get_engine = getattr(mod, "get_engine", None)
            if callable(get_engine):
                return await _maybe_await(get_engine())
        except Exception:
            continue
    return None


def _safe_engine_type(engine: Any) -> str:
    try:
        return type(engine).__name__
    except Exception:
        return "unknown"


def _normalize_key_name(name: Any) -> str:
    s = _strip(name)
    if not s:
        return ""
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def _canonicalize_sheet_name(sheet: str) -> str:
    s = _strip(sheet)
    if not s:
        return s

    candidates = [s, s.replace("-", "_"), s.replace(" ", "_")]
    for cand in candidates:
        for page in KNOWN_PAGES:
            if _normalize_key_name(cand) == _normalize_key_name(page):
                return page

    try:
        mod = import_module("core.sheets.page_catalog")
        for fn_name in ("normalize_page_name", "resolve_page_candidate", "resolve_page", "canonicalize_page"):
            fn = getattr(mod, fn_name, None)
            if callable(fn):
                try:
                    out = fn(s, allow_output_pages=True)
                except TypeError:
                    out = fn(s)
                if isinstance(out, str) and _strip(out):
                    return _strip(out)
    except Exception:
        pass

    return s.replace("-", "_").replace(" ", "_")


def _maybe_route_family(sheet: str) -> str:
    try:
        from core.sheets.page_catalog import get_route_family  # type: ignore
        return str(get_route_family(sheet))
    except Exception:
        if sheet == "Insights_Analysis":
            return "insights"
        if sheet == "Top_10_Investments":
            return "top10"
        if sheet == "Data_Dictionary":
            return "dictionary"
        return "instrument"


def _get_sheet_spec(sheet: str) -> Optional[Any]:
    try:
        from core.sheets.schema_registry import get_sheet_spec  # type: ignore
        return get_sheet_spec(sheet)
    except Exception:
        return None


def _column_attr(obj: Any, name: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, Mapping):
        return obj.get(name, default)
    return getattr(obj, name, default)


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


def _force_full_top10_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    incoming_headers = list(headers or [])
    incoming_keys = list(keys or [])
    out_headers = list(STANDARD_CANONICAL_HEADERS)
    out_keys = list(STANDARD_CANONICAL_KEYS)

    for idx, key in enumerate(incoming_keys):
        ks = _strip(key)
        if ks and ks in STANDARD_CANONICAL_KEYS:
            pos = STANDARD_CANONICAL_KEYS.index(ks)
            provided_h = _strip(incoming_headers[idx]) if idx < len(incoming_headers) else ""
            if provided_h:
                out_headers[pos] = provided_h

    incoming_header_by_key = {
        _strip(k): (_strip(incoming_headers[i]) if i < len(incoming_headers) else "")
        for i, k in enumerate(incoming_keys) if _strip(k)
    }
    for extra_key in TOP10_REQUIRED_FIELDS:
        out_keys.append(extra_key)
        out_headers.append(incoming_header_by_key.get(extra_key) or TOP10_REQUIRED_HEADERS[extra_key])

    return _complete_schema_contract(out_headers, out_keys)


def _schema_columns_from_any(spec: Any) -> List[Any]:
    if spec is None:
        return []
    if isinstance(spec, dict) and len(spec) == 1 and "columns" not in spec and "fields" not in spec:
        only_val = next(iter(spec.values()))
        if isinstance(only_val, dict) and ("columns" in only_val or "fields" in only_val):
            spec = only_val

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


def _schema_headers_keys_from_spec(sheet: str, spec: Any) -> Tuple[List[str], List[str]]:
    headers: List[str] = []
    keys: List[str] = []

    for c in _schema_columns_from_any(spec):
        if isinstance(c, Mapping):
            h = _strip(c.get("header") or c.get("display_header") or c.get("displayHeader") or c.get("label") or c.get("title"))
            k = _strip(c.get("key") or c.get("field") or c.get("name") or c.get("id"))
        else:
            h = _strip(getattr(c, "header", getattr(c, "display_header", getattr(c, "label", getattr(c, "title", None)))))
            k = _strip(getattr(c, "key", getattr(c, "field", getattr(c, "name", getattr(c, "id", None)))))
        if h or k:
            headers.append(h or k.replace("_", " ").title())
            keys.append(k or _normalize_key_name(h))

    if not headers and not keys and isinstance(spec, Mapping):
        headers2 = spec.get("headers") or spec.get("display_headers")
        keys2 = spec.get("keys") or spec.get("fields") or spec.get("columns")
        if isinstance(headers2, list):
            headers = [_strip(x) for x in headers2 if _strip(x)]
        if isinstance(keys2, list):
            keys = [_strip(x) for x in keys2 if _strip(x)]

    headers, keys = _complete_schema_contract(headers, keys)
    if sheet == "Top_10_Investments":
        headers, keys = _force_full_top10_contract(headers, keys)
    return headers, keys


def _schema_headers_keys(sheet: str) -> Tuple[List[str], List[str], str]:
    spec = _get_sheet_spec(sheet)
    if spec is not None:
        headers, keys = _schema_headers_keys_from_spec(sheet, spec)
        if headers and keys:
            return headers, keys, "schema_registry.get_sheet_spec"

    if sheet == "Top_10_Investments":
        headers, keys = _force_full_top10_contract(TOP10_HEADERS, TOP10_KEYS)
        return headers, keys, "fallback:top10_83"

    if sheet == "Insights_Analysis":
        return (*_complete_schema_contract(INSIGHTS_HEADERS, INSIGHTS_KEYS), "fallback:insights_7")

    if sheet == "Data_Dictionary":
        return (*_complete_schema_contract(DATA_DICTIONARY_HEADERS, DATA_DICTIONARY_KEYS), "fallback:data_dictionary_9")

    return (*_complete_schema_contract(STANDARD_CANONICAL_HEADERS, STANDARD_CANONICAL_KEYS), "fallback:instrument_80")


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


def _key_variants(key: str) -> List[str]:
    k = _strip(key)
    if not k:
        return []
    variants = [k, k.lower(), k.upper(), k.replace("_", " "), k.replace("_", "").lower()]
    for alias in FIELD_ALIAS_HINTS.get(k, []):
        variants.extend([alias, alias.lower(), alias.upper(), alias.replace("_", " "), alias.replace("_", "").lower()])
    out: List[str] = []
    seen = set()
    for v in variants:
        s = _strip(v)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
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


def _normalize_to_schema_keys(*, schema_keys: Sequence[str], schema_headers: Sequence[str], raw: Mapping[str, Any]) -> Dict[str, Any]:
    raw_dict = dict(raw or {})
    header_by_key = {str(k): str(h) for k, h in zip(schema_keys, schema_headers)}
    out: Dict[str, Any] = {}
    for key in schema_keys:
        ks = str(key)
        v = _extract_from_raw(raw_dict, _key_variants(ks))
        if v is None:
            header = header_by_key.get(ks, "")
            if header:
                v = _extract_from_raw(raw_dict, [header, header.lower(), header.upper()])
        out[ks] = _json_safe(v)

    if "symbol" in out and not out.get("symbol"):
        sym = _extract_from_raw(raw_dict, _key_variants("symbol"))
        out["symbol"] = _normalize_symbol_token(sym) if sym else sym
    if "ticker" in out and not out.get("ticker"):
        tic = _extract_from_raw(raw_dict, _key_variants("ticker"))
        out["ticker"] = _normalize_symbol_token(tic) if tic else tic
    return out


def _project_row(keys: Sequence[str], row: Dict[str, Any]) -> Dict[str, Any]:
    kk = [str(k) for k in keys]
    return {k: _json_safe(row.get(k, None)) for k in kk}


def _rows_to_matrix(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    kk = [str(k) for k in keys]
    return [[_json_safe(row.get(k)) for k in kk] for row in rows]


def _matrix_to_rows(rows_matrix: Sequence[Sequence[Any]], keys: Sequence[str]) -> List[Dict[str, Any]]:
    kk = [str(k) for k in keys]
    out: List[Dict[str, Any]] = []
    for row in rows_matrix or []:
        vals = list(row) if isinstance(row, (list, tuple)) else [row]
        out.append({kk[i]: (vals[i] if i < len(vals) else None) for i in range(len(kk))})
    return out


def _slice_rows(rows: Sequence[Mapping[str, Any]], offset: int, limit: int) -> List[Dict[str, Any]]:
    start = max(0, int(offset or 0))
    items = list(rows or [])
    if int(limit or 0) <= 0:
        return [dict(r) for r in items[start:]]
    end = start + int(limit)
    return [dict(r) for r in items[start:end]]


def _placeholder_value_for_key(page: str, key: str, symbol: str, row_index: int) -> Any:
    kk = _normalize_key_name(key)
    if kk in {"symbol", "ticker"}:
        return symbol
    if kk == "name":
        return f"{page} {symbol}"
    if kk == "asset_class":
        return "Equity"
    if kk == "exchange":
        return "Tadawul" if symbol.endswith(".SR") else ""
    if kk == "currency":
        return "SAR" if symbol.endswith(".SR") else None
    if kk == "country":
        return "Saudi Arabia" if symbol.endswith(".SR") else None
    if kk == "source":
        return "advanced_analysis.placeholder_fallback"
    if kk == "last_updated_utc":
        return datetime.utcnow().isoformat()
    if kk == "recommendation":
        return "Watch"
    if kk == "recommendation_reason":
        return "Placeholder fallback because live engine returned no usable rows."
    if kk in {"top10_rank", "rank_overall"}:
        return row_index
    if kk == "selection_reason":
        return "Placeholder fallback because live builder returned no usable rows."
    if kk == "criteria_snapshot":
        return "{}"
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
    if page == "Top_10_Investments":
        _top10_fill_required(rows, offset=offset)
    return rows


def _extract_keys_like(payload: Any, depth: int = 0) -> List[str]:
    if depth > 8 or not isinstance(payload, Mapping):
        return []
    for name in ("keys", "fields", "column_keys", "schema_keys"):
        value = payload.get(name)
        if isinstance(value, list):
            out = [_strip(x) for x in value if _strip(x)]
            if out:
                return out
    columns = payload.get("columns")
    if isinstance(columns, list):
        keys: List[str] = []
        for c in columns:
            if isinstance(c, Mapping):
                keys.append(_strip(c.get("key") or c.get("field") or c.get("name") or c.get("id")))
        keys = [x for x in keys if x]
        if keys:
            return keys
    for name in ("payload", "result", "response", "output", "data"):
        nested = payload.get(name)
        if isinstance(nested, Mapping):
            found = _extract_keys_like(nested, depth + 1)
            if found:
                return found
    return []


def _extract_headers_like(payload: Any, depth: int = 0) -> List[str]:
    if depth > 8 or not isinstance(payload, Mapping):
        return []
    for name in ("display_headers", "sheet_headers", "column_headers", "headers"):
        value = payload.get(name)
        if isinstance(value, list):
            out = [_strip(x) for x in value if _strip(x)]
            if out:
                return out
    columns = payload.get("columns")
    if isinstance(columns, list):
        hdrs: List[str] = []
        for c in columns:
            if isinstance(c, Mapping):
                hdrs.append(_strip(c.get("header") or c.get("display_header") or c.get("label") or c.get("title")))
        hdrs = [x for x in hdrs if x]
        if hdrs:
            return hdrs
    for name in ("payload", "result", "response", "output", "data"):
        nested = payload.get(name)
        if isinstance(nested, Mapping):
            found = _extract_headers_like(nested, depth + 1)
            if found:
                return found
    return []


def _extract_rows_like(payload: Any, depth: int = 0) -> List[Dict[str, Any]]:
    if payload is None or depth > 8:
        return []

    if isinstance(payload, list):
        if not payload:
            return []
        if all(isinstance(x, Mapping) for x in payload):
            return [dict(x) for x in payload]
        return []

    if not isinstance(payload, Mapping):
        return []

    for name in ("row_objects", "records", "items", "data", "quotes", "results"):
        value = payload.get(name)
        if isinstance(value, list) and value and isinstance(value[0], Mapping):
            return [dict(x) for x in value]

    rows_value = payload.get("rows")
    if isinstance(rows_value, list):
        if rows_value and isinstance(rows_value[0], Mapping):
            return [dict(x) for x in rows_value]
        if rows_value and isinstance(rows_value[0], (list, tuple)):
            keys_like = _extract_keys_like(payload) or [_normalize_key_name(h) for h in _extract_headers_like(payload)]
            if keys_like:
                return _matrix_to_rows(rows_value, keys_like)

    rows_matrix = payload.get("rows_matrix") or payload.get("matrix")
    if isinstance(rows_matrix, list) and rows_matrix and isinstance(rows_matrix[0], (list, tuple)):
        keys_like = _extract_keys_like(payload) or [_normalize_key_name(h) for h in _extract_headers_like(payload)]
        if keys_like:
            return _matrix_to_rows(rows_matrix, keys_like)

    for name in ("payload", "result", "response", "output", "data"):
        nested = payload.get(name)
        if isinstance(nested, Mapping):
            found = _extract_rows_like(nested, depth + 1)
            if found:
                return found

    return []


def _extract_matrix_like(payload: Any, depth: int = 0) -> Optional[List[List[Any]]]:
    if depth > 8 or not isinstance(payload, Mapping):
        return None
    for name in ("rows_matrix", "matrix"):
        value = payload.get(name)
        if isinstance(value, list):
            return [list(r) if isinstance(r, (list, tuple)) else [r] for r in value]
    rows_value = payload.get("rows")
    if isinstance(rows_value, list) and rows_value and isinstance(rows_value[0], (list, tuple)):
        return [list(r) if isinstance(r, (list, tuple)) else [r] for r in rows_value]
    for name in ("payload", "result", "response", "output", "data"):
        nested = payload.get(name)
        if isinstance(nested, Mapping):
            found = _extract_matrix_like(nested, depth + 1)
            if found is not None:
                return found
    return None


def _extract_status_error(payload: Any) -> Tuple[str, Optional[str], Dict[str, Any]]:
    if not isinstance(payload, Mapping):
        return "success", None, {}
    status_out = _strip(payload.get("status")) or "success"
    error_out = payload.get("error")
    if error_out in (None, ""):
        error_out = payload.get("detail") or payload.get("message")
    meta_out = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    return status_out, (str(error_out) if error_out is not None else None), meta_out


def _payload_has_real_rows(payload: Any) -> bool:
    return bool(_extract_rows_like(payload) or _extract_matrix_like(payload))


def _payload_quality_score(payload: Any, page: str = "") -> int:
    if payload is None:
        return -10
    if isinstance(payload, list):
        return 100 if payload else 0
    if not isinstance(payload, Mapping):
        return 0

    score = 0
    rows_like = _extract_rows_like(payload)
    matrix_like = _extract_matrix_like(payload)
    headers_like = _extract_headers_like(payload)
    keys_like = _extract_keys_like(payload)

    if rows_like:
        score += 100 + min(25, len(rows_like))
    if matrix_like:
        score += 85 + min(15, len(matrix_like))
    if headers_like:
        score += 8
    if keys_like:
        score += 8

    if page == "Top_10_Investments" and rows_like:
        for field in TOP10_REQUIRED_FIELDS:
            if any(isinstance(r, Mapping) and r.get(field) not in (None, "", [], {}) for r in rows_like):
                score += 10

    status_out, error_out, _ = _extract_status_error(payload)
    if status_out.lower() == "success":
        score += 4
    elif status_out.lower() == "partial":
        score += 2
    elif status_out.lower() in {"error", "failed", "fail"}:
        score -= 3
    if _strip(error_out):
        score -= 6
    return score


def _payload_looks_hard_failure(payload: Any) -> bool:
    if not isinstance(payload, Mapping):
        return False
    status_out, error_out, _ = _extract_status_error(payload)
    if status_out.lower() in {"error", "failed", "fail"} and not _payload_has_real_rows(payload):
        return True
    err = _strip(error_out).lower()
    return bool(err and not _payload_has_real_rows(payload))


async def _fetch_analysis_rows(engine: Any, symbols: List[str], *, mode: str, settings: Any, schema: Any) -> Dict[str, Any]:
    if not symbols or engine is None:
        return {}

    preferred = [
        "get_analysis_rows_batch", "get_analysis_quotes_batch", "get_enriched_quotes_batch",
        "get_quotes_batch", "quotes_batch", "get_enriched_quotes", "get_quotes",
    ]
    for method in preferred:
        fn = getattr(engine, method, None)
        if not callable(fn):
            continue
        try:
            res = await _call_function_flexible(fn, [
                ((symbols,), {"mode": mode, "schema": schema}),
                ((symbols,), {"schema": schema}),
                ((symbols,), {"mode": mode}),
                ((symbols,), {}),
                ((), {"symbols": symbols, "mode": mode, "schema": schema}),
                ((), {"symbols": symbols, "schema": schema}),
                ((), {"symbols": symbols, "mode": mode}),
                ((), {"symbols": symbols}),
            ])
            if isinstance(res, Mapping):
                if all(isinstance(k, str) for k in res.keys()) and any(k in set(symbols) for k in res.keys()):
                    return dict(res)
                data = res.get("data") or res.get("rows") or res.get("items") or res.get("quotes") or res.get("row_objects")
                if isinstance(data, Mapping):
                    return dict(data)
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
                out[s] = await _call_function_flexible(per_dict_fn, [
                    ((s,), {"mode": mode, "schema": schema}),
                    ((s,), {"schema": schema}),
                    ((s,), {"mode": mode}),
                    ((s,), {}),
                ])
            elif callable(per_fn):
                out[s] = await _call_function_flexible(per_fn, [
                    ((s,), {"mode": mode, "schema": schema}),
                    ((s,), {"schema": schema}),
                    ((s,), {"mode": mode}),
                    ((s,), {}),
                ])
            else:
                out[s] = {"symbol": s, "ticker": s, "error": "engine_missing_quote_method"}
        except Exception as e:
            out[s] = {"symbol": s, "ticker": s, "error": str(e)}
    return out


async def _engine_fetch_any(engine: Any, *, sheet: str, fetch_limit: int, mode: str, body: Dict[str, Any]) -> Any:
    if engine is None:
        raise RuntimeError("engine unavailable")

    candidates = []
    for name in (
        "get_sheet_rows", "get_page_rows", "sheet_rows", "build_sheet_rows", "execute_sheet_rows",
        "run_sheet_rows", "get_rows_for_sheet", "get_rows_for_page", "get_sheet", "get_cached_sheet_rows",
        "get_sheet_snapshot", "get_cached_sheet_snapshot",
    ):
        fn = getattr(engine, name, None)
        if callable(fn):
            candidates.append(fn)

    best_payload: Any = None
    best_score = -9999
    for fn in candidates:
        try:
            out = await _call_function_flexible(fn, [
                ((), {"sheet": sheet, "page": sheet, "sheet_name": sheet, "limit": fetch_limit, "offset": 0, "mode": mode, "body": body}),
                ((), {"sheet": sheet, "page": sheet, "sheet_name": sheet, "limit": fetch_limit, "offset": 0, "mode": mode}),
                ((), {"sheet": sheet, "limit": fetch_limit, "offset": 0, "body": body}),
                ((), {"page": sheet, "limit": fetch_limit, "offset": 0, "body": body}),
                ((), {"sheet_name": sheet, "limit": fetch_limit, "offset": 0, "body": body}),
                ((), {"sheet": sheet, "limit": fetch_limit, "offset": 0}),
                ((), {"page": sheet, "limit": fetch_limit, "offset": 0}),
                ((sheet,), {"limit": fetch_limit, "offset": 0, "mode": mode, "body": body}),
                ((sheet,), {"limit": fetch_limit, "offset": 0}),
                ((sheet,), {}),
            ])
            score = _payload_quality_score(out, page=sheet)
            if score > best_score:
                best_payload = out
                best_score = score
            if _payload_has_real_rows(out):
                return out
        except Exception:
            continue

    if best_payload is not None:
        return best_payload

    raise RuntimeError("Engine has no supported sheet-rows method")


async def _resolve_engine_symbols_for_sheet(engine: Any, sheet: str, body: Dict[str, Any], limit: int) -> List[str]:
    explicit = _get_list(body, "symbols", "tickers", "tickers_list", "selected_symbols", "selected_tickers")
    if explicit:
        return [_normalize_symbol_token(x) for x in explicit if _normalize_symbol_token(x)][:limit]

    if engine is not None:
        for name in ("get_sheet_symbols", "get_page_symbols", "list_symbols_for_page", "list_symbols", "get_symbols"):
            fn = getattr(engine, name, None)
            if not callable(fn):
                continue
            try:
                result = await _call_function_flexible(fn, [
                    ((), {"sheet": sheet, "limit": limit, "body": body}),
                    ((), {"page": sheet, "limit": limit, "body": body}),
                    ((), {"sheet_name": sheet, "limit": limit, "body": body}),
                    ((sheet,), {"limit": limit, "body": body}),
                    ((sheet,), {"limit": limit}),
                    ((sheet,), {}),
                ])
                symbols: List[str] = []
                if isinstance(result, list):
                    symbols = [_normalize_symbol_token(x) for x in result if _normalize_symbol_token(x)]
                elif isinstance(result, Mapping):
                    for key in ("symbols", "tickers", "selected_symbols", "selected_tickers", "universe", "top_symbols", "direct_symbols"):
                        value = result.get(key)
                        if isinstance(value, list):
                            symbols = [_normalize_symbol_token(x) for x in value if _normalize_symbol_token(x)]
                            break
                if symbols:
                    return symbols[:limit]
            except Exception:
                continue

    return [s for s in EMERGENCY_PAGE_SYMBOLS.get(sheet, []) if _normalize_symbol_token(s)][:limit]


def _top10_fill_required(rows: List[Dict[str, Any]], offset: int = 0) -> None:
    for idx, row in enumerate(rows, start=offset + 1):
        row.setdefault("top10_rank", idx)
        row.setdefault("selection_reason", "Selected by advanced_analysis fallback.")
        row.setdefault("criteria_snapshot", "{}")


async def _call_builder_best_effort(
    *,
    module_names: Sequence[str],
    function_names: Sequence[str],
    request: Request,
    settings: Any,
    engine: Any,
    mode: str,
    body: Dict[str, Any],
    page: str,
    fetch_limit: int,
    schema: Any,
    schema_headers: Sequence[str],
    schema_keys: Sequence[str],
    friendly_name: str,
    allow_symbol_rehydrate: bool = False,
) -> Tuple[Any, str, Dict[str, Any]]:
    imported_mod = None
    imported_name = ""
    last_err: Optional[Exception] = None

    for module_name in module_names:
        try:
            imported_mod = import_module(module_name)
            imported_name = module_name
            break
        except Exception as e:
            last_err = e
            continue

    if imported_mod is None:
        return None, "none", {"builder_import_failed": True, "builder_error": str(last_err) if last_err else f"{friendly_name} import failed"}

    chosen_fn = None
    chosen_name = ""
    for fn_name in function_names:
        cand = getattr(imported_mod, fn_name, None)
        if callable(cand):
            chosen_fn = cand
            chosen_name = fn_name
            break

    if chosen_fn is None:
        return None, "none", {"builder_missing": True, "builder_error": f"{friendly_name} builder missing callable", "builder_module": imported_name}

    criteria = dict(body.get("criteria") or {}) if isinstance(body.get("criteria"), dict) else {}
    if not criteria:
        criteria = dict(body)

    try:
        result = await _call_function_flexible(chosen_fn, [
            ((), {"request": request, "settings": settings, "engine": engine, "data_engine": engine, "quote_engine": engine, "cache_engine": engine,
                  "body": body, "payload": body, "criteria": criteria or None, "page": page, "sheet": page, "sheet_name": page,
                  "limit": fetch_limit, "offset": 0, "top_n": fetch_limit, "mode": mode, "schema": schema,
                  "schema_headers": list(schema_headers), "schema_keys": list(schema_keys)}),
            ((), {"engine": engine, "criteria": criteria or None, "symbols": _get_list(body, "symbols", "tickers", "tickers_list", "direct_symbols") or None,
                  "body": body, "limit": fetch_limit, "top_n": fetch_limit, "offset": 0, "mode": mode}),
            ((), {"engine": engine, "body": body, "limit": fetch_limit, "top_n": fetch_limit, "offset": 0, "mode": mode}),
            ((), {"payload": body, "limit": fetch_limit, "top_n": fetch_limit, "offset": 0, "mode": mode}),
            ((), {"criteria": criteria or None, "limit": fetch_limit, "top_n": fetch_limit, "offset": 0, "mode": mode}),
            ((body,), {}),
            ((engine, body), {}),
            ((engine,), {}),
            ((), {}),
        ])
    except Exception as e:
        return None, chosen_name or "none", {"builder_call_failed": True, "builder_error": str(e), "builder_module": imported_name, "builder_function": chosen_name}

    builder_meta: Dict[str, Any] = {
        "builder_module": imported_name,
        "builder_function": chosen_name,
        "raw_payload_quality": _payload_quality_score(result, page=page),
    }

    if allow_symbol_rehydrate and not _payload_has_real_rows(result):
        selected_symbols = []
        if isinstance(result, Mapping):
            for key in ("symbols", "tickers", "selected_symbols", "selected_tickers", "universe", "top_symbols", "direct_symbols"):
                value = result.get(key)
                if isinstance(value, list):
                    selected_symbols = [_normalize_symbol_token(x) for x in value if _normalize_symbol_token(x)]
                    break
        if selected_symbols and engine is not None:
            try:
                data_map = await _fetch_analysis_rows(engine, selected_symbols[:fetch_limit], mode=mode, settings=settings, schema=schema)
                rebuilt: List[Dict[str, Any]] = []
                for idx, sym in enumerate(selected_symbols[:fetch_limit], start=1):
                    raw = data_map.get(sym) or {"symbol": sym, "ticker": sym}
                    raw = raw if isinstance(raw, Mapping) else {}
                    rr = _normalize_to_schema_keys(schema_keys=schema_keys, schema_headers=schema_headers, raw=dict(raw))
                    if page == "Top_10_Investments":
                        rr.setdefault("top10_rank", idx)
                        rr.setdefault("selection_reason", "Selected by Top10 builder.")
                        rr.setdefault("criteria_snapshot", "{}")
                    rebuilt.append(_project_row(schema_keys, rr))
                result = {"status": "partial", "row_objects": rebuilt, "meta": {"symbols_only_result": True}}
                builder_meta["symbols_rehydrated"] = True
                builder_meta["rehydrated_count"] = len(rebuilt)
            except Exception as e:
                builder_meta["symbols_rehydrated"] = False
                builder_meta["rehydrate_error"] = str(e)

    return result, chosen_name or "none", builder_meta


def _known_schema_pages() -> List[str]:
    registry, _ = _collect_schema_registry()
    pages = sorted({_canonicalize_sheet_name(k) for k in registry.keys() if _strip(k)})
    if pages:
        return pages
    return list(KNOWN_PAGES)


def _collect_schema_registry() -> Tuple[Dict[str, Any], str]:
    try:
        mod = import_module("core.sheets.schema_registry")
    except Exception:
        mod = None

    if mod is not None:
        candidates = [
            ("get_all_sheet_specs", True),
            ("list_sheet_specs", True),
            ("get_schema_registry", False),
            ("schema_registry", False),
            ("SCHEMA_REGISTRY", False),
            ("SHEET_REGISTRY", False),
            ("REGISTRY", False),
            ("SHEETS", False),
        ]
        for name, should_call in candidates:
            obj = getattr(mod, name, None)
            if obj is None:
                continue
            try:
                out = obj() if should_call and callable(obj) else obj
            except Exception:
                continue
            if isinstance(out, dict):
                normalized = {_canonicalize_sheet_name(str(k)): v for k, v in out.items()}
                if normalized:
                    return normalized, f"schema_registry.{name}"
            if isinstance(out, (list, tuple)):
                normalized2: Dict[str, Any] = {}
                for spec in out:
                    sheet_name = _strip(_column_attr(spec, "sheet", "") or _column_attr(spec, "name", ""))
                    if sheet_name:
                        normalized2[_canonicalize_sheet_name(sheet_name)] = spec
                if normalized2:
                    return normalized2, f"schema_registry.{name}"

    normalized3: Dict[str, Any] = {}
    for page in KNOWN_PAGES:
        spec = _get_sheet_spec(page)
        if spec is not None:
            normalized3[_canonicalize_sheet_name(page)] = spec
    return normalized3, "schema_registry.get_sheet_spec_fallback"


def _sheet_spec_to_payload(sheet: str, spec: Any) -> Dict[str, Any]:
    headers, keys, _ = _schema_headers_keys(sheet)
    columns_payload: List[Dict[str, Any]] = []
    for c in _schema_columns_from_any(spec or {}):
        columns_payload.append({
            "group": _column_attr(c, "group"),
            "header": _column_attr(c, "header"),
            "key": _column_attr(c, "key"),
            "dtype": _column_attr(c, "dtype"),
            "fmt": _column_attr(c, "fmt"),
            "required": _column_attr(c, "required"),
            "source": _column_attr(c, "source"),
            "notes": _column_attr(c, "notes"),
        })

    if not columns_payload:
        # Build a minimal columns payload from headers/keys so schema endpoints stay useful.
        for h, k in zip(headers, keys):
            columns_payload.append({
                "group": None,
                "header": h,
                "key": k,
                "dtype": None,
                "fmt": None,
                "required": False,
                "source": "fallback",
                "notes": "",
            })

    criteria_fields_payload: List[Dict[str, Any]] = []
    cfs = getattr(spec, "criteria_fields", None) or _column_attr(spec, "criteria_fields", []) or []
    for cf in cfs:
        criteria_fields_payload.append({
            "key": _column_attr(cf, "key"),
            "header": _column_attr(cf, "header"),
            "dtype": _column_attr(cf, "dtype"),
            "required": _column_attr(cf, "required"),
            "default": _column_attr(cf, "default"),
            "notes": _column_attr(cf, "notes"),
        })

    return {
        "sheet": sheet,
        "headers": headers,
        "keys": keys,
        "columns": columns_payload,
        "criteria_fields": criteria_fields_payload,
        "column_count": len(headers),
        "criteria_field_count": len(criteria_fields_payload),
    }


def _schema_spec_payload(sheet_filter: Optional[str] = None) -> Dict[str, Any]:
    registry, source = _collect_schema_registry()

    if sheet_filter:
        wanted = _canonicalize_sheet_name(sheet_filter)
        spec = registry.get(wanted) or _get_sheet_spec(wanted)
        if spec is None and wanted not in KNOWN_PAGES:
            raise HTTPException(status_code=404, detail=f"Unknown sheet: {wanted}")
        payload = _sheet_spec_to_payload(wanted, spec or {})
        return _json_safe({
            "status": "success",
            "version": ADVANCED_ANALYSIS_VERSION,
            "source": source,
            "sheet": wanted,
            "headers": payload.get("headers", []),
            "display_headers": payload.get("headers", []),
            "sheet_headers": payload.get("headers", []),
            "column_headers": payload.get("headers", []),
            "keys": payload.get("keys", []),
            "fields": payload.get("keys", []),
            "columns": payload.get("columns", []),
            "criteria_fields": payload.get("criteria_fields", []),
            "column_count": payload.get("column_count", 0),
            "criteria_field_count": payload.get("criteria_field_count", 0),
            "spec": payload,
        })

    sheets_payload: Dict[str, Any] = {}
    for sheet_name in sorted(set(list(registry.keys()) + list(KNOWN_PAGES))):
        try:
            sheets_payload[sheet_name] = _sheet_spec_to_payload(sheet_name, registry.get(sheet_name) or {})
        except Exception as e:
            sheets_payload[sheet_name] = {"sheet": sheet_name, "error": f"{type(e).__name__}: {e}"}

    return _json_safe({
        "status": "success",
        "version": ADVANCED_ANALYSIS_VERSION,
        "source": source,
        "sheet_count": len(sheets_payload),
        "pages": list(sheets_payload.keys()),
        "sheets": sheets_payload,
    })


def _schema_pages_payload() -> Dict[str, Any]:
    registry, source = _collect_schema_registry()
    items: List[Dict[str, Any]] = []
    for page in _known_schema_pages():
        headers, keys, schema_source = _schema_headers_keys(page)
        items.append({
            "page": page,
            "sheet": page,
            "route_family": _strip(_maybe_route_family(page)) or "instrument",
            "column_count": len(headers),
            "header_count": len(headers),
            "key_count": len(keys),
            "schema_source": schema_source,
            "has_registry_entry": page in registry,
        })

    return _json_safe({
        "status": "success",
        "version": ADVANCED_ANALYSIS_VERSION,
        "route_owner": SCHEMA_ROUTE_OWNER,
        "route_family": SCHEMA_ROUTE_FAMILY,
        "sheet_count": len(items),
        "pages": [item["page"] for item in items],
        "items": items,
        "source": source,
    })


def _schema_root_payload(request: Optional[Request] = None) -> Dict[str, Any]:
    pages_payload = _schema_pages_payload()
    request_id = None
    if request is not None:
        try:
            request_id = getattr(request.state, "request_id", None)
        except Exception:
            request_id = None

    return _json_safe({
        "status": "success",
        "version": ADVANCED_ANALYSIS_VERSION,
        "route_owner": SCHEMA_ROUTE_OWNER,
        "route_family": SCHEMA_ROUTE_FAMILY,
        "root_path": "/v1/schema",
        "compat_root_path": "/schema",
        "request_id": request_id,
        "pages": pages_payload.get("pages", []),
        "sheet_count": pages_payload.get("sheet_count", 0),
        "supported_aliases": [
            "/v1/schema", "/v1/schema/health", "/v1/schema/pages", "/v1/schema/sheet-spec", "/v1/schema/spec",
            "/v1/schema/data-dictionary", "/schema", "/schema/health", "/schema/pages", "/schema/sheet-spec",
            "/schema/spec", "/schema/data-dictionary", "/sheet-rows",
        ],
    })


def _fallback_data_dictionary_rows() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    registry, _ = _collect_schema_registry()
    pages = sorted(set(list(registry.keys()) + list(KNOWN_PAGES)))
    for sheet_name in pages:
        spec = registry.get(sheet_name) or _get_sheet_spec(sheet_name)
        columns = _schema_columns_from_any(spec)
        if columns:
            for c in columns:
                rows.append({
                    "sheet": sheet_name,
                    "group": _column_attr(c, "group"),
                    "header": _column_attr(c, "header"),
                    "key": _column_attr(c, "key"),
                    "dtype": _column_attr(c, "dtype"),
                    "fmt": _column_attr(c, "fmt"),
                    "required": _column_attr(c, "required"),
                    "source": _column_attr(c, "source") or "schema_registry",
                    "notes": _column_attr(c, "notes"),
                })
            continue

        headers, keys, _ = _schema_headers_keys(sheet_name)
        for h, k in zip(headers, keys):
            rows.append({
                "sheet": sheet_name,
                "group": None,
                "header": h,
                "key": k,
                "dtype": None,
                "fmt": None,
                "required": False,
                "source": "fallback_contract",
                "notes": "",
            })
    return rows


def _build_data_dictionary_rows_payload(*, include_matrix: bool, request_id: str, started_at: float, limit: int, offset: int) -> Dict[str, Any]:
    headers, keys, schema_source = _schema_headers_keys("Data_Dictionary")
    rows: List[Dict[str, Any]] = []
    error_out: Optional[str] = None

    try:
        mod = import_module("core.sheets.data_dictionary")
        build_rows = getattr(mod, "build_data_dictionary_rows", None)
        if callable(build_rows):
            try:
                raw_rows = build_rows(include_meta_sheet=True)
            except TypeError:
                raw_rows = build_rows()
            for r in raw_rows or []:
                if isinstance(r, Mapping):
                    rr = _normalize_to_schema_keys(schema_keys=keys, schema_headers=headers, raw=dict(r))
                    rows.append(_project_row(keys, rr))
    except Exception as e:
        error_out = f"{type(e).__name__}: {e}"

    if not rows:
        try:
            for r in _fallback_data_dictionary_rows():
                rr = _normalize_to_schema_keys(schema_keys=keys, schema_headers=headers, raw=r)
                rows.append(_project_row(keys, rr))
        except Exception as e:
            if not error_out:
                error_out = f"{type(e).__name__}: {e}"

    if not rows:
        # Absolute last resort: guarantee at least the Data_Dictionary page itself exists.
        for h, k in zip(DATA_DICTIONARY_HEADERS, DATA_DICTIONARY_KEYS):
            rr = {
                "sheet": "Data_Dictionary",
                "group": None,
                "header": h,
                "key": k,
                "dtype": None,
                "fmt": None,
                "required": False,
                "source": "hard_fallback",
                "notes": "",
            }
            rows.append(_project_row(keys, _normalize_to_schema_keys(schema_keys=keys, schema_headers=headers, raw=rr)))

    total_rows = len(rows)
    rows = _slice_rows(rows, offset=offset, limit=limit)
    matrix = _rows_to_matrix(rows, keys) if include_matrix else []

    return _json_safe({
        "status": "success" if total_rows else "partial",
        "page": "Data_Dictionary",
        "sheet": "Data_Dictionary",
        "sheet_name": "Data_Dictionary",
        "route_family": "dictionary",
        "headers": headers,
        "display_headers": headers,
        "sheet_headers": headers,
        "column_headers": headers,
        "keys": keys,
        "columns": keys,
        "fields": keys,
        "rows": rows,
        "rows_matrix": matrix,
        "matrix": matrix,
        "row_objects": rows,
        "items": rows,
        "records": rows,
        "data": rows,
        "quotes": rows,
        "count": len(rows),
        "detail": error_out or "",
        "error": error_out,
        "version": ADVANCED_ANALYSIS_VERSION,
        "request_id": request_id,
        "meta": {
            "duration_ms": round((time.perf_counter() - started_at) * 1000.0, 3),
            "schema_source": schema_source,
            "dispatch": "data_dictionary",
            "generated": True,
            "source_row_count": total_rows,
            "offset": offset,
            "limit": limit,
            "count": len(rows),
            "row_object_count": len(rows),
            "matrix_row_count": len(matrix),
        },
    })


def _schema_only_payload(
    *,
    page: str,
    route_family: str,
    headers: Sequence[str],
    keys: Sequence[str],
    include_matrix: bool,
    request_id: str,
    started_at: float,
    schema_only: bool,
    headers_only: bool,
    meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    hdrs = list(headers or [])
    ks = list(keys or [])
    empty_matrix = [] if include_matrix else []
    return _json_safe({
        "status": "success",
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
        "rows": [],
        "rows_matrix": empty_matrix,
        "matrix": empty_matrix,
        "row_objects": [],
        "items": [],
        "records": [],
        "data": [],
        "quotes": [],
        "count": 0,
        "detail": "",
        "error": None,
        "version": ADVANCED_ANALYSIS_VERSION,
        "request_id": request_id,
        "meta": {
            "duration_ms": round((time.perf_counter() - started_at) * 1000.0, 3),
            "dispatch": "schema_only",
            "schema_only": bool(schema_only),
            "headers_only": bool(headers_only),
            "count": 0,
            "row_object_count": 0,
            "matrix_row_count": 0,
            **(meta or {}),
        },
    })


def _normalize_result_to_payload(
    *,
    result: Any,
    page: str,
    route_family: str,
    schema_headers: Sequence[str],
    schema_keys: Sequence[str],
    include_matrix: bool,
    request_id: str,
    started_at: float,
    dispatch: str,
    limit: int,
    offset: int,
    default_status: str = "success",
    default_error: Optional[str] = None,
    extra_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    result_headers = _extract_headers_like(result) or list(schema_headers or [])
    result_keys = _extract_keys_like(result) or list(schema_keys or [])

    if page == "Top_10_Investments":
        result_headers, result_keys = _force_full_top10_contract(result_headers, result_keys)

    proj_headers = list(schema_headers or result_headers or [])
    proj_keys = list(schema_keys or result_keys or [])
    if page == "Top_10_Investments":
        proj_headers, proj_keys = _force_full_top10_contract(proj_headers, proj_keys)

    raw_rows = _extract_rows_like(result)
    if not raw_rows:
        mx = _extract_matrix_like(result)
        if mx:
            raw_rows = _matrix_to_rows(mx, result_keys or proj_keys)

    source_row_count = len(raw_rows)
    raw_rows = _slice_rows(raw_rows, offset=offset, limit=limit)

    rows: List[Dict[str, Any]] = []
    for idx, r in enumerate(raw_rows, start=offset + 1):
        rr = _normalize_to_schema_keys(schema_keys=proj_keys, schema_headers=proj_headers, raw=dict(r))
        if page == "Top_10_Investments":
            rr.setdefault("top10_rank", idx)
            rr.setdefault("selection_reason", "Selected by advanced_analysis.")
            rr.setdefault("criteria_snapshot", "{}")
        rows.append(_project_row(proj_keys, rr))

    status_out, error_out, meta_in = _extract_status_error(result)
    final_status = _strip(status_out) or default_status
    if rows and final_status in {"error", "failed", "fail"}:
        final_status = "partial"
    if not rows and final_status == "success" and error_out:
        final_status = "partial"

    matrix = _rows_to_matrix(rows, proj_keys) if include_matrix else []

    return _json_safe({
        "status": final_status or default_status,
        "page": page,
        "sheet": page,
        "sheet_name": page,
        "route_family": route_family,
        "headers": list(proj_headers),
        "display_headers": list(proj_headers),
        "sheet_headers": list(proj_headers),
        "column_headers": list(proj_headers),
        "keys": list(proj_keys),
        "columns": list(proj_keys),
        "fields": list(proj_keys),
        "rows": rows,
        "rows_matrix": matrix,
        "matrix": matrix,
        "row_objects": rows,
        "items": rows,
        "records": rows,
        "data": rows,
        "quotes": rows,
        "count": len(rows),
        "detail": error_out or default_error or "",
        "error": error_out or default_error,
        "version": ADVANCED_ANALYSIS_VERSION,
        "request_id": request_id,
        "meta": {
            "duration_ms": round((time.perf_counter() - started_at) * 1000.0, 3),
            "dispatch": dispatch,
            "result_payload_quality": _payload_quality_score(result, page=page),
            "source_row_count": source_row_count,
            "offset": offset,
            "limit": limit,
            "count": len(rows),
            "row_object_count": len(rows),
            "matrix_row_count": len(matrix),
            **(meta_in or {}),
            **(extra_meta or {}),
        },
    })


async def _run_advanced_sheet_rows_impl(
    *,
    request: Request,
    body: Dict[str, Any],
    mode: str,
    include_matrix_q: Optional[bool],
    token: Optional[str],
    x_app_token: Optional[str],
    x_api_key: Optional[str],
    authorization: Optional[str],
    x_request_id: Optional[str],
) -> Dict[str, Any]:
    t0 = time.perf_counter()
    body = _safe_dict(body)

    _require_auth_or_401(
        token_query=token,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
        allow_public=_allow_public_root_sheet_rows(),
    )

    request_id = _request_id(request, x_request_id)

    raw_sheet = _extract_sheet_alias_from_mapping(body)
    if not raw_sheet:
        raise HTTPException(status_code=422, detail="Missing required field: sheet")

    sheet = _canonicalize_sheet_name(raw_sheet)
    limit = max(1, min(5000, _safe_int(body.get("limit"), 2000)))
    offset = max(0, _safe_int(body.get("offset"), 0))
    fetch_limit = min(5000, limit + offset)
    include_matrix_final = include_matrix_q if isinstance(include_matrix_q, bool) else _boolish(body.get("include_matrix"), True)
    schema_only = _boolish(body.get("schema_only"), False)
    headers_only = _boolish(body.get("headers_only"), False)

    schema_headers, schema_keys, schema_source = _schema_headers_keys(sheet)
    route_family = _strip(_maybe_route_family(sheet)) or "instrument"

    if schema_only or headers_only:
        return _schema_only_payload(
            page=sheet,
            route_family=route_family,
            headers=schema_headers,
            keys=schema_keys,
            include_matrix=bool(include_matrix_final),
            request_id=request_id,
            started_at=t0,
            schema_only=schema_only,
            headers_only=headers_only,
            meta={"schema_source": schema_source},
        )

    if sheet == "Data_Dictionary":
        try:
            return _build_data_dictionary_rows_payload(
                include_matrix=bool(include_matrix_final),
                request_id=request_id,
                started_at=t0,
                limit=limit,
                offset=offset,
            )
        except Exception as e:
            # Never allow Data_Dictionary to bubble out as 5xx.
            logger.exception("Data_Dictionary payload failed: %s", e)
            fallback = _build_data_dictionary_rows_payload(
                include_matrix=bool(include_matrix_final),
                request_id=request_id,
                started_at=t0,
                limit=max(1, limit),
                offset=max(0, offset),
            )
            fallback["status"] = "partial"
            fallback["detail"] = _strip(fallback.get("detail")) or str(e)
            fallback["error"] = _strip(fallback.get("error")) or str(e)
            fallback.setdefault("meta", {})
            fallback["meta"]["data_dictionary_exception"] = str(e)
            return _json_safe(fallback)

    try:
        settings = get_settings_cached()
    except Exception:
        settings = None

    engine = await _get_engine(request)
    schema = _get_sheet_spec(sheet)

    # Insights_Analysis: root path must stay reliable.
    if sheet == "Insights_Analysis":
        result, dispatch, builder_meta = await _call_builder_best_effort(
            module_names=INSIGHTS_MODULE_CANDIDATES,
            function_names=INSIGHTS_FUNCTION_CANDIDATES,
            request=request,
            settings=settings,
            engine=engine,
            mode=mode or "",
            body=body,
            page=sheet,
            fetch_limit=fetch_limit,
            schema=schema,
            schema_headers=schema_headers,
            schema_keys=schema_keys,
            friendly_name="Insights_Analysis",
            allow_symbol_rehydrate=False,
        )
        builder_score = _payload_quality_score(result, page=sheet) if result is not None else -9999

        engine_result = None
        engine_score = -9999
        if engine is not None:
            try:
                engine_result = await _engine_fetch_any(engine, sheet=sheet, fetch_limit=fetch_limit, mode=mode or "", body=body)
                engine_score = _payload_quality_score(engine_result, page=sheet)
            except Exception as e:
                logger.warning("Insights engine fallback failed: %s", e)

        chosen = result
        chosen_dispatch = dispatch or "insights_builder"
        if engine_result is not None and engine_score >= builder_score:
            chosen = engine_result
            chosen_dispatch = "insights_engine_fallback_best"

        if chosen is None:
            chosen = {"status": "partial", "row_objects": [], "error": "insights builder and engine returned no payload"}

        return _normalize_result_to_payload(
            result=chosen,
            page=sheet,
            route_family=route_family,
            schema_headers=schema_headers,
            schema_keys=schema_keys,
            include_matrix=bool(include_matrix_final),
            request_id=request_id,
            started_at=t0,
            dispatch=chosen_dispatch,
            limit=limit,
            offset=offset,
            default_status="partial",
            extra_meta={
                "schema_source": schema_source,
                "engine_available": bool(engine),
                "engine_type": _safe_engine_type(engine) if engine else "none",
                "fetch_limit": fetch_limit,
                "builder_payload_quality": builder_score,
                "engine_payload_quality": engine_score,
                **builder_meta,
            },
        )

    # Top_10_Investments: root path must remain the safe working special-page route.
    if sheet == "Top_10_Investments":
        result, dispatch, builder_meta = await _call_builder_best_effort(
            module_names=TOP10_MODULE_CANDIDATES,
            function_names=TOP10_FUNCTION_CANDIDATES,
            request=request,
            settings=settings,
            engine=engine,
            mode=mode or "",
            body=body,
            page=sheet,
            fetch_limit=fetch_limit,
            schema=schema,
            schema_headers=schema_headers,
            schema_keys=schema_keys,
            friendly_name="Top_10_Investments",
            allow_symbol_rehydrate=True,
        )
        builder_score = _payload_quality_score(result, page=sheet) if result is not None else -9999

        engine_result = None
        engine_score = -9999
        if engine is not None and (result is None or builder_score < 120 or _payload_looks_hard_failure(result)):
            try:
                engine_result = await _engine_fetch_any(engine, sheet=sheet, fetch_limit=fetch_limit, mode=mode or "", body=body)
                engine_score = _payload_quality_score(engine_result, page=sheet)
            except Exception as e:
                logger.warning("Top10 engine fallback failed: %s", e)

        chosen = result
        chosen_dispatch = dispatch or "top10_builder"
        if engine_result is not None and engine_score >= builder_score:
            chosen = engine_result
            chosen_dispatch = "engine_fallback_best"

        if chosen is None:
            chosen = {"status": "partial", "row_objects": [], "error": "top10 builder and engine returned no payload"}

        return _normalize_result_to_payload(
            result=chosen,
            page=sheet,
            route_family=route_family,
            schema_headers=schema_headers,
            schema_keys=schema_keys,
            include_matrix=bool(include_matrix_final),
            request_id=request_id,
            started_at=t0,
            dispatch=chosen_dispatch,
            limit=limit,
            offset=offset,
            default_status="partial",
            extra_meta={
                "schema_source": schema_source,
                "engine_available": bool(engine),
                "engine_type": _safe_engine_type(engine) if engine else "none",
                "builder_payload_quality": builder_score,
                "engine_payload_quality": engine_score,
                "fetch_limit": fetch_limit,
                **builder_meta,
            },
        )

    if engine is None:
        return _json_safe({
            "status": "degraded",
            "page": sheet,
            "sheet": sheet,
            "sheet_name": sheet,
            "route_family": route_family,
            "headers": schema_headers,
            "display_headers": schema_headers,
            "sheet_headers": schema_headers,
            "column_headers": schema_headers,
            "keys": schema_keys,
            "columns": schema_keys,
            "fields": schema_keys,
            "rows": [],
            "rows_matrix": [],
            "matrix": [],
            "row_objects": [],
            "items": [],
            "records": [],
            "data": [],
            "quotes": [],
            "count": 0,
            "detail": "",
            "error": None,
            "version": ADVANCED_ANALYSIS_VERSION,
            "request_id": request_id,
            "meta": {
                "duration_ms": round((time.perf_counter() - t0) * 1000.0, 3),
                "schema_source": schema_source,
                "engine_available": False,
                "path": "schema_only_no_engine",
                "fetch_limit": fetch_limit,
                "limit": limit,
                "offset": offset,
                "count": 0,
                "row_object_count": 0,
                "matrix_row_count": 0,
            },
        })

    symbols = _get_list(body, "symbols", "tickers", "tickers_list")
    if symbols:
        selected = symbols[offset:offset + limit] if (offset or len(symbols) > limit) else symbols[:limit]
        data_map = await _fetch_analysis_rows(engine, selected, mode=mode or "", settings=settings, schema=schema)
        rows: List[Dict[str, Any]] = []
        errors = 0
        for sym in selected:
            raw = data_map.get(sym) if isinstance(data_map, Mapping) else None
            if not isinstance(raw, Mapping):
                raw = {"symbol": sym, "ticker": sym, "error": "missing_row"}
                errors += 1
            elif raw.get("error"):
                errors += 1
            rr = _normalize_to_schema_keys(schema_keys=schema_keys, schema_headers=schema_headers, raw=dict(raw))
            if "symbol" in schema_keys and not rr.get("symbol"):
                rr["symbol"] = _normalize_symbol_token(sym)
            rows.append(_project_row(schema_keys, rr))

        matrix = _rows_to_matrix(rows, schema_keys) if include_matrix_final else []
        return _json_safe({
            "status": "success" if errors == 0 else ("partial" if errors < len(selected) else "error"),
            "page": sheet,
            "sheet": sheet,
            "sheet_name": sheet,
            "route_family": route_family,
            "headers": schema_headers,
            "display_headers": schema_headers,
            "sheet_headers": schema_headers,
            "column_headers": schema_headers,
            "keys": schema_keys,
            "columns": schema_keys,
            "fields": schema_keys,
            "rows": rows,
            "rows_matrix": matrix,
            "matrix": matrix,
            "row_objects": rows,
            "items": rows,
            "records": rows,
            "data": rows,
            "quotes": rows,
            "count": len(rows),
            "detail": f"{errors} errors" if errors else "",
            "error": f"{errors} errors" if errors else None,
            "version": ADVANCED_ANALYSIS_VERSION,
            "request_id": request_id,
            "meta": {
                "duration_ms": round((time.perf_counter() - t0) * 1000.0, 3),
                "schema_source": schema_source,
                "engine_available": True,
                "engine_type": _safe_engine_type(engine),
                "path": "instrument_batch",
                "requested": len(selected),
                "errors": errors,
                "offset": offset,
                "limit": limit,
                "count": len(rows),
                "row_object_count": len(rows),
                "matrix_row_count": len(matrix),
            },
        })

    try:
        engine_payload = await _engine_fetch_any(engine, sheet=sheet, fetch_limit=fetch_limit, mode=mode or "", body=body)
        engine_score = _payload_quality_score(engine_payload, page=sheet)
    except Exception as e:
        logger.warning("Engine table fetch failed for %s: %s", sheet, e)
        engine_payload = {"status": "partial", "row_objects": [], "error": str(e)}
        engine_score = _payload_quality_score(engine_payload, page=sheet)

    if not _payload_has_real_rows(engine_payload) and route_family == "instrument":
        synth_symbols = await _resolve_engine_symbols_for_sheet(engine, sheet, body, fetch_limit)
        if synth_symbols:
            data_map = await _fetch_analysis_rows(engine, synth_symbols, mode=mode or "", settings=settings, schema=schema)
            synth_rows: List[Dict[str, Any]] = []
            for sym in synth_symbols:
                raw = data_map.get(sym) if isinstance(data_map, Mapping) else None
                if not isinstance(raw, Mapping):
                    raw = {"symbol": sym, "ticker": sym}
                rr = _normalize_to_schema_keys(schema_keys=schema_keys, schema_headers=schema_headers, raw=dict(raw))
                if "symbol" in schema_keys and not rr.get("symbol"):
                    rr["symbol"] = _normalize_symbol_token(sym)
                synth_rows.append(_project_row(schema_keys, rr))
            if synth_rows:
                engine_payload = {
                    "status": "partial",
                    "row_objects": synth_rows,
                    "meta": {"synthesized_from_symbols": True, "resolved_symbol_count": len(synth_symbols)},
                }
                engine_score = _payload_quality_score(engine_payload, page=sheet)

    if not _payload_has_real_rows(engine_payload):
        requested_symbols = _get_list(body, "symbols", "tickers", "tickers_list", "selected_symbols", "selected_tickers")
        placeholder_rows = _build_placeholder_rows(
            page=sheet,
            keys=schema_keys,
            requested_symbols=requested_symbols,
            limit=limit,
            offset=offset,
        )
        if placeholder_rows:
            engine_payload = {
                "status": "partial",
                "row_objects": placeholder_rows,
                "error": _strip(_extract_status_error(engine_payload)[1]) or "engine_returned_no_usable_rows",
                "meta": {
                    "placeholder_fallback": True,
                    "placeholder_count": len(placeholder_rows),
                },
            }
            engine_score = _payload_quality_score(engine_payload, page=sheet)

    return _normalize_result_to_payload(
        result=engine_payload,
        page=sheet,
        route_family=route_family,
        schema_headers=schema_headers,
        schema_keys=schema_keys,
        include_matrix=bool(include_matrix_final),
        request_id=request_id,
        started_at=t0,
        dispatch="engine_table_mode",
        limit=limit,
        offset=offset,
        default_status="success",
        extra_meta={
            "schema_source": schema_source,
            "engine_available": True,
            "engine_type": _safe_engine_type(engine),
            "engine_payload_quality": engine_score,
            "fetch_limit": fetch_limit,
            "limit": limit,
            "offset": offset,
        },
    )


@schema_router_v1.get("")
@schema_router_compat.get("")
async def schema_root(
    request: Request,
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Dict[str, Any]:
    _require_auth_or_401(
        token_query=token,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
        allow_public=_allow_public_schema_reads(),
    )
    return _schema_root_payload(request)


@schema_router_v1.get("/health")
@schema_router_compat.get("/health")
async def schema_health(
    request: Request,
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Dict[str, Any]:
    _require_auth_or_401(
        token_query=token,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
        allow_public=_allow_public_schema_reads(),
    )
    registry, source = _collect_schema_registry()
    request_id = getattr(request.state, "request_id", None)
    return _json_safe({
        "status": "ok",
        "version": ADVANCED_ANALYSIS_VERSION,
        "route_owner": SCHEMA_ROUTE_OWNER,
        "route_family": SCHEMA_ROUTE_FAMILY,
        "request_id": request_id,
        "registry_source": source,
        "registry_entries": len(registry),
        "sheet_count": len(_known_schema_pages()),
        "root_sheet_rows_owner": ROOT_SHEET_ROWS_OWNER,
    })


@schema_router_v1.get("/pages")
@schema_router_compat.get("/pages")
async def schema_pages(
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Dict[str, Any]:
    _require_auth_or_401(
        token_query=token,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
        allow_public=_allow_public_schema_reads(),
    )
    return _schema_pages_payload()


@schema_router_v1.get("/sheet-spec")
@schema_router_v1.get("/spec")
@schema_router_compat.get("/sheet-spec")
@schema_router_compat.get("/spec")
async def schema_sheet_spec(
    sheet: Optional[str] = Query(default=None),
    page: Optional[str] = Query(default=None),
    sheet_name: Optional[str] = Query(default=None),
    name: Optional[str] = Query(default=None),
    tab: Optional[str] = Query(default=None),
    worksheet: Optional[str] = Query(default=None),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Dict[str, Any]:
    _require_auth_or_401(
        token_query=token,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
        allow_public=_allow_public_schema_reads(),
    )
    wanted = _strip(sheet) or _strip(page) or _strip(sheet_name) or _strip(name) or _strip(tab) or _strip(worksheet)
    return _schema_spec_payload(sheet_filter=wanted or None)


@schema_router_v1.post("/sheet-spec")
@schema_router_v1.post("/spec")
@schema_router_compat.post("/sheet-spec")
@schema_router_compat.post("/spec")
async def schema_sheet_spec_post(
    body: Dict[str, Any] = Body(default_factory=dict),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Dict[str, Any]:
    _require_auth_or_401(
        token_query=token,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
        allow_public=_allow_public_schema_reads(),
    )
    payload = _safe_dict(body)
    wanted = _extract_sheet_alias_from_mapping(payload) or _strip(payload.get("sheet_filter"))
    return _schema_spec_payload(sheet_filter=wanted or None)


@schema_router_v1.get("/data-dictionary")
@schema_router_compat.get("/data-dictionary")
async def schema_data_dictionary(
    request: Request,
    include_matrix: Optional[bool] = Query(default=None),
    limit: int = Query(default=2000, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    _require_auth_or_401(
        token_query=token,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
        allow_public=_allow_public_schema_reads(),
    )
    request_id = _request_id(request, x_request_id)
    started_at = time.perf_counter()
    return _build_data_dictionary_rows_payload(
        include_matrix=bool(include_matrix if isinstance(include_matrix, bool) else True),
        request_id=request_id,
        started_at=started_at,
        limit=limit,
        offset=offset,
    )


@_root_sheet_rows_router.get("/sheet-rows")
async def advanced_sheet_rows_get(
    request: Request,
    sheet: str = Query(default=""),
    sheet_name: str = Query(default=""),
    page: str = Query(default=""),
    page_name: str = Query(default=""),
    name: str = Query(default=""),
    tab: str = Query(default=""),
    worksheet: str = Query(default=""),
    symbols: str = Query(default=""),
    tickers: str = Query(default=""),
    limit: Optional[int] = Query(default=None),
    offset: Optional[int] = Query(default=None),
    include_matrix: Optional[bool] = Query(default=None),
    schema_only: Optional[bool] = Query(default=None),
    headers_only: Optional[bool] = Query(default=None),
    mode: str = Query(default=""),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    body: Dict[str, Any] = {}
    for k, v in {
        "sheet": sheet,
        "sheet_name": sheet_name,
        "page": page,
        "page_name": page_name,
        "name": name,
        "tab": tab,
        "worksheet": worksheet,
        "symbols": symbols,
        "tickers": tickers,
        "limit": limit,
        "offset": offset,
        "schema_only": schema_only,
        "headers_only": headers_only,
    }.items():
        if v not in (None, ""):
            body[k] = v

    return await _run_advanced_sheet_rows_impl(
        request=request,
        body=body,
        mode=mode or "",
        include_matrix_q=include_matrix,
        token=token,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
        x_request_id=x_request_id,
    )


@_root_sheet_rows_router.post("/sheet-rows")
async def advanced_sheet_rows_schema_driven(
    request: Request,
    body: Dict[str, Any] = Body(...),
    mode: str = Query(default=""),
    include_matrix: Optional[bool] = Query(default=None),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    return await _run_advanced_sheet_rows_impl(
        request=request,
        body=body or {},
        mode=mode or "",
        include_matrix_q=include_matrix,
        token=token,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
        x_request_id=x_request_id,
    )


router.include_router(_root_sheet_rows_router)
router.include_router(schema_router_v1)
router.include_router(schema_router_compat)

__all__ = ["router", "ADVANCED_ANALYSIS_VERSION"]
