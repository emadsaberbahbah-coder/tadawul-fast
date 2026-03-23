#!/usr/bin/env python3
"""
routes/investment_advisor.py
================================================================================
TFB Advanced / Investment Advisor Routes — v7.0.0
================================================================================
CANONICAL ADVANCED/ADVISOR SHEET-ROWS • PAGE-CONSISTENCY GUARDED
NO BLIND ENGINE-SHEET ACCEPTANCE • OBJECT + MATRIX CONTRACT SAFE
TOP10 / INSIGHTS / DATA_DICTIONARY SPECIAL SAFE • AUTH-BRIDGED
ASYNC+SYNC TOLERANT • ENGINE-FIRST WITH VALIDATED FALLBACKS • JSON-SAFE

What this revision fixes
------------------------
- FIX: generic pages no longer accept any non-empty engine_sheet payload blindly.
- FIX: candidate payloads are now scored for page consistency, row density,
       schema fit, and special-page coverage before being returned.
- FIX: sparse fallback rows like repeated 2222.SR on multiple pages are rejected
       unless they actually fit the requested page.
- FIX: /v1/advanced/* and /v1/advisor/* stay on one shared implementation.
- FIX: stable payload includes both dict rows and matrix rows.
- FIX: schema_only / headers_only remain fast and safe.
================================================================================
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
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, Query, Request
from fastapi.responses import JSONResponse, Response

# -----------------------------------------------------------------------------
# JSON-safe response handling
# -----------------------------------------------------------------------------
def _json_safe(obj: Any) -> Any:
    if obj is None:
        return None
    if isinstance(obj, bool):
        return obj
    if isinstance(obj, (int, str)):
        return obj
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, Decimal):
        try:
            f = float(obj)
            return None if (math.isnan(f) or math.isinf(f)) else f
        except Exception:
            return str(obj)
    if isinstance(obj, (datetime, date, dt_time)):
        try:
            return obj.isoformat()
        except Exception:
            return str(obj)
    if isinstance(obj, bytes):
        try:
            return obj.decode("utf-8", errors="replace")
        except Exception:
            return str(obj)
    if isinstance(obj, Mapping):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_json_safe(v) for v in obj]
    try:
        if hasattr(obj, "model_dump") and callable(getattr(obj, "model_dump")):
            return _json_safe(obj.model_dump(mode="python"))
    except Exception:
        pass
    try:
        if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
            return _json_safe(obj.dict())
    except Exception:
        pass
    try:
        d = getattr(obj, "__dict__", None)
        if isinstance(d, dict):
            return _json_safe(d)
    except Exception:
        pass
    try:
        return str(obj)
    except Exception:
        return None


try:
    import orjson  # type: ignore

    class BestJSONResponse(Response):
        media_type = "application/json"

        def render(self, content: Any) -> bytes:
            return orjson.dumps(_json_safe(content), default=str)

    def _json_dumps(v: Any) -> str:
        return orjson.dumps(_json_safe(v), default=str).decode("utf-8")

except Exception:
    import json as _json_std

    class BestJSONResponse(JSONResponse):
        def render(self, content: Any) -> bytes:
            return _json_std.dumps(_json_safe(content), default=str, ensure_ascii=False).encode("utf-8")

    def _json_dumps(v: Any) -> str:
        return _json_std.dumps(_json_safe(v), default=str, ensure_ascii=False)


logger = logging.getLogger("routes.investment_advisor")
logger.addHandler(logging.NullHandler())

ROUTER_VERSION = "7.0.0"
MODULE_NAME = "routes.investment_advisor"

router = APIRouter(tags=["advisor"])
_router_canonical = APIRouter(prefix="/v1/advisor", tags=["advisor"])
_router_advanced = APIRouter(prefix="/v1/advanced", tags=["advanced"])
_router_compat_us = APIRouter(prefix="/v1/investment_advisor", tags=["investment_advisor"])
_router_compat_dash = APIRouter(prefix="/v1/investment-advisor", tags=["investment-advisor"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "active"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "inactive"}

_TOP10_PAGE = "Top_10_Investments"
_INSIGHTS_PAGE = "Insights_Analysis"
_DICTIONARY_PAGE = "Data_Dictionary"

TOP10_REQUIRED_FIELDS = ("top10_rank", "selection_reason", "criteria_snapshot")

_TOP10_KEYS_FALLBACK: List[str] = [
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

_TOP10_HEADERS_FALLBACK: List[str] = [
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

_INSIGHTS_KEYS_FALLBACK: List[str] = [
    "section",
    "item",
    "symbol",
    "metric",
    "value",
    "notes",
    "last_updated_riyadh",
]

_INSIGHTS_HEADERS_FALLBACK: List[str] = [
    "Section",
    "Item",
    "Symbol",
    "Metric",
    "Value",
    "Notes",
    "Last Updated Riyadh",
]

_DATA_DICTIONARY_KEYS_FALLBACK: List[str] = [
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

_DATA_DICTIONARY_HEADERS_FALLBACK: List[str] = [
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

_GENERIC_KEYS_FALLBACK: List[str] = [
    "symbol",
    "ticker",
    "name",
    "current_price",
    "recommendation",
    "recommendation_reason",
    "overall_score",
    "risk_bucket",
    "forecast_confidence",
    "horizon_days",
    "invest_period_label",
    "updated_at",
]

_GENERIC_HEADERS_FALLBACK: List[str] = [
    "Symbol",
    "Ticker",
    "Name",
    "Current Price",
    "Recommendation",
    "Recommendation Reason",
    "Overall Score",
    "Risk Bucket",
    "Forecast Confidence",
    "Horizon Days",
    "Invest Period Label",
    "Updated At",
]

_FIELD_ALIAS_HINTS: Dict[str, List[str]] = {
    "symbol": ["ticker", "code", "instrument", "security", "requested_symbol", "symbol_normalized"],
    "ticker": ["symbol", "code", "instrument", "security", "requested_symbol"],
    "name": ["company_name", "security_name", "instrument_name", "title"],
    "current_price": ["price", "last_price", "last", "close", "market_price", "current", "spot", "nav"],
    "recommendation_reason": ["reason", "reco_reason", "recommendation_notes", "selection_reason"],
    "horizon_days": ["invest_period_days", "investment_period_days", "period_days"],
    "invest_period_label": ["investment_period_label", "period_label", "horizon_label"],
    "top10_rank": ["rank", "top_rank"],
    "selection_reason": ["selection_notes", "selector_reason"],
    "criteria_snapshot": ["criteria", "criteria_json", "snapshot"],
    "updated_at": ["timestamp", "as_of", "updated", "last_updated", "last_updated_riyadh"],
}

# -----------------------------------------------------------------------------
# Optional engine / schema helpers
# -----------------------------------------------------------------------------
try:
    from core.data_engine_v2 import (  # type: ignore
        STATIC_CANONICAL_SHEET_CONTRACTS as _ENGINE_STATIC_CONTRACTS,
        get_engine as _engine_get_engine_v2,
        get_sheet_spec as _engine_get_sheet_spec,
    )
except Exception:
    _ENGINE_STATIC_CONTRACTS = {}
    _engine_get_engine_v2 = None  # type: ignore
    _engine_get_sheet_spec = None  # type: ignore

try:
    from core.sheets.schema_registry import get_sheet_spec as _registry_get_sheet_spec  # type: ignore
except Exception:
    _registry_get_sheet_spec = None  # type: ignore

try:
    from core.sheets.page_catalog import normalize_page_name as _catalog_normalize_page_name  # type: ignore
except Exception:
    _catalog_normalize_page_name = None  # type: ignore


# -----------------------------------------------------------------------------
# Metrics
# -----------------------------------------------------------------------------
@dataclass(slots=True)
class _AdvisorMetrics:
    started_at: float
    requests_total: int = 0
    success_total: int = 0
    unauthorized_total: int = 0
    errors_total: int = 0
    last_latency_ms: float = 0.0
    last_error: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "uptime_sec": round(max(0.0, time.time() - self.started_at), 3),
            "requests_total": self.requests_total,
            "success_total": self.success_total,
            "unauthorized_total": self.unauthorized_total,
            "errors_total": self.errors_total,
            "last_latency_ms": round(float(self.last_latency_ms), 3),
            "last_error": (self.last_error or "")[:800],
        }


_METRICS = _AdvisorMetrics(started_at=time.time())
_METRICS_LOCK = asyncio.Lock()
_ENGINE_INIT_LOCK = asyncio.Lock()


# -----------------------------------------------------------------------------
# Basic helpers
# -----------------------------------------------------------------------------
def _safe_import(module_name: str) -> Any:
    try:
        return __import__(module_name, fromlist=["*"])
    except Exception:
        return None


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _request_id(request: Request) -> str:
    try:
        rid = getattr(getattr(request, "state", None), "request_id", None)
        if rid:
            return str(rid)
    except Exception:
        pass
    try:
        hdr = request.headers.get("X-Request-ID")
        if hdr:
            return str(hdr).strip()
    except Exception:
        pass
    return str(uuid.uuid4())[:12]


def _boolish(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    s = str(value).strip().lower()
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default


def _clean_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        s = str(v).strip()
    except Exception:
        return ""
    if not s:
        return ""
    if s.lower() in {"none", "null", "nil", "undefined"}:
        return ""
    return s


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
    if isinstance(v, Iterable):
        try:
            return list(v)
        except Exception:
            return [v]
    return [v]


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(float(v))
    except Exception:
        return None


def _snake_like(text: str) -> str:
    s = str(text or "").strip().replace("%", " pct").replace("/", " ")
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
    res = "".join(out).strip("_")
    while "__" in res:
        res = res.replace("__", "_")
    return res


def _split_symbols(raw: str) -> List[str]:
    s = _clean_str(raw).replace("\n", " ").replace("\t", " ").replace(",", " ")
    parts = [p.strip() for p in s.split(" ") if p.strip()]
    out: List[str] = []
    seen = set()
    for p in parts:
        u = p.upper()
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _looks_like_symbol_token(x: Any) -> bool:
    s = _clean_str(x).upper()
    if not s or " " in s or len(s) > 24:
        return False
    return bool(re.fullmatch(r"[A-Z0-9\.\=\-\^:_/]{1,24}", s))


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
        d = getattr(obj, "__dict__", None)
        if isinstance(d, dict):
            return dict(d)
    except Exception:
        pass
    return {}


async def _maybe_await(v: Any) -> Any:
    return await v if inspect.isawaitable(v) else v


async def _record_metrics(ok: bool, unauthorized: bool, latency_ms: float, err: str = "") -> None:
    async with _METRICS_LOCK:
        _METRICS.requests_total += 1
        _METRICS.last_latency_ms = float(latency_ms)
        if unauthorized:
            _METRICS.unauthorized_total += 1
        elif ok:
            _METRICS.success_total += 1
        else:
            _METRICS.errors_total += 1
            _METRICS.last_error = (err or "")[:800]


def _response(
    *,
    status_code: int = 200,
    status: str,
    request_id: str,
    **content: Any,
) -> BestJSONResponse:
    payload = {
        "status": status,
        "module": MODULE_NAME,
        "router_version": ROUTER_VERSION,
        "request_id": request_id,
        "timestamp_utc": _now_utc(),
        **content,
    }
    return BestJSONResponse(status_code=status_code, content=_json_safe(payload))


def _error(
    status_code: int,
    request_id: str,
    message: str,
    *,
    extra: Optional[Dict[str, Any]] = None,
) -> BestJSONResponse:
    payload: Dict[str, Any] = {"error": message}
    if extra:
        payload["meta"] = extra
    return _response(status_code=status_code, status="error", request_id=request_id, **payload)


# -----------------------------------------------------------------------------
# Auth helpers
# -----------------------------------------------------------------------------
def _env_bool(name: str, default: bool = False) -> bool:
    try:
        return _boolish(os.getenv(name, str(default)), default)
    except Exception:
        return default


def _safe_is_open_mode() -> Optional[bool]:
    try:
        from core.config import is_open_mode  # type: ignore
        if callable(is_open_mode):
            return bool(is_open_mode())
    except Exception:
        pass
    return None


def _safe_allow_query_token() -> Optional[bool]:
    try:
        from core.config import get_settings_cached  # type: ignore
        st = get_settings_cached()
        if st is not None and hasattr(st, "allow_query_token"):
            return bool(getattr(st, "allow_query_token", False))
    except Exception:
        pass
    try:
        return _env_bool("ALLOW_QUERY_TOKEN", False)
    except Exception:
        return None


def _extract_token(
    *,
    request: Request,
    token_query: Optional[str] = None,
    x_app_token: Optional[str] = None,
    authorization: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str]]:
    authz = _clean_str(authorization or request.headers.get("Authorization"))
    token = _clean_str(
        x_app_token
        or request.headers.get("X-APP-TOKEN")
        or request.headers.get("X-App-Token")
        or request.headers.get("X-API-KEY")
        or request.headers.get("X-Api-Key")
        or request.headers.get("X-TFB-Token")
    )

    if not token and authz.lower().startswith("bearer "):
        try:
            token = _clean_str(authz.split(" ", 1)[1])
        except Exception:
            pass

    allow_q = bool(_safe_allow_query_token())
    if allow_q and not token:
        qt = _clean_str(token_query or request.query_params.get("token"))
        if qt:
            token = qt

    return (token or None), (authz or None)


def _auth_ok(
    request: Request,
    *,
    token_query: Optional[str] = None,
    x_app_token: Optional[str] = None,
    authorization: Optional[str] = None,
) -> bool:
    try:
        from core.config import is_open_mode, auth_ok  # type: ignore

        if callable(is_open_mode) and bool(is_open_mode()):
            return True

        token, authz = _extract_token(
            request=request,
            token_query=token_query,
            x_app_token=x_app_token,
            authorization=authorization,
        )

        if callable(auth_ok):
            attempts = [
                {
                    "token": token,
                    "authorization": authz,
                    "headers": dict(request.headers),
                    "request": request,
                    "path": str(getattr(getattr(request, "url", None), "path", "") or ""),
                },
                {
                    "token": token,
                    "authorization": authz,
                    "headers": dict(request.headers),
                    "path": str(getattr(getattr(request, "url", None), "path", "") or ""),
                },
                {"token": token, "authorization": authz, "headers": dict(request.headers)},
                {"token": token, "authorization": authz},
                {"token": token},
            ]
            for kwargs in attempts:
                try:
                    return bool(auth_ok(**kwargs))
                except TypeError:
                    continue
                except Exception:
                    return False
        return False
    except Exception:
        return not _env_bool("REQUIRE_AUTH", True)


# -----------------------------------------------------------------------------
# Page / schema helpers
# -----------------------------------------------------------------------------
def _normalize_page_name(raw: Optional[str]) -> str:
    s = _clean_str(raw)
    if not s:
        return _TOP10_PAGE

    if callable(_catalog_normalize_page_name):
        try:
            out = _catalog_normalize_page_name(s, allow_output_pages=True)
            if out:
                return str(out)
        except TypeError:
            try:
                out = _catalog_normalize_page_name(s)
                if out:
                    return str(out)
            except Exception:
                pass
        except Exception:
            pass

    compact = s.replace("-", "_").replace(" ", "_").lower()
    mapping = {
        "top_10_investments": _TOP10_PAGE,
        "top10_investments": _TOP10_PAGE,
        "top10": _TOP10_PAGE,
        "advanced": _TOP10_PAGE,
        "advisor": _TOP10_PAGE,
        "investment_advisor": _TOP10_PAGE,
        "insights_analysis": _INSIGHTS_PAGE,
        "insights": _INSIGHTS_PAGE,
        "market_leaders": "Market_Leaders",
        "global_markets": "Global_Markets",
        "commodities_fx": "Commodities_FX",
        "mutual_funds": "Mutual_Funds",
        "my_portfolio": "My_Portfolio",
        "my_investments": "My_Investments",
        "data_dictionary": _DICTIONARY_PAGE,
    }
    return mapping.get(compact, s.replace(" ", "_"))


def _route_family(page: str) -> str:
    p = _normalize_page_name(page)
    if p == _TOP10_PAGE:
        return "top10"
    if p == _INSIGHTS_PAGE:
        return "insights"
    if p == _DICTIONARY_PAGE:
        return "schema"
    return "advisor"


def _complete_schema_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    raw_headers = list(headers or [])
    raw_keys = list(keys or [])
    max_len = max(len(raw_headers), len(raw_keys))

    out_headers: List[str] = []
    out_keys: List[str] = []

    for i in range(max_len):
        h = _clean_str(raw_headers[i]) if i < len(raw_headers) else ""
        k = _clean_str(raw_keys[i]) if i < len(raw_keys) else ""

        if h and not k:
            k = _snake_like(h)
        elif k and not h:
            h = k.replace("_", " ").title()
        elif not h and not k:
            h = f"Column {i + 1}"
            k = f"column_{i + 1}"

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
    headers: List[str] = []
    keys: List[str] = []

    cols = _schema_columns_from_any(spec)
    for c in cols:
        if isinstance(c, Mapping):
            h = _clean_str(c.get("header") or c.get("display_header") or c.get("displayHeader") or c.get("label") or c.get("title"))
            k = _clean_str(c.get("key") or c.get("field") or c.get("name") or c.get("id"))
        else:
            h = _clean_str(
                getattr(
                    c,
                    "header",
                    getattr(c, "display_header", getattr(c, "displayHeader", getattr(c, "label", getattr(c, "title", None)))),
                )
            )
            k = _clean_str(getattr(c, "key", getattr(c, "field", getattr(c, "name", getattr(c, "id", None)))))

        if h or k:
            headers.append(h or k.replace("_", " ").title())
            keys.append(k or _snake_like(h))

    if not headers and not keys and isinstance(spec, Mapping):
        headers2 = spec.get("headers") or spec.get("display_headers")
        keys2 = spec.get("keys") or spec.get("columns") or spec.get("fields")
        if isinstance(headers2, list):
            headers = [_clean_str(x) for x in headers2 if _clean_str(x)]
        if isinstance(keys2, list):
            keys = [_clean_str(x) for x in keys2 if _clean_str(x)]

    return _complete_schema_contract(headers, keys)


def _ensure_top10_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    out_headers = list(headers or [])
    out_keys = list(keys or [])

    mapping = {
        "top10_rank": "Top10 Rank",
        "selection_reason": "Selection Reason",
        "criteria_snapshot": "Criteria Snapshot",
    }
    for field in TOP10_REQUIRED_FIELDS:
        if field not in out_keys:
            out_keys.append(field)
            out_headers.append(mapping[field])

    return _complete_schema_contract(out_headers, out_keys)


def _schema_for_page(page: str) -> Tuple[List[str], List[str]]:
    p = _normalize_page_name(page)

    static_contract = _ENGINE_STATIC_CONTRACTS.get(p) if isinstance(_ENGINE_STATIC_CONTRACTS, dict) else None
    if isinstance(static_contract, Mapping):
        headers, keys = _complete_schema_contract(static_contract.get("headers", []), static_contract.get("keys", []))
        if p == _TOP10_PAGE:
            headers, keys = _ensure_top10_contract(headers, keys)
        if headers and keys:
            return headers, keys

    if callable(_engine_get_sheet_spec):
        try:
            spec = _engine_get_sheet_spec(p)
            headers, keys = _schema_keys_headers_from_spec(spec)
            if p == _TOP10_PAGE:
                headers, keys = _ensure_top10_contract(headers, keys)
            if headers and keys:
                return headers, keys
        except Exception:
            pass

    if callable(_registry_get_sheet_spec):
        try:
            spec = _registry_get_sheet_spec(p)
            headers, keys = _schema_keys_headers_from_spec(spec)
            if p == _TOP10_PAGE:
                headers, keys = _ensure_top10_contract(headers, keys)
            if headers and keys:
                return headers, keys
        except Exception:
            pass

    if p == _TOP10_PAGE:
        return _ensure_top10_contract(list(_TOP10_HEADERS_FALLBACK), list(_TOP10_KEYS_FALLBACK))
    if p == _INSIGHTS_PAGE:
        return _complete_schema_contract(list(_INSIGHTS_HEADERS_FALLBACK), list(_INSIGHTS_KEYS_FALLBACK))
    if p == _DICTIONARY_PAGE:
        return _complete_schema_contract(list(_DATA_DICTIONARY_HEADERS_FALLBACK), list(_DATA_DICTIONARY_KEYS_FALLBACK))

    return _complete_schema_contract(list(_GENERIC_HEADERS_FALLBACK), list(_GENERIC_KEYS_FALLBACK))


# -----------------------------------------------------------------------------
# Payload extraction / normalization
# -----------------------------------------------------------------------------
def _rows_from_matrix(rows_matrix: Any, cols: Sequence[str]) -> List[Dict[str, Any]]:
    if not isinstance(rows_matrix, list) or not cols:
        return []
    keys = [_clean_str(c) for c in cols if _clean_str(c)]
    if not keys:
        return []
    out: List[Dict[str, Any]] = []
    for row in rows_matrix:
        if not isinstance(row, (list, tuple)):
            continue
        vals = list(row)
        out.append({keys[i]: (vals[i] if i < len(vals) else None) for i in range(len(keys))})
    return out


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
    if keyset & {"recommendation", "overall_score", "current_price"}:
        return True
    return False


def _extract_rows_like(payload: Any, depth: int = 0) -> List[Dict[str, Any]]:
    if payload is None or depth > 8:
        return []

    if isinstance(payload, list):
        if not payload:
            return []
        if all(isinstance(x, Mapping) for x in payload):
            return [dict(x) for x in payload if isinstance(x, Mapping)]
        model_rows = [_model_to_dict(x) for x in payload]
        model_rows = [r for r in model_rows if isinstance(r, dict) and r]
        if model_rows and all(_looks_like_explicit_row_dict(r) or _clean_str(r.get("symbol") or r.get("ticker")) for r in model_rows):
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
        if not isinstance(v, Mapping):
            maybe_symbol_map = False
            break
        if not _looks_like_symbol_token(k):
            maybe_symbol_map = False
            break
        symbol_like_keys += 1
        row = dict(v)
        if not row.get("symbol") and not row.get("ticker"):
            row["symbol"] = _clean_str(k)
            row["ticker"] = _clean_str(k)
        rows_from_symbol_map.append(row)
    if maybe_symbol_map and symbol_like_keys > 0 and rows_from_symbol_map:
        return rows_from_symbol_map

    keys_like = None
    for name in ("keys", "columns", "fields"):
        value = payload.get(name)
        if isinstance(value, list) and value:
            keys_like = [_clean_str(v) for v in value if _clean_str(v)]
            break
    if not keys_like:
        hdrs = payload.get("headers") or payload.get("display_headers") or payload.get("sheet_headers") or payload.get("column_headers")
        if isinstance(hdrs, list) and hdrs:
            keys_like = [_snake_like(h) for h in hdrs if _clean_str(h)]

    for key in ("row_objects", "records", "items", "rows", "data", "results", "quotes", "recommendations"):
        value = payload.get(key)

        if isinstance(value, list):
            rows = _extract_rows_like(value, depth + 1)
            if rows:
                return rows
            if value and any(isinstance(x, (list, tuple)) for x in value) and keys_like:
                rows = _rows_from_matrix(value, keys_like)
                if rows:
                    return rows

        if isinstance(value, Mapping):
            rows = _extract_rows_like(value, depth + 1)
            if rows:
                return rows

    rows_matrix = payload.get("rows_matrix") or payload.get("matrix") or payload.get("values")
    if isinstance(rows_matrix, list) and keys_like:
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
    status_out = _clean_str(payload.get("status")) or "success"
    error_out = payload.get("error") or payload.get("detail") or payload.get("message")
    meta_out = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    return status_out, (str(error_out) if error_out is not None else None), meta_out


def _payload_has_real_rows(payload: Any) -> bool:
    rows = _extract_rows_like(payload)
    if rows:
        return True
    mx = _extract_matrix_like(payload)
    return bool(mx)


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

    if isinstance(payload.get("headers"), list) or isinstance(payload.get("display_headers"), list):
        score += 8
    if isinstance(payload.get("keys"), list) or isinstance(payload.get("columns"), list) or isinstance(payload.get("fields"), list):
        score += 8
    if isinstance(payload.get("row_objects"), list) or isinstance(payload.get("records"), list):
        score += 8

    if page == _TOP10_PAGE and rows_like:
        for field in TOP10_REQUIRED_FIELDS:
            if any(isinstance(r, Mapping) and r.get(field) not in (None, "", [], {}) for r in rows_like):
                score += 10

    status_out, error_out, meta_in = _extract_status_error(payload)
    if _clean_str(status_out).lower() == "success":
        score += 4
    elif _clean_str(status_out).lower() == "partial":
        score += 2
    elif _clean_str(status_out).lower() == "warn":
        score += 1
    elif _clean_str(status_out).lower() == "error":
        score -= 3

    if isinstance(meta_in, dict) and meta_in.get("ok"):
        score += 3
    if _clean_str(error_out):
        score -= 6

    return score


def _key_variants(key: str) -> List[str]:
    k = _clean_str(key)
    if not k:
        return []

    variants = [
        k,
        k.lower(),
        k.upper(),
        k.replace("_", " "),
        k.replace("_", "").lower(),
    ]

    for alias in _FIELD_ALIAS_HINTS.get(k, []):
        variants.extend(
            [
                alias,
                alias.lower(),
                alias.upper(),
                alias.replace("_", " "),
                alias.replace("_", "").lower(),
            ]
        )

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


def _normalize_to_schema_keys(
    *,
    schema_keys: Sequence[str],
    schema_headers: Sequence[str],
    raw: Mapping[str, Any],
) -> Dict[str, Any]:
    raw_dict = dict(raw or {})
    header_by_key = {str(k): str(h) for k, h in zip(schema_keys, schema_headers)}
    out: Dict[str, Any] = {}

    for k in schema_keys:
        ks = str(k)
        v = _extract_from_raw(raw_dict, _key_variants(ks))
        if v is None:
            h = header_by_key.get(ks, "")
            if h:
                v = _extract_from_raw(raw_dict, [h, h.lower(), h.upper()])
        out[ks] = _json_safe(v)

    if "symbol" in out and not out.get("symbol"):
        out["symbol"] = _extract_from_raw(raw_dict, _key_variants("symbol"))
    if "ticker" in out and not out.get("ticker"):
        out["ticker"] = _extract_from_raw(raw_dict, _key_variants("ticker"))
    if "current_price" in out and out.get("current_price") is None:
        out["current_price"] = _extract_from_raw(raw_dict, _key_variants("current_price"))

    return out


# -----------------------------------------------------------------------------
# Recommendation / context enrichment
# -----------------------------------------------------------------------------
def _extract_nested_dict(payload: Dict[str, Any], key: str) -> Dict[str, Any]:
    value = payload.get(key)
    return dict(value) if isinstance(value, Mapping) else {}


def _as_fraction(v: Any) -> Optional[float]:
    f = _safe_float(v)
    if f is None:
        return None
    if abs(f) > 1.5:
        return f / 100.0
    return f


def _format_pct(v: Any) -> str:
    f = _as_fraction(v)
    if f is None:
        return ""
    return f"{round(f * 100.0, 2)}%"


def _horizon_label(days: Optional[int]) -> Optional[str]:
    if days is None:
        return None
    if days <= 45:
        return "1M"
    if days <= 135:
        return "3M"
    if days <= 240:
        return "6M"
    return "12M"


def _infer_horizon_days(payload: Dict[str, Any]) -> Optional[int]:
    criteria = _extract_nested_dict(payload, "criteria")
    for src in (payload, criteria):
        for key in ("horizon_days", "invest_period_days", "investment_period_days", "period_days", "days"):
            v = _safe_int(src.get(key))
            if v is not None and v > 0:
                return v

    label = _clean_str(
        payload.get("invest_period_label")
        or payload.get("investment_period_label")
        or payload.get("period_label")
        or payload.get("horizon_label")
        or criteria.get("invest_period_label")
        or criteria.get("investment_period_label")
        or criteria.get("period_label")
        or criteria.get("horizon_label")
    ).upper()

    label_map = {
        "1M": 30,
        "30D": 30,
        "3M": 90,
        "90D": 90,
        "6M": 180,
        "180D": 180,
        "12M": 365,
        "1Y": 365,
        "365D": 365,
    }
    if label in label_map:
        return label_map[label]
    return None


def _infer_invest_period_label(payload: Dict[str, Any], days: Optional[int]) -> Optional[str]:
    criteria = _extract_nested_dict(payload, "criteria")
    label = _clean_str(
        payload.get("invest_period_label")
        or payload.get("investment_period_label")
        or payload.get("period_label")
        or payload.get("horizon_label")
        or criteria.get("invest_period_label")
        or criteria.get("investment_period_label")
        or criteria.get("period_label")
        or criteria.get("horizon_label")
    ).upper()
    if label:
        return label
    return _horizon_label(days)


def _criteria_snapshot(payload: Dict[str, Any]) -> str:
    criteria = _extract_nested_dict(payload, "criteria")
    compact = {
        "page": _normalize_page_name(
            payload.get("page")
            or payload.get("sheet_name")
            or payload.get("sheet")
            or payload.get("name")
            or payload.get("tab")
            or _TOP10_PAGE
        ),
        "risk_profile": criteria.get("risk_profile") or payload.get("risk_profile"),
        "allocation_strategy": criteria.get("allocation_strategy") or payload.get("allocation_strategy"),
        "invest_amount": criteria.get("invest_amount") or payload.get("invest_amount"),
        "horizon_days": _infer_horizon_days(payload),
        "invest_period_label": _infer_invest_period_label(payload, _infer_horizon_days(payload)),
        "top_n": criteria.get("top_n") or payload.get("top_n"),
        "symbols": criteria.get("symbols") or payload.get("symbols") or payload.get("tickers"),
        "pages_selected": criteria.get("pages_selected"),
    }
    compact = {k: v for k, v in compact.items() if v not in (None, "", [], {})}
    try:
        return _json_dumps(compact)
    except Exception:
        return str(compact)


def _build_recommendation_reason(row: Dict[str, Any], payload: Dict[str, Any]) -> Optional[str]:
    rec = _clean_str(row.get("recommendation")).upper()
    if not rec:
        return None

    horizon_days = _safe_int(row.get("horizon_days"))
    if horizon_days is None:
        horizon_days = _infer_horizon_days(payload)

    invest_period_label = _clean_str(row.get("invest_period_label"))
    if not invest_period_label:
        invest_period_label = _infer_invest_period_label(payload, horizon_days) or ""

    current_price = _safe_float(row.get("current_price") or row.get("price"))
    fp1 = _safe_float(row.get("forecast_price_1m"))
    fp3 = _safe_float(row.get("forecast_price_3m"))
    fp12 = _safe_float(row.get("forecast_price_12m"))

    roi1 = row.get("expected_roi_1m")
    roi3 = row.get("expected_roi_3m")
    roi12 = row.get("expected_roi_12m")

    overall = _safe_float(row.get("overall_score"))
    confidence = row.get("forecast_confidence")
    if confidence is None:
        confidence = row.get("confidence_score")
    risk_bucket = _clean_str(row.get("risk_bucket"))

    selected_roi = roi3
    selected_fp = fp3
    if horizon_days is not None:
        if horizon_days <= 45:
            selected_roi = roi1 if roi1 is not None else roi3
            selected_fp = fp1 if fp1 is not None else fp3
        elif horizon_days <= 135:
            selected_roi = roi3 if roi3 is not None else roi12
            selected_fp = fp3 if fp3 is not None else fp12
        else:
            selected_roi = roi12 if roi12 is not None else roi3
            selected_fp = fp12 if fp12 is not None else fp3

    parts: List[str] = []
    roi_txt = _format_pct(selected_roi)
    conf_txt = _format_pct(confidence)

    if roi_txt:
        parts.append(f"expected {invest_period_label or 'target-horizon'} return is {roi_txt}")
    if selected_fp is not None and current_price is not None:
        if selected_fp > current_price:
            parts.append(f"forecast price {round(selected_fp, 2)} is above current price {round(current_price, 2)}")
        elif selected_fp < current_price:
            parts.append(f"forecast price {round(selected_fp, 2)} is below current price {round(current_price, 2)}")
    if conf_txt:
        parts.append(f"confidence is {conf_txt}")
    if overall is not None:
        parts.append(f"overall score is {round(overall, 2)}")
    if risk_bucket:
        parts.append(f"risk bucket is {risk_bucket}")

    parts = [p for p in parts if p]
    if not parts:
        return f"{rec} based on the current risk-return profile."
    return f"{rec} because " + ", ".join(parts[:4]) + "."


def _ensure_item_context(item: Dict[str, Any], payload: Dict[str, Any], rank: Optional[int] = None) -> Dict[str, Any]:
    row = dict(item or {})
    criteria = _extract_nested_dict(payload, "criteria")

    inferred_horizon_days = _infer_horizon_days(payload)
    inferred_label = _infer_invest_period_label(payload, inferred_horizon_days)

    if row.get("horizon_days") is None and inferred_horizon_days is not None:
        row["horizon_days"] = inferred_horizon_days

    if not row.get("invest_period_label") and inferred_label:
        row["invest_period_label"] = inferred_label

    if row.get("recommendation") and not _clean_str(row.get("recommendation_reason")):
        row["recommendation_reason"] = _build_recommendation_reason(row, payload)

    if rank is not None and row.get("top10_rank") is None:
        row["top10_rank"] = rank

    if not row.get("selection_reason") and row.get("recommendation_reason"):
        row["selection_reason"] = row.get("recommendation_reason")

    if not row.get("criteria_snapshot"):
        row["criteria_snapshot"] = _criteria_snapshot(payload)

    if not row.get("symbol"):
        syms = payload.get("symbols") or criteria.get("symbols") or payload.get("tickers") or criteria.get("tickers")
        if isinstance(syms, list) and len(syms) == 1:
            row["symbol"] = _clean_str(syms[0])

    return row


# -----------------------------------------------------------------------------
# Candidate normalization and scoring
# -----------------------------------------------------------------------------
def _normalize_any_result(
    *,
    page: str,
    raw: Any,
    payload: Dict[str, Any],
    source: str,
) -> Dict[str, Any]:
    page_norm = _normalize_page_name(page)
    schema_headers, schema_keys = _schema_for_page(page_norm)

    norm: Dict[str, Any] = {
        "headers": list(schema_headers),
        "display_headers": list(schema_headers),
        "sheet_headers": list(schema_headers),
        "column_headers": list(schema_headers),
        "keys": list(schema_keys),
        "columns": list(schema_keys),
        "fields": list(schema_keys),
        "row_objects": [],
        "records": [],
        "items": [],
        "rows": [],
        "rows_matrix": [],
        "data": [],
        "quotes": [],
        "meta": {
            "ok": False,
            "source": source,
            "router_version": ROUTER_VERSION,
        },
    }

    if raw is None:
        return norm

    rows = _extract_rows_like(raw)
    matrix = _extract_matrix_like(raw)
    status_out, error_out, meta_out = _extract_status_error(raw if isinstance(raw, Mapping) else {})

    if not rows and matrix:
        rows = _rows_from_matrix(matrix, schema_keys)

    if isinstance(raw, list) and not rows and raw and all(isinstance(r, (list, tuple)) for r in raw):
        rows = _rows_from_matrix(raw, schema_keys)

    fixed_items: List[Dict[str, Any]] = []
    for i, item in enumerate(rows, start=1):
        row_map = dict(item) if isinstance(item, Mapping) else {}
        row_map = _ensure_item_context(row_map, payload, rank=i if page_norm == _TOP10_PAGE else None)
        fixed_items.append(
            _normalize_to_schema_keys(
                schema_keys=schema_keys,
                schema_headers=schema_headers,
                raw=row_map,
            )
        )

    count = len(fixed_items)
    meta = dict(meta_out or {})
    meta["ok"] = bool(meta.get("ok", False)) or bool(count > 0 or status_out.lower() in {"success", "warn", "partial"})
    meta["count"] = count
    meta["row_object_count"] = count
    meta["source"] = source
    meta["router_version"] = ROUTER_VERSION
    if error_out and not meta.get("error"):
        meta["error"] = error_out

    rows_matrix = [[row.get(k) for k in schema_keys] for row in fixed_items]

    norm["row_objects"] = fixed_items
    norm["records"] = fixed_items
    norm["items"] = fixed_items
    norm["rows"] = rows_matrix
    norm["rows_matrix"] = rows_matrix
    norm["data"] = fixed_items
    norm["quotes"] = fixed_items
    norm["meta"] = meta
    return norm


def _row_fill_ratio(row: Mapping[str, Any]) -> float:
    keys = list(row.keys())
    if not keys:
        return 0.0
    meaningful = 0
    for v in row.values():
        if v not in (None, "", [], {}, ()):
            meaningful += 1
    return meaningful / max(1, len(keys))


def _avg_fill_ratio(rows: Sequence[Mapping[str, Any]], sample_size: int = 5) -> float:
    subset = list(rows[:sample_size])
    if not subset:
        return 0.0
    return sum(_row_fill_ratio(r) for r in subset) / max(1, len(subset))


def _extract_symbols_from_rows(rows: Sequence[Mapping[str, Any]], sample_size: int = 12) -> List[str]:
    out: List[str] = []
    for row in list(rows[:sample_size]):
        sym = _clean_str(row.get("symbol") or row.get("ticker") or row.get("code"))
        if sym:
            out.append(sym.upper())
    return out


def _requested_symbols(payload: Mapping[str, Any]) -> List[str]:
    for key in ("symbols", "tickers", "tickers_list"):
        value = payload.get(key)
        if isinstance(value, list):
            out = []
            seen = set()
            for item in value:
                s = _clean_str(item).upper()
                if s and s not in seen:
                    seen.add(s)
                    out.append(s)
            if out:
                return out
        if isinstance(value, str) and _clean_str(value):
            return _split_symbols(value)
    return []


def _raw_page_matches(page: str, raw: Any) -> bool:
    if not isinstance(raw, Mapping):
        return False

    for key in ("page", "sheet", "sheet_name", "name", "tab"):
        value = raw.get(key)
        if _clean_str(value) and _normalize_page_name(value) == _normalize_page_name(page):
            return True

    for key in ("meta", "payload", "result", "response", "output", "data"):
        value = raw.get(key)
        if isinstance(value, Mapping) and _raw_page_matches(page, value):
            return True

    return False


def _generic_page_symbol_score(page: str, rows: Sequence[Mapping[str, Any]], payload: Mapping[str, Any]) -> int:
    syms = _extract_symbols_from_rows(rows)
    if not syms:
        return -8

    page_norm = _normalize_page_name(page)
    requested = _requested_symbols(payload)

    def _ratio(pred) -> float:
        return sum(1 for s in syms if pred(s)) / max(1, len(syms))

    sr_ratio = _ratio(lambda s: s.endswith(".SR") or bool(re.fullmatch(r"\d{4,5}\.SR", s)))
    commodity_fx_ratio = _ratio(lambda s: ("=F" in s) or ("/" in s) or ("USD" in s and "SAR" in s) or ("EUR" in s and "SAR" in s) or ("JPY" in s and "USD" in s))
    alpha_ratio = _ratio(lambda s: bool(re.fullmatch(r"[A-Z\.\-]{1,12}", s)))

    score = 0

    if requested:
        overlap = len(set(syms) & set(requested))
        if overlap > 0:
            score += 14
        else:
            score -= 8

    if page_norm == "Market_Leaders":
        if sr_ratio >= 0.5:
            score += 10
        elif sr_ratio <= 0.2 and alpha_ratio >= 0.5:
            score -= 12

    elif page_norm == "Global_Markets":
        if sr_ratio >= 0.5:
            score -= 18
        elif alpha_ratio >= 0.4:
            score += 8

    elif page_norm == "Commodities_FX":
        if commodity_fx_ratio >= 0.3:
            score += 12
        elif sr_ratio >= 0.5:
            score -= 18

    elif page_norm in {"My_Portfolio", "My_Investments"}:
        if requested:
            pass
        else:
            score += 2

    elif page_norm == "Mutual_Funds":
        # Funds may legitimately look like ETF/fund tickers, so keep this light.
        score += 2

    return score


def _candidate_score(page: str, payload: Mapping[str, Any], raw: Any, normalized: Mapping[str, Any]) -> int:
    page_norm = _normalize_page_name(page)
    base = _payload_quality_score(raw, page=page_norm)

    rows = normalized.get("row_objects") if isinstance(normalized.get("row_objects"), list) else []
    meta = normalized.get("meta") if isinstance(normalized.get("meta"), Mapping) else {}

    score = base

    if _raw_page_matches(page_norm, raw):
        score += 16

    fill_ratio = _avg_fill_ratio(rows)
    if rows:
        if fill_ratio < 0.03:
            score -= 60
        elif fill_ratio < 0.05:
            score -= 28
        elif fill_ratio < 0.08:
            score -= 10
        else:
            score += 4
    else:
        score -= 20

    if page_norm == _TOP10_PAGE:
        coverage = 0
        for field in TOP10_REQUIRED_FIELDS:
            if any(isinstance(r, Mapping) and r.get(field) not in (None, "", [], {}) for r in rows):
                coverage += 1
        score += coverage * 12
        if coverage < 2 and rows:
            score -= 18

    elif page_norm == _INSIGHTS_PAGE:
        insight_hits = 0
        for row in rows[:5]:
            if _clean_str(row.get("section")) or _clean_str(row.get("item")) or _clean_str(row.get("metric")):
                insight_hits += 1
        score += insight_hits * 5
        if rows and insight_hits == 0:
            score -= 25

    elif page_norm == _DICTIONARY_PAGE:
        dict_hits = 0
        for row in rows[:5]:
            if _clean_str(row.get("sheet")) and _clean_str(row.get("header")) and _clean_str(row.get("key")):
                dict_hits += 1
        score += dict_hits * 6
        if rows and dict_hits == 0:
            score -= 25

    else:
        score += _generic_page_symbol_score(page_norm, rows, payload)

    if bool(meta.get("ok", False)):
        score += 3
    if _clean_str(meta.get("error")):
        score -= 6

    return score


def _candidate_is_usable(page: str, payload: Mapping[str, Any], raw: Any, normalized: Mapping[str, Any]) -> bool:
    rows = normalized.get("row_objects") if isinstance(normalized.get("row_objects"), list) else []
    if not rows:
        return False

    score = _candidate_score(page, payload, raw, normalized)
    page_norm = _normalize_page_name(page)

    if page_norm == _TOP10_PAGE:
        return score >= 120
    if page_norm in {_INSIGHTS_PAGE, _DICTIONARY_PAGE}:
        return score >= 85
    return score >= 70


def _best_empty_contract(page: str, source: str, reason: str) -> Dict[str, Any]:
    headers, keys = _schema_for_page(page)
    return {
        "headers": headers,
        "display_headers": headers,
        "sheet_headers": headers,
        "column_headers": headers,
        "keys": keys,
        "columns": keys,
        "fields": keys,
        "row_objects": [],
        "records": [],
        "items": [],
        "rows": [],
        "rows_matrix": [],
        "data": [],
        "quotes": [],
        "meta": {
            "ok": False,
            "source": source,
            "router_version": ROUTER_VERSION,
            "error": reason,
        },
    }


# -----------------------------------------------------------------------------
# Engine and builder helpers
# -----------------------------------------------------------------------------
async def _get_engine(request: Request) -> Tuple[Optional[Any], str, Optional[str]]:
    state = getattr(getattr(request, "app", None), "state", None)
    for attr in ("engine", "data_engine", "quote_engine", "cache_engine"):
        try:
            if state is not None:
                value = getattr(state, attr, None)
                if value is not None:
                    return value, f"app.state.{attr}", None
        except Exception:
            pass

    async with _ENGINE_INIT_LOCK:
        state = getattr(getattr(request, "app", None), "state", None)
        for attr in ("engine", "data_engine", "quote_engine", "cache_engine"):
            try:
                if state is not None:
                    value = getattr(state, attr, None)
                    if value is not None:
                        return value, f"app.state.{attr}", None
            except Exception:
                pass

        last_err = None
        for mod_name, fn_name in (
            ("core.data_engine_v2", "get_engine"),
            ("core.data_engine_v2", "peek_engine"),
            ("core.data_engine_v2", "get_engine_if_ready"),
            ("core.data_engine", "get_engine"),
        ):
            try:
                mod = _safe_import(mod_name)
                fn = getattr(mod, fn_name, None) if mod is not None else None
                if callable(fn):
                    eng = fn()
                    eng = await _maybe_await(eng)
                    if eng is not None:
                        try:
                            request.app.state.engine = eng
                        except Exception:
                            pass
                        return eng, f"{mod_name}.{fn_name}", None
            except Exception as e:
                last_err = f"{mod_name}.{fn_name}: {type(e).__name__}: {e}"

        return None, "engine_init_failed", last_err


def _timeout_sec() -> float:
    try:
        raw = float(os.getenv("ADVISOR_ROUTE_TIMEOUT_SEC", "75") or "75")
    except Exception:
        raw = 75.0
    return max(5.0, min(180.0, raw))


async def _call_with_timeout(fn: Any, timeout_sec: float, *args: Any, **kwargs: Any) -> Any:
    async def _runner():
        result = fn(*args, **kwargs)
        return await _maybe_await(result)

    return await asyncio.wait_for(_runner(), timeout=timeout_sec)


def _extract_headers_like(payload: Any, depth: int = 0) -> List[str]:
    if depth > 8 or not isinstance(payload, Mapping):
        return []

    for name in ("display_headers", "sheet_headers", "column_headers", "headers"):
        value = payload.get(name)
        if isinstance(value, list):
            out = [_clean_str(x) for x in value if _clean_str(x)]
            if out:
                return out

    columns = payload.get("columns")
    if isinstance(columns, list):
        out = [_clean_str(x) for x in columns if _clean_str(x)]
        if out:
            return out

    for key in ("payload", "result", "response", "output", "data"):
        nested = payload.get(key)
        if isinstance(nested, Mapping):
            found = _extract_headers_like(nested, depth + 1)
            if found:
                return found

    return []


def _extract_keys_like(payload: Any, depth: int = 0) -> List[str]:
    if depth > 8 or not isinstance(payload, Mapping):
        return []

    for name in ("keys", "fields", "column_keys", "schema_keys", "columns"):
        value = payload.get(name)
        if isinstance(value, list):
            out = [_clean_str(x) for x in value if _clean_str(x)]
            if out:
                return out

    for key in ("payload", "result", "response", "output", "data"):
        nested = payload.get(key)
        if isinstance(nested, Mapping):
            found = _extract_keys_like(nested, depth + 1)
            if found:
                return found

    return []


def _derive_contract_from_payload(payload: Any) -> Tuple[List[str], List[str]]:
    headers = _extract_headers_like(payload)
    keys = _extract_keys_like(payload)

    if not keys and headers:
        keys = [_snake_like(h) for h in headers if _clean_str(h)]
    if not headers and keys:
        headers = [k.replace("_", " ").title() for k in keys]

    return _complete_schema_contract(headers, keys)


async def _call_candidate(fn: Any, page: str, payload: Dict[str, Any], engine: Any, request: Request, timeout_sec: float) -> Tuple[bool, Any, str]:
    call_specs: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = [
        ((), {"page": page, "payload": payload, "engine": engine, "request": request}),
        ((), {"sheet": page, "payload": payload, "engine": engine, "request": request}),
        ((), {"page": page, "body": payload, "engine": engine, "request": request}),
        ((), {"sheet_name": page, "body": payload, "engine": engine, "request": request}),
        ((payload,), {}),
        ((page, payload), {}),
        ((page,), {}),
        ((), {}),
    ]

    last_err = ""
    for args, kwargs in call_specs:
        try:
            raw = await _call_with_timeout(fn, timeout_sec, *args, **kwargs)
            return True, raw, ""
        except TypeError as e:
            last_err = f"{type(e).__name__}: {e}"
            continue
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            continue

    return False, None, last_err or "no_compatible_call_signature"


async def _run_engine_sheet_mode(
    *,
    page: str,
    payload: Dict[str, Any],
    request: Request,
    engine: Any,
    timeout_sec: float,
) -> Dict[str, Any]:
    candidates: List[Tuple[str, Any]] = []

    if engine is not None:
        for name in (
            "get_sheet_rows",
            "sheet_rows",
            "build_sheet_rows",
            "execute_sheet_rows",
            "run_sheet_rows",
            "get_page_rows",
            "get_rows_for_sheet",
            "get_rows_for_page",
        ):
            fn = getattr(engine, name, None)
            if callable(fn):
                candidates.append((f"engine.{name}", fn))

    for mod_name, fn_names in (
        ("core.data_engine_v2", ("get_sheet_rows", "sheet_rows", "build_sheet_rows", "get_page_rows")),
        ("core.data_engine", ("get_sheet_rows", "sheet_rows", "build_sheet_rows", "get_page_rows")),
    ):
        mod = _safe_import(mod_name)
        if mod is None:
            continue
        for fn_name in fn_names:
            fn = getattr(mod, fn_name, None)
            if callable(fn):
                candidates.append((f"{mod_name}.{fn_name}", fn))

    best_score = -9999
    best_norm: Optional[Dict[str, Any]] = None
    best_reason = ""

    for source, fn in candidates:
        ok, raw, err = await _call_candidate(fn, page, payload, engine, request, timeout_sec)
        if not ok:
            best_reason = err or best_reason
            continue

        norm = _normalize_any_result(page=page, raw=raw, payload=payload, source=source)
        score = _candidate_score(page, payload, raw, norm)
        if score > best_score:
            best_score = score
            best_norm = norm
            best_reason = ""

    if best_norm is not None:
        meta = dict(best_norm.get("meta") or {})
        meta["candidate_score"] = best_score
        best_norm["meta"] = meta
        return best_norm

    return _best_empty_contract(page, "engine_sheet", best_reason or "no_engine_sheet_candidate")


def _resolve_generic_advisor_callables(request: Request, engine: Any) -> List[Tuple[str, Any]]:
    candidates: List[Tuple[str, Any]] = []
    seen = set()

    def _add(source: str, fn: Any) -> None:
        if not callable(fn):
            return
        ident = id(fn)
        if ident in seen:
            return
        seen.add(ident)
        candidates.append((source, fn))

    state = getattr(getattr(request, "app", None), "state", None)
    for name in (
        "advisor_runner",
        "investment_advisor_runner",
        "advanced_builder",
        "advisor_builder",
        "investment_advisor_builder",
    ):
        try:
            obj = getattr(state, name, None)
            _add(f"app.state.{name}", obj)
            for method in ("run", "build", "build_rows", "build_sheet_rows", "get_rows", "recommendations"):
                fn = getattr(obj, method, None) if obj is not None else None
                _add(f"app.state.{name}.{method}", fn)
        except Exception:
            pass

    for mod_name, fn_names in (
        ("core.investment_advisor_engine", ("run_investment_advisor_engine", "run_investment_advisor", "build_sheet_rows", "get_sheet_rows")),
        ("core.investment_advisor", ("run_investment_advisor", "build_sheet_rows", "get_sheet_rows")),
        ("routes.enriched_quote", ("build_sheet_rows", "get_sheet_rows", "build_page_rows")),
        ("core.enriched_quote", ("build_sheet_rows", "get_sheet_rows", "build_page_rows")),
    ):
        mod = _safe_import(mod_name)
        if mod is None:
            continue
        for fn_name in fn_names:
            _add(f"{mod_name}.{fn_name}", getattr(mod, fn_name, None))

    if engine is not None:
        for method in ("build_sheet_rows", "get_sheet_rows", "sheet_rows", "get_page_rows", "run_investment_advisor_engine"):
            _add(f"engine.{method}", getattr(engine, method, None))

    return candidates


async def _run_generic_advisor(
    *,
    page: str,
    payload: Dict[str, Any],
    request: Request,
    engine: Any,
    timeout_sec: float,
) -> Dict[str, Any]:
    candidates = _resolve_generic_advisor_callables(request, engine)

    best_score = -9999
    best_norm: Optional[Dict[str, Any]] = None
    errors: List[str] = []

    for source, fn in candidates:
        ok, raw, err = await _call_candidate(fn, page, payload, engine, request, timeout_sec)
        if not ok:
            if err:
                errors.append(f"{source}: {err}")
            continue

        norm = _normalize_any_result(page=page, raw=raw, payload=payload, source=source)
        score = _candidate_score(page, payload, raw, norm)
        if score > best_score:
            best_score = score
            best_norm = norm

    if best_norm is not None:
        meta = dict(best_norm.get("meta") or {})
        meta["candidate_score"] = best_score
        if errors and not meta.get("error"):
            meta["error"] = " | ".join(errors)[:3000]
        best_norm["meta"] = meta
        return best_norm

    return _best_empty_contract(page, "generic_advisor", " | ".join(errors)[:3000] if errors else "no_generic_advisor_candidate")


async def _run_special_page_builder(
    *,
    page: str,
    payload: Dict[str, Any],
    request: Request,
    engine: Any,
    timeout_sec: float,
) -> Dict[str, Any]:
    page_norm = _normalize_page_name(page)

    if page_norm == _DICTIONARY_PAGE:
        rows: List[Dict[str, Any]] = []
        mod = _safe_import("core.sheets.data_dictionary")
        if mod is not None:
            fn = getattr(mod, "build_data_dictionary_rows", None)
            if callable(fn):
                try:
                    raw = await _call_with_timeout(fn, timeout_sec, include_meta_sheet=True)
                except TypeError:
                    raw = await _call_with_timeout(fn, timeout_sec)
                except Exception:
                    raw = []
                for item in _as_list(raw):
                    d = item if isinstance(item, Mapping) else _model_to_dict(item)
                    if isinstance(d, dict) and d:
                        rows.append(d)

        raw_payload = {"rows": rows, "status": "success" if rows else "partial"}
        norm = _normalize_any_result(page=page_norm, raw=raw_payload, payload=payload, source="core.sheets.data_dictionary")
        meta = dict(norm.get("meta") or {})
        meta["candidate_score"] = _candidate_score(page_norm, payload, raw_payload, norm)
        norm["meta"] = meta
        return norm

    module_candidates: Dict[str, Tuple[str, ...]] = {
        _TOP10_PAGE: (
            "core.analysis.top10_selector",
            "routes.top10_investments",
        ),
        _INSIGHTS_PAGE: (
            "core.analysis.insights_builder",
            "routes.advanced_analysis",
            "routes.ai_analysis",
        ),
    }

    fn_candidates: Dict[str, Tuple[str, ...]] = {
        _TOP10_PAGE: (
            "build_top10_investments_rows",
            "build_top10_rows",
            "build_top10_output_rows",
            "select_top10_rows",
            "get_top10_rows",
            "build_rows",
        ),
        _INSIGHTS_PAGE: (
            "build_insights_analysis_rows",
            "build_insights_rows",
            "build_insights_output_rows",
            "build_insights_analysis",
            "get_insights_rows",
            "build_rows",
        ),
    }

    best_score = -9999
    best_norm: Optional[Dict[str, Any]] = None
    errors: List[str] = []

    for mod_name in module_candidates.get(page_norm, ()):
        mod = _safe_import(mod_name)
        if mod is None:
            continue

        for fn_name in fn_candidates.get(page_norm, ()):
            fn = getattr(mod, fn_name, None)
            if not callable(fn):
                continue

            ok, raw, err = await _call_candidate(fn, page_norm, payload, engine, request, timeout_sec)
            if not ok:
                if err:
                    errors.append(f"{mod_name}.{fn_name}: {err}")
                continue

            norm = _normalize_any_result(page=page_norm, raw=raw, payload=payload, source=f"{mod_name}.{fn_name}")
            score = _candidate_score(page_norm, payload, raw, norm)
            if score > best_score:
                best_score = score
                best_norm = norm

    if best_norm is not None:
        meta = dict(best_norm.get("meta") or {})
        meta["candidate_score"] = best_score
        if errors and not meta.get("error"):
            meta["error"] = " | ".join(errors)[:3000]
        best_norm["meta"] = meta
        return best_norm

    return _best_empty_contract(page_norm, "special_builder", " | ".join(errors)[:3000] if errors else "no_special_builder_available")


async def _run_page_pipeline(
    *,
    page: str,
    payload: Dict[str, Any],
    request: Request,
    engine: Any,
    timeout_sec: float,
    prefer_engine_sheet: bool,
) -> Dict[str, Any]:
    page_norm = _normalize_page_name(page)

    # Special pages: evaluate all candidates and choose the best valid one.
    if page_norm in {_TOP10_PAGE, _INSIGHTS_PAGE, _DICTIONARY_PAGE}:
        candidates: List[Tuple[str, Dict[str, Any], int, bool]] = []

        special = await _run_special_page_builder(
            page=page_norm,
            payload=payload,
            request=request,
            engine=engine,
            timeout_sec=timeout_sec,
        )
        special_raw = {"rows": special.get("row_objects"), "meta": special.get("meta"), "headers": special.get("headers"), "keys": special.get("keys")}
        special_score = _candidate_score(page_norm, payload, special_raw, special)
        candidates.append(("special_builder", special, special_score, _candidate_is_usable(page_norm, payload, special_raw, special)))

        engine_sheet = await _run_engine_sheet_mode(
            page=page_norm,
            payload=payload,
            request=request,
            engine=engine,
            timeout_sec=timeout_sec,
        )
        engine_raw = {"rows": engine_sheet.get("row_objects"), "meta": engine_sheet.get("meta"), "headers": engine_sheet.get("headers"), "keys": engine_sheet.get("keys")}
        engine_score = _candidate_score(page_norm, payload, engine_raw, engine_sheet)
        candidates.append(("engine_sheet", engine_sheet, engine_score, _candidate_is_usable(page_norm, payload, engine_raw, engine_sheet)))

        generic = await _run_generic_advisor(
            page=page_norm,
            payload=payload,
            request=request,
            engine=engine,
            timeout_sec=timeout_sec,
        )
        generic_raw = {"rows": generic.get("row_objects"), "meta": generic.get("meta"), "headers": generic.get("headers"), "keys": generic.get("keys")}
        generic_score = _candidate_score(page_norm, payload, generic_raw, generic)
        candidates.append(("generic_advisor", generic, generic_score, _candidate_is_usable(page_norm, payload, generic_raw, generic)))

        candidates = sorted(candidates, key=lambda x: x[2], reverse=True)
        best_name, best_res, best_score, best_usable = candidates[0]

        meta = dict(best_res.get("meta") or {})
        meta["dispatch"] = best_name
        meta["candidate_score"] = best_score
        meta["candidate_scores"] = {name: score for name, _, score, _ in candidates}
        best_res["meta"] = meta

        if best_usable or _result_count(best_res) > 0:
            return best_res

        return _best_empty_contract(page_norm, best_name, "best candidate failed page-consistency guard")

    # Generic pages: do not blindly accept engine_sheet.
    engine_sheet = await _run_engine_sheet_mode(
        page=page_norm,
        payload=payload,
        request=request,
        engine=engine,
        timeout_sec=timeout_sec,
    )
    engine_raw = {"rows": engine_sheet.get("row_objects"), "meta": engine_sheet.get("meta"), "headers": engine_sheet.get("headers"), "keys": engine_sheet.get("keys")}
    engine_score = _candidate_score(page_norm, payload, engine_raw, engine_sheet)
    engine_ok = _candidate_is_usable(page_norm, payload, engine_raw, engine_sheet)

    generic = await _run_generic_advisor(
        page=page_norm,
        payload=payload,
        request=request,
        engine=engine,
        timeout_sec=timeout_sec,
    )
    generic_raw = {"rows": generic.get("row_objects"), "meta": generic.get("meta"), "headers": generic.get("headers"), "keys": generic.get("keys")}
    generic_score = _candidate_score(page_norm, payload, generic_raw, generic)
    generic_ok = _candidate_is_usable(page_norm, payload, generic_raw, generic)

    candidates = [
        ("engine_sheet", engine_sheet, engine_score, engine_ok),
        ("generic_advisor", generic, generic_score, generic_ok),
    ]
    if not prefer_engine_sheet:
        candidates = sorted(candidates, key=lambda x: x[2], reverse=True)
    else:
        candidates = sorted(candidates, key=lambda x: (1 if x[0] == "engine_sheet" else 0, x[2]), reverse=True)

    usable = [c for c in candidates if c[3]]
    ranked = sorted(candidates, key=lambda x: x[2], reverse=True)

    if usable:
        best_name, best_res, best_score, _ = sorted(usable, key=lambda x: x[2], reverse=True)[0]
        meta = dict(best_res.get("meta") or {})
        meta["dispatch"] = best_name
        meta["candidate_score"] = best_score
        meta["candidate_scores"] = {name: score for name, _, score, _ in ranked}
        best_res["meta"] = meta
        return best_res

    # Nothing usable: return schema-shaped partial rather than wrong rows.
    best_name, _, _, _ = ranked[0]
    return _best_empty_contract(page_norm, best_name, "all candidates failed page-consistency guard")


def _result_count(result: Mapping[str, Any]) -> int:
    if not isinstance(result, Mapping):
        return 0
    for key in ("row_objects", "records", "items", "rows", "data", "quotes", "rows_matrix"):
        value = result.get(key)
        if isinstance(value, list):
            return len(value)
    return 0


def _slice_rows(rows: Sequence[Mapping[str, Any]], limit: int, offset: int) -> List[Dict[str, Any]]:
    start = max(0, int(offset))
    if limit <= 0:
        return [dict(r) for r in rows[start:]]
    end = start + max(0, int(limit))
    return [dict(r) for r in rows[start:end]]


def _payload(
    *,
    page: str,
    route_family: str,
    headers: Sequence[str],
    keys: Sequence[str],
    rows: Sequence[Mapping[str, Any]],
    include_matrix: bool,
    request_id: str,
    started_at: float,
    status_out: str = "success",
    error_out: Optional[str] = None,
    meta_extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    rows_list = [dict(r) for r in rows]
    hdrs = list(headers)
    ks = list(keys)

    meta = {
        "duration_ms": round((time.time() - started_at) * 1000.0, 3),
        "count": len(rows_list),
        "row_object_count": len(rows_list),
    }
    if meta_extra:
        meta.update(meta_extra)

    rows_matrix = [[row.get(k) for k in ks] for row in rows_list] if include_matrix else []

    return _json_safe(
        {
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
            "row_objects": rows_list,
            "records": rows_list,
            "items": rows_list,
            "rows": rows_matrix if include_matrix else [],
            "rows_matrix": rows_matrix,
            "data": rows_list,
            "quotes": rows_list,
            "count": len(rows_list),
            "detail": error_out or "",
            "version": ROUTER_VERSION,
            "request_id": request_id,
            "meta": meta,
        }
    )


def _normalize_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(payload or {})
    chosen_page = (
        out.get("page")
        or out.get("sheet_name")
        or out.get("sheet")
        or out.get("name")
        or out.get("tab")
        or _TOP10_PAGE
    )
    chosen_page = _normalize_page_name(chosen_page)

    out["page"] = chosen_page
    out["sheet_name"] = chosen_page
    if "sheet" not in out or not _clean_str(out.get("sheet")):
        out["sheet"] = chosen_page

    symbol_list: List[str] = []
    for key in ("symbols", "tickers", "symbol", "ticker"):
        value = out.get(key)
        if isinstance(value, list):
            for item in value:
                s = _clean_str(item)
                if s:
                    symbol_list.append(s.upper())
        elif isinstance(value, str):
            symbol_list.extend(_split_symbols(value))

    seen = set()
    normalized_syms = []
    for s in symbol_list:
        if s not in seen:
            seen.add(s)
            normalized_syms.append(s)
    if normalized_syms:
        out["symbols"] = normalized_syms

    out["limit"] = max(1, min(5000, _safe_int(out.get("limit")) or 250))
    out["offset"] = max(0, _safe_int(out.get("offset")) or 0)
    out["top_n"] = max(1, min(5000, _safe_int(out.get("top_n")) or out["limit"]))

    return out


def _build_schema_only_payload(
    *,
    page: str,
    request_id: str,
    include_headers: bool,
    include_matrix: bool,
    schema_only: bool,
    headers_only: bool,
) -> Dict[str, Any]:
    page_norm = _normalize_page_name(page)
    headers, keys = _schema_for_page(page_norm)

    return {
        "status": "success",
        "page": page_norm,
        "sheet": page_norm,
        "sheet_name": page_norm,
        "route_family": _route_family(page_norm),
        "headers": headers if include_headers else [],
        "display_headers": headers if include_headers else [],
        "sheet_headers": headers if include_headers else [],
        "column_headers": headers if include_headers else [],
        "keys": keys,
        "columns": keys,
        "fields": keys,
        "row_objects": [],
        "records": [],
        "items": [],
        "rows": [],
        "rows_matrix": [] if include_matrix else [],
        "data": [],
        "quotes": [],
        "count": 0,
        "detail": "",
        "version": ROUTER_VERSION,
        "request_id": request_id,
        "meta": {
            "ok": True,
            "router_version": ROUTER_VERSION,
            "source": "schema_only",
            "count": 0,
            "row_object_count": 0,
            "sheet_mode": True,
            "page": page_norm,
            "route_family": _route_family(page_norm),
            "schema_only": bool(schema_only),
            "headers_only": bool(headers_only),
        },
    }


# -----------------------------------------------------------------------------
# Core request implementation
# -----------------------------------------------------------------------------
async def _sheet_rows_impl(
    *,
    request: Request,
    payload: Dict[str, Any],
    token: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
) -> BestJSONResponse:
    started = time.time()
    request_id = _request_id(request)

    if not _auth_ok(request, token_query=token, x_app_token=x_app_token, authorization=authorization):
        latency = (time.time() - started) * 1000.0
        await _record_metrics(ok=False, unauthorized=True, latency_ms=latency, err="unauthorized")
        return _error(401, request_id, "unauthorized")

    try:
        body = _normalize_payload(payload)
        page = _normalize_page_name(body.get("page"))
        include_headers = _boolish(body.get("include_headers"), True)
        include_matrix = _boolish(body.get("include_matrix"), True)
        schema_only = _boolish(body.get("schema_only"), False)
        headers_only = _boolish(body.get("headers_only"), False)
        prefer_engine_sheet = _boolish(body.get("prefer_engine_sheet"), True)
        limit = max(1, min(5000, _safe_int(body.get("limit")) or 250))
        offset = max(0, _safe_int(body.get("offset")) or 0)

        if schema_only or headers_only:
            result = _build_schema_only_payload(
                page=page,
                request_id=request_id,
                include_headers=include_headers,
                include_matrix=include_matrix,
                schema_only=schema_only,
                headers_only=headers_only,
            )
            latency = (time.time() - started) * 1000.0
            await _record_metrics(ok=True, unauthorized=False, latency_ms=latency)
            return _response(status="success", request_id=request_id, **result)

        engine, engine_source, engine_error = await _get_engine(request)

        candidate = await _run_page_pipeline(
            page=page,
            payload=body,
            request=request,
            engine=engine,
            timeout_sec=_timeout_sec(),
            prefer_engine_sheet=prefer_engine_sheet,
        )

        headers = candidate.get("headers") if isinstance(candidate.get("headers"), list) else []
        keys = candidate.get("keys") if isinstance(candidate.get("keys"), list) else []
        if not headers or not keys:
            headers, keys = _schema_for_page(page)

        rows = candidate.get("row_objects") if isinstance(candidate.get("row_objects"), list) else []
        rows = _slice_rows(rows, limit=limit, offset=offset)

        meta = dict(candidate.get("meta") or {})
        meta.update(
            {
                "engine_source": engine_source,
                "engine_error": engine_error,
                "include_headers": include_headers,
                "include_matrix": include_matrix,
                "prefer_engine_sheet": prefer_engine_sheet,
                "limit": limit,
                "offset": offset,
            }
        )

        status_out = "success" if rows else "partial"
        error_out = _clean_str(meta.get("error")) or None

        result = _payload(
            page=page,
            route_family=_route_family(page),
            headers=headers if include_headers else [],
            keys=keys,
            rows=rows,
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=started,
            status_out=status_out,
            error_out=error_out,
            meta_extra=meta,
        )

        latency = (time.time() - started) * 1000.0
        await _record_metrics(ok=True, unauthorized=False, latency_ms=latency)
        return _response(status=result.get("status", "success"), request_id=request_id, **result)

    except Exception as e:
        logger.exception("Unhandled investment_advisor sheet-rows error")
        latency = (time.time() - started) * 1000.0
        await _record_metrics(ok=False, unauthorized=False, latency_ms=latency, err=str(e))
        return _error(
            500,
            request_id,
            f"{type(e).__name__}: {e}",
            extra={"path": str(request.url.path), "router_version": ROUTER_VERSION},
        )


# -----------------------------------------------------------------------------
# Public handlers
# -----------------------------------------------------------------------------
async def advisor_health(request: Request) -> BestJSONResponse:
    request_id = _request_id(request)
    payload = {
        "status": "ok",
        "service": MODULE_NAME,
        "router_version": ROUTER_VERSION,
        "open_mode": _safe_is_open_mode(),
        "allow_query_token": _safe_allow_query_token(),
        "metrics": _METRICS.to_dict(),
        "path": str(request.url.path),
    }
    return _response(status="ok", request_id=request_id, **payload)


async def advisor_metrics(request: Request) -> BestJSONResponse:
    request_id = _request_id(request)
    return _response(status="ok", request_id=request_id, metrics=_METRICS.to_dict())


async def advisor_sheet_rows_get(
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
    include_headers: Optional[bool] = Query(default=None),
    include_matrix: Optional[bool] = Query(default=None),
    schema_only: Optional[bool] = Query(default=None),
    headers_only: Optional[bool] = Query(default=None),
    prefer_engine_sheet: Optional[bool] = Query(default=None),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> BestJSONResponse:
    payload: Dict[str, Any] = {}
    for k, v in {
        "page": page,
        "sheet": sheet,
        "sheet_name": sheet_name,
        "name": name,
        "tab": tab,
        "symbols": symbols,
        "tickers": tickers,
        "limit": limit,
        "offset": offset,
        "top_n": top_n,
        "include_headers": include_headers,
        "include_matrix": include_matrix,
        "schema_only": schema_only,
        "headers_only": headers_only,
        "prefer_engine_sheet": prefer_engine_sheet,
    }.items():
        if v not in (None, ""):
            payload[k] = v

    return await _sheet_rows_impl(
        request=request,
        payload=payload,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
    )


async def advisor_sheet_rows_post(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> BestJSONResponse:
    return await _sheet_rows_impl(
        request=request,
        payload=dict(body or {}),
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
    )


# -----------------------------------------------------------------------------
# Router wiring
# -----------------------------------------------------------------------------
def _wire_router(r: APIRouter) -> None:
    r.add_api_route("/health", advisor_health, methods=["GET"])
    r.add_api_route("/metrics", advisor_metrics, methods=["GET"])
    r.add_api_route("/sheet-rows", advisor_sheet_rows_get, methods=["GET"])
    r.add_api_route("/sheet-rows", advisor_sheet_rows_post, methods=["POST"])


for _r in (_router_canonical, _router_advanced, _router_compat_us, _router_compat_dash):
    _wire_router(_r)
    router.include_router(_r)

__all__ = ["router", "ROUTER_VERSION"]
