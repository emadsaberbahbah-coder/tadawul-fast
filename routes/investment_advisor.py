#!/usr/bin/env python3
"""
routes/investment_advisor.py
================================================================================
TFB Advanced / Investment Advisor Routes — v6.3.0
================================================================================
ENGINE-FIRST SHEET-ROWS • SPECIAL-BUILDER-AWARE • TOP10/INSIGHTS SAFE
GENERIC-ADVISOR FALLBACK • SCHEMA-CONTRACT PRESERVING • AUTH-BRIDGED
JSON-SAFE • WRAPPER-PAYLOAD SAFE • ASYNC+SYNC TOLERANT • CACHE/STATE TOLERANT
ROW-OBJECTS + MATRIX • HEADER/KEY ALIGNED • SCHEMA-ONLY / HEADERS-ONLY SAFE

What this revision improves
---------------------------
- ✅ FIX: sheet-rows now returns BOTH:
         - rows / rows_matrix (matrix aligned to keys)
         - row_objects / items / records (dict rows aligned to keys)
- ✅ FIX: display headers are kept separate from canonical keys to avoid all-null
         capture when tests look up values by the wrong field names.
- ✅ FIX: supports schema_only / headers_only for faster live contract testing.
- ✅ FIX: Data_Dictionary gets a stable schema-driven payload.
- ✅ FIX: row extraction now recognizes row_objects / records in addition to
         rows / items / data / quotes / rows_matrix.
- ✅ FIX: result_count / payload-quality scoring now understand row_objects too.
- ✅ SAFE: route aliases and engine-first / special-builder behavior are preserved.
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


# -----------------------------------------------------------------------------
# JSON response (orjson if available) + safe fallback
# -----------------------------------------------------------------------------
def _json_safe(obj: Any) -> Any:
    if obj is None:
        return None
    if isinstance(obj, bool):
        return obj
    if isinstance(obj, (int, str)):
        return obj
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, Decimal):
        try:
            f = float(obj)
            if math.isnan(f) or math.isinf(f):
                return None
            return f
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
    from fastapi.responses import Response as _BaseResponse

    class BestJSONResponse(_BaseResponse):
        media_type = "application/json"

        def render(self, content: Any) -> bytes:
            return orjson.dumps(_json_safe(content), default=str)

    def _json_dumps(v: Any) -> str:
        return orjson.dumps(_json_safe(v), default=str).decode("utf-8")

except Exception:
    import json as _json_std
    from fastapi.responses import JSONResponse as BestJSONResponse  # type: ignore

    def _json_dumps(v: Any) -> str:
        return _json_std.dumps(_json_safe(v), default=str, ensure_ascii=False)


logger = logging.getLogger("routes.investment_advisor")
logger.addHandler(logging.NullHandler())

ROUTER_VERSION = "6.3.0"
MODULE_NAME = "routes.investment_advisor"

router = APIRouter(tags=["advisor"])
_router_canonical = APIRouter(prefix="/v1/advisor", tags=["advisor"])
_router_advanced = APIRouter(prefix="/v1/advanced", tags=["advanced"])
_router_compat_us = APIRouter(prefix="/v1/investment_advisor", tags=["investment_advisor"])
_router_compat_dash = APIRouter(prefix="/v1/investment-advisor", tags=["investment-advisor"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "active"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "inactive"}

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
    "name",
    "current_price",
    "recommendation",
    "recommendation_reason",
    "overall_score",
    "risk_bucket",
    "forecast_confidence",
    "horizon_days",
    "invest_period_label",
]

_GENERIC_HEADERS_FALLBACK: List[str] = [
    "Symbol",
    "Name",
    "Current Price",
    "Recommendation",
    "Recommendation Reason",
    "Overall Score",
    "Risk Bucket",
    "Forecast Confidence",
    "Horizon Days",
    "Invest Period Label",
]

_FIELD_ALIAS_HINTS: Dict[str, List[str]] = {
    "symbol": ["ticker", "code", "instrument", "security", "requested_symbol", "symbol_normalized"],
    "ticker": ["symbol", "code", "instrument", "security", "requested_symbol"],
    "name": ["company_name", "security_name", "instrument_name", "title"],
    "current_price": ["price", "last_price", "last", "close", "market_price", "current", "spot", "nav"],
    "recommendation_reason": ["reason", "reco_reason", "recommendation_notes"],
    "horizon_days": ["invest_period_days", "investment_period_days", "period_days"],
    "invest_period_label": ["investment_period_label", "period_label", "horizon_label"],
    "top10_rank": ["rank", "top_rank"],
    "selection_reason": ["selection_notes", "selector_reason"],
    "criteria_snapshot": ["criteria", "criteria_json", "snapshot"],
}

# Optional canonical contracts / engine helpers
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
# In-module metrics
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


def _unique_keep_order(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        s = _clean_str(value)
        if not s:
            continue
        key = s.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(s)
    return out


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


def _result_count(result: Mapping[str, Any]) -> int:
    if not isinstance(result, Mapping):
        return 0
    for key in ("row_objects", "records", "items", "rows", "data", "quotes", "rows_matrix"):
        value = result.get(key)
        if isinstance(value, list):
            return len(value)
    return 0


def _top10_fields_coverage(result: Mapping[str, Any]) -> int:
    if not isinstance(result, Mapping):
        return 0

    items = None
    for key in ("row_objects", "records", "items", "rows", "data"):
        value = result.get(key)
        if isinstance(value, list) and value:
            items = value
            break

    if not isinstance(items, list) or not items:
        return 0

    coverage = 0
    for field in TOP10_REQUIRED_FIELDS:
        if any(isinstance(item, Mapping) and item.get(field) not in (None, "", [], {}) for item in items):
            coverage += 1
    return coverage


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
                {
                    "token": token,
                    "authorization": authz,
                    "headers": dict(request.headers),
                },
                {
                    "token": token,
                    "authorization": authz,
                },
                {
                    "token": token,
                },
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
        if _env_bool("REQUIRE_AUTH", True):
            return False
        return True


# -----------------------------------------------------------------------------
# Page / schema helpers
# -----------------------------------------------------------------------------
def _normalize_page_name(raw: Optional[str]) -> str:
    s = _clean_str(raw)
    if not s:
        return "Top_10_Investments"

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
        "top_10_investments": "Top_10_Investments",
        "top10_investments": "Top_10_Investments",
        "top10": "Top_10_Investments",
        "advanced": "Top_10_Investments",
        "advisor": "Top_10_Investments",
        "investment_advisor": "Top_10_Investments",
        "insights_analysis": "Insights_Analysis",
        "insights": "Insights_Analysis",
        "market_leaders": "Market_Leaders",
        "global_markets": "Global_Markets",
        "commodities_fx": "Commodities_FX",
        "mutual_funds": "Mutual_Funds",
        "my_portfolio": "My_Portfolio",
        "my_investments": "My_Investments",
        "data_dictionary": "Data_Dictionary",
    }
    return mapping.get(compact, s.replace(" ", "_"))


def _route_family(page: str) -> str:
    p = _normalize_page_name(page)
    if p == "Top_10_Investments":
        return "top10"
    if p == "Insights_Analysis":
        return "insights"
    if p == "Data_Dictionary":
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
        if p == "Top_10_Investments":
            headers, keys = _ensure_top10_contract(headers, keys)
        if headers and keys:
            return headers, keys

    if callable(_engine_get_sheet_spec):
        try:
            spec = _engine_get_sheet_spec(p)
            headers, keys = _schema_keys_headers_from_spec(spec)
            if p == "Top_10_Investments":
                headers, keys = _ensure_top10_contract(headers, keys)
            if headers and keys:
                return headers, keys
        except Exception:
            pass

    if callable(_registry_get_sheet_spec):
        try:
            spec = _registry_get_sheet_spec(p)
            headers, keys = _schema_keys_headers_from_spec(spec)
            if p == "Top_10_Investments":
                headers, keys = _ensure_top10_contract(headers, keys)
            if headers and keys:
                return headers, keys
        except Exception:
            pass

    if p == "Top_10_Investments":
        return _ensure_top10_contract(list(_TOP10_HEADERS_FALLBACK), list(_TOP10_KEYS_FALLBACK))
    if p == "Insights_Analysis":
        return _complete_schema_contract(list(_INSIGHTS_HEADERS_FALLBACK), list(_INSIGHTS_KEYS_FALLBACK))
    if p == "Data_Dictionary":
        return _complete_schema_contract(list(_DATA_DICTIONARY_HEADERS_FALLBACK), list(_DATA_DICTIONARY_KEYS_FALLBACK))

    return _complete_schema_contract(list(_GENERIC_HEADERS_FALLBACK), list(_GENERIC_KEYS_FALLBACK))


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
        "rows": [] if include_matrix else [],
        "rows_matrix": [] if include_matrix else [],
        "row_objects": [],
        "items": [],
        "records": [],
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


def _build_data_dictionary_rows() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    mod = _safe_import("core.sheets.data_dictionary")
    if mod is not None:
        build_rows = getattr(mod, "build_data_dictionary_rows", None)
        if callable(build_rows):
            try:
                raw_rows = build_rows(include_meta_sheet=True)
            except TypeError:
                raw_rows = build_rows()
            except Exception:
                raw_rows = []
            if isinstance(raw_rows, list):
                for item in raw_rows:
                    d = item if isinstance(item, Mapping) else _model_to_dict(item)
                    if isinstance(d, dict) and d:
                        rows.append(dict(d))

    if rows:
        return rows

    spec_sources = []
    if callable(_registry_get_sheet_spec):
        spec_sources.append(("registry", _registry_get_sheet_spec))
    if callable(_engine_get_sheet_spec):
        spec_sources.append(("engine", _engine_get_sheet_spec))

    pages_to_try = [
        "Market_Leaders",
        "Global_Markets",
        "Mutual_Funds",
        "Commodities_FX",
        "My_Portfolio",
        "Insights_Analysis",
        "Top_10_Investments",
        "Data_Dictionary",
    ]

    for source_name, fn in spec_sources:
        for page in pages_to_try:
            try:
                spec = fn(page)
                headers, keys = _schema_keys_headers_from_spec(spec)
                headers, keys = _complete_schema_contract(headers, keys)
                for h, k in zip(headers, keys):
                    rows.append(
                        {
                            "sheet": page,
                            "group": "",
                            "header": h,
                            "key": k,
                            "dtype": "",
                            "fmt": "",
                            "required": "",
                            "source": source_name,
                            "notes": "",
                        }
                    )
            except Exception:
                continue

    return rows


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
            or "Top_10_Investments"
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
    risk_score = _safe_float(row.get("risk_score"))
    valuation = _safe_float(row.get("valuation_score"))
    momentum = _safe_float(row.get("momentum_score"))
    quality = _safe_float(row.get("quality_score"))
    opportunity = _safe_float(row.get("opportunity_score"))

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

    if rec in {"BUY", "STRONG_BUY", "ACCUMULATE"}:
        if roi_txt:
            parts.append(f"expected {invest_period_label or 'target-horizon'} upside is {roi_txt}")
        if selected_fp is not None and current_price is not None and selected_fp > current_price:
            parts.append(f"forecast price {round(selected_fp, 2)} is above current price {round(current_price, 2)}")
        if conf_txt:
            parts.append(f"confidence is {conf_txt}")
        if overall is not None:
            parts.append(f"overall score is {round(overall, 2)}")
        if valuation is not None and valuation >= 60:
            parts.append("valuation is supportive")
        if momentum is not None and momentum >= 60:
            parts.append("momentum is positive")
        if quality is not None and quality >= 60:
            parts.append("quality profile is solid")
        if opportunity is not None and opportunity >= 60:
            parts.append("opportunity score is favorable")
        if risk_bucket:
            parts.append(f"risk bucket is {risk_bucket}")
        elif risk_score is not None:
            parts.append(f"risk score is {round(risk_score, 2)}")
    elif rec in {"SELL", "REDUCE"}:
        if roi_txt:
            parts.append(f"expected {invest_period_label or 'target-horizon'} return is {roi_txt}")
        if selected_fp is not None and current_price is not None and selected_fp < current_price:
            parts.append(f"forecast price {round(selected_fp, 2)} is below current price {round(current_price, 2)}")
        if overall is not None:
            parts.append(f"overall score is {round(overall, 2)}")
        if momentum is not None and momentum <= 40:
            parts.append("momentum is weak")
        if risk_bucket:
            parts.append(f"risk bucket is {risk_bucket}")
        elif risk_score is not None:
            parts.append(f"risk score is {round(risk_score, 2)}")
    else:
        if roi_txt:
            parts.append(f"expected {invest_period_label or 'target-horizon'} return is {roi_txt}")
        if overall is not None:
            parts.append(f"overall score is {round(overall, 2)}")
        if risk_bucket:
            parts.append(f"risk bucket is {risk_bucket}")
        elif risk_score is not None:
            parts.append(f"risk score is {round(risk_score, 2)}")
        if conf_txt:
            parts.append(f"confidence is {conf_txt}")

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
# Row extraction / normalization
# -----------------------------------------------------------------------------
def _rows_from_matrix(rows_matrix: Any, cols: Sequence[str]) -> List[Dict[str, Any]]:
    if not isinstance(rows_matrix, list) or not rows_matrix or not cols:
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

    if page == "Top_10_Investments" and rows_like:
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
        "keys": list(schema_keys),
        "row_objects": [],
        "records": [],
        "items": [],
        "rows": [],
        "rows_matrix": [],
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

    if isinstance(raw, list) and not rows:
        if raw and all(isinstance(r, (list, tuple)) for r in raw):
            rows = _rows_from_matrix(raw, schema_keys)

    fixed_items: List[Dict[str, Any]] = []
    for i, item in enumerate(rows, start=1):
        row_map = dict(item) if isinstance(item, Mapping) else {}
        row_map = _ensure_item_context(row_map, payload, rank=i)
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
    norm["meta"] = meta
    return norm


# -----------------------------------------------------------------------------
# Auth / engine helpers
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


# -----------------------------------------------------------------------------
# Generic advisor runner discovery
# -----------------------------------------------------------------------------
def _iter_object_callables(obj: Any, names: Sequence[str]) -> List[Any]:
    out: List[Any] = []
    if obj is None:
        return out
    for name in names:
        try:
            fn = getattr(obj, name, None)
            if callable(fn):
                out.append(fn)
        except Exception:
            continue
    return out


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
            for method in (
                "run",
                "build",
                "build_rows",
                "build_sheet_rows",
                "get_rows",
                "recommendations",
            ):
                for fn in _iter_object_callables(obj, [method]):
                    _add(f"app.state.{name}.{method}", fn)
        except Exception:
            pass

    for mod_name, fn_names in (
        (
            "core.investment_advisor_engine",
            [
                "run_investment_advisor_engine",
                "run_investment_advisor",
                "build_sheet_rows",
                "get_sheet_rows",
            ],
        ),
        (
            "core.analysis.top10_selector",
            ["build_top10_rows", "build_top10_output_rows", "build_top10_investments_rows", "get_top10_rows"],
        ),
    ):
        mod = _safe_import(mod_name)
        if mod is None:
            continue
        for fn_name in fn_names:
            try:
                _add(f"{mod_name}.{fn_name}", getattr(mod, fn_name, None))
            except Exception:
                continue

    if engine is not None:
        for method in (
            "build_top10_rows",
            "build_sheet_rows",
            "get_sheet_rows",
            "sheet_rows",
            "get_page_rows",
            "run_investment_advisor_engine",
        ):
            for fn in _iter_object_callables(engine, [method]):
                _add(f"engine.{method}", fn)

    return candidates


def _resolve_special_callables(page: str, request: Request, engine: Any) -> List[Tuple[str, Any]]:
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

    page_norm = _normalize_page_name(page)
    state = getattr(getattr(request, "app", None), "state", None)

    common_state_names = [
        "advanced_builder",
        "advisor_builder",
        "investment_advisor_builder",
        "advisor_runner",
        "investment_advisor_runner",
    ]
    top10_state_names = [
        "top10_builder",
        "top10_selector",
        "top10_runner",
        "advanced_top10_builder",
    ]
    insights_state_names = [
        "insights_builder",
        "insights_runner",
        "insights_analysis_builder",
    ]

    for name in common_state_names:
        try:
            _add(f"app.state.{name}", getattr(state, name, None))
        except Exception:
            pass

    if page_norm == "Top_10_Investments":
        for name in top10_state_names:
            try:
                obj = getattr(state, name, None)
                _add(f"app.state.{name}", obj)
                for method in (
                    "run",
                    "build",
                    "build_rows",
                    "build_sheet_rows",
                    "build_top10_rows",
                    "run_top10_selector",
                    "select_top10",
                    "get_rows",
                ):
                    for fn in _iter_object_callables(obj, [method]):
                        _add(f"app.state.{name}.{method}", fn)
            except Exception:
                pass

    if page_norm == "Insights_Analysis":
        for name in insights_state_names:
            try:
                obj = getattr(state, name, None)
                _add(f"app.state.{name}", obj)
                for method in ("run", "build", "build_rows", "build_sheet_rows", "get_rows"):
                    for fn in _iter_object_callables(obj, [method]):
                        _add(f"app.state.{name}.{method}", fn)
            except Exception:
                pass

    module_specs: List[Tuple[str, List[str]]] = [
        (
            "core.investment_advisor_engine",
            [
                "run_investment_advisor_engine",
                "run_investment_advisor",
                "build_sheet_rows",
                "get_sheet_rows",
            ],
        ),
    ]

    if page_norm == "Top_10_Investments":
        module_specs = [
            (
                "core.analysis.top10_selector",
                [
                    "build_top10_rows",
                    "build_top10_output_rows",
                    "build_top10_investments_rows",
                    "build_top_10_investments_rows",
                    "get_top10_rows",
                    "select_top10",
                    "select_top10_symbols",
                ],
            ),
            *module_specs,
        ]
    elif page_norm == "Insights_Analysis":
        module_specs = [
            (
                "core.analysis.insights_builder",
                [
                    "build_insights_analysis_rows",
                    "build_insights_rows",
                    "build_insights_output_rows",
                    "build_insights_analysis",
                    "get_insights_rows",
                ],
            ),
            *module_specs,
        ]

    for mod_name, fn_names in module_specs:
        mod = _safe_import(mod_name)
        if mod is None:
            continue
        for fn_name in fn_names:
            try:
                _add(f"{mod_name}.{fn_name}", getattr(mod, fn_name, None))
            except Exception:
                continue

    if engine is not None:
        engine_methods = [
            "build_top10_rows",
            "build_insights_rows",
            "build_sheet_rows",
            "get_sheet_rows",
            "sheet_rows",
            "build_page_rows",
            "get_page_rows",
            "get_cached_sheet_rows",
            "get_sheet_snapshot",
            "get_cached_sheet_snapshot",
        ]
        for method in engine_methods:
            for fn in _iter_object_callables(engine, [method]):
                _add(f"engine.{method}", fn)

    return candidates


async def _call_candidate(
    fn: Any,
    *,
    page: str,
    payload: Dict[str, Any],
    engine: Any,
    request: Request,
    timeout_sec: float,
) -> Tuple[bool, Any, str]:
    page_norm = _normalize_page_name(page)

    kw_attempts = [
        {
            "payload": payload,
            "page": page_norm,
            "sheet_name": page_norm,
            "sheet": page_norm,
            "request": request,
            "engine": engine,
            "data_engine": engine,
            "quote_engine": engine,
            "cache_engine": engine,
        },
        {
            "payload": payload,
            "page": page_norm,
            "engine": engine,
            "request": request,
        },
        {
            "payload": payload,
            "sheet_name": page_norm,
            "engine": engine,
        },
        {
            "request": payload,
            "page": page_norm,
            "engine": engine,
        },
        {
            "body": payload,
            "page": page_norm,
            "engine": engine,
        },
        {
            "page": page_norm,
            "sheet_name": page_norm,
            "engine": engine,
        },
        {
            "payload": payload,
            "engine": engine,
        },
        {
            "request": payload,
            "engine": engine,
        },
        {
            "body": payload,
            "engine": engine,
        },
        {"payload": payload},
        {"request": payload},
        {"body": payload},
        {"page": page_norm},
        {},
    ]

    positional_attempts: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = [
        ((payload,), {}),
        ((payload, engine), {}),
        ((page_norm, payload), {}),
        ((page_norm,), {}),
    ]

    last_err = ""

    for kwargs in kw_attempts:
        try:
            if inspect.iscoroutinefunction(fn):
                out = await asyncio.wait_for(fn(**kwargs), timeout=timeout_sec)
            else:
                out = await asyncio.wait_for(asyncio.to_thread(fn, **kwargs), timeout=timeout_sec)
                out = await _maybe_await(out)
            return True, out, ""
        except TypeError as e:
            last_err = f"TypeError: {e}"
            continue
        except asyncio.TimeoutError:
            return False, None, f"timeout({timeout_sec}s)"
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            continue

    for args, kwargs in positional_attempts:
        try:
            if inspect.iscoroutinefunction(fn):
                out = await asyncio.wait_for(fn(*args, **kwargs), timeout=timeout_sec)
            else:
                out = await asyncio.wait_for(asyncio.to_thread(fn, *args, **kwargs), timeout=timeout_sec)
                out = await _maybe_await(out)
            return True, out, ""
        except TypeError as e:
            last_err = f"TypeError: {e}"
            continue
        except asyncio.TimeoutError:
            return False, None, f"timeout({timeout_sec}s)"
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            continue

    return False, None, last_err


# -----------------------------------------------------------------------------
# Engine-first sheet mode
# -----------------------------------------------------------------------------
async def _run_engine_sheet_mode(
    *,
    page: str,
    payload: Dict[str, Any],
    request: Request,
    engine: Any,
    timeout_sec: float,
) -> Dict[str, Any]:
    page_norm = _normalize_page_name(page)
    method_names = [
        "get_sheet_rows",
        "sheet_rows",
        "build_sheet_rows",
        "execute_sheet_rows",
        "run_sheet_rows",
        "get_page_rows",
        "build_page_rows",
        "get_rows_for_sheet",
        "get_rows_for_page",
        "get_cached_sheet_rows",
        "get_sheet_snapshot",
        "get_cached_sheet_snapshot",
        "get_page_snapshot",
    ]
    errors: List[str] = []
    best: Optional[Dict[str, Any]] = None
    best_score = -9999

    if engine is None:
        headers, keys = _schema_for_page(page_norm)
        return {
            "headers": headers,
            "keys": keys,
            "row_objects": [],
            "records": [],
            "items": [],
            "rows": [],
            "rows_matrix": [],
            "meta": {
                "ok": False,
                "source": "engine_sheet_mode",
                "router_version": ROUTER_VERSION,
                "error": "engine_unavailable",
            },
        }

    for method_name in method_names:
        fn = getattr(engine, method_name, None)
        if not callable(fn):
            continue

        ok, raw, err = await _call_candidate(
            fn,
            page=page_norm,
            payload=payload,
            engine=engine,
            request=request,
            timeout_sec=timeout_sec,
        )
        if not ok:
            if err:
                errors.append(f"engine.{method_name}: {err}")
            continue

        normalized = _normalize_any_result(page=page_norm, raw=raw, payload=payload, source=f"engine.{method_name}")
        score = _payload_quality_score(normalized, page=page_norm)

        if score > best_score:
            best = normalized
            best_score = score

        if _result_count(normalized) > 0:
            return normalized

        meta = normalized.get("meta") if isinstance(normalized.get("meta"), Mapping) else {}
        if bool(meta.get("ok", False)):
            return normalized

    if best is not None:
        meta = best.get("meta") if isinstance(best.get("meta"), Mapping) else {}
        meta = dict(meta)
        if errors and not meta.get("error"):
            meta["error"] = " | ".join(errors)[:3000]
        best["meta"] = meta
        return best

    headers, keys = _schema_for_page(page_norm)
    return {
        "headers": headers,
        "keys": keys,
        "row_objects": [],
        "records": [],
        "items": [],
        "rows": [],
        "rows_matrix": [],
        "meta": {
            "ok": False,
            "source": "engine_sheet_mode",
            "router_version": ROUTER_VERSION,
            "error": " | ".join(errors)[:3000] if errors else "no_engine_sheet_method_available",
        },
    }


# -----------------------------------------------------------------------------
# Generic advisor fallback
# -----------------------------------------------------------------------------
async def _run_generic_advisor(
    *,
    page: str,
    payload: Dict[str, Any],
    request: Request,
    engine: Any,
    timeout_sec: float,
) -> Dict[str, Any]:
    candidates = _resolve_generic_advisor_callables(request, engine)
    best: Optional[Dict[str, Any]] = None
    best_score = -9999
    errors: List[str] = []

    for source, fn in candidates:
        ok, raw, err = await _call_candidate(
            fn,
            page=page,
            payload=payload,
            engine=engine,
            request=request,
            timeout_sec=timeout_sec,
        )
        if not ok:
            if err:
                errors.append(f"{source}: {err}")
            continue

        normalized = _normalize_any_result(page=page, raw=raw, payload=payload, source=source)
        score = _payload_quality_score(normalized, page=page)
        if score > best_score:
            best = normalized
            best_score = score
        if _result_count(normalized) > 0:
            return normalized

    if best is not None:
        meta = best.get("meta") if isinstance(best.get("meta"), Mapping) else {}
        meta = dict(meta)
        if errors and not meta.get("error"):
            meta["error"] = " | ".join(errors)[:3000]
        best["meta"] = meta
        return best

    headers, keys = _schema_for_page(page)
    return {
        "headers": headers,
        "keys": keys,
        "row_objects": [],
        "records": [],
        "items": [],
        "rows": [],
        "rows_matrix": [],
        "meta": {
            "ok": False,
            "source": "generic_advisor",
            "router_version": ROUTER_VERSION,
            "error": " | ".join(errors)[:3000] if errors else "no_generic_advisor_callable_available",
        },
    }


# -----------------------------------------------------------------------------
# Special-page builder pipeline
# -----------------------------------------------------------------------------
async def _run_special_page_builder(
    *,
    page: str,
    payload: Dict[str, Any],
    request: Request,
    engine: Any,
    timeout_sec: float,
) -> Dict[str, Any]:
    candidates = _resolve_special_callables(page, request, engine)
    best: Optional[Dict[str, Any]] = None
    best_score: Tuple[int, int, int, int] = (-1, -1, -1, -1)
    errors: List[str] = []

    def _score(res: Mapping[str, Any]) -> Tuple[int, int, int, int]:
        count = _result_count(res)
        coverage = _top10_fields_coverage(res) if _normalize_page_name(page) == "Top_10_Investments" else 0
        meta = res.get("meta") if isinstance(res.get("meta"), Mapping) else {}
        ok_flag = 1 if bool(meta.get("ok", False)) else 0
        keys_len = len(res.get("keys") or []) if isinstance(res.get("keys"), list) else 0
        return (count, coverage, ok_flag, keys_len)

    for source, fn in candidates:
        ok, raw, err = await _call_candidate(
            fn,
            page=page,
            payload=payload,
            engine=engine,
            request=request,
            timeout_sec=timeout_sec,
        )
        if not ok:
            if err:
                errors.append(f"{source}: {err}")
            continue

        normalized = _normalize_any_result(page=page, raw=raw, payload=payload, source=source)
        current_score = _score(normalized)

        if current_score > best_score:
            best = normalized
            best_score = current_score

        if current_score[0] > 0 and current_score[2] == 1:
            if _normalize_page_name(page) != "Top_10_Investments" or current_score[1] >= 2:
                return normalized

    if best is not None:
        meta = best.get("meta") if isinstance(best.get("meta"), Mapping) else {}
        meta = dict(meta)
        if errors and not meta.get("error"):
            meta["error"] = " | ".join(errors)[:3000]
        best["meta"] = meta
        return best

    headers, keys = _schema_for_page(page)
    return {
        "headers": headers,
        "keys": keys,
        "row_objects": [],
        "records": [],
        "items": [],
        "rows": [],
        "rows_matrix": [],
        "meta": {
            "ok": False,
            "source": "special_builder",
            "router_version": ROUTER_VERSION,
            "error": " | ".join(errors)[:3000] if errors else "no_special_builder_available",
        },
    }


# -----------------------------------------------------------------------------
# Page pipelines
# -----------------------------------------------------------------------------
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

    if page_norm == "Top_10_Investments":
        special = await _run_special_page_builder(
            page=page_norm,
            payload=payload,
            request=request,
            engine=engine,
            timeout_sec=timeout_sec,
        )
        engine_sheet = await _run_engine_sheet_mode(
            page=page_norm,
            payload=payload,
            request=request,
            engine=engine,
            timeout_sec=timeout_sec,
        )
        generic = await _run_generic_advisor(
            page=page_norm,
            payload=payload,
            request=request,
            engine=engine,
            timeout_sec=timeout_sec,
        )

        def _score(res: Mapping[str, Any]) -> Tuple[int, int, int, int, int]:
            count = _result_count(res)
            coverage = _top10_fields_coverage(res)
            meta = res.get("meta") if isinstance(res.get("meta"), Mapping) else {}
            ok_flag = 1 if bool(meta.get("ok", False)) else 0
            no_error = 1 if not _clean_str(meta.get("error")) else 0
            keys_len = len(res.get("keys") or []) if isinstance(res.get("keys"), list) else 0
            return (count, coverage, ok_flag, no_error, keys_len)

        candidates = [special, engine_sheet, generic]
        if prefer_engine_sheet:
            candidates = [engine_sheet, special, generic]

        best = sorted(candidates, key=_score, reverse=True)[0]
        return dict(best)

    if page_norm == "Insights_Analysis":
        special = await _run_special_page_builder(
            page=page_norm,
            payload=payload,
            request=request,
            engine=engine,
            timeout_sec=timeout_sec,
        )
        engine_sheet = await _run_engine_sheet_mode(
            page=page_norm,
            payload=payload,
            request=request,
            engine=engine,
            timeout_sec=timeout_sec,
        )
        generic = await _run_generic_advisor(
            page=page_norm,
            payload=payload,
            request=request,
            engine=engine,
            timeout_sec=timeout_sec,
        )

        def _score(res: Mapping[str, Any]) -> Tuple[int, int, int]:
            count = _result_count(res)
            meta = res.get("meta") if isinstance(res.get("meta"), Mapping) else {}
            ok_flag = 1 if bool(meta.get("ok", False)) else 0
            no_error = 1 if not _clean_str(meta.get("error")) else 0
            return (count, ok_flag, no_error)

        candidates = [special, engine_sheet, generic]
        if prefer_engine_sheet:
            candidates = [engine_sheet, special, generic]
        best = sorted(candidates, key=_score, reverse=True)[0]
        return dict(best)

    if prefer_engine_sheet:
        engine_sheet = await _run_engine_sheet_mode(
            page=page_norm,
            payload=payload,
            request=request,
            engine=engine,
            timeout_sec=timeout_sec,
        )
        if _result_count(engine_sheet) > 0 or bool((engine_sheet.get("meta") or {}).get("ok", False)):
            return engine_sheet

    generic = await _run_generic_advisor(
        page=page_norm,
        payload=payload,
        request=request,
        engine=engine,
        timeout_sec=timeout_sec,
    )
    if _result_count(generic) > 0 or bool((generic.get("meta") or {}).get("ok", False)):
        return generic

    engine_sheet = await _run_engine_sheet_mode(
        page=page_norm,
        payload=payload,
        request=request,
        engine=engine,
        timeout_sec=timeout_sec,
    )
    return engine_sheet


# -----------------------------------------------------------------------------
# Payload builders
# -----------------------------------------------------------------------------
def _normalize_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(payload or {})

    chosen_page = (
        out.get("page")
        or out.get("sheet_name")
        or out.get("sheet")
        or out.get("name")
        or out.get("tab")
        or "Top_10_Investments"
    )
    chosen_page = _normalize_page_name(chosen_page)

    out["page"] = chosen_page
    out["sheet_name"] = chosen_page
    if "sheet" not in out or not _clean_str(out.get("sheet")):
        out["sheet"] = chosen_page

    symbol_list: List[str] = []
    for key in ("symbols", "tickers", "symbol", "ticker", "direct_symbols"):
        value = out.get(key)
        if isinstance(value, list):
            for item in value:
                symbol_list.extend(_split_symbols(str(item)))
        elif value is not None:
            symbol_list.extend(_split_symbols(str(value)))

    symbol_list = _unique_keep_order([s.upper() for s in symbol_list])
    if symbol_list:
        out["symbols"] = symbol_list
        out["tickers"] = symbol_list
        out.setdefault("direct_symbols", symbol_list)

    if out.get("limit") is not None and out.get("top_n") is None:
        try:
            out["top_n"] = int(out.get("limit"))
        except Exception:
            pass

    if out.get("top_n") is None:
        out["top_n"] = 10 if chosen_page == "Top_10_Investments" else 20
    if out.get("limit") is None:
        out["limit"] = out.get("top_n")

    if out.get("include_headers") is None:
        out["include_headers"] = True
    else:
        out["include_headers"] = _boolish(out.get("include_headers"), True)

    if out.get("include_matrix") is None:
        out["include_matrix"] = True
    else:
        out["include_matrix"] = _boolish(out.get("include_matrix"), True)

    if out.get("schema_only") is None:
        out["schema_only"] = False
    else:
        out["schema_only"] = _boolish(out.get("schema_only"), False)

    if out.get("headers_only") is None:
        out["headers_only"] = False
    else:
        out["headers_only"] = _boolish(out.get("headers_only"), False)

    if out.get("invest_period_days") is None and out.get("horizon_days") is not None:
        out["invest_period_days"] = out.get("horizon_days")
    if out.get("horizon_days") is None and out.get("invest_period_days") is not None:
        out["horizon_days"] = out.get("invest_period_days")

    criteria = dict(out.get("criteria") or {}) if isinstance(out.get("criteria"), Mapping) else {}
    for key in (
        "page",
        "sheet",
        "sheet_name",
        "symbols",
        "tickers",
        "direct_symbols",
        "top_n",
        "limit",
        "risk_profile",
        "allocation_strategy",
        "invest_amount",
        "horizon_days",
        "invest_period_days",
        "invest_period_label",
        "include_headers",
        "include_matrix",
        "schema_only",
        "headers_only",
    ):
        if out.get(key) is not None and criteria.get(key) is None:
            criteria[key] = out.get(key)

    if chosen_page == "Top_10_Investments":
        criteria.setdefault("prefer_canonical_schema", True)
        criteria.setdefault("force_full_schema", True)
        out.setdefault("prefer_canonical_schema", True)
        out.setdefault("force_full_schema", True)

    out["criteria"] = criteria
    return out


def _payload_from_query(
    *,
    page: Optional[str],
    symbols: str,
    tickers: str,
    top_n: Optional[int],
    invest_amount: Optional[float],
    allocation_strategy: Optional[str],
    risk_profile: Optional[str],
    horizon_days: Optional[int],
    invest_period_label: Optional[str],
    include_headers: Optional[str],
    include_matrix: Optional[str],
    schema_only: Optional[str],
    headers_only: Optional[str],
    debug: bool,
) -> Dict[str, Any]:
    sym_list: List[str] = []
    if symbols:
        sym_list.extend(_split_symbols(symbols))
    if tickers:
        sym_list.extend(_split_symbols(tickers))
    sym_list = _unique_keep_order([s.upper() for s in sym_list])

    payload: Dict[str, Any] = {
        "page": page or "Top_10_Investments",
        "symbols": sym_list,
        "tickers": sym_list,
        "direct_symbols": sym_list,
        "top_n": top_n,
        "limit": top_n,
        "invest_amount": invest_amount,
        "allocation_strategy": allocation_strategy,
        "risk_profile": risk_profile,
        "horizon_days": horizon_days,
        "invest_period_days": horizon_days,
        "invest_period_label": invest_period_label,
        "include_headers": include_headers if include_headers is not None else True,
        "include_matrix": include_matrix if include_matrix is not None else True,
        "schema_only": schema_only if schema_only is not None else False,
        "headers_only": headers_only if headers_only is not None else False,
        "debug": bool(debug),
    }
    payload = {k: v for k, v in payload.items() if v is not None and v != ""}
    return _normalize_payload(payload)


def _payload_from_sheet_rows_query(
    *,
    page: Optional[str],
    sheet_name: Optional[str],
    sheet: Optional[str],
    name: Optional[str],
    tab: Optional[str],
    symbols: Optional[str],
    tickers: Optional[str],
    top_n: Optional[int],
    limit: Optional[int],
    invest_amount: Optional[float],
    allocation_strategy: Optional[str],
    risk_profile: Optional[str],
    horizon_days: Optional[int],
    invest_period_label: Optional[str],
    include_headers: Optional[str],
    include_matrix: Optional[str],
    schema_only: Optional[str],
    headers_only: Optional[str],
    debug: bool,
) -> Dict[str, Any]:
    chosen_page = page or sheet_name or sheet or name or tab or "Top_10_Investments"
    payload = _payload_from_query(
        page=chosen_page,
        symbols=symbols or "",
        tickers=tickers or "",
        top_n=(limit if (limit is not None and limit > 0) else top_n),
        invest_amount=invest_amount,
        allocation_strategy=allocation_strategy,
        risk_profile=risk_profile,
        horizon_days=horizon_days,
        invest_period_label=invest_period_label,
        include_headers=include_headers,
        include_matrix=include_matrix,
        schema_only=schema_only,
        headers_only=headers_only,
        debug=debug,
    )
    return _normalize_payload(payload)


# -----------------------------------------------------------------------------
# Outward envelopes
# -----------------------------------------------------------------------------
def _rows_matrix_from_objects(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    out: List[List[Any]] = []
    for row in rows:
        out.append([row.get(k) for k in keys])
    return out


def _result_to_sheet_payload(
    *,
    result: Dict[str, Any],
    payload: Dict[str, Any],
    request_id: str,
) -> Dict[str, Any]:
    page_norm = _normalize_page_name(
        payload.get("page")
        or payload.get("sheet_name")
        or payload.get("sheet")
        or payload.get("name")
        or payload.get("tab")
        or "Top_10_Investments"
    )
    route_family = _route_family(page_norm)

    include_headers = _boolish(payload.get("include_headers"), True)
    include_matrix = _boolish(payload.get("include_matrix"), True)

    schema_headers, schema_keys = _schema_for_page(page_norm)
    keys = list(schema_keys)
    headers = list(schema_headers)

    source_items = None
    for k in ("row_objects", "records", "items", "data", "quotes"):
        value = result.get(k)
        if isinstance(value, list):
            source_items = value
            break

    if source_items is None:
        source_items = []

    normalized_items: List[Dict[str, Any]] = []
    for i, item in enumerate(source_items, start=1):
        row_map = dict(item) if isinstance(item, Mapping) else {}
        row_map = _ensure_item_context(row_map, payload, rank=i)
        normalized_items.append(
            _normalize_to_schema_keys(
                schema_keys=keys,
                schema_headers=headers,
                raw=row_map,
            )
        )

    matrix = _rows_matrix_from_objects(normalized_items, keys)
    meta = result.get("meta") if isinstance(result.get("meta"), Mapping) else {}
    meta = dict(meta)
    ok_flag = bool(meta.get("ok", False)) or bool(normalized_items)
    error = _clean_str(meta.get("error")) or None

    meta["router_version"] = ROUTER_VERSION
    meta["count"] = len(normalized_items)
    meta["row_object_count"] = len(normalized_items)
    meta["sheet_mode"] = True
    meta["page"] = page_norm
    meta["route_family"] = route_family

    status = "success" if ok_flag else ("warn" if error else "error")

    return {
        "status": status,
        "page": page_norm,
        "sheet": page_norm,
        "sheet_name": page_norm,
        "route_family": route_family,
        "headers": headers if include_headers else [],
        "display_headers": headers if include_headers else [],
        "sheet_headers": headers if include_headers else [],
        "column_headers": headers if include_headers else [],
        "keys": keys,
        "columns": keys,
        "fields": keys,
        "rows": matrix if include_matrix else [],
        "rows_matrix": matrix if include_matrix else [],
        "row_objects": normalized_items,
        "items": normalized_items,
        "records": normalized_items,
        "data": normalized_items,
        "quotes": normalized_items,
        "count": len(normalized_items),
        "detail": error or "",
        "error": error,
        "version": ROUTER_VERSION,
        "request_id": request_id,
        "meta": meta,
    }


def _result_to_run_envelope(
    *,
    result: Dict[str, Any],
    request_id: str,
) -> Dict[str, Any]:
    meta = result.get("meta") if isinstance(result.get("meta"), Mapping) else {}
    ok_flag = bool(meta.get("ok", False)) or bool(_result_count(result))
    return {
        "status": "ok" if ok_flag else "error",
        "request_id": request_id,
        "timestamp_utc": _now_utc(),
        "version": ROUTER_VERSION,
        "data": result,
    }


# -----------------------------------------------------------------------------
# Handlers
# -----------------------------------------------------------------------------
async def advisor_health_handler(request: Request) -> BestJSONResponse:
    rid = _request_id(request)
    open_mode = _safe_is_open_mode()
    allow_query_token = _safe_allow_query_token()
    eng, source, err = await _get_engine(request)

    payload = {
        "engine": {
            "available": eng is not None,
            "source": source,
            "error": err,
            "type": type(eng).__name__ if eng is not None else None,
        },
        "auth": {
            "open_mode": open_mode,
            "allow_query_token": allow_query_token,
            "require_auth": _env_bool("REQUIRE_AUTH", True),
        },
        "metrics": _METRICS.to_dict(),
        "route_prefixes": [
            "/v1/advisor",
            "/v1/advanced",
            "/v1/investment_advisor",
            "/v1/investment-advisor",
        ],
    }
    return _response(status="ok" if eng is not None else "degraded", request_id=rid, **payload)


async def advisor_metrics_handler(request: Request) -> BestJSONResponse:
    rid = _request_id(request)
    return _response(status="ok", request_id=rid, metrics=_METRICS.to_dict())


async def advisor_recommendations_handler(
    request: Request,
    symbols: str = Query(default=""),
    tickers: str = Query(default=""),
    top_n: Optional[int] = Query(default=20),
    invest_amount: Optional[float] = Query(default=0.0),
    allocation_strategy: Optional[str] = Query(default="maximum_sharpe"),
    risk_profile: Optional[str] = Query(default="moderate"),
    horizon_days: Optional[int] = Query(default=None),
    invest_period_label: Optional[str] = Query(default=None),
    page: Optional[str] = Query(default=None),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    debug: bool = Query(default=False),
) -> BestJSONResponse:
    rid = _request_id(request)
    t0 = time.time()

    if not _auth_ok(request, token_query=token, x_app_token=x_app_token, authorization=authorization):
        await _record_metrics(False, True, (time.time() - t0) * 1000.0, "unauthorized")
        return _error(401, rid, "Unauthorized", extra={"open_mode": _safe_is_open_mode()})

    engine, engine_source, engine_err = await _get_engine(request)
    if engine is None:
        await _record_metrics(False, False, (time.time() - t0) * 1000.0, engine_err or "engine_unavailable")
        return _error(
            503,
            rid,
            "Advisor engine unavailable",
            extra={"engine_source": engine_source, "engine_error": engine_err},
        )

    payload = _payload_from_query(
        page=page,
        symbols=symbols,
        tickers=tickers,
        top_n=top_n,
        invest_amount=invest_amount,
        allocation_strategy=allocation_strategy,
        risk_profile=risk_profile,
        horizon_days=horizon_days,
        invest_period_label=invest_period_label,
        include_headers="true",
        include_matrix="true",
        schema_only="false",
        headers_only="false",
        debug=debug,
    )

    result = await _run_page_pipeline(
        page=payload.get("page") or "Top_10_Investments",
        payload=payload,
        request=request,
        engine=engine,
        timeout_sec=_timeout_sec(),
        prefer_engine_sheet=False,
    )

    ok = bool((result.get("meta") or {}).get("ok", False)) or bool(_result_count(result))
    err_txt = _clean_str((result.get("meta") or {}).get("error"))
    await _record_metrics(ok, False, (time.time() - t0) * 1000.0, err_txt)

    return BestJSONResponse(content=_json_safe(_result_to_run_envelope(result=result, request_id=rid)))


async def advisor_run_handler(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> BestJSONResponse:
    rid = _request_id(request)
    t0 = time.time()

    if not _auth_ok(request, token_query=token, x_app_token=x_app_token, authorization=authorization):
        await _record_metrics(False, True, (time.time() - t0) * 1000.0, "unauthorized")
        return _error(401, rid, "Unauthorized", extra={"open_mode": _safe_is_open_mode()})

    engine, engine_source, engine_err = await _get_engine(request)
    if engine is None:
        await _record_metrics(False, False, (time.time() - t0) * 1000.0, engine_err or "engine_unavailable")
        return _error(
            503,
            rid,
            "Advisor engine unavailable",
            extra={"engine_source": engine_source, "engine_error": engine_err},
        )

    payload = _normalize_payload(dict(body or {}))
    result = await _run_page_pipeline(
        page=payload.get("page") or "Top_10_Investments",
        payload=payload,
        request=request,
        engine=engine,
        timeout_sec=_timeout_sec(),
        prefer_engine_sheet=False,
    )

    ok = bool((result.get("meta") or {}).get("ok", False)) or bool(_result_count(result))
    err_txt = _clean_str((result.get("meta") or {}).get("error"))
    await _record_metrics(ok, False, (time.time() - t0) * 1000.0, err_txt)

    return BestJSONResponse(content=_json_safe(_result_to_run_envelope(result=result, request_id=rid)))


async def advisor_sheet_rows_get_handler(
    request: Request,
    page: Optional[str] = Query(default=None),
    sheet_name: Optional[str] = Query(default=None),
    sheet: Optional[str] = Query(default=None),
    name: Optional[str] = Query(default=None),
    tab: Optional[str] = Query(default=None),
    symbols: Optional[str] = Query(default=None),
    tickers: Optional[str] = Query(default=None),
    top_n: Optional[int] = Query(default=20),
    limit: Optional[int] = Query(default=None),
    invest_amount: Optional[float] = Query(default=0.0),
    allocation_strategy: Optional[str] = Query(default="maximum_sharpe"),
    risk_profile: Optional[str] = Query(default="moderate"),
    horizon_days: Optional[int] = Query(default=None),
    invest_period_label: Optional[str] = Query(default=None),
    include_headers: Optional[str] = Query(default="true"),
    include_matrix: Optional[str] = Query(default="true"),
    schema_only: Optional[str] = Query(default="false"),
    headers_only: Optional[str] = Query(default="false"),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    debug: bool = Query(default=False),
) -> BestJSONResponse:
    rid = _request_id(request)
    t0 = time.time()

    if not _auth_ok(request, token_query=token, x_app_token=x_app_token, authorization=authorization):
        await _record_metrics(False, True, (time.time() - t0) * 1000.0, "unauthorized")
        return _error(401, rid, "Unauthorized", extra={"open_mode": _safe_is_open_mode()})

    payload = _payload_from_sheet_rows_query(
        page=page,
        sheet_name=sheet_name,
        sheet=sheet,
        name=name,
        tab=tab,
        symbols=symbols,
        tickers=tickers,
        top_n=top_n,
        limit=limit,
        invest_amount=invest_amount,
        allocation_strategy=allocation_strategy,
        risk_profile=risk_profile,
        horizon_days=horizon_days,
        invest_period_label=invest_period_label,
        include_headers=include_headers,
        include_matrix=include_matrix,
        schema_only=schema_only,
        headers_only=headers_only,
        debug=debug,
    )

    page_norm = _normalize_page_name(payload.get("page"))
    include_headers_bool = _boolish(payload.get("include_headers"), True)
    include_matrix_bool = _boolish(payload.get("include_matrix"), True)
    schema_only_bool = _boolish(payload.get("schema_only"), False)
    headers_only_bool = _boolish(payload.get("headers_only"), False)

    if page_norm == "Data_Dictionary":
        if schema_only_bool or headers_only_bool:
            sheet_payload = _build_schema_only_payload(
                page=page_norm,
                request_id=rid,
                include_headers=include_headers_bool,
                include_matrix=include_matrix_bool,
                schema_only=schema_only_bool,
                headers_only=headers_only_bool,
            )
        else:
            headers, keys = _schema_for_page(page_norm)
            raw_rows = _build_data_dictionary_rows()
            row_objects = []
            for item in raw_rows:
                row_objects.append(
                    _normalize_to_schema_keys(
                        schema_keys=keys,
                        schema_headers=headers,
                        raw=item,
                    )
                )
            matrix = _rows_matrix_from_objects(row_objects, keys)
            sheet_payload = {
                "status": "success",
                "page": page_norm,
                "sheet": page_norm,
                "sheet_name": page_norm,
                "route_family": _route_family(page_norm),
                "headers": headers if include_headers_bool else [],
                "display_headers": headers if include_headers_bool else [],
                "sheet_headers": headers if include_headers_bool else [],
                "column_headers": headers if include_headers_bool else [],
                "keys": keys,
                "columns": keys,
                "fields": keys,
                "rows": matrix if include_matrix_bool else [],
                "rows_matrix": matrix if include_matrix_bool else [],
                "row_objects": row_objects,
                "items": row_objects,
                "records": row_objects,
                "data": row_objects,
                "quotes": row_objects,
                "count": len(row_objects),
                "detail": "",
                "error": None,
                "version": ROUTER_VERSION,
                "request_id": rid,
                "meta": {
                    "ok": True,
                    "router_version": ROUTER_VERSION,
                    "source": "data_dictionary",
                    "count": len(row_objects),
                    "row_object_count": len(row_objects),
                    "sheet_mode": True,
                    "page": page_norm,
                    "route_family": _route_family(page_norm),
                    "schema_only": False,
                    "headers_only": False,
                },
            }
        await _record_metrics(True, False, (time.time() - t0) * 1000.0, "")
        return BestJSONResponse(content=_json_safe(sheet_payload))

    if schema_only_bool or headers_only_bool:
        sheet_payload = _build_schema_only_payload(
            page=page_norm,
            request_id=rid,
            include_headers=include_headers_bool,
            include_matrix=include_matrix_bool,
            schema_only=schema_only_bool,
            headers_only=headers_only_bool,
        )
        await _record_metrics(True, False, (time.time() - t0) * 1000.0, "")
        return BestJSONResponse(content=_json_safe(sheet_payload))

    engine, engine_source, engine_err = await _get_engine(request)
    if engine is None:
        await _record_metrics(False, False, (time.time() - t0) * 1000.0, engine_err or "engine_unavailable")
        return _error(
            503,
            rid,
            "Advisor engine unavailable",
            extra={"engine_source": engine_source, "engine_error": engine_err},
        )

    result = await _run_page_pipeline(
        page=payload.get("page") or "Top_10_Investments",
        payload=payload,
        request=request,
        engine=engine,
        timeout_sec=_timeout_sec(),
        prefer_engine_sheet=True,
    )

    ok = bool((result.get("meta") or {}).get("ok", False)) or bool(_result_count(result))
    err_txt = _clean_str((result.get("meta") or {}).get("error"))
    await _record_metrics(ok, False, (time.time() - t0) * 1000.0, err_txt)

    sheet_payload = _result_to_sheet_payload(result=result, payload=payload, request_id=rid)
    return BestJSONResponse(content=_json_safe(sheet_payload))


async def advisor_sheet_rows_post_handler(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> BestJSONResponse:
    rid = _request_id(request)
    t0 = time.time()

    if not _auth_ok(request, token_query=token, x_app_token=x_app_token, authorization=authorization):
        await _record_metrics(False, True, (time.time() - t0) * 1000.0, "unauthorized")
        return _error(401, rid, "Unauthorized", extra={"open_mode": _safe_is_open_mode()})

    payload = _normalize_payload(dict(body or {}))
    page_norm = _normalize_page_name(payload.get("page"))
    include_headers_bool = _boolish(payload.get("include_headers"), True)
    include_matrix_bool = _boolish(payload.get("include_matrix"), True)
    schema_only_bool = _boolish(payload.get("schema_only"), False)
    headers_only_bool = _boolish(payload.get("headers_only"), False)

    if page_norm == "Data_Dictionary":
        if schema_only_bool or headers_only_bool:
            sheet_payload = _build_schema_only_payload(
                page=page_norm,
                request_id=rid,
                include_headers=include_headers_bool,
                include_matrix=include_matrix_bool,
                schema_only=schema_only_bool,
                headers_only=headers_only_bool,
            )
        else:
            headers, keys = _schema_for_page(page_norm)
            raw_rows = _build_data_dictionary_rows()
            row_objects = []
            for item in raw_rows:
                row_objects.append(
                    _normalize_to_schema_keys(
                        schema_keys=keys,
                        schema_headers=headers,
                        raw=item,
                    )
                )
            matrix = _rows_matrix_from_objects(row_objects, keys)
            sheet_payload = {
                "status": "success",
                "page": page_norm,
                "sheet": page_norm,
                "sheet_name": page_norm,
                "route_family": _route_family(page_norm),
                "headers": headers if include_headers_bool else [],
                "display_headers": headers if include_headers_bool else [],
                "sheet_headers": headers if include_headers_bool else [],
                "column_headers": headers if include_headers_bool else [],
                "keys": keys,
                "columns": keys,
                "fields": keys,
                "rows": matrix if include_matrix_bool else [],
                "rows_matrix": matrix if include_matrix_bool else [],
                "row_objects": row_objects,
                "items": row_objects,
                "records": row_objects,
                "data": row_objects,
                "quotes": row_objects,
                "count": len(row_objects),
                "detail": "",
                "error": None,
                "version": ROUTER_VERSION,
                "request_id": rid,
                "meta": {
                    "ok": True,
                    "router_version": ROUTER_VERSION,
                    "source": "data_dictionary",
                    "count": len(row_objects),
                    "row_object_count": len(row_objects),
                    "sheet_mode": True,
                    "page": page_norm,
                    "route_family": _route_family(page_norm),
                    "schema_only": False,
                    "headers_only": False,
                },
            }
        await _record_metrics(True, False, (time.time() - t0) * 1000.0, "")
        return BestJSONResponse(content=_json_safe(sheet_payload))

    if schema_only_bool or headers_only_bool:
        sheet_payload = _build_schema_only_payload(
            page=page_norm,
            request_id=rid,
            include_headers=include_headers_bool,
            include_matrix=include_matrix_bool,
            schema_only=schema_only_bool,
            headers_only=headers_only_bool,
        )
        await _record_metrics(True, False, (time.time() - t0) * 1000.0, "")
        return BestJSONResponse(content=_json_safe(sheet_payload))

    engine, engine_source, engine_err = await _get_engine(request)
    if engine is None:
        await _record_metrics(False, False, (time.time() - t0) * 1000.0, engine_err or "engine_unavailable")
        return _error(
            503,
            rid,
            "Advisor engine unavailable",
            extra={"engine_source": engine_source, "engine_error": engine_err},
        )

    result = await _run_page_pipeline(
        page=payload.get("page") or "Top_10_Investments",
        payload=payload,
        request=request,
        engine=engine,
        timeout_sec=_timeout_sec(),
        prefer_engine_sheet=True,
    )

    ok = bool((result.get("meta") or {}).get("ok", False)) or bool(_result_count(result))
    err_txt = _clean_str((result.get("meta") or {}).get("error"))
    await _record_metrics(ok, False, (time.time() - t0) * 1000.0, err_txt)

    sheet_payload = _result_to_sheet_payload(result=result, payload=payload, request_id=rid)
    return BestJSONResponse(content=_json_safe(sheet_payload))


# -----------------------------------------------------------------------------
# Route registration
# -----------------------------------------------------------------------------
def _register_common_routes(r: APIRouter) -> None:
    r.add_api_route("/health", advisor_health_handler, methods=["GET"], response_class=BestJSONResponse)
    r.add_api_route("/metrics", advisor_metrics_handler, methods=["GET"], response_class=BestJSONResponse)
    r.add_api_route("/recommendations", advisor_recommendations_handler, methods=["GET"], response_class=BestJSONResponse)
    r.add_api_route("/run", advisor_run_handler, methods=["POST"], response_class=BestJSONResponse)
    r.add_api_route("/sheet-rows", advisor_sheet_rows_get_handler, methods=["GET"], response_class=BestJSONResponse)
    r.add_api_route("/sheet-rows", advisor_sheet_rows_post_handler, methods=["POST"], response_class=BestJSONResponse)


_register_common_routes(_router_canonical)
_register_common_routes(_router_advanced)
_register_common_routes(_router_compat_us)
_register_common_routes(_router_compat_dash)

router.include_router(_router_canonical)
router.include_router(_router_advanced)
router.include_router(_router_compat_us)
router.include_router(_router_compat_dash)

__all__ = ["router", "ROUTER_VERSION", "_run_page_pipeline"]
