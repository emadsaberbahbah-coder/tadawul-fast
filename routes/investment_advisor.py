#!/usr/bin/env python3
"""
routes/investment_advisor.py
================================================================================
TFB Advanced / Investment Advisor Routes — v6.0.0
(ADVANCED-ALIAS / TOP10-RESOLVER / BUILDER-FALLBACK / SHEET-ROWS SAFE /
 ENGINE-LAZY / AUTH-BRIDGED / PROMETHEUS-FREE / JSON-SAFE)
================================================================================

What this revision fixes
- ✅ FIX: this router now owns the /v1/advanced/* family directly
- ✅ FIX: Top_10_Investments no longer relies only on the generic advisor core
- ✅ FIX: dedicated Top10/Insights builder discovery added with tolerant fallbacks
- ✅ FIX: advanced sheet-rows can recover from:
      - items-only payloads
      - dict rows
      - matrix rows
      - rows_matrix-only payloads
      - sparse meta envelopes
- ✅ FIX: generic advisor execution remains lazy and worker-thread safe
- ✅ FIX: no prometheus_client usage in this file
- ✅ FIX: auth/open-mode still bridges to core.config when available
- ✅ FIX: compatibility aliases preserved:
      - /v1/advisor/*
      - /v1/advanced/*
      - /v1/investment_advisor/*
      - /v1/investment-advisor/*
- ✅ FIX: Top10 compatibility fields are backfilled when enough context exists:
      - recommendation_reason
      - horizon_days
      - invest_period_label
      - top10_rank
      - selection_reason
      - criteria_snapshot

Endpoints
---------
Canonical advisor family
- /v1/advisor/health
- /v1/advisor/metrics
- /v1/advisor/recommendations          (GET)
- /v1/advisor/run                      (POST)
- /v1/advisor/sheet-rows               (GET/POST)

Advanced family
- /v1/advanced/health
- /v1/advanced/metrics
- /v1/advanced/recommendations         (GET)
- /v1/advanced/run                     (POST)
- /v1/advanced/sheet-rows              (GET/POST)

Compatibility aliases
- /v1/investment_advisor/*
- /v1/investment-advisor/*
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
import os
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, Query, Request


# -----------------------------------------------------------------------------
# JSON response (orjson if available) + safe fallbacks
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
    from fastapi.responses import Response as StarletteResponse

    class BestJSONResponse(StarletteResponse):
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

ROUTER_VERSION = "6.0.0"
MODULE_NAME = "routes.investment_advisor"

router = APIRouter(tags=["advisor"])
_router_canonical = APIRouter(prefix="/v1/advisor", tags=["advisor"])
_router_advanced = APIRouter(prefix="/v1/advanced", tags=["advanced"])
_router_compat_us = APIRouter(prefix="/v1/investment_advisor", tags=["investment_advisor"])
_router_compat_dash = APIRouter(prefix="/v1/investment-advisor", tags=["investment-advisor"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "active"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "inactive"}


# -----------------------------------------------------------------------------
# In-module metrics (NO prometheus)
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
# Small helpers
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


def _headers_index(headers: Sequence[Any]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for i, h in enumerate(headers or []):
        s = _clean_str(h)
        if s:
            out[s] = i
    return out


def _result_count(result: Mapping[str, Any]) -> int:
    if not isinstance(result, Mapping):
        return 0
    for key in ("items", "rows", "data", "quotes", "rows_matrix"):
        value = result.get(key)
        if isinstance(value, list):
            return len(value)
    return 0


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
    payload: Dict[str, Any] = {
        "error": message,
    }
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
            token = token

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
# Lazy engine
# -----------------------------------------------------------------------------
async def _maybe_await(v: Any) -> Any:
    return await v if inspect.isawaitable(v) else v


async def _get_engine(request: Request) -> Tuple[Optional[Any], str, Optional[str]]:
    try:
        eng = getattr(getattr(request, "app", None), "state", None)
        if eng is not None:
            state_engine = getattr(eng, "engine", None)
            if state_engine is not None:
                return state_engine, "app.state.engine", None
    except Exception:
        pass

    async with _ENGINE_INIT_LOCK:
        try:
            eng = getattr(getattr(request, "app", None), "state", None)
            if eng is not None:
                state_engine = getattr(eng, "engine", None)
                if state_engine is not None:
                    return state_engine, "app.state.engine", None
        except Exception:
            pass

        last_err = None

        for mod_name, fn_name in (
            ("core.data_engine_v2", "get_engine"),
            ("core.data_engine", "get_engine"),
        ):
            try:
                mod = _safe_import(mod_name)
                fn = getattr(mod, fn_name, None) if mod is not None else None
                if callable(fn):
                    eng = fn()
                    eng = await _maybe_await(eng)
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
# Page / schema helpers
# -----------------------------------------------------------------------------
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


def _normalize_page_name(raw: Optional[str]) -> str:
    s = _clean_str(raw)
    if not s:
        return "Top_10_Investments"

    try:
        from core.sheets.page_catalog import normalize_page_name  # type: ignore

        try:
            out = normalize_page_name(s, allow_output_pages=True)
        except TypeError:
            out = normalize_page_name(s)
        if out:
            return str(out)
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
        "data_dictionary": "Data_Dictionary",
    }
    return mapping.get(compact, s)


def _route_family(page: str) -> str:
    p = _normalize_page_name(page)
    if p == "Top_10_Investments":
        return "top10"
    if p == "Insights_Analysis":
        return "insights"
    return "advisor"


def _schema_for_page(page: str) -> Tuple[List[str], List[str]]:
    p = _normalize_page_name(page)

    try:
        from core.sheets.schema_registry import get_sheet_spec  # type: ignore

        spec = get_sheet_spec(p)
        columns = list(getattr(spec, "columns", None) or [])
        headers: List[str] = []
        keys: List[str] = []
        if columns:
            for c in columns:
                if isinstance(c, Mapping):
                    h = _clean_str(c.get("header"))
                    k = _clean_str(c.get("key"))
                else:
                    h = _clean_str(getattr(c, "header", ""))
                    k = _clean_str(getattr(c, "key", ""))
                if h:
                    headers.append(h)
                if k:
                    keys.append(k)
        if headers and keys:
            return headers, keys
    except Exception:
        pass

    if p == "Top_10_Investments":
        return list(_TOP10_HEADERS_FALLBACK), list(_TOP10_KEYS_FALLBACK)

    return list(_TOP10_HEADERS_FALLBACK), list(_TOP10_KEYS_FALLBACK)


def _align_row_to_keys(row: Mapping[str, Any], keys: Sequence[str], *, symbol_fallback: str = "") -> Dict[str, Any]:
    src = dict(row or {})
    out: Dict[str, Any] = {}
    for k in keys:
        out[k] = src.get(k)
    if symbol_fallback:
        if "symbol" in out and not out.get("symbol"):
            out["symbol"] = symbol_fallback
        if "ticker" in out and not out.get("ticker"):
            out["ticker"] = symbol_fallback
    return out


# -----------------------------------------------------------------------------
# Recommendation / context enrichment
# -----------------------------------------------------------------------------
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
    for key in ("horizon_days", "invest_period_days", "investment_period_days", "period_days", "days"):
        v = _safe_int(payload.get(key))
        if v is not None and v > 0:
            return v

    label = _clean_str(
        payload.get("invest_period_label")
        or payload.get("investment_period_label")
        or payload.get("period_label")
        or payload.get("horizon_label")
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

    if payload.get("required_roi_1m") is not None:
        return 30
    if payload.get("required_roi_3m") is not None:
        return 90
    if payload.get("required_roi_12m") is not None:
        return 365

    return None


def _infer_invest_period_label(payload: Dict[str, Any], days: Optional[int]) -> Optional[str]:
    label = _clean_str(
        payload.get("invest_period_label")
        or payload.get("investment_period_label")
        or payload.get("period_label")
        or payload.get("horizon_label")
    ).upper()
    if label:
        return label
    return _horizon_label(days)


def _criteria_snapshot(payload: Dict[str, Any]) -> str:
    compact = {
        "page": _normalize_page_name(
            payload.get("page")
            or payload.get("sheet_name")
            or payload.get("sheet")
            or payload.get("name")
            or payload.get("tab")
            or "Top_10_Investments"
        ),
        "risk_profile": payload.get("risk_profile"),
        "allocation_strategy": payload.get("allocation_strategy"),
        "invest_amount": payload.get("invest_amount"),
        "horizon_days": _infer_horizon_days(payload),
        "invest_period_label": _infer_invest_period_label(payload, _infer_horizon_days(payload)),
        "top_n": payload.get("top_n"),
        "symbols": payload.get("symbols") or payload.get("tickers"),
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

    if rec in {"BUY", "STRONG_BUY"}:
        roi_txt = _format_pct(selected_roi)
        if roi_txt:
            parts.append(f"expected {invest_period_label or 'target-horizon'} upside is {roi_txt}")
        if selected_fp is not None and current_price is not None and selected_fp > current_price:
            parts.append(f"forecast price {round(selected_fp, 2)} is above current price {round(current_price, 2)}")
        if confidence is not None:
            c_txt = _format_pct(confidence)
            if c_txt:
                parts.append(f"confidence is {c_txt}")
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

    elif rec == "SELL":
        roi_txt = _format_pct(selected_roi)
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
        roi_txt = _format_pct(selected_roi)
        if roi_txt:
            parts.append(f"expected {invest_period_label or 'target-horizon'} return is {roi_txt}")
        if overall is not None:
            parts.append(f"overall score is {round(overall, 2)}")
        if risk_bucket:
            parts.append(f"risk bucket is {risk_bucket}")
        elif risk_score is not None:
            parts.append(f"risk score is {round(risk_score, 2)}")
        if confidence is not None:
            c_txt = _format_pct(confidence)
            if c_txt:
                parts.append(f"confidence is {c_txt}")

    parts = [p for p in parts if p]
    if not parts:
        return f"{rec} based on the current risk-return profile."
    return f"{rec} because " + ", ".join(parts[:4]) + "."


def _ensure_item_context(item: Dict[str, Any], payload: Dict[str, Any], rank: Optional[int] = None) -> Dict[str, Any]:
    row = dict(item or {})

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

    return row


# -----------------------------------------------------------------------------
# Output normalization
# -----------------------------------------------------------------------------
def _rows_to_items_from_matrix(headers: Sequence[Any], rows: Sequence[Any]) -> List[Dict[str, Any]]:
    hdrs = [_clean_str(h) for h in headers or []]
    keys = [_snake_like(h) for h in hdrs]
    items: List[Dict[str, Any]] = []
    for row in rows or []:
        if isinstance(row, Mapping):
            items.append(dict(row))
            continue
        vals = list(row) if isinstance(row, (list, tuple)) else [row]
        item: Dict[str, Any] = {}
        for i, k in enumerate(keys):
            item[k] = vals[i] if i < len(vals) else None
        items.append(item)
    return items


def _headers_from_items(items: Sequence[Mapping[str, Any]]) -> Tuple[List[str], List[str]]:
    keys: List[str] = []
    headers: List[str] = []
    seen = set()
    for item in items or []:
        for k in item.keys():
            s = _clean_str(k)
            if not s or s in seen:
                continue
            seen.add(s)
            keys.append(s)
            headers.append(s.replace("_", " ").title())
    return headers, keys


def _normalize_any_result(
    *,
    page: str,
    raw: Any,
    payload: Dict[str, Any],
    source: str,
) -> Dict[str, Any]:
    schema_headers, schema_keys = _schema_for_page(page)
    norm: Dict[str, Any] = {
        "headers": list(schema_headers),
        "keys": list(schema_keys),
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

    if isinstance(raw, Mapping):
        data = dict(raw)

        headers = _as_list(
            data.get("headers")
            or data.get("display_headers")
            or data.get("sheet_headers")
            or data.get("column_headers")
        )
        keys = _as_list(data.get("keys") or data.get("fields") or data.get("column_keys") or data.get("schema_keys"))
        rows = data.get("rows")
        items = data.get("items")
        data_rows = data.get("data")
        quotes = data.get("quotes")
        rows_matrix = data.get("rows_matrix")
        values = data.get("values")

        if not isinstance(items, list):
            if isinstance(rows, list) and rows and all(isinstance(r, Mapping) for r in rows):
                items = [dict(r) for r in rows]
            elif isinstance(data_rows, list) and data_rows and all(isinstance(r, Mapping) for r in data_rows):
                items = [dict(r) for r in data_rows]
            elif isinstance(quotes, list) and quotes and all(isinstance(r, Mapping) for r in quotes):
                items = [dict(r) for r in quotes]
            elif isinstance(rows, list) and headers:
                items = _rows_to_items_from_matrix(headers, rows)
            elif isinstance(data_rows, list) and headers:
                items = _rows_to_items_from_matrix(headers, data_rows)
            elif isinstance(rows_matrix, list) and headers:
                items = _rows_to_items_from_matrix(headers, rows_matrix)
            elif isinstance(values, list) and headers:
                items = _rows_to_items_from_matrix(headers, values)

        if isinstance(items, list) and items:
            fixed_items: List[Dict[str, Any]] = []
            for i, item in enumerate(items, start=1):
                row_map = dict(item) if isinstance(item, Mapping) else {}
                row_map = _ensure_item_context(row_map, payload, rank=i)
                fixed_items.append(row_map)
            items = fixed_items
        else:
            items = []

        if headers:
            headers = [_clean_str(h) for h in headers if _clean_str(h)]
        if keys:
            keys = [_clean_str(k) for k in keys if _clean_str(k)]

        if not headers and items:
            headers, _ = _headers_from_items(items)
        if not keys and items:
            _, keys = _headers_from_items(items)

        if not headers:
            headers = list(schema_headers)
        if not keys:
            keys = list(schema_keys)

        norm["headers"] = list(headers)
        norm["keys"] = list(keys)
        norm["items"] = list(items)
        norm["rows"] = list(items)
        norm["rows_matrix"] = [[item.get(k) for k in keys] for item in items]

        meta = data.get("meta") if isinstance(data.get("meta"), Mapping) else {}
        meta = dict(meta)
        explicit_ok = meta.get("ok")
        count = len(items)
        status_txt = _clean_str(data.get("status")).lower()
        meta["ok"] = bool(explicit_ok) if explicit_ok is not None else bool(count > 0 or status_txt in {"ok", "success", "partial"})
        meta["count"] = count if meta.get("count") is None else meta.get("count")
        meta["source"] = source
        meta["router_version"] = ROUTER_VERSION
        if "error" in data and not meta.get("error"):
            meta["error"] = data.get("error")
        norm["meta"] = meta
        return norm

    if isinstance(raw, list):
        if raw and all(isinstance(r, Mapping) for r in raw):
            items = [_ensure_item_context(dict(r), payload, rank=i + 1) for i, r in enumerate(raw)]
            headers, keys = _headers_from_items(items)
            if not headers:
                headers = list(schema_headers)
            if not keys:
                keys = list(schema_keys)
            norm["headers"] = headers
            norm["keys"] = keys
            norm["items"] = items
            norm["rows"] = items
            norm["rows_matrix"] = [[item.get(k) for k in keys] for item in items]
            norm["meta"] = {"ok": True, "count": len(items), "source": source, "router_version": ROUTER_VERSION}
            return norm

        if raw and all(isinstance(r, (list, tuple)) for r in raw):
            headers = list(schema_headers)
            keys = list(schema_keys)
            items = _rows_to_items_from_matrix(headers, raw)
            items = [_ensure_item_context(dict(r), payload, rank=i + 1) for i, r in enumerate(items)]
            norm["headers"] = headers
            norm["keys"] = keys
            norm["items"] = items
            norm["rows"] = items
            norm["rows_matrix"] = [[item.get(k) for k in keys] for item in items]
            norm["meta"] = {"ok": bool(items), "count": len(items), "source": source, "router_version": ROUTER_VERSION}
            return norm

    if isinstance(raw, tuple) and len(raw) == 2:
        headers = _as_list(raw[0])
        rows = _as_list(raw[1])
        items = _rows_to_items_from_matrix(headers, rows)
        items = [_ensure_item_context(dict(r), payload, rank=i + 1) for i, r in enumerate(items)]
        keys = [_snake_like(h) for h in headers] if headers else list(schema_keys)
        norm["headers"] = [_clean_str(h) for h in headers] if headers else list(schema_headers)
        norm["keys"] = keys
        norm["items"] = items
        norm["rows"] = items
        norm["rows_matrix"] = [[item.get(k) for k in keys] for item in items]
        norm["meta"] = {"ok": bool(items), "count": len(items), "source": source, "router_version": ROUTER_VERSION}
        return norm

    return norm


# -----------------------------------------------------------------------------
# Builder / runner discovery
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
                    "run_top10_selector",
                    "build_top10_rows",
                    "build_top10_sheet",
                    "build_top10",
                    "run_top10",
                    "select_top10",
                    "run_selector",
                    "build_top_10_investments",
                    "get_top10_rows",
                    "get_rows",
                ],
            ),
            (
                "core.top10_selector",
                [
                    "run_top10_selector",
                    "build_top10_rows",
                    "build_top10",
                    "select_top10",
                ],
            ),
            *module_specs,
        ]
    elif page_norm == "Insights_Analysis":
        module_specs = [
            (
                "core.analysis.insights_analysis",
                [
                    "build_insights_rows",
                    "build_insights_sheet",
                    "run_insights",
                    "run_insights_analysis",
                    "build_insights_analysis",
                    "get_rows",
                ],
            ),
            (
                "core.analysis.insights_builder",
                [
                    "build_insights_rows",
                    "build_insights_sheet",
                    "run_insights",
                    "get_rows",
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

        for factory_name in (
            "get_service",
            "get_builder",
            "create_service",
            "create_builder",
            "get_selector",
            "create_selector",
        ):
            try:
                factory = getattr(mod, factory_name, None)
                if callable(factory):
                    obj = factory()
                    if obj is not None:
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
                                _add(f"{mod_name}.{factory_name}().{method}", fn)
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
    kwargs_attempts = [
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
        {
            "payload": payload,
        },
        {
            "request": payload,
        },
        {
            "body": payload,
        },
        {
            "page": page_norm,
        },
        {},
    ]

    last_err = ""

    for kwargs in kwargs_attempts:
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

    return False, None, last_err


# -----------------------------------------------------------------------------
# Generic advisor runner
# -----------------------------------------------------------------------------
def _run_engine_call_variants(payload: Dict[str, Any], engine: Any) -> Dict[str, Any]:
    errors: List[str] = []

    try:
        from core.investment_advisor_engine import run_investment_advisor_engine as run_engine  # type: ignore

        attempts = [
            {"payload": payload, "data_engine": engine, "quote_engine": engine, "cache_engine": engine},
            {"payload": payload, "engine": engine},
            {"request": payload, "engine": engine},
            {"body": payload, "engine": engine},
            {"payload": payload},
            {"request": payload},
            {"body": payload},
        ]

        for kwargs in attempts:
            try:
                out = run_engine(**kwargs)
                if isinstance(out, dict):
                    return out
                if out is not None:
                    return {"data": out}
            except TypeError as e:
                errors.append(f"run_investment_advisor_engine TypeError: {e}")
                continue
            except Exception as e:
                errors.append(f"run_investment_advisor_engine {type(e).__name__}: {e}")
                break
    except Exception as e:
        errors.append(f"import run_investment_advisor_engine failed: {type(e).__name__}: {e}")

    try:
        from core.investment_advisor_engine import run_investment_advisor as run_engine_legacy  # type: ignore

        attempts = [
            {"payload": payload, "engine": engine},
            {"request": payload, "engine": engine},
            {"body": payload, "engine": engine},
            {"payload": payload, "data_engine": engine, "quote_engine": engine},
            {"payload": payload},
            {"request": payload},
            {"body": payload},
        ]

        for kwargs in attempts:
            try:
                out = run_engine_legacy(**kwargs)
                if isinstance(out, dict):
                    return out
                if out is not None:
                    return {"data": out}
            except TypeError as e:
                errors.append(f"run_investment_advisor TypeError: {e}")
                continue
            except Exception as e:
                errors.append(f"run_investment_advisor {type(e).__name__}: {e}")
                break
    except Exception as e:
        errors.append(f"import run_investment_advisor failed: {type(e).__name__}: {e}")

    return {
        "headers": [],
        "rows": [],
        "items": [],
        "meta": {"ok": False, "error": " | ".join(errors)[:3000]},
    }


async def _run_generic_advisor(payload: Dict[str, Any], *, page: str, engine: Any, timeout_sec: float) -> Dict[str, Any]:
    def _call() -> Dict[str, Any]:
        return _run_engine_call_variants(payload or {}, engine)

    try:
        raw = await asyncio.wait_for(asyncio.to_thread(_call), timeout=timeout_sec)
    except asyncio.TimeoutError:
        raw = {
            "headers": [],
            "rows": [],
            "items": [],
            "meta": {"ok": False, "error": f"timeout({timeout_sec}s)"},
        }
    except Exception as e:
        raw = {
            "headers": [],
            "rows": [],
            "items": [],
            "meta": {"ok": False, "error": f"{type(e).__name__}: {e}"},
        }

    return _normalize_any_result(page=page, raw=raw, payload=payload, source="generic_advisor")


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
    best_count = -1
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
        count = _result_count(normalized)
        meta = normalized.get("meta") if isinstance(normalized.get("meta"), Mapping) else {}
        ok_flag = bool(meta.get("ok", False))

        if count > best_count:
            best = normalized
            best_count = count

        if ok_flag and count > 0:
            return normalized

    if best is not None:
        meta = best.get("meta") if isinstance(best.get("meta"), Mapping) else {}
        meta = dict(meta)
        if errors and not meta.get("error"):
            meta["error"] = " | ".join(errors)[:3000]
        best["meta"] = meta
        return best

    return {
        "headers": list(_schema_for_page(page)[0]),
        "keys": list(_schema_for_page(page)[1]),
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


async def _run_engine_page_fallback(
    *,
    page: str,
    payload: Dict[str, Any],
    request: Request,
    engine: Any,
    timeout_sec: float,
) -> Dict[str, Any]:
    page_norm = _normalize_page_name(page)
    method_names = [
        "build_sheet_rows",
        "get_sheet_rows",
        "sheet_rows",
        "build_page_rows",
        "get_page_rows",
        "get_cached_sheet_rows",
        "get_sheet_snapshot",
        "get_cached_sheet_snapshot",
    ]
    errors: List[str] = []

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
        if _result_count(normalized) > 0:
            return normalized

    return {
        "headers": list(_schema_for_page(page_norm)[0]),
        "keys": list(_schema_for_page(page_norm)[1]),
        "items": [],
        "rows": [],
        "rows_matrix": [],
        "meta": {
            "ok": False,
            "source": "engine_page_fallback",
            "router_version": ROUTER_VERSION,
            "error": " | ".join(errors)[:3000] if errors else "no_engine_page_method_available",
        },
    }


async def _run_page_pipeline(
    *,
    page: str,
    payload: Dict[str, Any],
    request: Request,
    engine: Any,
    timeout_sec: float,
) -> Dict[str, Any]:
    page_norm = _normalize_page_name(page)

    # Top10: prefer the richest successful result, not merely the first non-empty one.
    if page_norm == "Top_10_Investments":
        candidates: List[Dict[str, Any]] = []

        special = await _run_special_page_builder(
            page=page_norm,
            payload=payload,
            request=request,
            engine=engine,
            timeout_sec=timeout_sec,
        )
        candidates.append(special)

        generic = await _run_generic_advisor(payload, page=page_norm, engine=engine, timeout_sec=timeout_sec)
        candidates.append(generic)

        engine_fb = await _run_engine_page_fallback(
            page=page_norm,
            payload=payload,
            request=request,
            engine=engine,
            timeout_sec=timeout_sec,
        )
        candidates.append(engine_fb)

        def _score(res: Mapping[str, Any]) -> Tuple[int, int, int]:
            count = _result_count(res)
            meta = res.get("meta") if isinstance(res.get("meta"), Mapping) else {}
            ok_flag = 1 if bool(meta.get("ok", False)) else 0
            has_error = 0 if _clean_str(meta.get("error")) else 1
            return (count, ok_flag, has_error)

        candidates = [c for c in candidates if isinstance(c, Mapping)]
        if not candidates:
            return special

        best = sorted(candidates, key=_score, reverse=True)[0]
        return dict(best)

    # Insights: keep special/builder-first behavior to stay lightweight and aligned.
    if page_norm == "Insights_Analysis":
        special = await _run_special_page_builder(
            page=page_norm,
            payload=payload,
            request=request,
            engine=engine,
            timeout_sec=timeout_sec,
        )
        if _result_count(special) > 0:
            return special

        generic = await _run_generic_advisor(payload, page=page_norm, engine=engine, timeout_sec=timeout_sec)
        if _result_count(generic) > 0:
            return generic

        engine_fb = await _run_engine_page_fallback(
            page=page_norm,
            payload=payload,
            request=request,
            engine=engine,
            timeout_sec=timeout_sec,
        )
        return engine_fb

    # Standard advisor pages: generic first, engine-page fallback second.
    generic = await _run_generic_advisor(payload, page=page_norm, engine=engine, timeout_sec=timeout_sec)
    if _result_count(generic) > 0:
        return generic

    engine_fb = await _run_engine_page_fallback(
        page=page_norm,
        payload=payload,
        request=request,
        engine=engine,
        timeout_sec=timeout_sec,
    )
    return engine_fb


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
    for key in ("symbols", "tickers", "symbol", "ticker"):
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

    if out.get("limit") is not None and out.get("top_n") is None:
        try:
            out["top_n"] = int(out.get("limit"))
        except Exception:
            pass

    if out.get("top_n") is None:
        out["top_n"] = 10 if chosen_page == "Top_10_Investments" else 20

    if out.get("include_headers") is None:
        out["include_headers"] = "true"
    if out.get("include_matrix") is None:
        out["include_matrix"] = "true"

    if out.get("invest_period_days") is None and out.get("horizon_days") is not None:
        out["invest_period_days"] = out.get("horizon_days")
    if out.get("horizon_days") is None and out.get("invest_period_days") is not None:
        out["horizon_days"] = out.get("invest_period_days")

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
        "top_n": top_n,
        "invest_amount": invest_amount,
        "allocation_strategy": allocation_strategy,
        "risk_profile": risk_profile,
        "horizon_days": horizon_days,
        "invest_period_days": horizon_days,
        "invest_period_label": invest_period_label,
        "include_headers": include_headers or "true",
        "include_matrix": include_matrix or "true",
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
        debug=debug,
    )
    return _normalize_payload(payload)


# -----------------------------------------------------------------------------
# Result -> outward envelopes
# -----------------------------------------------------------------------------
def _rows_matrix(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
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
    keys = list(schema_keys or _TOP10_KEYS_FALLBACK)
    headers = list(schema_headers or _TOP10_HEADERS_FALLBACK)

    items = result.get("items")
    rows = result.get("rows")
    result_headers = result.get("headers")

    normalized_items: List[Dict[str, Any]] = []

    if isinstance(items, list) and items:
        for i, item in enumerate(items, start=1):
            row_map = dict(item) if isinstance(item, Mapping) else {}
            row_map = _ensure_item_context(row_map, payload, rank=i)
            normalized_items.append(_align_row_to_keys(row_map, keys, symbol_fallback=_clean_str(row_map.get("symbol"))))

    elif isinstance(rows, list) and rows:
        if all(isinstance(r, Mapping) for r in rows):
            for i, item in enumerate(rows, start=1):
                row_map = _ensure_item_context(dict(item), payload, rank=i)
                normalized_items.append(_align_row_to_keys(row_map, keys, symbol_fallback=_clean_str(row_map.get("symbol"))))
        elif isinstance(result_headers, list):
            row_items = _rows_to_items_from_matrix(result_headers, rows)
            for i, item in enumerate(row_items, start=1):
                row_map = _ensure_item_context(dict(item), payload, rank=i)
                normalized_items.append(_align_row_to_keys(row_map, keys, symbol_fallback=_clean_str(row_map.get("symbol"))))

    matrix = _rows_matrix(normalized_items, keys)

    meta = result.get("meta") if isinstance(result.get("meta"), Mapping) else {}
    meta = dict(meta)
    ok_flag = bool(meta.get("ok", False)) or bool(normalized_items)
    error = _clean_str(meta.get("error")) or None

    meta["router_version"] = ROUTER_VERSION
    meta["count"] = len(normalized_items)
    meta["sheet_mode"] = True
    meta["page"] = page_norm

    return {
        "status": "success" if ok_flag else ("partial" if normalized_items else "error"),
        "page": page_norm,
        "route_family": route_family,
        "headers": headers if include_headers else [],
        "keys": keys,
        "rows": normalized_items,
        "rows_matrix": matrix if include_matrix else [],
        "quotes": normalized_items,
        "data": normalized_items,
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
        debug=debug,
    )

    result = await _run_page_pipeline(
        page=payload.get("page") or "Top_10_Investments",
        payload=payload,
        request=request,
        engine=engine,
        timeout_sec=_timeout_sec(),
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
        debug=debug,
    )

    result = await _run_page_pipeline(
        page=payload.get("page") or "Top_10_Investments",
        payload=payload,
        request=request,
        engine=engine,
        timeout_sec=_timeout_sec(),
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

__all__ = ["router", "ROUTER_VERSION"]
