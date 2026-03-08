#!/usr/bin/env python3
"""
routes/investment_advisor.py
================================================================================
TFB Investment Advisor Routes — v5.9.0
(PROMETHEUS-SAFE / ENGINE-LAZY / CORE-CONFIG-BRIDGE / CONTEXT-AWARE /
 COMPAT-ALIAS / SHEET-ROWS COMPAT)
================================================================================

What this revision fixes
- ✅ Keeps NO prometheus_client metric creation in this module
- ✅ Keeps engine-lazy behavior (no heavy imports / no network at import time)
- ✅ Uses core.config as auth/open-mode source of truth when available
- ✅ Delegates execution to core/investment_advisor_engine.py as canonical executor
- ✅ FIX: post-processes advisor result items + rows to ensure:
      - recommendation_reason
      - horizon_days
      - invest_period_label
      are filled when enough context exists
- ✅ FIX: keeps response envelope backward-compatible
- ✅ FIX: thread-safe execution of core advisor in worker thread
- ✅ FIX: supports both engine entrypoints:
      - run_investment_advisor_engine(...)
      - run_investment_advisor(...)
- ✅ FIX: tolerant to dict-rows OR matrix-rows OR items-only returns
- ✅ NEW: compatibility aliases added:
      - /v1/investment_advisor/*
      - /v1/investment-advisor/*
- ✅ NEW: GET/POST /sheet-rows compatibility routes added
- ✅ NEW: sheet-rows output can be used by legacy/live checkers expecting:
      - headers
      - keys
      - rows
      - rows_matrix
- ✅ NEW: Top10 compatibility fields are backfilled when possible:
      - top10_rank
      - selection_reason
      - criteria_snapshot

Endpoints
---------
Canonical
- /v1/advisor/health
- /v1/advisor/metrics
- /v1/advisor/recommendations   (GET)
- /v1/advisor/run               (POST)
- /v1/advisor/sheet-rows        (GET/POST)

Compatibility aliases
- /v1/investment_advisor/health
- /v1/investment_advisor/metrics
- /v1/investment_advisor/recommendations
- /v1/investment_advisor/run
- /v1/investment_advisor/sheet-rows     (GET/POST)

- /v1/investment-advisor/health
- /v1/investment-advisor/metrics
- /v1/investment-advisor/recommendations
- /v1/investment-advisor/run
- /v1/investment-advisor/sheet-rows     (GET/POST)
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Query, Request

# -----------------------------------------------------------------------------
# JSON response (orjson if available) + NaN safe
# -----------------------------------------------------------------------------
def _clean_nans(obj: Any) -> Any:
    try:
        import math

        if isinstance(obj, dict):
            return {k: _clean_nans(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_clean_nans(v) for v in obj]
        if isinstance(obj, tuple):
            return [_clean_nans(v) for v in obj]
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
    except Exception:
        pass
    return obj


try:
    import orjson  # type: ignore
    from fastapi.responses import Response as StarletteResponse

    class BestJSONResponse(StarletteResponse):
        media_type = "application/json"

        def render(self, content: Any) -> bytes:
            return orjson.dumps(_clean_nans(content), default=str)

    def _json_dumps(v: Any) -> str:
        return orjson.dumps(_clean_nans(v), default=str).decode("utf-8")

except Exception:
    import json as _json_std
    from fastapi.responses import JSONResponse as BestJSONResponse  # type: ignore

    def _json_dumps(v: Any) -> str:
        return _json_std.dumps(_clean_nans(v), default=str, ensure_ascii=False)


logger = logging.getLogger("routes.investment_advisor")

ROUTER_VERSION = "5.9.0"

# Root router plus compatibility subrouters
router = APIRouter(tags=["advisor"])
_router_canonical = APIRouter(prefix="/v1/advisor", tags=["advisor"])
_router_compat_us = APIRouter(prefix="/v1/investment_advisor", tags=["investment_advisor"])
_router_compat_dash = APIRouter(prefix="/v1/investment-advisor", tags=["investment-advisor"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "active"}

# -----------------------------------------------------------------------------
# Small in-module metrics (NO prometheus)
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
        up = max(0.0, time.time() - self.started_at)
        return {
            "uptime_sec": round(up, 3),
            "requests_total": self.requests_total,
            "success_total": self.success_total,
            "unauthorized_total": self.unauthorized_total,
            "errors_total": self.errors_total,
            "last_latency_ms": round(float(self.last_latency_ms), 3),
            "last_error": (self.last_error or "")[:600],
        }


_METRICS = _AdvisorMetrics(started_at=time.time())
_METRICS_LOCK = asyncio.Lock()
_ENGINE_INIT_LOCK = asyncio.Lock()


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _request_id(request: Request) -> str:
    try:
        rid = getattr(request.state, "request_id", None)
        if rid:
            return str(rid)
    except Exception:
        pass
    return request.headers.get("X-Request-ID") or str(uuid.uuid4())[:12]


def _split_symbols(raw: str) -> List[str]:
    s = (raw or "").replace("\n", " ").replace("\t", " ").replace(",", " ").strip()
    parts = [p.strip() for p in s.split(" ") if p.strip()]
    out: List[str] = []
    seen = set()
    for p in parts:
        u = p.upper()
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _env_bool(name: str, default: bool = False) -> bool:
    try:
        v = (os.getenv(name, str(default)) or "").strip().lower()
        return v in _TRUTHY
    except Exception:
        return default


def _safe_is_open_mode() -> Optional[bool]:
    try:
        from core.config import is_open_mode  # type: ignore

        if callable(is_open_mode):
            return bool(is_open_mode())
        return None
    except Exception:
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


def _extract_token(request: Request) -> Tuple[Optional[str], Optional[str]]:
    authz = request.headers.get("Authorization")
    token = (
        request.headers.get("X-APP-TOKEN")
        or request.headers.get("X-App-Token")
        or request.headers.get("X-API-KEY")
        or request.headers.get("X-Api-Key")
    )

    if (not token) and authz and authz.strip().lower().startswith("bearer "):
        try:
            token = authz.strip().split(" ", 1)[1].strip()
        except Exception:
            token = token

    allow_q = False
    try:
        from core.config import get_settings_cached  # type: ignore

        st = get_settings_cached()
        allow_q = bool(getattr(st, "allow_query_token", False)) if st else False
    except Exception:
        allow_q = _env_bool("ALLOW_QUERY_TOKEN", False)

    if allow_q and not token:
        qt = request.query_params.get("token")
        if qt:
            token = qt

    tok = token.strip() if isinstance(token, str) and token.strip() else None
    return tok, authz


def _auth_ok(request: Request) -> bool:
    try:
        from core.config import is_open_mode, auth_ok  # type: ignore

        if callable(is_open_mode) and is_open_mode():
            return True

        token, authz = _extract_token(request)

        if callable(auth_ok):
            return bool(auth_ok(token=token, authorization=authz, headers=dict(request.headers)))
        return False
    except Exception:
        if _env_bool("REQUIRE_AUTH", True):
            return False
        return True


async def _maybe_await(v: Any) -> Any:
    return await v if inspect.isawaitable(v) else v


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


def _get_list(payload: Mapping[str, Any], *keys: str) -> List[str]:
    for k in keys:
        v = payload.get(k)
        if isinstance(v, list):
            out: List[str] = []
            for item in v:
                s = str(item or "").strip()
                if s:
                    out.append(s)
            if out:
                return out
        if isinstance(v, str) and v.strip():
            parts = [p.strip() for p in v.replace("\n", ",").split(",") if p.strip()]
            if parts:
                return parts
    return []


def _rows_matrix(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    out: List[List[Any]] = []
    for row in rows:
        out.append([row.get(k) for k in keys])
    return out


def _model_to_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return dict(obj)

    try:
        if hasattr(obj, "model_dump"):
            d = obj.model_dump()  # type: ignore[attr-defined]
            return d if isinstance(d, dict) else {}
    except Exception:
        pass

    try:
        if hasattr(obj, "dict"):
            d = obj.dict()  # type: ignore[attr-defined]
            return d if isinstance(d, dict) else {}
    except Exception:
        pass

    try:
        d = getattr(obj, "__dict__", None)
        if isinstance(d, dict):
            return dict(d)
    except Exception:
        pass

    return {}


def _snake_like(header: str) -> str:
    s = str(header or "").strip().replace("%", " pct").replace("/", " ")
    out = []
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


def _headers_index(headers: List[Any]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for i, h in enumerate(headers or []):
        txt = str(h or "").strip()
        if txt:
            out[txt] = i
    return out


# -----------------------------------------------------------------------------
# Lazy engine
# -----------------------------------------------------------------------------
async def _get_engine(request: Request) -> Tuple[Optional[Any], str, Optional[str]]:
    try:
        eng = getattr(request.app.state, "engine", None)
        if eng is not None:
            return eng, "app.state.engine", None
    except Exception:
        pass

    async with _ENGINE_INIT_LOCK:
        try:
            eng = getattr(request.app.state, "engine", None)
            if eng is not None:
                return eng, "app.state.engine", None
        except Exception:
            pass

        last_err = None

        try:
            from core.data_engine_v2 import get_engine  # type: ignore

            e = get_engine()
            eng = await _maybe_await(e)
            try:
                request.app.state.engine = eng
            except Exception:
                pass
            return eng, "core.data_engine_v2.get_engine", None
        except Exception as e:
            last_err = f"core.data_engine_v2.get_engine: {type(e).__name__}: {e}"

        try:
            from core.data_engine import get_engine as get_engine_legacy  # type: ignore

            e = get_engine_legacy()
            eng = await _maybe_await(e)
            try:
                request.app.state.engine = eng
            except Exception:
                pass
            return eng, "core.data_engine.get_engine", None
        except Exception as e:
            last_err = f"{last_err} | core.data_engine.get_engine: {type(e).__name__}: {e}"

        return None, "engine_init_failed", last_err


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
            _METRICS.last_error = (err or "")[:600]


def _error(
    status_code: int,
    request_id: str,
    message: str,
    *,
    extra: Optional[Dict[str, Any]] = None,
) -> BestJSONResponse:
    payload: Dict[str, Any] = {
        "status": "error",
        "error": message,
        "request_id": request_id,
        "timestamp_utc": _now_utc(),
        "version": ROUTER_VERSION,
    }
    if extra:
        payload["meta"] = extra
    return BestJSONResponse(status_code=status_code, content=_clean_nans(payload))


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
    s = str(raw or "").strip()
    if not s:
        return "Top_10_Investments"

    try:
        from core.sheets.page_catalog import normalize_page_name  # type: ignore

        out = normalize_page_name(s, allow_output_pages=True)
        if out:
            return str(out)
    except Exception:
        pass

    compact = s.replace("-", "_").replace(" ", "_").lower()
    mapping = {
        "top_10_investments": "Top_10_Investments",
        "top10_investments": "Top_10_Investments",
        "top10": "Top_10_Investments",
        "insights_analysis": "Insights_Analysis",
        "insights": "Insights_Analysis",
        "advisor": "Top_10_Investments",
        "investment_advisor": "Top_10_Investments",
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
        cols = list(getattr(spec, "columns", None) or [])

        headers: List[str] = []
        keys: List[str] = []

        if cols:
            for c in cols:
                if isinstance(c, Mapping):
                    h = str(c.get("header") or "").strip()
                    k = str(c.get("key") or "").strip()
                else:
                    h = str(getattr(c, "header", "") or "").strip()
                    k = str(getattr(c, "key", "") or "").strip()
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

    generic_keys = list(_TOP10_KEYS_FALLBACK)
    generic_headers = list(_TOP10_HEADERS_FALLBACK)
    return generic_headers, generic_keys


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
# Helpers for advisory-context propagation
# -----------------------------------------------------------------------------
def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        f = float(v)
        if f != f:
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


def _clean_reason_text(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    return " ".join(s.split()).strip(" |;,-.")


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

    label = str(
        payload.get("invest_period_label")
        or payload.get("investment_period_label")
        or payload.get("period_label")
        or payload.get("horizon_label")
        or ""
    ).strip().upper()

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
    label = str(
        payload.get("invest_period_label")
        or payload.get("investment_period_label")
        or payload.get("period_label")
        or payload.get("horizon_label")
        or ""
    ).strip().upper()
    if label:
        return label
    return _horizon_label(days)


def _criteria_snapshot(payload: Dict[str, Any]) -> str:
    compact = {
        "risk_profile": payload.get("risk_profile"),
        "allocation_strategy": payload.get("allocation_strategy"),
        "invest_amount": payload.get("invest_amount"),
        "horizon_days": _infer_horizon_days(payload),
        "invest_period_label": _infer_invest_period_label(payload, _infer_horizon_days(payload)),
        "top_n": payload.get("top_n"),
    }
    compact = {k: v for k, v in compact.items() if v not in (None, "", [], {})}
    try:
        return _json_dumps(compact)
    except Exception:
        return str(compact)


def _build_recommendation_reason(row: Dict[str, Any], payload: Dict[str, Any]) -> Optional[str]:
    rec = str(row.get("recommendation") or "").strip().upper()
    if not rec:
        return None

    horizon_days = _safe_int(row.get("horizon_days"))
    if horizon_days is None:
        horizon_days = _infer_horizon_days(payload)

    invest_period_label = str(row.get("invest_period_label") or "").strip()
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
    risk_bucket = str(row.get("risk_bucket") or "").strip()
    risk_score = _safe_float(row.get("risk_score"))
    valuation = _safe_float(row.get("valuation_score"))
    momentum = _safe_float(row.get("momentum_score"))
    quality = _safe_float(row.get("quality_score"))
    opportunity = _safe_float(row.get("opportunity_score"))
    liquidity_score = _safe_float(row.get("liquidity_score"))

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

    if rec == "BUY":
        roi_txt = _format_pct(selected_roi)
        if roi_txt:
            parts.append(f"expected {invest_period_label or 'target-horizon'} upside is {roi_txt}")
        if selected_fp is not None and current_price is not None and selected_fp > current_price:
            parts.append(f"forecast price {round(selected_fp, 2)} is above current price {round(current_price, 2)}")
        if confidence is not None:
            c_txt = _format_pct(confidence)
            if c_txt:
                parts.append(f"confidence is {c_txt}")
        if overall is not None and overall >= 65:
            parts.append(f"overall score is strong at {round(overall, 2)}")
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
        elif risk_score is not None and risk_score <= 45:
            parts.append(f"risk score is contained at {round(risk_score, 2)}")

    elif rec == "SELL":
        roi_txt = _format_pct(selected_roi)
        if roi_txt:
            parts.append(f"expected {invest_period_label or 'target-horizon'} return is {roi_txt}")
        if selected_fp is not None and current_price is not None and selected_fp < current_price:
            parts.append(f"forecast price {round(selected_fp, 2)} is below current price {round(current_price, 2)}")
        if overall is not None and overall <= 40:
            parts.append(f"overall score is weak at {round(overall, 2)}")
        if momentum is not None and momentum <= 40:
            parts.append("momentum is weak")
        if risk_bucket:
            parts.append(f"risk bucket is {risk_bucket}")
        elif risk_score is not None and risk_score >= 65:
            parts.append(f"risk score is elevated at {round(risk_score, 2)}")
        if confidence is not None:
            c_txt = _format_pct(confidence)
            if c_txt:
                parts.append(f"confidence is {c_txt}")

    else:
        roi_txt = _format_pct(selected_roi)
        if roi_txt:
            parts.append(f"expected {invest_period_label or 'target-horizon'} return is {roi_txt}")
        if overall is not None:
            parts.append(f"overall score is balanced at {round(overall, 2)}")
        if risk_bucket:
            parts.append(f"risk bucket is {risk_bucket}")
        elif risk_score is not None:
            parts.append(f"risk score is {round(risk_score, 2)}")
        if confidence is not None:
            c_txt = _format_pct(confidence)
            if c_txt:
                parts.append(f"confidence is {c_txt}")

    if liquidity_score is not None:
        parts.append(f"liquidity score is {round(liquidity_score, 2)}")

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

    if row.get("recommendation") and not _clean_reason_text(row.get("recommendation_reason")):
        row["recommendation_reason"] = _build_recommendation_reason(row, payload)

    if rank is not None and row.get("top10_rank") is None:
        row["top10_rank"] = rank

    if not row.get("selection_reason") and row.get("recommendation_reason"):
        row["selection_reason"] = row.get("recommendation_reason")

    if not row.get("criteria_snapshot"):
        row["criteria_snapshot"] = _criteria_snapshot(payload)

    return row


def _normalize_result_items(result: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    items = result.get("items")
    if not isinstance(items, list):
        return result

    changed = False
    new_items: List[Any] = []
    for i, item in enumerate(items, start=1):
        if isinstance(item, dict):
            fixed = _ensure_item_context(item, payload, rank=i)
            new_items.append(fixed)
            if fixed != item:
                changed = True
        else:
            new_items.append(item)

    if changed:
        result["items"] = new_items
    return result


def _normalize_result_rows(result: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(result, dict):
        return result

    rows = result.get("rows")
    headers = result.get("headers")

    if isinstance(rows, list) and rows and all(isinstance(r, dict) for r in rows):
        changed_rows = False
        new_rows: List[Any] = []
        for i, item in enumerate(rows, start=1):
            row = dict(item)
            fixed = _ensure_item_context(row, payload, rank=i)
            new_rows.append(fixed)
            if fixed != item:
                changed_rows = True
        if changed_rows:
            result["rows"] = new_rows
        return result

    if not (isinstance(headers, list) and isinstance(rows, list)):
        return result

    idx = _headers_index(headers)

    need_reason = "Recommendation Reason" not in idx
    need_horizon = "Horizon Days" not in idx
    need_label = "Invest Period Label" not in idx
    need_rank = "Top10 Rank" not in idx
    need_select_reason = "Selection Reason" not in idx
    need_criteria_snapshot = "Criteria Snapshot" not in idx

    if need_reason:
        headers = list(headers) + ["Recommendation Reason"]
    if need_horizon:
        headers = list(headers) + ["Horizon Days"]
    if need_label:
        headers = list(headers) + ["Invest Period Label"]
    if need_rank:
        headers = list(headers) + ["Top10 Rank"]
    if need_select_reason:
        headers = list(headers) + ["Selection Reason"]
    if need_criteria_snapshot:
        headers = list(headers) + ["Criteria Snapshot"]

    idx = _headers_index(headers)
    changed_rows = need_reason or need_horizon or need_label or need_rank or need_select_reason or need_criteria_snapshot
    new_rows: List[Any] = []

    for row_no, r in enumerate(rows, start=1):
        if not isinstance(r, (list, tuple)):
            new_rows.append(r)
            continue

        rr = list(r)
        while len(rr) < len(headers):
            rr.append(None)

        row_dict: Dict[str, Any] = {}
        for h, i in idx.items():
            if i < len(rr):
                row_dict[_snake_like(h)] = rr[i]

        fixed = _ensure_item_context(row_dict, payload, rank=row_no)

        pairs = [
            ("Recommendation Reason", fixed.get("recommendation_reason")),
            ("Horizon Days", fixed.get("horizon_days")),
            ("Invest Period Label", fixed.get("invest_period_label")),
            ("Top10 Rank", fixed.get("top10_rank")),
            ("Selection Reason", fixed.get("selection_reason")),
            ("Criteria Snapshot", fixed.get("criteria_snapshot")),
        ]

        for header_name, value in pairs:
            if header_name in idx and rr[idx[header_name]] != value:
                rr[idx[header_name]] = value
                changed_rows = True

        new_rows.append(rr)

    if changed_rows:
        result["headers"] = headers
        result["rows"] = new_rows

    return result


def _normalize_result_meta(result: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(result, dict):
        return result

    meta = result.get("meta")
    if not isinstance(meta, dict):
        meta = {}

    horizon_days = _infer_horizon_days(payload)
    invest_period_label = _infer_invest_period_label(payload, horizon_days)

    criteria = meta.get("criteria")
    if not isinstance(criteria, dict):
        criteria = {}

    if criteria.get("horizon_days") is None and horizon_days is not None:
        criteria["horizon_days"] = horizon_days
    if not criteria.get("invest_period_label") and invest_period_label:
        criteria["invest_period_label"] = invest_period_label
    if criteria.get("invest_period_days") is None and horizon_days is not None:
        criteria["invest_period_days"] = horizon_days

    meta["criteria"] = criteria
    meta["router_version"] = ROUTER_VERSION
    meta["context_post_processed"] = True

    result["meta"] = meta
    return result


def _rows_to_items_from_matrix(headers: List[Any], rows: List[Any]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    hdrs = [str(h or "").strip() for h in headers]
    keys = [_snake_like(h) for h in hdrs]

    for r in rows:
        if isinstance(r, dict):
            items.append(dict(r))
            continue
        if isinstance(r, (list, tuple)):
            rr = list(r)
            item = {}
            for i, k in enumerate(keys):
                item[k] = rr[i] if i < len(rr) else None
            items.append(item)
    return items


def _normalize_result(result: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(result, dict):
        return {
            "headers": [],
            "rows": [],
            "items": [],
            "meta": {"ok": False, "error": "invalid_result_type", "router_version": ROUTER_VERSION},
        }

    result = dict(result)

    if not isinstance(result.get("items"), list):
        rows = result.get("rows")
        headers = result.get("headers")
        if isinstance(rows, list):
            if rows and all(isinstance(r, dict) for r in rows):
                result["items"] = [dict(r) for r in rows]
            elif isinstance(headers, list):
                result["items"] = _rows_to_items_from_matrix(headers, rows)
            else:
                result["items"] = []
        else:
            result["items"] = []

    result = _normalize_result_items(result, payload)
    result = _normalize_result_rows(result, payload)
    result = _normalize_result_meta(result, payload)
    return result


# -----------------------------------------------------------------------------
# Engine execution
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


async def _run_core_advisor(payload: Dict[str, Any], *, engine: Any, timeout_sec: float) -> Dict[str, Any]:
    def _call() -> Dict[str, Any]:
        return _run_engine_call_variants(payload or {}, engine)

    try:
        raw = await asyncio.wait_for(asyncio.to_thread(_call), timeout=timeout_sec)
    except asyncio.TimeoutError:
        raw = {
            "headers": [],
            "rows": [],
            "items": [],
            "meta": {"ok": False, "error": "timeout", "timeout_sec": timeout_sec},
        }
    except Exception as e:
        raw = {
            "headers": [],
            "rows": [],
            "items": [],
            "meta": {"ok": False, "error": f"{type(e).__name__}: {e}"},
        }

    return _normalize_result(raw, payload)


# -----------------------------------------------------------------------------
# Query payload builder
# -----------------------------------------------------------------------------
def _payload_from_query(
    symbols: str,
    tickers: str,
    *,
    top_n: Optional[int],
    invest_amount: Optional[float],
    allocation_strategy: Optional[str],
    risk_profile: Optional[str],
    horizon_days: Optional[int],
    invest_period_label: Optional[str],
    debug: bool,
) -> Dict[str, Any]:
    sym_list: List[str] = []
    if symbols:
        sym_list.extend(_split_symbols(symbols))
    if tickers:
        sym_list.extend(_split_symbols(tickers))

    seen = set()
    sym_list = [s for s in sym_list if not (s in seen or seen.add(s))]

    payload: Dict[str, Any] = {
        "symbols": sym_list,
        "tickers": sym_list,
        "top_n": top_n,
        "invest_amount": invest_amount,
        "allocation_strategy": allocation_strategy,
        "risk_profile": risk_profile,
        "horizon_days": horizon_days,
        "invest_period_days": horizon_days,
        "invest_period_label": invest_period_label,
        "debug": bool(debug),
    }
    return {k: v for k, v in payload.items() if v is not None and v != ""}


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
    payload = _payload_from_query(
        symbols=symbols or "",
        tickers=tickers or "",
        top_n=(limit if (limit is not None and limit > 0) else top_n),
        invest_amount=invest_amount,
        allocation_strategy=allocation_strategy,
        risk_profile=risk_profile,
        horizon_days=horizon_days,
        invest_period_label=invest_period_label,
        debug=debug,
    )

    chosen_page = page or sheet_name or sheet or name or tab or "Top_10_Investments"
    payload["page"] = chosen_page
    payload["sheet_name"] = chosen_page
    payload["include_headers"] = str(include_headers or "true")
    payload["include_matrix"] = str(include_matrix or "true")
    return payload


# -----------------------------------------------------------------------------
# Result -> sheet payload
# -----------------------------------------------------------------------------
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

    include_headers = str(payload.get("include_headers", "true")).strip().lower() not in {"0", "false", "no", "off"}
    include_matrix = str(payload.get("include_matrix", "true")).strip().lower() not in {"0", "false", "no", "off"}

    schema_headers, schema_keys = _schema_for_page(page_norm)
    keys = list(schema_keys or _TOP10_KEYS_FALLBACK)
    headers = list(schema_headers or _TOP10_HEADERS_FALLBACK)

    items = result.get("items")
    rows = result.get("rows")
    result_headers = result.get("headers")

    normalized_items: List[Dict[str, Any]] = []

    if isinstance(items, list) and items:
        for i, item in enumerate(items, start=1):
            row_map = item if isinstance(item, Mapping) else _model_to_dict(item)
            if not row_map and isinstance(item, (list, tuple)) and isinstance(result_headers, list):
                item_headers = [str(h or "").strip() for h in result_headers]
                item_keys = [_snake_like(h) for h in item_headers]
                vals = list(item)
                row_map = {item_keys[x]: (vals[x] if x < len(vals) else None) for x in range(len(item_keys))}
            fixed = _ensure_item_context(dict(row_map or {}), payload, rank=i)
            normalized_items.append(_align_row_to_keys(fixed, keys, symbol_fallback=str(fixed.get("symbol") or "")))

    elif isinstance(rows, list) and rows:
        if all(isinstance(r, dict) for r in rows):
            for i, item in enumerate(rows, start=1):
                fixed = _ensure_item_context(dict(item), payload, rank=i)
                normalized_items.append(_align_row_to_keys(fixed, keys, symbol_fallback=str(fixed.get("symbol") or "")))
        elif isinstance(result_headers, list):
            item_headers = [str(h or "").strip() for h in result_headers]
            item_keys = [_snake_like(h) for h in item_headers]
            for i, r in enumerate(rows, start=1):
                vals = list(r) if isinstance(r, (list, tuple)) else [r]
                base = {item_keys[x]: (vals[x] if x < len(vals) else None) for x in range(len(item_keys))}
                fixed = _ensure_item_context(base, payload, rank=i)
                normalized_items.append(_align_row_to_keys(fixed, keys, symbol_fallback=str(fixed.get("symbol") or "")))

    rows_matrix = _rows_matrix(normalized_items, keys)

    meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}
    ok = bool(meta.get("ok", False))
    error = str(meta.get("error") or "") if meta.get("error") else None

    return {
        "status": "success" if ok else ("partial" if normalized_items else "error"),
        "page": page_norm,
        "route_family": route_family,
        "headers": headers if include_headers else [],
        "keys": keys,
        "rows": normalized_items,
        "rows_matrix": rows_matrix if include_matrix else None,
        "quotes": normalized_items,
        "data": normalized_items,
        "error": error,
        "request_id": request_id,
        "version": ROUTER_VERSION,
        "meta": {
            **meta,
            "router_version": ROUTER_VERSION,
            "count": len(normalized_items),
            "sheet_mode": True,
            "page": page_norm,
        },
    }


# -----------------------------------------------------------------------------
# Route handlers
# -----------------------------------------------------------------------------
async def advisor_health_handler(request: Request) -> BestJSONResponse:
    rid = _request_id(request)
    open_mode = _safe_is_open_mode()
    allow_query_token = _safe_allow_query_token()
    eng, source, err = await _get_engine(request)

    payload = {
        "status": "ok" if eng is not None else "degraded",
        "request_id": rid,
        "timestamp_utc": _now_utc(),
        "version": ROUTER_VERSION,
        "auth": {
            "open_mode": open_mode,
            "allow_query_token": allow_query_token,
        },
        "engine": {
            "available": eng is not None,
            "source": source,
            "error": err,
            "type": type(eng).__name__ if eng is not None else None,
        },
        "metrics": _METRICS.to_dict(),
    }
    return BestJSONResponse(content=_clean_nans(payload))


async def advisor_metrics_handler(request: Request) -> BestJSONResponse:
    rid = _request_id(request)
    payload = {
        "status": "ok",
        "request_id": rid,
        "timestamp_utc": _now_utc(),
        "version": ROUTER_VERSION,
        "metrics": _METRICS.to_dict(),
    }
    return BestJSONResponse(content=_clean_nans(payload))


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
    debug: bool = Query(default=False),
) -> BestJSONResponse:
    rid = _request_id(request)
    t0 = time.time()

    if not _auth_ok(request):
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
        symbols=symbols,
        tickers=tickers,
        top_n=top_n,
        invest_amount=invest_amount,
        allocation_strategy=allocation_strategy,
        risk_profile=risk_profile,
        horizon_days=horizon_days,
        invest_period_label=invest_period_label,
        debug=debug,
    )

    result = await _run_core_advisor(payload, engine=engine, timeout_sec=_timeout_sec())
    ok = bool((result.get("meta") or {}).get("ok", False))
    err_txt = str((result.get("meta") or {}).get("error") or "")

    await _record_metrics(ok, False, (time.time() - t0) * 1000.0, err_txt)

    envelope = {
        "status": "ok" if ok else "error",
        "request_id": rid,
        "timestamp_utc": _now_utc(),
        "version": ROUTER_VERSION,
        "data": result,
    }
    return BestJSONResponse(content=_clean_nans(envelope))


async def advisor_run_handler(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
) -> BestJSONResponse:
    rid = _request_id(request)
    t0 = time.time()

    if not _auth_ok(request):
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

    payload = dict(body or {})
    result = await _run_core_advisor(payload, engine=engine, timeout_sec=_timeout_sec())

    ok = bool((result.get("meta") or {}).get("ok", False))
    err_txt = str((result.get("meta") or {}).get("error") or "")

    await _record_metrics(ok, False, (time.time() - t0) * 1000.0, err_txt)

    envelope = {
        "status": "ok" if ok else "error",
        "request_id": rid,
        "timestamp_utc": _now_utc(),
        "version": ROUTER_VERSION,
        "data": result,
    }
    return BestJSONResponse(content=_clean_nans(envelope))


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
    debug: bool = Query(default=False),
) -> BestJSONResponse:
    rid = _request_id(request)
    t0 = time.time()

    if not _auth_ok(request):
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

    result = await _run_core_advisor(payload, engine=engine, timeout_sec=_timeout_sec())
    ok = bool((result.get("meta") or {}).get("ok", False))
    err_txt = str((result.get("meta") or {}).get("error") or "")

    await _record_metrics(ok, False, (time.time() - t0) * 1000.0, err_txt)

    sheet_payload = _result_to_sheet_payload(result=result, payload=payload, request_id=rid)
    return BestJSONResponse(content=_clean_nans(sheet_payload))


async def advisor_sheet_rows_post_handler(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
) -> BestJSONResponse:
    rid = _request_id(request)
    t0 = time.time()

    if not _auth_ok(request):
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

    payload = dict(body or {})
    page_guess = (
        payload.get("page")
        or payload.get("sheet_name")
        or payload.get("sheet")
        or payload.get("name")
        or payload.get("tab")
        or "Top_10_Investments"
    )
    payload["page"] = page_guess

    result = await _run_core_advisor(payload, engine=engine, timeout_sec=_timeout_sec())
    ok = bool((result.get("meta") or {}).get("ok", False))
    err_txt = str((result.get("meta") or {}).get("error") or "")

    await _record_metrics(ok, False, (time.time() - t0) * 1000.0, err_txt)

    sheet_payload = _result_to_sheet_payload(result=result, payload=payload, request_id=rid)
    return BestJSONResponse(content=_clean_nans(sheet_payload))


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
_register_common_routes(_router_compat_us)
_register_common_routes(_router_compat_dash)

router.include_router(_router_canonical)
router.include_router(_router_compat_us)
router.include_router(_router_compat_dash)

__all__ = ["router", "ROUTER_VERSION"]
