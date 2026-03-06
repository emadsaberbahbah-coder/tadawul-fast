#!/usr/bin/env python3
"""
routes/investment_advisor.py
================================================================================
TFB Investment Advisor Routes — v5.8.0
(PROMETHEUS-SAFE / ENGINE-LAZY / CORE-CONFIG-BRIDGE / CONTEXT-AWARE)
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

Endpoints
- /v1/advisor/health
- /v1/advisor/metrics
- /v1/advisor/recommendations   (GET)
- /v1/advisor/run               (POST)
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

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
    import json
    from fastapi.responses import JSONResponse as BestJSONResponse  # type: ignore

    def _json_dumps(v: Any) -> str:
        return json.dumps(_clean_nans(v), default=str, ensure_ascii=False)


logger = logging.getLogger("routes.investment_advisor")

ROUTER_VERSION = "5.8.0"
router = APIRouter(prefix="/v1/advisor", tags=["advisor"])

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


def _error(status_code: int, request_id: str, message: str, *, extra: Optional[Dict[str, Any]] = None) -> BestJSONResponse:
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
            parts.append(f"valuation is supportive")
        if momentum is not None and momentum >= 60:
            parts.append(f"momentum is positive")
        if quality is not None and quality >= 60:
            parts.append(f"quality profile is solid")
        if opportunity is not None and opportunity >= 60:
            parts.append(f"opportunity score is favorable")
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
            parts.append(f"momentum is weak")
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


def _ensure_item_context(item: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    row = dict(item or {})

    inferred_horizon_days = _infer_horizon_days(payload)
    inferred_label = _infer_invest_period_label(payload, inferred_horizon_days)

    if row.get("horizon_days") is None and inferred_horizon_days is not None:
        row["horizon_days"] = inferred_horizon_days

    if not row.get("invest_period_label") and inferred_label:
        row["invest_period_label"] = inferred_label

    if row.get("recommendation") and not _clean_reason_text(row.get("recommendation_reason")):
        row["recommendation_reason"] = _build_recommendation_reason(row, payload)

    return row


def _normalize_result_items(result: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    items = result.get("items")
    if not isinstance(items, list):
        return result

    changed = False
    new_items: List[Any] = []
    for item in items:
        if isinstance(item, dict):
            fixed = _ensure_item_context(item, payload)
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
        for item in rows:
            row = dict(item)
            fixed = _ensure_item_context(row, payload)
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

    if need_reason:
        headers = list(headers) + ["Recommendation Reason"]
    if need_horizon:
        headers = list(headers) + ["Horizon Days"]
    if need_label:
        headers = list(headers) + ["Invest Period Label"]

    idx = _headers_index(headers)
    changed_rows = need_reason or need_horizon or need_label
    new_rows: List[Any] = []

    for r in rows:
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

        fixed = _ensure_item_context(row_dict, payload)

        reason = fixed.get("recommendation_reason")
        horizon_days = fixed.get("horizon_days")
        invest_period_label = fixed.get("invest_period_label")

        if "Recommendation Reason" in idx:
            if rr[idx["Recommendation Reason"]] != reason:
                rr[idx["Recommendation Reason"]] = reason
                changed_rows = True

        if "Horizon Days" in idx:
            if rr[idx["Horizon Days"]] != horizon_days:
                rr[idx["Horizon Days"]] = horizon_days
                changed_rows = True

        if "Invest Period Label" in idx:
            if rr[idx["Invest Period Label"]] != invest_period_label:
                rr[idx["Invest Period Label"]] = invest_period_label
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


def _normalize_result(result: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(result, dict):
        return {
            "headers": [],
            "rows": [],
            "items": [],
            "meta": {"ok": False, "error": "invalid_result_type", "router_version": ROUTER_VERSION},
        }

    result = dict(result)
    result = _normalize_result_items(result, payload)
    result = _normalize_result_rows(result, payload)
    result = _normalize_result_meta(result, payload)
    return result


# -----------------------------------------------------------------------------
# Engine execution
# -----------------------------------------------------------------------------
async def _run_core_advisor(payload: Dict[str, Any], *, engine: Any, timeout_sec: float) -> Dict[str, Any]:
    def _call() -> Dict[str, Any]:
        try:
            from core.investment_advisor_engine import run_investment_advisor_engine as run_engine  # type: ignore

            out = run_engine(payload or {}, data_engine=engine, quote_engine=engine, cache_engine=engine)
            if isinstance(out, dict):
                return out
        except Exception:
            pass

        try:
            from core.investment_advisor_engine import run_investment_advisor as run_engine_legacy  # type: ignore

            out = run_engine_legacy(payload or {}, engine=engine)
            if isinstance(out, dict):
                return out
        except Exception as e:
            return {
                "headers": [],
                "rows": [],
                "items": [],
                "meta": {"ok": False, "error": f"{type(e).__name__}: {e}"},
            }

        return {
            "headers": [],
            "rows": [],
            "items": [],
            "meta": {"ok": False, "error": "core_return_non_dict"},
        }

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


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@router.get("/health", response_class=BestJSONResponse)
async def advisor_health(request: Request) -> BestJSONResponse:
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


@router.get("/metrics", response_class=BestJSONResponse)
async def advisor_metrics(request: Request) -> BestJSONResponse:
    rid = _request_id(request)
    payload = {
        "status": "ok",
        "request_id": rid,
        "timestamp_utc": _now_utc(),
        "version": ROUTER_VERSION,
        "metrics": _METRICS.to_dict(),
    }
    return BestJSONResponse(content=_clean_nans(payload))


@router.get("/recommendations", response_class=BestJSONResponse)
async def advisor_recommendations(
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
        return _error(503, rid, "Advisor engine unavailable", extra={"engine_source": engine_source, "engine_error": engine_err})

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


@router.post("/run", response_class=BestJSONResponse)
async def advisor_run(
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
        return _error(503, rid, "Advisor engine unavailable", extra={"engine_source": engine_source, "engine_error": engine_err})

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
