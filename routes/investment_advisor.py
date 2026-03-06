#!/usr/bin/env python3
"""
routes/investment_advisor.py
================================================================================
TFB Investment Advisor Routes — v5.7.0
(PROMETHEUS-SAFE / ENGINE-LAZY / CORE-CONFIG-BRIDGE / REASON-AWARE)
================================================================================

What this revision fixes
- ✅ Keeps NO prometheus_client metric creation in this module
- ✅ Keeps engine-lazy behavior (no heavy imports / no network at import time)
- ✅ Uses core.config as auth/open-mode source of truth when available
- ✅ Delegates execution to core/investment_advisor_engine.py as canonical executor
- ✅ FIX: post-processes advisor result rows to ensure recommendation_reason is filled
      when recommendation exists but reason is blank
- ✅ FIX: fills horizon_days / invest_period_label when payload provides enough context
- ✅ FIX: keeps response envelope backward-compatible
- ✅ FIX: thread-safe execution of core advisor in worker thread

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

ROUTER_VERSION = "5.7.0"
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


_ENGINE_INIT_LOCK = asyncio.Lock()


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


async def _run_core_advisor(payload: Dict[str, Any], *, engine: Any, timeout_sec: float) -> Dict[str, Any]:
    def _call() -> Dict[str, Any]:
        from core.investment_advisor_engine import run_investment_advisor as run_engine  # type: ignore

        out = run_engine(payload or {}, engine=engine)
        if isinstance(out, dict):
            return out
        return {"headers": [], "rows": [], "items": [], "meta": {"ok": False, "error": "core_return_non_dict"}}

    try:
        return await asyncio.wait_for(asyncio.to_thread(_call), timeout=timeout_sec)
    except asyncio.TimeoutError:
        return {"headers": [], "rows": [], "items": [], "meta": {"ok": False, "error": "timeout", "timeout_sec": timeout_sec}}
    except Exception as e:
        return {"headers": [], "rows": [], "items": [], "meta": {"ok": False, "error": f"{type(e).__name__}: {e}"}}


def _payload_from_query(
    symbols: str,
    tickers: str,
    *,
    top_n: Optional[int],
    invest_amount: Optional[float],
    allocation_strategy: Optional[str],
    risk_profile: Optional[str],
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
        "debug": bool(debug),
    }
    return {k: v for k, v in payload.items() if v is not None and v != ""}


def _timeout_sec() -> float:
    try:
        raw = float(os.getenv("ADVISOR_ROUTE_TIMEOUT_SEC", "75") or "75")
    except Exception:
        raw = 75.0
    return max(5.0, min(180.0, raw))


# -----------------------------------------------------------------------------
# Recommendation-reason post-processing
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
    return "12M"


def _build_recommendation_reason(row: Dict[str, Any], payload: Dict[str, Any]) -> Optional[str]:
    rec = str(row.get("recommendation") or "").strip()
    if not rec:
        return None

    parts: List[str] = [f"Recommendation={rec}"]

    roi = row.get("expected_roi_3m")
    if roi is None:
        roi = row.get("expected_roi_1m")
    if roi is None:
        roi = row.get("expected_roi_12m")
    roi_txt = _format_pct(roi)
    if roi_txt:
        parts.append(f"Expected ROI={roi_txt}")

    conf = row.get("forecast_confidence")
    conf_txt = _format_pct(conf)
    if conf_txt:
        parts.append(f"Confidence={conf_txt}")

    risk_bucket = str(row.get("risk_bucket") or "").strip()
    if risk_bucket:
        parts.append(f"Risk={risk_bucket}")

    overall = _safe_float(row.get("overall_score"))
    if overall is not None:
        parts.append(f"Overall={round(overall, 2)}")

    horizon_days = row.get("horizon_days")
    if horizon_days is None:
        horizon_days = payload.get("invest_period_days")
    try:
        horizon_days_int = int(float(horizon_days)) if horizon_days is not None else None
    except Exception:
        horizon_days_int = None

    label = row.get("invest_period_label") or _horizon_label(horizon_days_int)
    if label:
        parts.append(f"Horizon={label}")

    return " | ".join(parts)


def _normalize_result_rows(result: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(result, dict):
        return result

    rows = result.get("rows")
    changed_rows = False

    if isinstance(rows, list):
        new_rows: List[Any] = []
        for item in rows:
            if isinstance(item, dict):
                row = dict(item)

                if row.get("recommendation") and not row.get("recommendation_reason"):
                    row["recommendation_reason"] = _build_recommendation_reason(row, payload)

                if row.get("horizon_days") is None and payload.get("invest_period_days") is not None:
                    row["horizon_days"] = payload.get("invest_period_days")

                if not row.get("invest_period_label"):
                    try:
