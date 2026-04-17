#!/usr/bin/env python3
"""
routes/investment_advisor.py
--------------------------------------------------------------------------------
ADVANCED TOP10 / INVESTMENT ADVISOR ROUTER — v2.2.0
--------------------------------------------------------------------------------
STABILITY-FIRST • TIMEOUT-GUARDED • SCHEMA-FIRST • TOP10-FIELD-HARDENED
UNCONSTRAINED-REQUEST SAFE • FALLBACK SAFE • GATEWAY-ESCALATION RESISTANT
SCHEMA-CHAIN v3.4.0 ALIGNED • TRADE-SETUP EQUIPPED

Why this revision (v2.2.0 vs v2.1.0)
--------------------------------------
- FIX: _ensure_top10_keys_present now appends all 7 required Top10 fields.
  v2.1.0 only backfilled 3 (top10_rank, selection_reason, criteria_snapshot).
  v2.2.0 adds the 4 trade setup fields from top10_selector v4.9.0:
    entry_price, stop_loss_suggested, take_profit_suggested, risk_reward_ratio.
  Without this, schema_projection stripped these fields from every row even
  when top10_selector had already computed them.

- FIX: _apply_top10_field_backfill now initialises trade setup fields to None
  if absent so schema projection never produces shorter-than-expected rows.

- SAFE: _load_schema_defaults() already reads from schema_registry dynamically
  and automatically picks up the v3.4.0 106-column Top10 schema. No changes
  needed there.

Preserved from v2.1.0 (no other behavioral changes):
  All timeout guards, fallback retry logic, engine resolution, auth helpers,
  criteria preparation, and route endpoint definitions.

Primary endpoints
-----------------
- POST /v1/advanced/top10-investments
- POST /v1/advanced/top10
- POST /v1/advanced/investment-advisor
- POST /v1/advanced/advisor
- GET  /v1/advanced/health
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
import time
import uuid
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, status
from fastapi.encoders import jsonable_encoder

logger = logging.getLogger("routes.investment_advisor")
logger.addHandler(logging.NullHandler())

INVESTMENT_ADVISOR_VERSION = "2.2.0"
TOP10_PAGE_NAME = "Top_10_Investments"

router = APIRouter(prefix="/v1/advanced", tags=["advanced"])


# =============================================================================
# Auth (best-effort, consistent with other routers)
# =============================================================================
try:
    from core.config import auth_ok, get_settings_cached, is_open_mode  # type: ignore
except Exception:
    auth_ok = None  # type: ignore
    is_open_mode = None  # type: ignore

    def get_settings_cached(*args: Any, **kwargs: Any) -> Any:  # type: ignore
        return None


def _extract_auth_token(
    *,
    token_query: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
) -> str:
    auth_token = (x_app_token or "").strip()

    if authorization and authorization.strip().lower().startswith("bearer "):
        auth_token = authorization.strip().split(" ", 1)[1].strip()

    if token_query and not auth_token:
        allow_query = False
        try:
            settings = get_settings_cached()
            allow_query = bool(getattr(settings, "allow_query_token", False))
        except Exception:
            allow_query = False
        if allow_query:
            auth_token = token_query.strip()

    return auth_token


def _require_auth_or_401(
    *,
    token_query: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
) -> None:
    try:
        if callable(is_open_mode) and bool(is_open_mode()):
            return
    except Exception:
        pass

    if auth_ok is None:
        return

    auth_token = _extract_auth_token(
        token_query=token_query,
        x_app_token=x_app_token,
        authorization=authorization,
    )

    if not auth_ok(
        token=auth_token,
        authorization=authorization,
        headers={"X-APP-TOKEN": x_app_token, "Authorization": authorization},
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
        )


# =============================================================================
# Engine accessor (lazy + safe)
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
            mod = __import__(modpath, fromlist=["get_engine"])
            get_engine = getattr(mod, "get_engine", None)
            if callable(get_engine):
                eng = get_engine()
                if hasattr(eng, "__await__"):
                    eng = await eng
                return eng
        except Exception:
            continue

    return None


# =============================================================================
# Generic helpers
# =============================================================================
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


def _s(v: Any) -> str:
    try:
        if v is None:
            return ""
        s = str(v).strip()
        return "" if s.lower() == "none" else s
    except Exception:
        return ""


def _is_blank(v: Any) -> bool:
    return v is None or (isinstance(v, str) and not v.strip())


def _as_dict(v: Any) -> Dict[str, Any]:
    if isinstance(v, dict):
        return dict(v)
    if isinstance(v, Mapping):
        return dict(v)
    return {}


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


def _rows_to_matrix(rows: List[Dict[str, Any]], keys: List[str]) -> List[List[Any]]:
    return [[row.get(k) for k in keys] for row in rows]


def _env_int(name: str, default: int) -> int:
    try:
        raw = os.getenv(name, "").strip()
        return int(raw) if raw else int(default)
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    try:
        raw = os.getenv(name, "").strip()
        return float(raw) if raw else float(default)
    except Exception:
        return float(default)


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
    out: List[str] = []
    seen = set()

    if value is None:
        return []

    if isinstance(value, str):
        parts = value.replace(";", ",").replace("\n", ",").split(",")
        seq = parts
    elif isinstance(value, (list, tuple, set)):
        seq = list(value)
    else:
        seq = [value]

    for item in seq:
        s = _s(item)
        if s and s not in seen:
            seen.add(s)
            out.append(s)

    return out


def _flatten_criteria(body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Accept criteria from:
      - body["criteria"] dict
      - body["filters"] dict
      - body["settings"]["criteria"] dict
      - selected top-level keys
    Top-level values override nested values.
    """
    crit: Dict[str, Any] = {}

    if isinstance(body.get("criteria"), dict):
        crit.update(body["criteria"])

    if isinstance(body.get("filters"), dict):
        crit.update(body["filters"])

    settings = body.get("settings")
    if isinstance(settings, dict):
        if isinstance(settings.get("criteria"), dict):
            crit.update(settings["criteria"])

    for k in (
        "pages_selected",
        "pages",
        "selected_pages",
        "direct_symbols",
        "symbols",
        "tickers",
        "invest_period_days",
        "investment_period_days",
        "horizon_days",
        "min_expected_roi",
        "max_risk_score",
        "min_confidence",
        "min_ai_confidence",
        "min_volume",
        "use_liquidity_tiebreak",
        "enforce_risk_confidence",
        "top_n",
        "enrich_final",
        "risk_level",
        "confidence_bucket",
        "invest_period_label",
        "include_positions",
        "schema_only",
        "preview",
    ):
        if k in body and body.get(k) is not None:
            crit[k] = body.get(k)

    return crit


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
    ):
        val = row.get(key)
        if isinstance(val, (int, float)):
            score_parts.append(f"{label}={round(float(val), 2)}")

    roi_parts: List[str] = []
    for label, key in (
        ("1M", "expected_roi_1m"),
        ("3M", "expected_roi_3m"),
        ("12M", "expected_roi_12m"),
    ):
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


def _apply_top10_field_backfill(
    rows: List[Dict[str, Any]],
    *,
    keys: List[str],
    criteria: Dict[str, Any],
) -> List[Dict[str, Any]]:
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

        # v2.2.0: initialise trade setup fields to None if absent
        # so schema_projection never produces shorter-than-expected rows
        for _ts_key in (
            "entry_price",
            "stop_loss_suggested",
            "take_profit_suggested",
            "risk_reward_ratio",
        ):
            if _ts_key in keys and _ts_key not in r:
                r[_ts_key] = None

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


def _ensure_top10_keys_present(
    keys: List[str], headers: List[str]
) -> Tuple[List[str], List[str]]:
    """
    v2.2.0: appends all 7 required Top10 fields if absent.
    v2.1.0 only appended 3 (top10_rank, selection_reason, criteria_snapshot).
    v2.2.0 adds 4 trade setup fields from top10_selector v4.9.0.
    """
    # (key, display_header) pairs — order matters for column alignment
    extras = [
        ("top10_rank",            "Top 10 Rank"),
        ("selection_reason",      "Selection Reason"),
        ("criteria_snapshot",     "Criteria Snapshot"),
        ("entry_price",           "Entry Price"),           # v2.2.0
        ("stop_loss_suggested",   "Stop Loss (AI)"),        # v2.2.0
        ("take_profit_suggested", "Take Profit (AI)"),      # v2.2.0
        ("risk_reward_ratio",     "Risk/Reward"),           # v2.2.0
    ]

    out_keys    = list(keys    or [])
    out_headers = list(headers or [])

    for key, header in extras:
        if key not in out_keys:
            out_keys.append(key)
            out_headers.append(header)

    return out_keys, out_headers


def _load_schema_defaults() -> Tuple[List[str], List[str]]:
    """
    Read Top_10_Investments schema headers and keys from schema_registry.
    Multi-path fallback: core.sheets.schema_registry → core.schema_registry → schema_registry.
    Returns ([], []) if unavailable — callers fall back to _ensure_top10_keys_present.
    """
    for _sreg_path in ("core.sheets.schema_registry", "core.schema_registry", "schema_registry"):
        try:
            import importlib as _il
            _sreg = _il.import_module(_sreg_path)
            get_sheet_spec = getattr(_sreg, "get_sheet_spec", None)
            if not callable(get_sheet_spec):
                continue
            spec = get_sheet_spec(TOP10_PAGE_NAME)
            cols = getattr(spec, "columns", None) or []
            keys    = [getattr(c, "key",    "") for c in cols]
            headers = [getattr(c, "header", "") for c in cols]
            keys    = [k for k in keys    if isinstance(k, str) and k]
            headers = [h for h in headers if isinstance(h, str) and h]
            if keys and headers:
                return headers, keys
        except Exception:
            continue
    return [], []


def _schema_only_payload(
    *,
    request_id: str,
    headers: List[str],
    keys: List[str],
    include_matrix: bool,
    meta: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "status":      "partial",
        "page":        TOP10_PAGE_NAME,
        "sheet":       TOP10_PAGE_NAME,
        "headers":     headers,
        "keys":        keys,
        "rows":        [],
        "rows_matrix": [] if (include_matrix and keys) else None,
        "version":     INVESTMENT_ADVISOR_VERSION,
        "request_id":  request_id,
        "meta":        meta,
    }


def _normalize_selector_payload(
    payload: Dict[str, Any],
    *,
    criteria_used: Dict[str, Any],
    eff_limit: int,
) -> Tuple[List[str], List[str], List[Dict[str, Any]], Dict[str, Any], str]:
    headers    = payload.get("headers") or []
    keys       = payload.get("keys")    or []
    rows       = payload.get("rows")    or []
    status_out = _s(payload.get("status")) or "success"

    if not isinstance(headers, list): headers = []
    if not isinstance(keys,    list): keys    = []
    if not isinstance(rows,    list): rows    = []

    if not headers or not keys:
        schema_headers, schema_keys = _load_schema_defaults()
        if not headers: headers = schema_headers
        if not keys:    keys    = schema_keys

    keys, headers = _ensure_top10_keys_present(list(keys), list(headers))

    dict_rows = [dict(r) for r in rows if isinstance(r, dict)]
    dict_rows = _apply_top10_field_backfill(dict_rows, keys=keys, criteria=criteria_used)
    dict_rows = _rank_rows_in_order(dict_rows)
    norm_rows  = _ensure_schema_projection(dict_rows, keys)
    norm_rows  = norm_rows[:eff_limit]

    meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    return headers, keys, norm_rows, dict(meta), status_out


def _load_top10_builder() -> Any:
    from core.analysis.top10_selector import build_top10_rows  # type: ignore
    return build_top10_rows


def _effective_limit(body: Dict[str, Any], limit_q: Optional[int]) -> int:
    max_limit     = max(1, _env_int("ADV_TOP10_MAX_LIMIT",     50))
    default_limit = max(1, _env_int("ADV_TOP10_DEFAULT_LIMIT", 10))

    if isinstance(limit_q, int):
        eff = limit_q
    else:
        eff = _safe_int(
            body.get("limit") or body.get("top_n")
            or body.get("criteria", {}).get("top_n") or default_limit,
            default_limit,
        )

    return max(1, min(max_limit, int(eff)))


def _is_request_unconstrained(criteria: Dict[str, Any]) -> bool:
    pages  = _normalize_list(
        criteria.get("pages_selected") or criteria.get("pages") or criteria.get("selected_pages")
    )
    direct = _normalize_list(
        criteria.get("direct_symbols") or criteria.get("symbols") or criteria.get("tickers")
    )
    return (not pages) and (not direct)


def _prepare_effective_criteria(
    body: Dict[str, Any], eff_limit: int
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    crit           = _flatten_criteria(body or {})
    pages          = _normalize_list(crit.get("pages_selected") or crit.get("pages") or crit.get("selected_pages"))
    direct_symbols = _normalize_list(crit.get("direct_symbols") or crit.get("symbols") or crit.get("tickers"))

    request_unconstrained = (not pages) and (not direct_symbols)
    pages_explicit        = bool(pages)

    if request_unconstrained:
        pages = _env_csv("ADV_TOP10_DEFAULT_PAGES", ["Market_Leaders", "Global_Markets"])
        crit["pages_selected"] = pages

    max_pages     = max(1, _env_int("ADV_TOP10_MAX_PAGES", 5))
    pages_trimmed = False
    if pages and len(pages) > max_pages:
        pages = pages[:max_pages]
        crit["pages_selected"] = pages
        pages_trimmed = True

    if direct_symbols:
        crit["direct_symbols"] = direct_symbols

    crit["top_n"] = eff_limit

    if "enrich_final" not in crit:
        if direct_symbols:
            crit["enrich_final"] = True
        else:
            crit["enrich_final"] = len(pages) <= 2

    if crit.get("invest_period_days") is None:
        if crit.get("investment_period_days") is not None:
            crit["invest_period_days"] = crit.get("investment_period_days")
        elif crit.get("horizon_days") is not None:
            crit["invest_period_days"] = crit.get("horizon_days")

    prep_meta = {
        "request_unconstrained": request_unconstrained,
        "pages_explicit":        pages_explicit,
        "pages_effective":       list(pages),
        "direct_symbols_count":  len(direct_symbols),
        "pages_trimmed":         pages_trimmed,
        "allow_row_fallback":    True,
    }

    return crit, prep_meta


def _narrow_criteria_for_fallback(criteria: Dict[str, Any], eff_limit: int) -> Dict[str, Any]:
    narrowed = copy.deepcopy(criteria)

    fallback_pages_cap = max(1, _env_int("ADV_TOP10_FALLBACK_MAX_PAGES", 2))
    fallback_top_n     = max(1, min(eff_limit, _env_int("ADV_TOP10_FALLBACK_TOP_N", min(3, eff_limit))))

    direct_symbols = _normalize_list(
        narrowed.get("direct_symbols") or narrowed.get("symbols") or narrowed.get("tickers")
    )
    pages = _normalize_list(
        narrowed.get("pages_selected") or narrowed.get("pages") or narrowed.get("selected_pages")
    )

    if direct_symbols:
        narrowed["direct_symbols"] = direct_symbols[: max(1, min(len(direct_symbols), eff_limit))]
        narrowed["top_n"] = min(eff_limit, len(narrowed["direct_symbols"]))
    else:
        if not pages:
            pages = _env_csv("ADV_TOP10_DEFAULT_PAGES", ["Market_Leaders"])
        pages = pages[:fallback_pages_cap]
        narrowed["pages_selected"] = pages
        narrowed["top_n"]          = fallback_top_n

    narrowed["enrich_final"] = False
    return narrowed


async def _run_selector_with_timeout(
    *,
    builder: Any,
    engine: Any,
    criteria: Dict[str, Any],
    eff_limit: int,
    mode: str,
    timeout_sec: float,
) -> Dict[str, Any]:
    coro = builder(
        engine=engine,
        criteria=criteria,
        limit=eff_limit,
        mode=mode or "",
    )
    return await asyncio.wait_for(coro, timeout=timeout_sec)


# =============================================================================
# Health
# =============================================================================
@router.get("/health")
async def advanced_health(request: Request) -> Dict[str, Any]:
    engine = await _get_engine(request)
    return jsonable_encoder(
        {
            "status":         "ok" if engine else "degraded",
            "version":        INVESTMENT_ADVISOR_VERSION,
            "engine_available": bool(engine),
            "engine_type":    type(engine).__name__ if engine else "none",
            "service":        "advanced_top10",
        }
    )


# =============================================================================
# Main endpoint
# =============================================================================
@router.post("/top10-investments")
@router.post("/top10")
@router.post("/investment-advisor")
@router.post("/advisor")
async def advanced_top10_investments(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    mode: str = Query(default="", description="Optional mode hint for engine/provider"),
    include_matrix: Optional[bool] = Query(default=None, description="Return rows_matrix for legacy clients"),
    limit: Optional[int] = Query(default=None, ge=1, le=50, description="How many items to return (1..50)"),
    schema_only: Optional[bool] = Query(default=None, description="Return schema with no rows"),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    t0         = time.perf_counter()
    stages:    Dict[str, float] = {}
    request_id = x_request_id or getattr(request.state, "request_id", None) or str(uuid.uuid4())

    # ---------------- Auth ----------------
    s0 = time.perf_counter()
    _require_auth_or_401(
        token_query=token,
        x_app_token=x_app_token,
        authorization=authorization,
    )
    stages["auth_ms"] = round((time.perf_counter() - s0) * 1000.0, 3)

    include_matrix_final = (
        include_matrix if isinstance(include_matrix, bool)
        else _coerce_bool(body.get("include_matrix"), True)
    )
    schema_only_final = (
        schema_only if isinstance(schema_only, bool)
        else _coerce_bool(body.get("schema_only"), False)
    )
    eff_limit = _effective_limit(body or {}, limit)

    schema_headers, schema_keys = _load_schema_defaults()
    schema_keys, schema_headers = _ensure_top10_keys_present(schema_keys, schema_headers)

    # ---------------- Engine ----------------
    s1     = time.perf_counter()
    engine = await _get_engine(request)
    stages["engine_ms"] = round((time.perf_counter() - s1) * 1000.0, 3)

    if schema_only_final:
        meta = {
            "route_version": INVESTMENT_ADVISOR_VERSION,
            "request_id":    request_id,
            "limit":         eff_limit,
            "mode":          mode or "",
            "schema_aligned": bool(schema_keys),
            "build_status":  "SCHEMA_ONLY",
            "dispatch":      "advanced_top10",
            "stage_durations_ms": stages,
            "duration_ms":   round((time.perf_counter() - t0) * 1000.0, 3),
        }
        return jsonable_encoder(
            _schema_only_payload(
                request_id=request_id,
                headers=schema_headers,
                keys=schema_keys,
                include_matrix=include_matrix_final,
                meta=meta,
            )
        )

    if engine is None:
        meta = {
            "route_version": INVESTMENT_ADVISOR_VERSION,
            "request_id":    request_id,
            "limit":         eff_limit,
            "mode":          mode or "",
            "schema_aligned": bool(schema_keys),
            "build_status":  "DEGRADED",
            "dispatch":      "advanced_top10",
            "warning":       "engine_unavailable",
            "stage_durations_ms": stages,
            "duration_ms":   round((time.perf_counter() - t0) * 1000.0, 3),
        }
        return jsonable_encoder(
            _schema_only_payload(
                request_id=request_id,
                headers=schema_headers,
                keys=schema_keys,
                include_matrix=include_matrix_final,
                meta=meta,
            )
        )

    # ---------------- Builder import ----------------
    s2 = time.perf_counter()
    try:
        build_top10_rows    = _load_top10_builder()
        builder_import_error = None
    except Exception as e:
        build_top10_rows    = None
        builder_import_error = f"{type(e).__name__}: {e}"
    stages["builder_import_ms"] = round((time.perf_counter() - s2) * 1000.0, 3)

    if build_top10_rows is None:
        meta = {
            "route_version": INVESTMENT_ADVISOR_VERSION,
            "request_id":    request_id,
            "limit":         eff_limit,
            "mode":          mode or "",
            "schema_aligned": bool(schema_keys),
            "build_status":  "DEGRADED",
            "dispatch":      "advanced_top10",
            "warning":       "top10_builder_unavailable",
            "detail":        builder_import_error,
            "stage_durations_ms": stages,
            "duration_ms":   round((time.perf_counter() - t0) * 1000.0, 3),
        }
        return jsonable_encoder(
            _schema_only_payload(
                request_id=request_id,
                headers=schema_headers,
                keys=schema_keys,
                include_matrix=include_matrix_final,
                meta=meta,
            )
        )

    # ---------------- Criteria preparation ----------------
    s3 = time.perf_counter()
    effective_criteria, prep_meta = _prepare_effective_criteria(body or {}, eff_limit)
    stages["criteria_prepare_ms"] = round((time.perf_counter() - s3) * 1000.0, 3)

    primary_timeout_sec  = max(3.0, _env_float("ADV_TOP10_TIMEOUT_SEC",          45.0))
    fallback_timeout_sec = max(2.0, _env_float("ADV_TOP10_FALLBACK_TIMEOUT_SEC", 15.0))

    selected_payload: Optional[Dict[str, Any]] = None
    selected_criteria  = copy.deepcopy(effective_criteria)
    fallback_used      = False
    fallback_reason    = ""
    warnings: List[str] = []

    # ---------------- Primary run ----------------
    s4 = time.perf_counter()
    try:
        payload_primary = await _run_selector_with_timeout(
            builder=build_top10_rows,
            engine=engine,
            criteria=effective_criteria,
            eff_limit=eff_limit,
            mode=mode or "",
            timeout_sec=primary_timeout_sec,
        )
        if not isinstance(payload_primary, dict):
            raise ValueError("selector returned non-dict payload")
        selected_payload = payload_primary
    except asyncio.TimeoutError:
        fallback_used   = True
        fallback_reason = f"primary_timeout_{primary_timeout_sec}s"
        warnings.append("primary_selector_timeout")
    except Exception as e:
        fallback_used   = True
        fallback_reason = f"primary_error:{type(e).__name__}"
        warnings.append(f"primary_selector_error:{type(e).__name__}:{e}")
    stages["primary_selector_ms"] = round((time.perf_counter() - s4) * 1000.0, 3)

    # If primary returned empty rows on an unconstrained request, narrow automatically
    try:
        if isinstance(selected_payload, dict):
            rows_candidate = selected_payload.get("rows")
            if prep_meta.get("request_unconstrained") and (
                not isinstance(rows_candidate, list) or len(rows_candidate) == 0
            ):
                fallback_used    = True
                fallback_reason  = "primary_empty_unconstrained"
                selected_payload = None
                warnings.append("primary_empty_for_unconstrained_request")
    except Exception:
        pass

    # ---------------- Fallback run ----------------
    if selected_payload is None:
        s5               = time.perf_counter()
        narrowed_criteria = _narrow_criteria_for_fallback(effective_criteria, eff_limit)
        try:
            payload_fallback = await _run_selector_with_timeout(
                builder=build_top10_rows,
                engine=engine,
                criteria=narrowed_criteria,
                eff_limit=min(eff_limit, _safe_int(narrowed_criteria.get("top_n"), eff_limit)),
                mode=mode or "",
                timeout_sec=fallback_timeout_sec,
            )
            if not isinstance(payload_fallback, dict):
                raise ValueError("fallback selector returned non-dict payload")
            selected_payload  = payload_fallback
            selected_criteria = narrowed_criteria
        except asyncio.TimeoutError:
            warnings.append("fallback_selector_timeout")
            fallback_reason = (
                (fallback_reason + "; " if fallback_reason else "")
                + f"fallback_timeout_{fallback_timeout_sec}s"
            )
        except Exception as e:
            warnings.append(f"fallback_selector_error:{type(e).__name__}:{e}")
            fallback_reason = (
                (fallback_reason + "; " if fallback_reason else "")
                + f"fallback_error:{type(e).__name__}"
            )
        stages["fallback_selector_ms"] = round((time.perf_counter() - s5) * 1000.0, 3)

    # ---------------- Final safe output ----------------
    if not isinstance(selected_payload, dict):
        meta = {
            "route_version":        INVESTMENT_ADVISOR_VERSION,
            "request_id":           request_id,
            "limit":                eff_limit,
            "mode":                 mode or "",
            "schema_aligned":       bool(schema_keys),
            "build_status":         "DEGRADED",
            "dispatch":             "advanced_top10",
            "request_unconstrained": prep_meta.get("request_unconstrained", False),
            "pages_explicit":       prep_meta.get("pages_explicit",         False),
            "pages_effective":      prep_meta.get("pages_effective",        []),
            "direct_symbols_count": prep_meta.get("direct_symbols_count",   0),
            "fallback_used":        fallback_used,
            "fallback_reason":      fallback_reason,
            "criteria_used":        _jsonable_snapshot(selected_criteria),
            "warnings":             warnings,
            "stage_durations_ms":   stages,
            "engine_present":       True,
            "engine_type":          type(engine).__name__,
            "duration_ms":          round((time.perf_counter() - t0) * 1000.0, 3),
        }
        return jsonable_encoder(
            _schema_only_payload(
                request_id=request_id,
                headers=schema_headers,
                keys=schema_keys,
                include_matrix=include_matrix_final,
                meta=meta,
            )
        )

    # ---------------- Normalize selector payload ----------------
    s6 = time.perf_counter()
    headers, keys, norm_rows, meta_in, status_out = _normalize_selector_payload(
        selected_payload,
        criteria_used=selected_criteria,
        eff_limit=eff_limit,
    )
    stages["normalize_ms"] = round((time.perf_counter() - s6) * 1000.0, 3)

    build_status = _s(meta_in.get("build_status"))
    if not build_status:
        build_status = "OK" if norm_rows else "WARN"

    meta_warnings   = meta_in.get("warnings")
    merged_warnings: List[str] = []
    if isinstance(meta_warnings, list):
        merged_warnings.extend([_s(x) for x in meta_warnings if _s(x)])
    merged_warnings.extend([_s(x) for x in warnings if _s(x)])

    seen_warn:      set       = set()
    dedup_warnings: List[str] = []
    for w in merged_warnings:
        if w and w not in seen_warn:
            seen_warn.add(w)
            dedup_warnings.append(w)

    meta = dict(meta_in)
    meta.update(
        {
            "route_version":        INVESTMENT_ADVISOR_VERSION,
            "request_id":           request_id,
            "limit":                eff_limit,
            "mode":                 mode or "",
            "duration_ms":          round((time.perf_counter() - t0) * 1000.0, 3),
            "schema_aligned":       bool(keys),
            "top10_fields_backfilled": True,
            "criteria_used":        _jsonable_snapshot(selected_criteria),
            "request_unconstrained": prep_meta.get("request_unconstrained", False),
            "pages_explicit":       prep_meta.get("pages_explicit",         False),
            "pages_effective":      prep_meta.get("pages_effective",        []),
            "direct_symbols_count": prep_meta.get("direct_symbols_count",   0),
            "pages_trimmed":        prep_meta.get("pages_trimmed",          False),
            "allow_row_fallback":   prep_meta.get("allow_row_fallback",     True),
            "fallback_used":        fallback_used,
            "fallback_reason":      fallback_reason,
            "engine_present":       True,
            "engine_type":          type(engine).__name__,
            "warnings":             dedup_warnings,
            "build_status":         build_status,
            "dispatch":             _s(meta_in.get("dispatch")) or "advanced_top10",
            "stage_durations_ms":   stages,
        }
    )

    response = {
        "status":      status_out or ("success" if norm_rows else "partial"),
        "page":        TOP10_PAGE_NAME,
        "sheet":       TOP10_PAGE_NAME,
        "headers":     headers,
        "keys":        keys,
        "rows":        norm_rows,
        "rows_matrix": _rows_to_matrix(norm_rows, keys) if (include_matrix_final and keys) else None,
        "version":     INVESTMENT_ADVISOR_VERSION,
        "request_id":  request_id,
        "meta":        meta,
    }

    return jsonable_encoder(response)


__all__ = ["router", "INVESTMENT_ADVISOR_VERSION"]
