#!/usr/bin/env python3
"""
routes/investment_advisor.py
--------------------------------------------------------------------------------
ADVANCED TOP10 / INVESTMENT ADVISOR ROUTER — v5.2.0
--------------------------------------------------------------------------------
CANONICAL ADVANCED OWNER • ADVANCED SHEET-ROWS OWNER • STABILITY-FIRST
TIMEOUT-GUARDED • SCHEMA-FIRST • TOP10-FIELD-HARDENED • DELEGATION SAFE
UNCONSTRAINED-REQUEST SAFE • FALLBACK SAFE • TRADE-SETUP EQUIPPED

Canonical-owner note (read first)
---------------------------------
Per `main._CONTROLLED_CANONICAL_OWNER_MAP`, this module is the canonical owner
of:
    /v1/advanced
    /v1/advanced/sheet-rows
    /v1/investment_advisor
    /v1/investment-advisor

Per `main._CONTROLLED_ROUTE_PLAN`, this module mounts BEFORE
`routes.advanced_sheet_rows`, so any signature collision on /v1/advanced/*
resolves in this module's favor (the later router is signature-skipped by
`_mount_routes_controlled`).

Per `main._allowed_prefixes_for_key["investment_advisor"]`, all paths this
module declares MUST start with one of:
    /v1/advanced
    /v1/investment_advisor
    /v1/investment-advisor
Otherwise they get filtered out by `_clone_filtered_router`.

Why this revision (v5.2.0 vs v5.1.0)
------------------------------------
- FIX: `_require_auth_or_401` now matches the project-wide flexible auth
       dispatch pattern — 6-level signature fallback that includes `path`,
       `request`, `settings`, `api_key`. Matches
       `main._call_auth_ok_flexible`, `analysis_sheet_rows._auth_passed`,
       `data_dictionary._auth_passed`, `config._call_auth_ok_flexible`.
       v5.1.0 had only a 3-level dispatch and couldn't pass `request`/`path`,
       which made it brittle against modern `core.config.auth_ok`.
- FIX: delegate resolution is now lazy + cached via `_resolve_delegate()`.
       Previously the module-level `try: from routes.advanced_analysis import
       _run_advanced_sheet_rows_impl` captured None if the order-of-imports
       was unlucky (e.g. reloaded in tests). Now a missed import is retried
       at the next call.
- FIX: `_get_engine` now checks the full engine-state chain
       (`engine`, `data_engine`, `quote_engine`, `cache_engine`) matching
       `analysis_sheet_rows`, `enriched_quote`, `advisor`. v5.1.0 only
       checked `engine`.
- FIX: `_coerce_bool` now matches the project-wide _TRUTHY/_FALSY sets used
       in main.py — adds `t`, `f`, `enabled`, `disabled`.
- FIX: `_load_top10_builder` now tries multiple module paths
       (`core.analysis.top10_selector` → `core.selectors.top10_selector` →
       `core.top10_selector` → `top10_selector`) with caching.
       v5.1.0 hard-imported a single path.
- FIX: `/health` and root responses now expose `route_owner`,
       `route_family`, `timestamp_utc`, `request_id` — consistent with other
       revised routers (config v5.9.0, data_dictionary v2.7.0, analysis
       v4.1.0) and aids `main`'s canonical-path owner diagnostics.
- FIX: `/health` now reports `delegate_import_error` when the
       advanced_analysis delegate is unavailable — aids operator diagnosis.
- FIX: Top10 endpoint meta now includes `route_owner`, `route_family` for
       consistency with the delegated sheet-rows path.
- KEEP: all existing advanced sheet-rows endpoints delegate to
       `routes.advanced_analysis._run_advanced_sheet_rows_impl` (which
       handles its own auth, so these endpoints don't need a separate
       `_require_auth_or_401` call).
- KEEP: full Top10/advisor local logic — selector, fallback retry, criteria
       preparation, schema projection, timeout guards.
- KEEP: `inspect.isawaitable` for coroutine detection (replaces v5.1.0's
       `hasattr(..., "__await__")` for consistency).

Primary endpoints
-----------------
- GET  /v1/advanced
- GET  /v1/advanced/health
- GET  /v1/advanced/sheet-rows
- GET  /v1/advanced/sheet_rows
- POST /v1/advanced/sheet-rows
- POST /v1/advanced/sheet_rows
- POST /v1/advanced/top10-investments
- POST /v1/advanced/top10
- POST /v1/advanced/investment-advisor
- POST /v1/advanced/advisor

Long-form aliases
-----------------
- GET /v1/investment_advisor, /v1/investment_advisor/health
- GET/POST /v1/investment_advisor/sheet-rows, /sheet_rows
- GET /v1/investment-advisor, /v1/investment-advisor/health
- GET/POST /v1/investment-advisor/sheet-rows, /sheet_rows
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
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, status
from fastapi.encoders import jsonable_encoder

logger = logging.getLogger("routes.investment_advisor")
logger.addHandler(logging.NullHandler())

INVESTMENT_ADVISOR_VERSION = "5.2.0"
ROUTE_OWNER_NAME = "investment_advisor"
ROUTE_FAMILY_NAME = "advanced"
TOP10_PAGE_NAME = "Top_10_Investments"

# IMPORTANT: no router prefix. main.py controlled mounting filters by allowed
# prefixes for this module, and we expose the exact canonical public paths
# explicitly. Paths must start with /v1/advanced, /v1/investment_advisor, or
# /v1/investment-advisor to survive `_clone_filtered_router`.
router = APIRouter(tags=["advanced", "investment-advisor"])


# =============================================================================
# Auth (best-effort, aligned with the project-wide flexible dispatch pattern)
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
    x_api_key: Optional[str],
    authorization: Optional[str],
) -> str:
    auth_token = (x_app_token or "").strip() or (x_api_key or "").strip()

    if authorization and authorization.strip().lower().startswith("bearer "):
        auth_token = authorization.strip().split(" ", 1)[1].strip()

    if token_query and not auth_token:
        allow_query = False
        try:
            settings = get_settings_cached()
            allow_query = bool(
                getattr(settings, "ALLOW_QUERY_TOKEN", False)
                or getattr(settings, "allow_query_token", False)
            )
        except Exception:
            allow_query = False
        if allow_query:
            auth_token = token_query.strip()

    return auth_token


def _require_auth_or_401(
    *,
    request: Optional[Request],
    token_query: Optional[str],
    x_app_token: Optional[str],
    x_api_key: Optional[str],
    authorization: Optional[str],
) -> None:
    """
    Flexible multi-signature dispatch for `core.config.auth_ok`. Matches the
    pattern used by main._call_auth_ok_flexible / analysis_sheet_rows /
    data_dictionary / config. Starts with the richest signature (path +
    request + settings + api_key) and degrades to {token} only.
    """
    # Open-mode short-circuit
    try:
        if callable(is_open_mode) and bool(is_open_mode()):
            return
    except Exception:
        pass

    if auth_ok is None:
        # Auth module not importable — fail open (matches v5.1.0 behavior
        # and the broader project convention for optional auth).
        return

    auth_token = _extract_auth_token(
        token_query=token_query,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
    )

    # Build rich context for the richest signature attempts
    path = ""
    headers_dict: Dict[str, str] = {}
    if request is not None:
        try:
            path = str(getattr(getattr(request, "url", None), "path", "") or "")
        except Exception:
            path = ""
        try:
            headers_dict = dict(request.headers)
        except Exception:
            headers_dict = {}

    # Make sure the auth-relevant headers are present even when request is None
    # or when the client used non-standard casing.
    if x_app_token:
        headers_dict.setdefault("X-APP-TOKEN", x_app_token)
    if x_api_key:
        headers_dict.setdefault("X-API-Key", x_api_key)
    if authorization:
        headers_dict.setdefault("Authorization", authorization)

    settings = None
    try:
        settings = get_settings_cached()
    except Exception:
        settings = None

    attempts: Tuple[Dict[str, Any], ...] = (
        {
            "token": auth_token or None,
            "authorization": authorization,
            "headers": headers_dict,
            "api_key": x_api_key,
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
        {
            "token": auth_token or None,
            "authorization": authorization,
        },
        {
            "token": auth_token or None,
        },
    )

    ok = False
    for kwargs in attempts:
        try:
            ok = bool(auth_ok(**kwargs))
            break
        except TypeError:
            continue
        except Exception:
            ok = False
            break

    if not ok:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
        )


# =============================================================================
# Lazy + cached delegate resolution (routes.advanced_analysis)
# =============================================================================
_DELEGATE_CACHE: Dict[str, Any] = {
    "impl": None,
    "module": None,
    "callable_name": None,
    "import_error": "",
    "attempted": False,
}


def _resolve_delegate() -> Tuple[Optional[Any], Dict[str, str]]:
    """
    Resolve `_run_advanced_sheet_rows_impl` from `routes.advanced_analysis`,
    retrying at call time if an earlier attempt failed. Keeps the first
    successful import cached.
    """
    cached = _DELEGATE_CACHE.get("impl")
    if callable(cached):
        return cached, {
            "delegate_module": str(_DELEGATE_CACHE.get("module") or ""),
            "delegate_callable": str(_DELEGATE_CACHE.get("callable_name") or ""),
        }

    # Try a primary path first, then a couple of obvious alternatives. In
    # practice only the first should succeed inside this project; the
    # alternatives exist for partial-repo diagnostics.
    probe_order: Tuple[Tuple[str, str], ...] = (
        ("routes.advanced_analysis", "_run_advanced_sheet_rows_impl"),
        ("routes.advanced_sheet_rows", "_run_advanced_sheet_rows_impl"),
    )

    last_error = ""
    for module_name, callable_name in probe_order:
        try:
            mod = importlib.import_module(module_name)
        except Exception as e:
            last_error = f"{module_name}: {type(e).__name__}: {e}"
            continue
        fn = getattr(mod, callable_name, None)
        if callable(fn):
            _DELEGATE_CACHE["impl"] = fn
            _DELEGATE_CACHE["module"] = module_name
            _DELEGATE_CACHE["callable_name"] = callable_name
            _DELEGATE_CACHE["import_error"] = ""
            _DELEGATE_CACHE["attempted"] = True
            return fn, {
                "delegate_module": module_name,
                "delegate_callable": callable_name,
            }

    _DELEGATE_CACHE["import_error"] = last_error or "delegate not found"
    _DELEGATE_CACHE["attempted"] = True
    return None, {
        "delegate_module": "",
        "delegate_callable": "",
        "delegate_import_error": _DELEGATE_CACHE["import_error"],
    }


# =============================================================================
# Engine accessor (lazy + safe)
# =============================================================================
async def _get_engine(request: Request) -> Optional[Any]:
    """
    Resolve a data engine from:
      1) request.app.state.{engine, data_engine, quote_engine, cache_engine}
      2) core.data_engine_v2.get_engine()
      3) core.data_engine.get_engine()
    """
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
            mod = importlib.import_module(modpath)
            get_engine = getattr(mod, "get_engine", None)
            if callable(get_engine):
                eng = get_engine()
                if inspect.isawaitable(eng):
                    eng = await eng
                if eng is not None:
                    return eng
        except Exception:
            continue

    return None


# =============================================================================
# Generic helpers
# =============================================================================
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


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
        if s in _TRUTHY:
            return True
        if s in _FALSY:
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
    extras = [
        ("top10_rank", "Top 10 Rank"),
        ("selection_reason", "Selection Reason"),
        ("criteria_snapshot", "Criteria Snapshot"),
        ("entry_price", "Entry Price"),
        ("stop_loss_suggested", "Stop Loss (AI)"),
        ("take_profit_suggested", "Take Profit (AI)"),
        ("risk_reward_ratio", "Risk/Reward"),
    ]

    out_keys = list(keys or [])
    out_headers = list(headers or [])

    for key, header in extras:
        if key not in out_keys:
            out_keys.append(key)
            out_headers.append(header)

    return out_keys, out_headers


def _load_schema_defaults() -> Tuple[List[str], List[str]]:
    """
    Read Top_10_Investments schema headers and keys from schema_registry.
    Multi-path fallback:
      core.sheets.schema_registry → core.schema_registry → schema_registry.
    Returns ([], []) if unavailable — callers fall back to
    _ensure_top10_keys_present.
    """
    for _sreg_path in ("core.sheets.schema_registry", "core.schema_registry", "schema_registry"):
        try:
            _sreg = importlib.import_module(_sreg_path)
            get_sheet_spec = getattr(_sreg, "get_sheet_spec", None)
            if not callable(get_sheet_spec):
                continue
            spec = get_sheet_spec(TOP10_PAGE_NAME)
            cols = getattr(spec, "columns", None) or []
            keys = [getattr(c, "key", "") for c in cols]
            headers = [getattr(c, "header", "") for c in cols]
            keys = [k for k in keys if isinstance(k, str) and k]
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
        "status": "partial",
        "page": TOP10_PAGE_NAME,
        "sheet": TOP10_PAGE_NAME,
        "headers": headers,
        "keys": keys,
        "rows": [],
        "rows_matrix": [] if (include_matrix and keys) else None,
        "version": INVESTMENT_ADVISOR_VERSION,
        "route_owner": ROUTE_OWNER_NAME,
        "route_family": ROUTE_FAMILY_NAME,
        "request_id": request_id,
        "meta": meta,
    }


def _normalize_selector_payload(
    payload: Dict[str, Any],
    *,
    criteria_used: Dict[str, Any],
    eff_limit: int,
) -> Tuple[List[str], List[str], List[Dict[str, Any]], Dict[str, Any], str]:
    headers = payload.get("headers") or []
    keys = payload.get("keys") or []
    rows = payload.get("rows") or []
    status_out = _s(payload.get("status")) or "success"

    if not isinstance(headers, list):
        headers = []
    if not isinstance(keys, list):
        keys = []
    if not isinstance(rows, list):
        rows = []

    if not headers or not keys:
        schema_headers, schema_keys = _load_schema_defaults()
        if not headers:
            headers = schema_headers
        if not keys:
            keys = schema_keys

    keys, headers = _ensure_top10_keys_present(list(keys), list(headers))

    dict_rows = [dict(r) for r in rows if isinstance(r, dict)]
    dict_rows = _apply_top10_field_backfill(dict_rows, keys=keys, criteria=criteria_used)
    dict_rows = _rank_rows_in_order(dict_rows)
    norm_rows = _ensure_schema_projection(dict_rows, keys)
    norm_rows = norm_rows[:eff_limit]

    meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    return headers, keys, norm_rows, dict(meta), status_out


# Top10 builder cache (multi-path, retry-on-miss)
_TOP10_BUILDER_CACHE: Dict[str, Any] = {"fn": None, "module": "", "error": ""}


def _load_top10_builder() -> Any:
    """
    Multi-path fallback for the Top10 selector builder:
      core.analysis.top10_selector.build_top10_rows
      core.selectors.top10_selector.build_top10_rows
      core.top10_selector.build_top10_rows
      top10_selector.build_top10_rows
    Caches first success; retries at each call if still unresolved.
    Raises the last error if all paths fail (preserves v5.1.0 contract that
    the caller catches and degrades gracefully).
    """
    cached = _TOP10_BUILDER_CACHE.get("fn")
    if callable(cached):
        return cached

    last_error: Optional[Exception] = None
    for module_name in (
        "core.analysis.top10_selector",
        "core.selectors.top10_selector",
        "core.top10_selector",
        "top10_selector",
    ):
        try:
            mod = importlib.import_module(module_name)
            fn = getattr(mod, "build_top10_rows", None)
            if callable(fn):
                _TOP10_BUILDER_CACHE["fn"] = fn
                _TOP10_BUILDER_CACHE["module"] = module_name
                _TOP10_BUILDER_CACHE["error"] = ""
                return fn
        except Exception as e:
            last_error = e
            continue

    _TOP10_BUILDER_CACHE["error"] = f"{type(last_error).__name__}: {last_error}" if last_error else "build_top10_rows not found"
    if last_error is not None:
        raise last_error
    raise ImportError("Could not resolve build_top10_rows in any known path")


def _effective_limit(body: Dict[str, Any], limit_q: Optional[int]) -> int:
    max_limit = max(1, _env_int("ADV_TOP10_MAX_LIMIT", 50))
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


def _prepare_effective_criteria(
    body: Dict[str, Any], eff_limit: int
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    crit = _flatten_criteria(body or {})
    pages = _normalize_list(crit.get("pages_selected") or crit.get("pages") or crit.get("selected_pages"))
    direct_symbols = _normalize_list(crit.get("direct_symbols") or crit.get("symbols") or crit.get("tickers"))

    request_unconstrained = (not pages) and (not direct_symbols)
    pages_explicit = bool(pages)

    if request_unconstrained:
        pages = _env_csv("ADV_TOP10_DEFAULT_PAGES", ["Market_Leaders", "Global_Markets"])
        crit["pages_selected"] = pages

    max_pages = max(1, _env_int("ADV_TOP10_MAX_PAGES", 5))
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
        "pages_explicit": pages_explicit,
        "pages_effective": list(pages),
        "direct_symbols_count": len(direct_symbols),
        "pages_trimmed": pages_trimmed,
        "allow_row_fallback": True,
    }

    return crit, prep_meta


def _narrow_criteria_for_fallback(criteria: Dict[str, Any], eff_limit: int) -> Dict[str, Any]:
    narrowed = copy.deepcopy(criteria)

    fallback_pages_cap = max(1, _env_int("ADV_TOP10_FALLBACK_MAX_PAGES", 2))
    fallback_top_n = max(1, min(eff_limit, _env_int("ADV_TOP10_FALLBACK_TOP_N", min(3, eff_limit))))

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
        narrowed["top_n"] = fallback_top_n

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
# Advanced sheet-rows delegation
# =============================================================================
def _build_get_body(
    *,
    page: str,
    sheet: str,
    sheet_name: str,
    name: str,
    tab: str,
    symbols: str,
    tickers: str,
    direct_symbols: str,
    symbol: str,
    ticker: str,
    code: str,
    requested_symbol: str,
    limit: Optional[int],
    offset: Optional[int],
    top_n: Optional[int],
    schema_only: Optional[bool],
    headers_only: Optional[bool],
) -> Dict[str, Any]:
    body: Dict[str, Any] = {}
    for k, v in {
        "page": page,
        "sheet": sheet,
        "sheet_name": sheet_name,
        "name": name,
        "tab": tab,
        "symbols": symbols,
        "tickers": tickers,
        "direct_symbols": direct_symbols,
        "symbol": symbol,
        "ticker": ticker,
        "code": code,
        "requested_symbol": requested_symbol,
        "limit": limit,
        "offset": offset,
        "top_n": top_n,
        "schema_only": schema_only,
        "headers_only": headers_only,
    }.items():
        if v not in (None, ""):
            body[k] = v
    return body


async def _delegate_advanced_sheet_rows(
    *,
    request: Request,
    body: Dict[str, Any],
    mode: str = "",
    include_matrix_q: Optional[bool] = None,
    token: Optional[str] = None,
    x_app_token: Optional[str] = None,
    x_api_key: Optional[str] = None,
    authorization: Optional[str] = None,
    x_request_id: Optional[str] = None,
):
    impl, impl_meta = _resolve_delegate()

    if impl is None:
        return {
            "status": "error",
            "error": "advanced_analysis delegate unavailable",
            "detail": _DELEGATE_CACHE.get("import_error", "") or impl_meta.get("delegate_import_error", ""),
            "route_family": ROUTE_FAMILY_NAME,
            "route_owner": ROUTE_OWNER_NAME,
            "version": INVESTMENT_ADVISOR_VERSION,
            "timestamp_utc": _now_utc(),
            "advanced_delegate_import_error": _DELEGATE_CACHE.get("import_error", ""),
        }

    payload = await impl(
        request=request,
        body=body,
        mode=mode,
        include_matrix_q=include_matrix_q,
        token=token,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
        x_request_id=x_request_id,
    )

    if isinstance(payload, dict):
        meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
        meta = dict(meta)
        meta.setdefault("advanced_delegate", f"{impl_meta.get('delegate_module')}.{impl_meta.get('delegate_callable')}")
        meta.setdefault("advanced_family_owner", "routes.investment_advisor")
        meta.setdefault("delegate_module", impl_meta.get("delegate_module"))
        meta.setdefault("delegate_callable", impl_meta.get("delegate_callable"))
        meta.setdefault("route_owner", ROUTE_OWNER_NAME)
        meta.setdefault("route_family", ROUTE_FAMILY_NAME)
        payload["meta"] = meta
        payload["route_family"] = ROUTE_FAMILY_NAME
        payload["route_owner"] = ROUTE_OWNER_NAME
        payload["advanced_owner"] = "routes.investment_advisor"

    return payload


# =============================================================================
# Root / health
# =============================================================================
@router.get("/v1/advanced")
@router.get("/v1/investment_advisor")
@router.get("/v1/investment-advisor")
async def advanced_root(request: Request) -> Dict[str, Any]:
    impl, impl_meta = _resolve_delegate()
    request_id = _s(getattr(getattr(request, "state", None), "request_id", "")) or uuid.uuid4().hex[:12]
    return jsonable_encoder(
        {
            "status": "ok",
            "service": "investment_advisor",
            "version": INVESTMENT_ADVISOR_VERSION,
            "route_owner": ROUTE_OWNER_NAME,
            "route_family": ROUTE_FAMILY_NAME,
            "delegate_available": impl is not None,
            "delegate_module": impl_meta.get("delegate_module"),
            "delegate_callable": impl_meta.get("delegate_callable"),
            "canonical_paths": [
                "/v1/advanced",
                "/v1/advanced/sheet-rows",
                "/v1/advanced/sheet_rows",
            ],
            "alias_paths": [
                "/v1/investment_advisor",
                "/v1/investment_advisor/sheet-rows",
                "/v1/investment_advisor/sheet_rows",
                "/v1/investment-advisor",
                "/v1/investment-advisor/sheet-rows",
                "/v1/investment-advisor/sheet_rows",
            ],
            "top10_paths": [
                "/v1/advanced/top10-investments",
                "/v1/advanced/top10",
                "/v1/advanced/investment-advisor",
                "/v1/advanced/advisor",
            ],
            "request_id": request_id,
            "timestamp_utc": _now_utc(),
        }
    )


@router.get("/v1/advanced/health")
@router.get("/v1/investment_advisor/health")
@router.get("/v1/investment-advisor/health")
async def advanced_health(request: Request) -> Dict[str, Any]:
    engine = await _get_engine(request)
    impl, impl_meta = _resolve_delegate()
    request_id = _s(getattr(getattr(request, "state", None), "request_id", "")) or uuid.uuid4().hex[:12]

    # Capability flags for core.config integration (mirrors config router health)
    capabilities = {
        "auth_ok_callable": callable(auth_ok),
        "is_open_mode_callable": callable(is_open_mode),
        "get_settings_cached_callable": callable(get_settings_cached),
    }

    return jsonable_encoder(
        {
            "status": "ok" if engine else "degraded",
            "service": "advanced_top10",
            "version": INVESTMENT_ADVISOR_VERSION,
            "route_owner": ROUTE_OWNER_NAME,
            "route_family": ROUTE_FAMILY_NAME,
            "engine_available": bool(engine),
            "engine_type": type(engine).__name__ if engine else "none",
            "delegate_available": impl is not None,
            "delegate_module": impl_meta.get("delegate_module"),
            "delegate_callable": impl_meta.get("delegate_callable"),
            "delegate_import_error": _DELEGATE_CACHE.get("import_error", "") if impl is None else "",
            "top10_builder_cached": bool(_TOP10_BUILDER_CACHE.get("fn")),
            "top10_builder_module": _TOP10_BUILDER_CACHE.get("module", ""),
            "capabilities": capabilities,
            "path": str(getattr(getattr(request, "url", None), "path", "")),
            "request_id": request_id,
            "timestamp_utc": _now_utc(),
        }
    )


# =============================================================================
# Advanced sheet-rows endpoints — delegate to routes.advanced_analysis
# (auth is handled inside the delegate; no _require_auth_or_401 here)
# =============================================================================
@router.get("/v1/advanced/sheet-rows")
@router.get("/v1/advanced/sheet_rows")
@router.get("/v1/investment_advisor/sheet-rows")
@router.get("/v1/investment_advisor/sheet_rows")
@router.get("/v1/investment-advisor/sheet-rows")
@router.get("/v1/investment-advisor/sheet_rows")
async def advanced_sheet_rows_get(
    request: Request,
    page: str = Query(default=""),
    sheet: str = Query(default=""),
    sheet_name: str = Query(default=""),
    name: str = Query(default=""),
    tab: str = Query(default=""),
    symbols: str = Query(default=""),
    tickers: str = Query(default=""),
    direct_symbols: str = Query(default=""),
    symbol: str = Query(default=""),
    ticker: str = Query(default=""),
    code: str = Query(default=""),
    requested_symbol: str = Query(default=""),
    limit: Optional[int] = Query(default=None),
    offset: Optional[int] = Query(default=None),
    top_n: Optional[int] = Query(default=None),
    mode: str = Query(default=""),
    include_matrix_q: Optional[bool] = Query(default=None, alias="include_matrix"),
    schema_only: Optional[bool] = Query(default=None),
    headers_only: Optional[bool] = Query(default=None),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
):
    body = _build_get_body(
        page=page,
        sheet=sheet,
        sheet_name=sheet_name,
        name=name,
        tab=tab,
        symbols=symbols,
        tickers=tickers,
        direct_symbols=direct_symbols,
        symbol=symbol,
        ticker=ticker,
        code=code,
        requested_symbol=requested_symbol,
        limit=limit,
        offset=offset,
        top_n=top_n,
        schema_only=schema_only,
        headers_only=headers_only,
    )

    return await _delegate_advanced_sheet_rows(
        request=request,
        body=body,
        mode=mode,
        include_matrix_q=include_matrix_q,
        token=token,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
        x_request_id=x_request_id,
    )


@router.post("/v1/advanced/sheet-rows")
@router.post("/v1/advanced/sheet_rows")
@router.post("/v1/investment_advisor/sheet-rows")
@router.post("/v1/investment_advisor/sheet_rows")
@router.post("/v1/investment-advisor/sheet-rows")
@router.post("/v1/investment-advisor/sheet_rows")
async def advanced_sheet_rows_post(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    mode: str = Query(default=""),
    include_matrix_q: Optional[bool] = Query(default=None, alias="include_matrix"),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
):
    return await _delegate_advanced_sheet_rows(
        request=request,
        body=body,
        mode=mode,
        include_matrix_q=include_matrix_q,
        token=token,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
        x_request_id=x_request_id,
    )


# =============================================================================
# Existing Top10 endpoints (local selector pipeline — NOT delegated)
# =============================================================================
@router.post("/v1/advanced/top10-investments")
@router.post("/v1/advanced/top10")
@router.post("/v1/advanced/investment-advisor")
@router.post("/v1/advanced/advisor")
async def advanced_top10_investments(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    mode: str = Query(default="", description="Optional mode hint for engine/provider"),
    include_matrix: Optional[bool] = Query(default=None, description="Return rows_matrix for legacy clients"),
    limit: Optional[int] = Query(default=None, ge=1, le=50, description="How many items to return (1..50)"),
    schema_only: Optional[bool] = Query(default=None, description="Return schema with no rows"),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    t0 = time.perf_counter()
    stages: Dict[str, float] = {}
    request_id = x_request_id or _s(getattr(getattr(request, "state", None), "request_id", "")) or uuid.uuid4().hex[:12]

    s0 = time.perf_counter()
    _require_auth_or_401(
        request=request,
        token_query=token,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
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

    s1 = time.perf_counter()
    engine = await _get_engine(request)
    stages["engine_ms"] = round((time.perf_counter() - s1) * 1000.0, 3)

    if schema_only_final:
        meta = {
            "route_version": INVESTMENT_ADVISOR_VERSION,
            "route_owner": ROUTE_OWNER_NAME,
            "route_family": ROUTE_FAMILY_NAME,
            "request_id": request_id,
            "limit": eff_limit,
            "mode": mode or "",
            "schema_aligned": bool(schema_keys),
            "build_status": "SCHEMA_ONLY",
            "dispatch": "advanced_top10",
            "stage_durations_ms": stages,
            "duration_ms": round((time.perf_counter() - t0) * 1000.0, 3),
            "timestamp_utc": _now_utc(),
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
            "route_owner": ROUTE_OWNER_NAME,
            "route_family": ROUTE_FAMILY_NAME,
            "request_id": request_id,
            "limit": eff_limit,
            "mode": mode or "",
            "schema_aligned": bool(schema_keys),
            "build_status": "DEGRADED",
            "dispatch": "advanced_top10",
            "warning": "engine_unavailable",
            "stage_durations_ms": stages,
            "duration_ms": round((time.perf_counter() - t0) * 1000.0, 3),
            "timestamp_utc": _now_utc(),
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

    s2 = time.perf_counter()
    try:
        build_top10_rows = _load_top10_builder()
        builder_import_error = None
    except Exception as e:
        build_top10_rows = None
        builder_import_error = f"{type(e).__name__}: {e}"
    stages["builder_import_ms"] = round((time.perf_counter() - s2) * 1000.0, 3)

    if build_top10_rows is None:
        meta = {
            "route_version": INVESTMENT_ADVISOR_VERSION,
            "route_owner": ROUTE_OWNER_NAME,
            "route_family": ROUTE_FAMILY_NAME,
            "request_id": request_id,
            "limit": eff_limit,
            "mode": mode or "",
            "schema_aligned": bool(schema_keys),
            "build_status": "DEGRADED",
            "dispatch": "advanced_top10",
            "warning": "top10_builder_unavailable",
            "detail": builder_import_error,
            "stage_durations_ms": stages,
            "duration_ms": round((time.perf_counter() - t0) * 1000.0, 3),
            "timestamp_utc": _now_utc(),
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

    s3 = time.perf_counter()
    effective_criteria, prep_meta = _prepare_effective_criteria(body or {}, eff_limit)
    stages["criteria_prepare_ms"] = round((time.perf_counter() - s3) * 1000.0, 3)

    primary_timeout_sec = max(3.0, _env_float("ADV_TOP10_TIMEOUT_SEC", 45.0))
    fallback_timeout_sec = max(2.0, _env_float("ADV_TOP10_FALLBACK_TIMEOUT_SEC", 15.0))

    selected_payload: Optional[Dict[str, Any]] = None
    selected_criteria = copy.deepcopy(effective_criteria)
    fallback_used = False
    fallback_reason = ""
    warnings: List[str] = []

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
        fallback_used = True
        fallback_reason = f"primary_timeout_{primary_timeout_sec}s"
        warnings.append("primary_selector_timeout")
    except Exception as e:
        fallback_used = True
        fallback_reason = f"primary_error:{type(e).__name__}"
        warnings.append(f"primary_selector_error:{type(e).__name__}:{e}")
    stages["primary_selector_ms"] = round((time.perf_counter() - s4) * 1000.0, 3)

    try:
        if isinstance(selected_payload, dict):
            rows_candidate = selected_payload.get("rows")
            if prep_meta.get("request_unconstrained") and (
                not isinstance(rows_candidate, list) or len(rows_candidate) == 0
            ):
                fallback_used = True
                fallback_reason = "primary_empty_unconstrained"
                selected_payload = None
                warnings.append("primary_empty_for_unconstrained_request")
    except Exception:
        pass

    if selected_payload is None:
        s5 = time.perf_counter()
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
            selected_payload = payload_fallback
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

    if not isinstance(selected_payload, dict):
        meta = {
            "route_version": INVESTMENT_ADVISOR_VERSION,
            "route_owner": ROUTE_OWNER_NAME,
            "route_family": ROUTE_FAMILY_NAME,
            "request_id": request_id,
            "limit": eff_limit,
            "mode": mode or "",
            "schema_aligned": bool(schema_keys),
            "build_status": "DEGRADED",
            "dispatch": "advanced_top10",
            "request_unconstrained": prep_meta.get("request_unconstrained", False),
            "pages_explicit": prep_meta.get("pages_explicit", False),
            "pages_effective": prep_meta.get("pages_effective", []),
            "direct_symbols_count": prep_meta.get("direct_symbols_count", 0),
            "fallback_used": fallback_used,
            "fallback_reason": fallback_reason,
            "criteria_used": _jsonable_snapshot(selected_criteria),
            "warnings": warnings,
            "stage_durations_ms": stages,
            "engine_present": True,
            "engine_type": type(engine).__name__,
            "duration_ms": round((time.perf_counter() - t0) * 1000.0, 3),
            "timestamp_utc": _now_utc(),
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

    meta_warnings = meta_in.get("warnings")
    merged_warnings: List[str] = []
    if isinstance(meta_warnings, list):
        merged_warnings.extend([_s(x) for x in meta_warnings if _s(x)])
    merged_warnings.extend([_s(x) for x in warnings if _s(x)])

    seen_warn: set = set()
    dedup_warnings: List[str] = []
    for w in merged_warnings:
        if w and w not in seen_warn:
            seen_warn.add(w)
            dedup_warnings.append(w)

    meta = dict(meta_in)
    meta.update(
        {
            "route_version": INVESTMENT_ADVISOR_VERSION,
            "route_owner": ROUTE_OWNER_NAME,
            "route_family": ROUTE_FAMILY_NAME,
            "request_id": request_id,
            "limit": eff_limit,
            "mode": mode or "",
            "duration_ms": round((time.perf_counter() - t0) * 1000.0, 3),
            "schema_aligned": bool(keys),
            "top10_fields_backfilled": True,
            "criteria_used": _jsonable_snapshot(selected_criteria),
            "request_unconstrained": prep_meta.get("request_unconstrained", False),
            "pages_explicit": prep_meta.get("pages_explicit", False),
            "pages_effective": prep_meta.get("pages_effective", []),
            "direct_symbols_count": prep_meta.get("direct_symbols_count", 0),
            "pages_trimmed": prep_meta.get("pages_trimmed", False),
            "allow_row_fallback": prep_meta.get("allow_row_fallback", True),
            "fallback_used": fallback_used,
            "fallback_reason": fallback_reason,
            "engine_present": True,
            "engine_type": type(engine).__name__,
            "warnings": dedup_warnings,
            "build_status": build_status,
            "dispatch": _s(meta_in.get("dispatch")) or "advanced_top10",
            "stage_durations_ms": stages,
            "timestamp_utc": _now_utc(),
        }
    )

    response = {
        "status": status_out or ("success" if norm_rows else "partial"),
        "page": TOP10_PAGE_NAME,
        "sheet": TOP10_PAGE_NAME,
        "headers": headers,
        "keys": keys,
        "rows": norm_rows,
        "rows_matrix": _rows_to_matrix(norm_rows, keys) if (include_matrix_final and keys) else None,
        "version": INVESTMENT_ADVISOR_VERSION,
        "route_owner": ROUTE_OWNER_NAME,
        "route_family": ROUTE_FAMILY_NAME,
        "request_id": request_id,
        "meta": meta,
    }

    return jsonable_encoder(response)


__all__ = [
    "router",
    "INVESTMENT_ADVISOR_VERSION",
    "ROUTE_OWNER_NAME",
    "ROUTE_FAMILY_NAME",
]
