#!/usr/bin/env python3
"""
routes/advisor.py
================================================================================
ADVISOR ROUTER — v6.8.2
================================================================================
SPECIAL-PAGE PROXY-FIRST • ROOT/ANALYSIS ALIGNED • SHORT-ADVISOR HARDENED •
JSON-SAFE • GET+POST SAFE • FAIL-SOFT • CONTRACT-PROJECTED • ENGINE-TOLERANT

v6.8.2 Fixes (vs v6.8.1)
------------------------
- FIX [HIGH]: `KNOWN_CANONICAL_HEADER_COUNTS` aligned to canonical schema
  (matches engine v5.47.4, analysis route v4.1.2, advisor v5.1.1):
      Market_Leaders / Global_Markets / Commodities_FX / Mutual_Funds /
      My_Portfolio = 80 each (was 99 / 112 / 86 / 94 / 110)
      Top_10_Investments = 83 (was 106)
      Insights_Analysis = 7 (was 9)
      Data_Dictionary = 9 (unchanged)
  v6.8.1 carried inflated non-canonical counts that the analysis route's
  v4.1.0 changelog already flagged as wrong. Downstream consumers
  validating header count against this map were rejecting valid payloads.

- FIX [HIGH — silent data loss]: header/key mismatch in row projection.
  `_resolve_contract_headers` was returning DISPLAY HEADERS ("Symbol",
  "P/B", "52W High") but row dicts in `row_objects` are keyed by
  CANONICAL KEYS ("symbol", "pb_ratio", "week_52_high"). When
  `_mapping_list_to_rows(rows, headers)` then did `row.get(header)`,
  it returned None for every cell. Most rendered cells were silently
  blank when the projection code path ran. Replaced with
  `_resolve_contract_headers_keys()` which returns paired (headers, keys),
  and `_mapping_list_to_rows_with_keys()` which looks up by KEY and
  outputs in HEADER-ordered positions.

- FIX: `TOP10_SPECIAL_FIELDS` split into required canonical (3) and
  optional trade-setup (4). v6.8.1 always padded all 7 fields onto
  Top_10 rows even when delegating to a bridge that returns the
  canonical 83-col Top_10 contract — yielding 87 cols misaligned with
  the downstream Sheets contract. Now: 3 canonical extras are always
  ensured; 4 trade-setup fields are appended only if present in the
  source data (preserves top10_selector v4.9.0 enrichment without
  breaking bridge-delegated flows).

- FIX: `_looks_like_fallback_only_top10` now also inspects `data_provider`
  for known placeholder/fallback markers (`placeholder_no_live_data`
  from analysis route v4.1.2, `advisor_fallback_no_live_data` from
  investment_advisor v5.1.1). v6.8.1 only checked `selection_reason`
  text and missed the new no-live-data markers.

- FIX: defensively strips internal coordination fields
  (`_skip_recommendation_synthesis`, `_internal_*`, `_meta_*`,
  `_debug_*`, `_trace_*`, `unit_normalization_warnings`,
  `intrinsic_value_source`) from every row before final emission.

- FIX: bridge results' status="warn" (engine v5.47.4 emits this for
  partial-success cases) is now treated as usable when rows are present.
  v6.8.1's `_has_usable_payload` didn't differentiate warn from error.

v6.8.1 Notes (preserved)
------------------------
- _run_advisor_logic accepts x_api_key and forwards to bridges + auth.
- _auth_ok_via_env_match includes x_api_key in presented credentials.
- Both bridge delegates accept and forward x_api_key.
- Public API, __all__, route paths, and all public-route signatures preserved.
"""

from __future__ import annotations

import asyncio
import hmac
import inspect
import logging
import math
import os
import time
import uuid
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal
from enum import Enum
from importlib import import_module
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from fastapi import Body, Header, HTTPException, Query, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.routing import APIRouter

logger = logging.getLogger("routes.advisor")
logger.addHandler(logging.NullHandler())

ADVISOR_VERSION = "6.8.2"


def _secure_equals(a: str, b: str) -> bool:
    """Constant-time string comparison (prevents timing attacks)."""
    try:
        return hmac.compare_digest(str(a), str(b))
    except Exception:
        return str(a) == str(b)

router = APIRouter(prefix="/v1/advisor", tags=["advisor"])

DEFAULT_ADVISOR_PAGE = "Top_10_Investments"

BASE_PAGES = {
    "Market_Leaders",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
    "My_Portfolio",
}
DERIVED_PAGES = {"Top_10_Investments", "Insights_Analysis", "Data_Dictionary"}

# v6.8.2: aligned to canonical schema (engine v5.47.4 / route v4.1.2 / advisor v5.1.1).
# Used as hardcoded fallback ONLY when schema_registry is unavailable.
# Primary source remains _contract_header_count() → _resolve_contract_headers_keys()
# → schema_registry.
KNOWN_CANONICAL_HEADER_COUNTS: Dict[str, int] = {
    "Market_Leaders":     80,
    "Global_Markets":     80,
    "Commodities_FX":     80,
    "Mutual_Funds":       80,
    "My_Portfolio":       80,
    "Insights_Analysis":   7,
    "Top_10_Investments": 83,   # 80 canonical + 3 Top10 extras
    "Data_Dictionary":     9,
}

# v6.8.2: split TOP10 extras into REQUIRED (canonical schema contract) and
# OPTIONAL (top10_selector v4.9.0 trade-setup enrichment). Required fields
# are always ensured; optional fields are appended only if present in source.
TOP10_REQUIRED_EXTRAS: Tuple[str, ...] = (
    "top10_rank", "selection_reason", "criteria_snapshot",
)
TOP10_REQUIRED_EXTRA_HEADERS: Dict[str, str] = {
    "top10_rank": "Top10 Rank",
    "selection_reason": "Selection Reason",
    "criteria_snapshot": "Criteria Snapshot",
}
TOP10_OPTIONAL_TRADE_FIELDS: Tuple[str, ...] = (
    "entry_price", "stop_loss_suggested", "take_profit_suggested", "risk_reward_ratio",
)
TOP10_OPTIONAL_TRADE_HEADERS: Dict[str, str] = {
    "entry_price": "Entry Price",
    "stop_loss_suggested": "Stop Loss",
    "take_profit_suggested": "Take Profit",
    "risk_reward_ratio": "Risk/Reward Ratio",
}

# Backward-compat name preserved (some external code may reference it).
# v6.8.2: now equals required + optional, but only required fields are
# always padded. Optional fields are added per-row only if present.
TOP10_SPECIAL_FIELDS: Tuple[str, ...] = TOP10_REQUIRED_EXTRAS + TOP10_OPTIONAL_TRADE_FIELDS

TOP10_BUSINESS_SIGNAL_FIELDS: Tuple[str, ...] = (
    "symbol", "name", "asset_class", "exchange", "currency", "country", "sector", "industry",
    "current_price", "price_change", "percent_change", "risk_score", "valuation_score",
    "overall_score", "opportunity_score", "risk_bucket", "confidence_bucket", "recommendation",
)

# v6.8.2: financial signal fields — at least one of these populated indicates
# the row contains real upstream data, not just an identity-only placeholder.
TOP10_FINANCIAL_SIGNAL_FIELDS: Tuple[str, ...] = (
    "current_price", "previous_close", "overall_score", "opportunity_score",
    "value_score", "quality_score", "momentum_score", "growth_score",
    "expected_roi_3m", "expected_roi_1m", "expected_roi_12m",
    "forecast_confidence", "confidence_score", "pe_ttm", "market_cap",
    "recommendation", "rsi_14", "beta_5y",
)

# v6.8.2: known placeholder/fallback data_provider markers from upstream.
# When seen, the row is a fallback-only row with no live data.
PLACEHOLDER_DATA_PROVIDERS: Tuple[str, ...] = (
    "placeholder_no_live_data",
    "advisor_fallback_no_live_data",
    "analysis_sheet_rows.placeholder_fallback",  # legacy v4.1.1 marker
    "advisor_fallback",
)

# v6.8.2: internal coordination flags that should be stripped from every row
# before final emission. Engine v5.47.4 strips these at source but proxies,
# legacy engines, and cached snapshots may still carry them.
_INTERNAL_FIELD_PREFIXES: Tuple[str, ...] = (
    "_skip_", "_internal_", "_meta_", "_debug_", "_trace_",
)
_INTERNAL_FIELDS_TO_STRIP: set = {
    "_skip_recommendation_synthesis",
    "_placeholder",
    "unit_normalization_warnings",
    "intrinsic_value_source",
}


def _strip_internal_fields(row: Any) -> Any:
    """v6.8.2: Remove engine internal coordination flags from a row dict."""
    if not isinstance(row, dict):
        return row
    keys_to_remove: List[str] = []
    for k in list(row.keys()):
        ks = str(k)
        if ks in _INTERNAL_FIELDS_TO_STRIP:
            keys_to_remove.append(k)
            continue
        if any(ks.startswith(prefix) for prefix in _INTERNAL_FIELD_PREFIXES):
            keys_to_remove.append(k)
    for k in keys_to_remove:
        try:
            del row[k]
        except Exception:
            pass
    return row


PROMETHEUS_AVAILABLE = False
try:
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest  # type: ignore
    PROMETHEUS_AVAILABLE = True
except Exception:
    CONTENT_TYPE_LATEST = "text/plain"
    generate_latest = None  # type: ignore

try:
    from core.config import auth_ok, get_settings_cached, is_open_mode  # type: ignore
except Exception:
    auth_ok = None  # type: ignore
    is_open_mode = None  # type: ignore
    def get_settings_cached(*args: Any, **kwargs: Any) -> Any:  # type: ignore
        return None

# v6.8.2: cache stores PAIRED (headers, keys) tuples instead of just headers
_SCHEMA_CONTRACT_CACHE: Dict[str, Tuple[List[str], List[str]]] = {}
AUTH_ENV_TOKEN_NAMES: Tuple[str, ...] = (
    "TFB_TOKEN", "APP_TOKEN", "BACKEND_TOKEN", "BACKUP_APP_TOKEN",
    "X_APP_TOKEN", "AUTH_TOKEN", "TOKEN", "TFB_APP_TOKEN",
)


def _strip(v: Any) -> str:
    if v is None:
        return ""
    try:
        s = str(v).strip()
    except Exception:
        return ""
    return "" if s.lower() in {"none", "null", "nil", "undefined"} else s


def _safe_dict(v: Any) -> Dict[str, Any]:
    return dict(v) if isinstance(v, Mapping) else {}


def _safe_int(v: Any, default: int) -> int:
    try:
        if v is None or isinstance(v, bool):
            return default
        return int(float(v))
    except Exception:
        return default


def _safe_float(v: Any, default: float) -> float:
    try:
        if v is None or isinstance(v, bool):
            return default
        out = float(v)
        if math.isnan(out) or math.isinf(out):
            return default
        return out
    except Exception:
        return default


def _boolish(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    s = _strip(v).lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


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


def _get_list(data: Mapping[str, Any], *keys: str) -> List[str]:
    for key in keys:
        value = data.get(key)
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
            out = _split_csv(value)
            if out:
                return out
    return []


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _request_id(request: Request, x_request_id: Optional[str]) -> str:
    rid = _strip(x_request_id)
    if rid:
        return rid
    try:
        state_rid = _strip(getattr(request.state, "request_id", ""))
        if state_rid:
            return state_rid
    except Exception:
        pass
    return uuid.uuid4().hex[:12]


def _safe_engine_type(engine: Any) -> str:
    if engine is None:
        return "none"
    try:
        return f"{engine.__class__.__module__}.{engine.__class__.__name__}"
    except Exception:
        return type(engine).__name__


def _timeout_seconds(env_name: str, default: float) -> float:
    return max(0.1, _safe_float(os.getenv(env_name), default))


def _resolver_timeout(stage: str, *, page: str) -> float:
    page = _strip(page)
    defaults = {
        "bridge": 12.0 if page in DERIVED_PAGES else 18.0,
        "engine": 18.0 if page in DERIVED_PAGES else 25.0,
        "runner": 12.0 if page in DERIVED_PAGES else 22.0,
        "top10": 14.0,
    }
    env_map = {
        "bridge": "TFB_ADVISOR_BRIDGE_TIMEOUT_SEC",
        "engine": "TFB_ADVISOR_ENGINE_TIMEOUT_SEC",
        "runner": "TFB_ADVISOR_RUNNER_TIMEOUT_SEC",
        "top10": "TFB_TOP10_BUILDER_TIMEOUT_SEC",
    }
    return _timeout_seconds(env_map[stage], defaults[stage])


def _row_count(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (list, tuple, set)):
        return len(value)
    if isinstance(value, Mapping):
        return len(value)
    return 1


def _json_safe(value: Any) -> Any:
    def _clean(obj: Any) -> Any:
        if obj is None:
            return None
        if isinstance(obj, (str, int, bool)):
            return obj
        if isinstance(obj, float):
            return None if math.isnan(obj) or math.isinf(obj) else obj
        if isinstance(obj, Decimal):
            try:
                f = float(obj)
                return None if math.isnan(f) or math.isinf(f) else f
            except Exception:
                return _strip(obj)
        if isinstance(obj, (datetime, date, dt_time)):
            try:
                return obj.isoformat()
            except Exception:
                return _strip(obj)
        if isinstance(obj, Enum):
            return _clean(obj.value)
        if is_dataclass(obj):
            try:
                return _clean(asdict(obj))
            except Exception:
                return _strip(obj)
        if isinstance(obj, Mapping):
            return {str(k): _clean(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, set)):
            return [_clean(x) for x in obj]
        if hasattr(obj, "model_dump") and callable(getattr(obj, "model_dump")):
            try:
                return _clean(obj.model_dump(mode="python"))
            except Exception:
                pass
        if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
            try:
                return _clean(obj.dict())
            except Exception:
                pass
        try:
            return jsonable_encoder(obj)
        except Exception:
            return _strip(obj)
    return _clean(value)


def _is_open_mode_enabled() -> bool:
    try:
        if callable(is_open_mode):
            result = is_open_mode()
            if inspect.isawaitable(result):
                return False
            return bool(result)
    except Exception:
        pass
    for name in ("OPEN_MODE", "TFB_OPEN_MODE", "AUTH_DISABLED"):
        env_v = _strip(os.getenv(name))
        if env_v:
            return _boolish(env_v, False)
    return False


def _auth_ok_via_hook(token_query: Optional[str], x_app_token: Optional[str], x_api_key: Optional[str] = None, authorization: Optional[str] = None) -> bool:
    if auth_ok is None:
        return False
    token = _strip(token_query)
    # X-API-Key accepted alongside X-APP-TOKEN
    x_token = _strip(x_app_token) or _strip(x_api_key)
    authz = _strip(authorization)
    candidate_kwargs: Tuple[Dict[str, Any], ...] = (
        {"token": token or None, "x_app_token": x_token or None, "authorization": authz or None},
        {"token_query": token or None, "x_app_token": x_token or None, "authorization": authz or None},
        {"token": token or None, "authorization": authz or None},
        {"authorization": authz or None},
        {"x_app_token": x_token or None},
        {"token": token or None},
        {},
    )
    for kwargs in candidate_kwargs:
        try:
            result = auth_ok(**kwargs)  # type: ignore[misc]
            if inspect.isawaitable(result):
                return False
            return bool(result)
        except TypeError:
            continue
        except Exception:
            continue
    return False


def _candidate_tokens_from_env() -> List[str]:
    out: List[str] = []
    seen = set()
    for name in AUTH_ENV_TOKEN_NAMES:
        token = _strip(os.getenv(name))
        if token and token not in seen:
            seen.add(token)
            out.append(token)
    return out


def _auth_ok_via_env_match(token_query: Optional[str], x_app_token: Optional[str], x_api_key: Optional[str] = None, authorization: Optional[str] = None) -> bool:
    env_tokens = _candidate_tokens_from_env()
    if not env_tokens:
        return False
    presented: List[str] = []
    # v6.8.1: include x_api_key in the presented-credentials iteration.
    for value in (token_query, x_app_token, x_api_key):
        token = _strip(value)
        if token:
            presented.append(token)
    authz = _strip(authorization)
    if authz:
        lowered = authz.lower()
        if lowered.startswith("bearer "):
            bearer = _strip(authz[7:])
            if bearer:
                presented.append(bearer)
        else:
            presented.append(authz)
    return any(_secure_equals(t, env_t) for t in presented for env_t in env_tokens)


def _require_auth_or_401(*, token_query: Optional[str], x_app_token: Optional[str], x_api_key: Optional[str] = None, authorization: Optional[str] = None) -> None:
    if _is_open_mode_enabled():
        return
    if _auth_ok_via_hook(token_query=token_query, x_app_token=x_app_token, x_api_key=x_api_key, authorization=authorization):
        return
    if _auth_ok_via_env_match(token_query=token_query, x_app_token=x_app_token, x_api_key=x_api_key, authorization=authorization):
        return
    raise HTTPException(status_code=401, detail="Unauthorized")


def _import_module_safely(name: str) -> Optional[Any]:
    try:
        return import_module(name)
    except Exception:
        return None


def _get_engine_from_state_obj(state_obj: Any) -> Any:
    if state_obj is None:
        return None
    for attr in ("engine", "data_engine", "engine_v2", "data_engine_v2", "advisor_engine", "investment_advisor_engine"):
        try:
            obj = getattr(state_obj, attr, None)
            if obj is not None:
                return obj
        except Exception:
            continue
    return None


async def _get_engine(request: Request) -> Any:
    try:
        engine = _get_engine_from_state_obj(request.app.state)
        if engine is not None:
            return engine
    except Exception:
        pass
    for module_name in ("core.data_engine_v2", "core.data_engine", "core.investment_advisor_engine"):
        module = _import_module_safely(module_name)
        if module is None:
            continue
        for attr in ("engine", "data_engine", "ENGINE", "DATA_ENGINE", "advisor_engine", "investment_advisor_engine", "get_engine", "get_data_engine", "build_engine", "create_engine"):
            candidate = getattr(module, attr, None)
            if candidate is None:
                continue
            try:
                if callable(candidate):
                    out = candidate()
                    if inspect.isawaitable(out):
                        out = await out
                    if out is not None:
                        return out
                elif candidate is not None:
                    return candidate
            except Exception:
                continue
    return None


def _canonicalize_page_name(value: Any) -> str:
    raw = _strip(value)
    if not raw:
        return ""
    page_catalog = None
    for _pcat_path in ("core.sheets.page_catalog", "core.page_catalog", "page_catalog"):
        try:
            page_catalog = import_module(_pcat_path)
            break
        except Exception:
            continue
    if page_catalog is not None:
        for fn_name in ("canonicalize_page_name", "canonical_page_name", "normalize_page_name", "resolve_page_name", "resolve_canonical_page_name"):
            fn = getattr(page_catalog, fn_name, None)
            if callable(fn):
                try:
                    out = fn(raw)
                    out_s = _strip(out)
                    if out_s:
                        return out_s
                except Exception:
                    continue
        for attr_name in ("PAGE_ALIASES", "ALIASES", "PAGE_ALIAS_MAP"):
            aliases = getattr(page_catalog, attr_name, None)
            if isinstance(aliases, Mapping):
                try:
                    hit = aliases.get(raw) or aliases.get(raw.lower()) or aliases.get(raw.upper())
                    hit_s = _strip(hit)
                    if hit_s:
                        return hit_s
                except Exception:
                    pass
        canonical_pages = getattr(page_catalog, "CANONICAL_PAGES", None)
        if isinstance(canonical_pages, (list, tuple, set)):
            lower_map = {_strip(x).lower(): _strip(x) for x in canonical_pages if _strip(x)}
            if raw.lower() in lower_map:
                return lower_map[raw.lower()]
    fallback_map = {
        "market leaders": "Market_Leaders", "market_leaders": "Market_Leaders",
        "global markets": "Global_Markets", "global_markets": "Global_Markets",
        "commodities fx": "Commodities_FX", "commodities_fx": "Commodities_FX", "commodities-fx": "Commodities_FX",
        "mutual funds": "Mutual_Funds", "mutual_funds": "Mutual_Funds",
        "my portfolio": "My_Portfolio", "my_portfolio": "My_Portfolio",
        "insights analysis": "Insights_Analysis", "insights_analysis": "Insights_Analysis",
        "top 10 investments": "Top_10_Investments", "top_10_investments": "Top_10_Investments", "top10_investments": "Top_10_Investments",
        "data dictionary": "Data_Dictionary", "data_dictionary": "Data_Dictionary",
    }
    return fallback_map.get(raw.lower(), raw)


def _extract_page(data: Mapping[str, Any], default_page: str = DEFAULT_ADVISOR_PAGE) -> str:
    raw = data.get("page") or data.get("sheet") or data.get("sheet_name") or data.get("name") or data.get("tab") or default_page
    page = _canonicalize_page_name(raw)
    return page or default_page


def _normalize_mode(value: Any) -> str:
    raw = _strip(value).lower()
    if raw in {"", "live", "default"}:
        return "live"
    if raw in {"live_quotes", "quotes", "quote", "market"}:
        return "live_quotes"
    if raw in {"snapshot", "cached", "cache"}:
        return "snapshot"
    return _strip(value) or "live"


def _default_payload(data: Mapping[str, Any], default_page: str = DEFAULT_ADVISOR_PAGE) -> Dict[str, Any]:
    payload = _safe_dict(data)
    page = _extract_page(payload, default_page=default_page)
    mode = _normalize_mode(payload.get("mode") or payload.get("advisor_mode") or payload.get("data_mode"))
    payload.update({
        "page": page, "sheet": page, "sheet_name": page, "name": page, "page_name": page, "tab": page, "target_sheet": page,
        "mode": mode, "advisor_mode": _strip(payload.get("advisor_mode")) or mode, "data_mode": _strip(payload.get("data_mode")) or mode,
        "advisor_data_mode": _strip(payload.get("advisor_data_mode")) or mode, "format": _strip(payload.get("format")) or "rows",
    })
    symbols = _get_list(payload, "symbols", "tickers", "symbol", "ticker")
    if symbols:
        payload["symbols"] = symbols
        payload["tickers"] = list(symbols)
    payload["top_n"] = max(1, min(100, _safe_int(payload.get("top_n"), 10)))
    payload["limit"] = max(1, min(5000, _safe_int(payload.get("limit"), 200)))
    payload["offset"] = max(0, _safe_int(payload.get("offset"), 0))
    payload["include_matrix"] = _boolish(payload.get("include_matrix"), True)
    payload["include_headers"] = _boolish(payload.get("include_headers"), True)
    payload["schema_only"] = _boolish(payload.get("schema_only"), False)
    payload["headers_only"] = _boolish(payload.get("headers_only"), False)
    return payload


def _prepare_payload_for_downstream(payload: Mapping[str, Any], *, request_id: str, operation: str) -> Dict[str, Any]:
    out = _default_payload(payload)
    page = _extract_page(out)
    out.update({
        "page": page, "sheet": page, "sheet_name": page, "name": page, "page_name": page, "tab": page,
        "target_sheet": page, "request_id": request_id, "operation": operation, "router": "advisor_short",
        "advisor_version": ADVISOR_VERSION, "format": _strip(out.get("format")) or "rows",
        "include_headers": _boolish(out.get("include_headers"), True),
        "include_matrix": _boolish(out.get("include_matrix"), True),
        "schema_only": _boolish(out.get("schema_only"), False),
        "headers_only": _boolish(out.get("headers_only"), False),
    })
    return out


def _payload_from_get(*, page: Optional[str], sheet: Optional[str], sheet_name: Optional[str], name: Optional[str], tab: Optional[str], symbol: Optional[str], ticker: Optional[str], symbols: Optional[str], tickers: Optional[str], mode: Optional[str], include_matrix: Optional[bool], schema_only: Optional[bool], headers_only: Optional[bool], top_n: Optional[int], limit: Optional[int], offset: Optional[int]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    for k, v in {
        "page": page, "sheet": sheet, "sheet_name": sheet_name, "name": name, "tab": tab,
        "mode": mode, "include_matrix": include_matrix, "schema_only": schema_only,
        "headers_only": headers_only, "top_n": top_n, "limit": limit, "offset": offset,
    }.items():
        if v not in (None, ""):
            payload[k] = v
    symbol_list: List[str] = []
    for raw in (symbols, tickers, symbol, ticker):
        if isinstance(raw, str) and raw.strip():
            for item in _split_csv(raw):
                if item not in symbol_list:
                    symbol_list.append(item)
    if symbol_list:
        payload["symbols"] = symbol_list
        payload["tickers"] = list(symbol_list)
    return payload


async def _invoke_callable(fn: Callable[..., Any], *args: Any, timeout_seconds: float, **kwargs: Any) -> Any:
    if inspect.iscoroutinefunction(fn):
        return await asyncio.wait_for(fn(*args, **kwargs), timeout=timeout_seconds)
    async def _runner() -> Any:
        return await asyncio.to_thread(fn, *args, **kwargs)
    return await asyncio.wait_for(_runner(), timeout=timeout_seconds)


def _looks_like_signature_type_error(exc: TypeError) -> bool:
    message = _strip(exc).lower()
    signature_markers = (
        "unexpected keyword", "unexpected positional", "positional argument", "required positional argument",
        "takes ", "got an unexpected", "multiple values for argument", "missing 1 required", "missing required", "keyword-only argument",
    )
    return any(marker in message for marker in signature_markers)


async def _call_with_tolerant_signatures(fn: Callable[..., Any], *, timeout_seconds: float, args: Sequence[Any] = (), kwargs: Optional[Dict[str, Any]] = None) -> Any:
    payload_kwargs = kwargs or {}
    attempts = [
        (tuple(args), payload_kwargs),
        ((), payload_kwargs),
        ((), {"request": payload_kwargs.get("request"), "body": payload_kwargs.get("body"), "payload": payload_kwargs.get("payload")}),
        ((), {"payload": payload_kwargs.get("payload"), "mode": payload_kwargs.get("mode")}),
        ((), {"body": payload_kwargs.get("body"), "mode": payload_kwargs.get("mode")}),
        ((), {"payload": payload_kwargs.get("payload")}),
        ((), {"body": payload_kwargs.get("body")}),
        ((), {"request": payload_kwargs.get("request")}),
        ((), {
            "page": payload_kwargs.get("page"), "sheet": payload_kwargs.get("sheet"), "sheet_name": payload_kwargs.get("sheet_name"),
            "symbols": payload_kwargs.get("symbols"), "tickers": payload_kwargs.get("tickers"), "top_n": payload_kwargs.get("top_n"),
            "limit": payload_kwargs.get("limit"), "offset": payload_kwargs.get("offset"), "mode": payload_kwargs.get("mode"),
        }),
        ((), {}),
    ]
    last_error: Optional[Exception] = None
    for call_args, call_kwargs in attempts:
        call_kwargs = {k: v for k, v in call_kwargs.items() if v is not None}
        try:
            return await _invoke_callable(fn, *call_args, timeout_seconds=timeout_seconds, **call_kwargs)
        except TypeError as exc:
            last_error = exc
            if _looks_like_signature_type_error(exc):
                continue
            raise
        except asyncio.TimeoutError as exc:
            last_error = exc
            raise
        except Exception as exc:
            last_error = exc
            raise
    if last_error is not None:
        raise last_error
    return None


# =============================================================================
# Schema contract resolution (v6.8.2: paired headers + keys)
# =============================================================================

def _extract_headers_and_keys_from_spec(spec: Any) -> Tuple[List[str], List[str]]:
    """v6.8.2: Extract paired (headers, keys) from a SheetSpec / dict.

    Replaces v6.8.1's `_extract_headers_from_spec` which collapsed headers
    and keys into a single list — causing downstream `row.get(header)`
    lookups to fail because rows are keyed by canonical KEYS not headers.
    """
    if spec is None:
        return [], []

    headers: List[str] = []
    keys: List[str] = []

    # Try dataclass / object with `.columns` attribute (engine SheetContract style)
    cols = getattr(spec, "columns", None)
    if cols is None and isinstance(spec, Mapping):
        cols = spec.get("columns") or spec.get("fields")

    if isinstance(cols, (list, tuple)) and cols:
        for col in cols:
            if isinstance(col, Mapping):
                h = _strip(
                    col.get("display_header") or col.get("header")
                    or col.get("label") or col.get("title") or col.get("name")
                )
                k = _strip(
                    col.get("key") or col.get("field") or col.get("name") or col.get("id")
                )
            else:
                h = _strip(getattr(col, "display_header",
                          getattr(col, "header",
                                  getattr(col, "label",
                                          getattr(col, "title",
                                                  getattr(col, "name", None))))))
                k = _strip(getattr(col, "key",
                          getattr(col, "field",
                                  getattr(col, "name",
                                          getattr(col, "id", None)))))
            if not h and not k:
                continue
            if h and not k:
                k = h.lower().replace(" ", "_").replace("/", "_").replace("(", "").replace(")", "").replace("%", "pct")
            elif k and not h:
                h = k.replace("_", " ").title()
            headers.append(h)
            keys.append(k)
        if headers and keys and len(headers) == len(keys):
            return headers, keys

    # Try explicit `headers` / `keys` lists
    if isinstance(spec, Mapping):
        raw_headers = spec.get("headers") or spec.get("display_headers") or spec.get("sheet_headers")
        raw_keys = spec.get("keys") or spec.get("fields") or spec.get("columns")
        if isinstance(raw_headers, list):
            headers = [_strip(x) for x in raw_headers if _strip(x)]
        if isinstance(raw_keys, list):
            keys = [_strip(x) for x in raw_keys if _strip(x)]
        if headers and not keys:
            keys = [h.lower().replace(" ", "_").replace("/", "_").replace("(", "").replace(")", "").replace("%", "pct") for h in headers]
        elif keys and not headers:
            headers = [k.replace("_", " ").title() for k in keys]
        if headers and keys and len(headers) == len(keys):
            return headers, keys

    # Try nested specs
    if isinstance(spec, Mapping):
        for key in ("sheet", "spec", "schema", "contract", "definition"):
            nested = spec.get(key)
            h, k = _extract_headers_and_keys_from_spec(nested)
            if h and k and len(h) == len(k):
                return h, k

    return [], []


def _resolve_contract_headers_keys(page: str) -> Tuple[List[str], List[str]]:
    """v6.8.2: Resolve paired (headers, keys) for a page from schema registry.

    Replaces v6.8.1's `_resolve_contract_headers` which returned only
    headers and caused silent data loss in `_mapping_list_to_rows`.
    """
    page = _canonicalize_page_name(page)
    if not page:
        return [], []

    if page in _SCHEMA_CONTRACT_CACHE:
        cached_h, cached_k = _SCHEMA_CONTRACT_CACHE[page]
        return list(cached_h), list(cached_k)

    for module_name in ("core.sheets.schema_registry", "core.schema_registry", "core.sheets.page_catalog"):
        module = _import_module_safely(module_name)
        if module is None:
            continue
        for map_name in ("SCHEMA_REGISTRY", "SHEET_SPEC", "SHEET_SPECS", "PAGE_SPECS", "SPECS", "SHEET_CONTRACTS", "STATIC_CANONICAL_SHEET_CONTRACTS"):
            spec_map = getattr(module, map_name, None)
            if isinstance(spec_map, Mapping):
                try:
                    hit = spec_map.get(page) or spec_map.get(page.lower()) or spec_map.get(page.upper())
                except Exception:
                    hit = None
                h, k = _extract_headers_and_keys_from_spec(hit)
                if h and k:
                    _SCHEMA_CONTRACT_CACHE[page] = (h, k)
                    return list(h), list(k)
        for fn_name in ("get_sheet_spec", "get_schema_for_sheet", "get_schema", "resolve_sheet_spec", "resolve_sheet_schema", "get_contract_for_sheet"):
            fn = getattr(module, fn_name, None)
            if not callable(fn):
                continue
            for kwargs in ({"sheet": page}, {"page": page}, {"sheet_name": page}, {"name": page}, {}):
                try:
                    out = fn(**kwargs) if kwargs else fn(page)
                except TypeError:
                    continue
                except Exception:
                    out = None
                if inspect.isawaitable(out):
                    continue
                h, k = _extract_headers_and_keys_from_spec(out)
                if h and k:
                    _SCHEMA_CONTRACT_CACHE[page] = (h, k)
                    return list(h), list(k)

    return [], []


def _resolve_contract_headers(page: str) -> List[str]:
    """v6.8.2 back-compat: returns only headers. Internally now uses the
    paired resolver. External code that reads only headers still works."""
    headers, _ = _resolve_contract_headers_keys(page)
    return headers


def _contract_header_count(page: str) -> int:
    headers, _ = _resolve_contract_headers_keys(page)
    if headers:
        return len(headers)
    return KNOWN_CANONICAL_HEADER_COUNTS.get(page, 0)


def _append_missing(items: List[str], extras: Sequence[str]) -> List[str]:
    out = list(items)
    for x in extras:
        if x not in out:
            out.append(x)
    return out


def _mapping_list_to_rows_with_keys(rows: Iterable[Mapping[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    """v6.8.2: project row dicts to a matrix using KEYS for lookup.

    Replaces v6.8.1's `_mapping_list_to_rows(rows, headers)` which used
    DISPLAY HEADERS for `row.get(...)` — but rows are keyed by canonical
    keys, so most cells came back None.
    """
    matrix: List[List[Any]] = []
    for row in rows:
        matrix.append([row.get(k) for k in keys])
    return matrix


# Back-compat shim — preserve the v6.8.1 public name for external callers.
def _mapping_list_to_rows(rows: Iterable[Mapping[str, Any]], headers: Sequence[str]) -> List[List[Any]]:
    """v6.8.2 back-compat. New code should call _mapping_list_to_rows_with_keys."""
    return _mapping_list_to_rows_with_keys(rows, headers)


def _rows_matrix_to_objects(matrix: Sequence[Sequence[Any]], keys: Sequence[str]) -> List[Dict[str, Any]]:
    """v6.8.2: build row dicts using KEYS as dict keys (was using HEADERS)."""
    out: List[Dict[str, Any]] = []
    for row in matrix:
        row_list = list(row)
        item: Dict[str, Any] = {}
        for idx, k in enumerate(keys):
            item[str(k)] = row_list[idx] if idx < len(row_list) else None
        out.append(item)
    return out


def _pad_or_trim_matrix(matrix: Sequence[Sequence[Any]], width: int) -> List[List[Any]]:
    out: List[List[Any]] = []
    for row in matrix:
        row_list = list(row)
        if len(row_list) < width:
            row_list = row_list + [None] * (width - len(row_list))
        elif len(row_list) > width:
            row_list = row_list[:width]
        out.append(row_list)
    return out


def _extract_meta_mapping(result: Mapping[str, Any]) -> Dict[str, Any]:
    meta = result.get("meta")
    return dict(meta) if isinstance(meta, Mapping) else {}


def _effective_headers_keys_from_result(result: Mapping[str, Any]) -> Tuple[List[str], List[str]]:
    """v6.8.2: extract paired headers + keys from an upstream payload."""
    headers: List[str] = []
    keys: List[str] = []
    for hk in ("display_headers", "headers", "sheet_headers", "column_headers"):
        v = result.get(hk)
        if isinstance(v, list) and v:
            headers = [_strip(x) for x in v if _strip(x)]
            if headers:
                break
    for kk in ("keys", "fields", "columns"):
        v = result.get(kk)
        if isinstance(v, list) and v and all(isinstance(x, str) for x in v):
            keys = [_strip(x) for x in v if _strip(x)]
            if keys:
                break
    if headers and not keys:
        keys = [h.lower().replace(" ", "_").replace("/", "_").replace("(", "").replace(")", "").replace("%", "pct") for h in headers]
    elif keys and not headers:
        headers = [k.replace("_", " ").title() for k in keys]
    return headers, keys


def _extract_object_rows_any(result: Mapping[str, Any]) -> List[Dict[str, Any]]:
    for key in ("row_objects", "rowObjects", "rows", "items", "data", "quotes", "results", "records"):
        candidate = result.get(key)
        if isinstance(candidate, list) and candidate and all(isinstance(x, Mapping) for x in candidate):
            return [dict(x) for x in candidate]
    return []


def _extract_matrix_rows_any(result: Mapping[str, Any]) -> List[List[Any]]:
    for key in ("rows_matrix", "matrix", "rows"):
        candidate = result.get(key)
        if isinstance(candidate, list) and candidate and all(not isinstance(x, Mapping) for x in candidate):
            return [list(x) if isinstance(x, (list, tuple)) else [x] for x in candidate]
    return []


def _project_to_contract_headers_keys(*, page: str, headers: List[str], keys: List[str], row_objects: List[Dict[str, Any]], rows_matrix: List[List[Any]]) -> Tuple[List[str], List[str], List[Dict[str, Any]], List[List[Any]]]:
    """v6.8.2: project rows to paired (headers, keys) from schema registry.

    Major rewrite of v6.8.1's `_project_to_contract_headers`:
    - Uses canonical KEYS (not display headers) to extract values from row dicts
    - Top10 required extras (3) always ensured; trade-setup optionals (4)
      only added if any row in the source has them populated
    """
    contract_headers, contract_keys = _resolve_contract_headers_keys(page)

    # Prefer the schema registry contract; fall back to upstream payload
    effective_headers = list(contract_headers or headers or [])
    effective_keys = list(contract_keys or keys or [])

    # If headers but no keys (or vice versa), derive the other from the one we have
    if effective_headers and not effective_keys:
        effective_keys = [h.lower().replace(" ", "_").replace("/", "_").replace("(", "").replace(")", "").replace("%", "pct") for h in effective_headers]
    elif effective_keys and not effective_headers:
        effective_headers = [k.replace("_", " ").title() for k in effective_keys]

    # Top10: ensure required extras always present
    if page == "Top_10_Investments":
        for extra_key in TOP10_REQUIRED_EXTRAS:
            if extra_key not in effective_keys:
                effective_keys.append(extra_key)
                effective_headers.append(TOP10_REQUIRED_EXTRA_HEADERS[extra_key])

        # Optional trade-setup fields: only append if any source row has them populated
        if row_objects:
            for opt_key in TOP10_OPTIONAL_TRADE_FIELDS:
                if opt_key in effective_keys:
                    continue
                has_value = any(
                    row.get(opt_key) not in (None, "", [], {}, ())
                    for row in row_objects
                )
                if has_value:
                    effective_keys.append(opt_key)
                    effective_headers.append(TOP10_OPTIONAL_TRADE_HEADERS[opt_key])

    # If we still have nothing, derive from observed row keys
    if not effective_keys and row_objects:
        seen: List[str] = []
        for row in row_objects:
            for key in row.keys():
                key_s = _strip(key)
                if key_s and key_s not in seen and not any(key_s.startswith(p) for p in _INTERNAL_FIELD_PREFIXES):
                    seen.append(key_s)
        effective_keys = seen
        effective_headers = [k.replace("_", " ").title() for k in effective_keys]

    # Top10: backfill rank/selection_reason/criteria_snapshot using row position
    if page == "Top_10_Investments":
        for idx, row in enumerate(row_objects, start=1):
            row.setdefault("top10_rank", idx)
            row.setdefault("selection_reason", row.get("selection_reason") or None)
            row.setdefault("criteria_snapshot", row.get("criteria_snapshot") or None)

    # v6.8.2: strip internal fields from every row before projection
    for row in row_objects:
        _strip_internal_fields(row)

    # Project: build matrix using KEYS for value lookup
    if row_objects and effective_keys:
        rows_matrix = _mapping_list_to_rows_with_keys(row_objects, effective_keys)
    elif rows_matrix and effective_keys:
        rows_matrix = _pad_or_trim_matrix(rows_matrix, len(effective_keys))
        row_objects = _rows_matrix_to_objects(rows_matrix, effective_keys)

    return effective_headers, effective_keys, row_objects, rows_matrix


def _ensure_tabular_shape(result: Dict[str, Any], *, page: str) -> Dict[str, Any]:
    """v6.8.2: now uses paired (headers, keys) projection."""
    upstream_headers, upstream_keys = _effective_headers_keys_from_result(result)
    object_rows: List[Dict[str, Any]] = _extract_object_rows_any(result)
    matrix_rows: List[List[Any]] = _extract_matrix_rows_any(result)

    effective_headers, effective_keys, object_rows, matrix_rows = _project_to_contract_headers_keys(
        page=page,
        headers=upstream_headers,
        keys=upstream_keys,
        row_objects=object_rows,
        rows_matrix=matrix_rows,
    )

    result["headers"] = effective_headers
    result["display_headers"] = list(effective_headers)
    result["keys"] = list(effective_keys)
    if object_rows:
        result["row_objects"] = object_rows
        result["items"] = object_rows
        if page != "Data_Dictionary":
            result["data"] = object_rows
        else:
            result.setdefault("data", object_rows)
    if matrix_rows:
        result["rows_matrix"] = matrix_rows
        result["rows"] = matrix_rows if not object_rows else object_rows
    elif object_rows:
        result["rows_matrix"] = _mapping_list_to_rows_with_keys(object_rows, effective_keys)
        result["rows"] = object_rows
    meta = _extract_meta_mapping(result)
    if effective_headers:
        meta.setdefault("contract_header_count", _contract_header_count(page) or len(effective_headers))
        meta.setdefault("actual_header_count", len(effective_headers))
    result["meta"] = meta
    return result


def _row_data_provider_is_placeholder(row: Mapping[str, Any]) -> bool:
    """v6.8.2: detect rows from known placeholder/fallback sources."""
    provider = _strip(row.get("data_provider")).lower()
    if not provider:
        return False
    return any(marker in provider for marker in (m.lower() for m in PLACEHOLDER_DATA_PROVIDERS))


def _row_has_financial_signal(row: Mapping[str, Any]) -> bool:
    """v6.8.2: True if at least one financial signal field is populated."""
    for f in TOP10_FINANCIAL_SIGNAL_FIELDS:
        v = row.get(f)
        if v not in (None, "", [], {}, ()):
            return True
    return False


def _looks_like_fallback_only_top10(result: Mapping[str, Any]) -> bool:
    """v6.8.2: enhanced detection of fallback-only Top10 payloads.

    Adds checks for:
    - data_provider matching known placeholder markers
    - absence of financial signals (current_price, scores, recommendation)
    """
    page = _strip(result.get("page"))
    if page != "Top_10_Investments":
        return False
    meta = _extract_meta_mapping(result)
    fallback_flag = _boolish(meta.get("fallback"), False)
    reason = _strip(meta.get("reason")).lower()
    rows: List[Mapping[str, Any]] = _extract_object_rows_any(result)
    if not rows:
        _, keys = _effective_headers_keys_from_result(result)
        matrix_rows = _extract_matrix_rows_any(result)
        if keys and matrix_rows:
            rows = _rows_matrix_to_objects(matrix_rows, keys)
    if not rows:
        return False

    placeholder_provider_hits = 0
    fallback_selection_hits = 0
    no_financial_signal_rows = 0

    for row in rows:
        if _row_data_provider_is_placeholder(row):
            placeholder_provider_hits += 1
        selection_reason = _strip(row.get("selection_reason")).lower()
        if "fallback candidate" in selection_reason or "no live data" in selection_reason or "no live advisor data" in selection_reason:
            fallback_selection_hits += 1
        if not _row_has_financial_signal(row):
            no_financial_signal_rows += 1

    threshold = max(1, math.ceil(len(rows) * 0.6))

    # v6.8.2: any of these conditions individually signals "fallback only"
    if placeholder_provider_hits >= threshold:
        return True
    if no_financial_signal_rows >= threshold:
        return True
    if fallback_flag and fallback_selection_hits >= threshold:
        return True
    if "engine_unavailable_or_empty" in reason and no_financial_signal_rows >= threshold:
        return True
    return False


def _has_usable_payload(result: Mapping[str, Any], *, page: str) -> bool:
    """v6.8.2: also accepts status='warn' payloads when rows are present."""
    if _boolish(result.get("schema_only"), False) or _boolish(result.get("headers_only"), False):
        return _row_count(result.get("headers")) > 0
    if page == "Data_Dictionary":
        if _row_count(result.get("rows_matrix")) > 0 or _row_count(result.get("row_objects")) > 0:
            return True
        return _row_count(result.get("headers")) > 0

    # v6.8.2: explicit error status → not usable
    status = _strip(result.get("status")).lower()
    if status in {"error", "failed", "fail"} and _row_count(result.get("row_objects")) == 0:
        return False

    has_rows = any(_row_count(result.get(k)) > 0 for k in ("rows_matrix", "row_objects", "rows", "items", "data", "quotes"))
    if not has_rows:
        return False
    if page == "Top_10_Investments" and _looks_like_fallback_only_top10(result):
        return False
    return True


def _normalize_result_payload(out: Any, *, page: str) -> Dict[str, Any]:
    safe = _json_safe(out)
    if isinstance(safe, Mapping):
        result = dict(safe)
    elif isinstance(safe, list):
        result = {"status": "success", "page": page, "data": safe, "items": safe}
        if safe and all(isinstance(x, Mapping) for x in safe):
            result["row_objects"] = safe
    else:
        result = {"status": "success", "page": page, "data": safe}
    result.setdefault("status", "success")
    result.setdefault("page", page)
    result.setdefault("sheet", result.get("page") or page)
    result.setdefault("sheet_name", result.get("page") or page)
    result.setdefault("meta", {})
    result = _ensure_tabular_shape(result, page=page)
    return result


def _merge_meta_resolver(result: Dict[str, Any], resolver_meta: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    meta = _extract_meta_mapping(result)
    if resolver_meta:
        meta["resolver"] = dict(resolver_meta)
    result["meta"] = meta
    return result


def _envelope_from_payload_result(*, result: Dict[str, Any], page: str, request_id: str, started_at: float, resolver_meta: Optional[Dict[str, Any]] = None, operation: str = "sheet_rows") -> Dict[str, Any]:
    out = _normalize_result_payload(result, page=page)
    out = _merge_meta_resolver(out, resolver_meta)
    meta = _extract_meta_mapping(out)
    meta.update({
        "request_id": request_id,
        "elapsed_ms": round((time.perf_counter() - started_at) * 1000.0, 2),
        "router": "advisor_short",
        "advisor_version": ADVISOR_VERSION,
        "page": page,
        "operation": operation,
        "contract_header_count": _contract_header_count(page),
        "actual_header_count": len(out.get("headers", []) or []),
        "degraded_top10_rejected": False,
    })
    out["status"] = _strip(out.get("status")) or "success"
    out["page"] = _strip(out.get("page")) or page
    out["sheet"] = _strip(out.get("sheet")) or out["page"]
    out["sheet_name"] = _strip(out.get("sheet_name")) or out["page"]
    out["meta"] = meta
    return _json_safe(out)


async def _resolve_analysis_bridge_impl() -> Tuple[Optional[Callable[..., Any]], Dict[str, Any]]:
    module = _import_module_safely("routes.analysis_sheet_rows")
    if module is None:
        return None, {}
    for name in ("_analysis_sheet_rows_impl", "analysis_sheet_rows_impl", "_analysis_sheet_rows_impl_core"):
        impl = getattr(module, name, None)
        if callable(impl):
            return impl, {"module": "routes.analysis_sheet_rows", "callable": name, "kind": "bridge"}
    return None, {}


async def _resolve_advanced_bridge_impl() -> Tuple[Optional[Callable[..., Any]], Dict[str, Any]]:
    module = _import_module_safely("routes.advanced_analysis")
    if module is None:
        return None, {}
    for name in ("_run_advanced_sheet_rows_impl", "run_advanced_sheet_rows_impl", "_advanced_sheet_rows_impl"):
        impl = getattr(module, name, None)
        if callable(impl):
            return impl, {"module": "routes.advanced_analysis", "callable": name, "kind": "bridge"}
    return None, {}


async def _resolve_advisor_runner(request: Request, *, page: str) -> Tuple[Optional[Callable[..., Any]], Dict[str, Any]]:
    try:
        state = request.app.state
    except Exception:
        state = None
    if state is not None:
        for attr_name in ("advisor_service", "investment_advisor_service", "advisor_runner", "advisor_engine", "investment_advisor_engine", "engine", "service"):
            obj = getattr(state, attr_name, None)
            for method_name in ("run_investment_advisor_engine", "run_investment_advisor", "run_advisor", "advisor_run", "get_recommendations", "recommendations", "run", "execute"):
                fn = getattr(obj, method_name, None)
                if callable(fn):
                    return fn, {"module": "app.state", "object": attr_name, "callable": method_name, "kind": "state_method"}
    for module_name in ("routes.investment_advisor", "core.investment_advisor", "core.investment_advisor_engine"):
        module = _import_module_safely(module_name)
        if module is None:
            continue
        for fn_name in ("_run_investment_advisor_impl", "_run_advisor_impl", "run_investment_advisor_engine", "run_investment_advisor", "run_advisor", "advisor_run", "advisor_recommendations", "get_recommendations", "recommendations", "build_recommendations", "execute", "run"):
            fn = getattr(module, fn_name, None)
            if callable(fn):
                return fn, {"module": module_name, "callable": fn_name, "kind": "function"}
    return None, {}


async def _resolve_top10_builder(request: Request) -> Tuple[Optional[Callable[..., Any]], Dict[str, Any]]:
    try:
        state = request.app.state
    except Exception:
        state = None
    if state is not None:
        for attr_name in ("build_top10_output_rows", "build_top10_rows", "build_top10_investments_rows", "build_top10_investments", "build_top10", "get_top10_rows"):
            candidate = getattr(state, attr_name, None)
            if callable(candidate):
                return candidate, {"module": "app.state", "callable": attr_name, "kind": "state_function"}
    for module_name in ("core.analysis.top10_selector", "routes.investment_advisor", "core.investment_advisor", "core.investment_advisor_engine"):
        module = _import_module_safely(module_name)
        if module is None:
            continue
        for fn_name in ("build_top10_output_rows", "build_top10_rows", "build_top10_investments_rows", "build_top10_investments", "build_top10", "get_top10_rows"):
            fn = getattr(module, fn_name, None)
            if callable(fn):
                return fn, {"module": module_name, "callable": fn_name, "kind": "function"}
    return None, {}


async def _delegate_to_analysis_bridge(*, request: Request, payload: Dict[str, Any], token: Optional[str], x_app_token: Optional[str], x_api_key: Optional[str] = None, authorization: Optional[str], x_request_id: Optional[str]) -> Optional[Dict[str, Any]]:
    """v6.8.1: accepts and forwards x_api_key into the bridge call."""
    impl, meta = await _resolve_analysis_bridge_impl()
    if impl is None:
        return None
    page = _extract_page(payload)
    try:
        out = await _call_with_tolerant_signatures(
            impl,
            timeout_seconds=_resolver_timeout("bridge", page=page),
            kwargs={
                "request": request,
                "body": payload,
                "mode": _strip(payload.get("mode")) or "live",
                "include_matrix_q": _boolish(payload.get("include_matrix"), True),
                "token": token,
                "x_app_token": x_app_token,
                "x_api_key": x_api_key,
                "authorization": authorization,
                "x_request_id": x_request_id,
            },
        )
        result = _normalize_result_payload(out, page=page)
        if not _has_usable_payload(result, page=page):
            return None
        return _merge_meta_resolver(result, meta)
    except Exception as exc:
        logger.warning("Analysis bridge failed. page=%s error=%s", page, exc, exc_info=True)
        return None


async def _delegate_to_advanced_bridge(*, request: Request, payload: Dict[str, Any], token: Optional[str], x_app_token: Optional[str], x_api_key: Optional[str] = None, authorization: Optional[str], x_request_id: Optional[str]) -> Optional[Dict[str, Any]]:
    """v6.8.1: accepts and forwards x_api_key into the bridge call."""
    impl, meta = await _resolve_advanced_bridge_impl()
    if impl is None:
        return None
    page = _extract_page(payload)
    try:
        out = await _call_with_tolerant_signatures(
            impl,
            timeout_seconds=_resolver_timeout("bridge", page=page),
            kwargs={
                "request": request,
                "body": payload,
                "mode": _strip(payload.get("mode")) or "live",
                "include_matrix_q": _boolish(payload.get("include_matrix"), True),
                "token": token,
                "x_app_token": x_app_token,
                "x_api_key": x_api_key,
                "authorization": authorization,
                "x_request_id": x_request_id,
                "page": page,
                "sheet": page,
                "sheet_name": page,
            },
        )
        result = _normalize_result_payload(out, page=page)
        if not _has_usable_payload(result, page=page):
            return None
        return _merge_meta_resolver(result, meta)
    except Exception as exc:
        logger.warning("Advanced bridge failed. page=%s error=%s", page, exc, exc_info=True)
        return None


async def _run_engine_sheet_rows_fallback(*, request: Request, payload: Dict[str, Any], request_id: str) -> Optional[Dict[str, Any]]:
    engine = await _get_engine(request)
    if engine is None:
        return None
    page = _extract_page(payload)
    include_matrix = _boolish(payload.get("include_matrix"), True)
    mode = _strip(payload.get("mode")) or "live"
    last_error: Optional[Exception] = None
    for method_name in ("get_sheet_rows", "sheet_rows", "fetch_sheet_rows", "build_sheet_rows", "read_sheet_rows", "run_sheet_rows"):
        method = getattr(engine, method_name, None)
        if not callable(method):
            continue
        attempts = [
            {"request": request, "body": payload, "payload": payload, "mode": mode, "include_matrix": include_matrix,
             "page": page, "sheet": page, "sheet_name": page, "target_sheet": page,
             "limit": payload.get("limit"), "offset": payload.get("offset"), "symbols": payload.get("symbols"),
             "tickers": payload.get("tickers"), "top_n": payload.get("top_n")},
            {"body": payload, "payload": payload, "mode": mode, "include_matrix": include_matrix,
             "page": page, "sheet": page, "sheet_name": page, "target_sheet": page,
             "limit": payload.get("limit"), "offset": payload.get("offset"), "symbols": payload.get("symbols"),
             "tickers": payload.get("tickers"), "top_n": payload.get("top_n")},
            {"body": payload, "page": page, "sheet": page, "sheet_name": page, "target_sheet": page},
            {"page": page, "sheet": page, "sheet_name": page, "target_sheet": page},
            {"sheet": page}, {},
        ]
        for kwargs in attempts:
            kwargs = {k: v for k, v in kwargs.items() if v is not None}
            try:
                out = await _call_with_tolerant_signatures(method, timeout_seconds=_resolver_timeout("engine", page=page), kwargs=kwargs)
                result = _normalize_result_payload(out, page=page)
                if not _has_usable_payload(result, page=page):
                    continue
                result = _merge_meta_resolver(result, {"source": _safe_engine_type(engine), "callable": method_name, "kind": "engine_fallback"})
                meta = _extract_meta_mapping(result)
                meta.setdefault("request_id", request_id)
                result["meta"] = meta
                return result
            except TypeError as exc:
                last_error = exc
                continue
            except Exception as exc:
                last_error = exc
                continue
    if last_error is not None:
        logger.warning("Engine sheet-rows fallback failed. error=%s", last_error, exc_info=True)
    return None


async def _run_top10_builder(*, request: Request, payload: Dict[str, Any], page: str) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    builder, resolver_meta = await _resolve_top10_builder(request)
    if builder is None:
        return None, {}
    try:
        out = await _call_with_tolerant_signatures(
            builder,
            timeout_seconds=_resolver_timeout("top10", page=page),
            kwargs={
                "request": request, "body": payload, "payload": payload, "page": page, "sheet": page, "sheet_name": page,
                "mode": _strip(payload.get("mode")) or "live", "symbols": payload.get("symbols"), "tickers": payload.get("tickers"),
                "top_n": payload.get("top_n"), "limit": payload.get("limit"), "offset": payload.get("offset"),
            },
        )
        result = _normalize_result_payload(out, page=page)
        if _has_usable_payload(result, page=page):
            return result, resolver_meta
        return None, resolver_meta
    except Exception as exc:
        logger.warning("Top10 builder failed; page=%s error=%s", page, exc, exc_info=True)
        return None, resolver_meta or {}


async def _run_advisor_runner(*, request: Request, payload: Dict[str, Any], page: str) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    runner, resolver_meta = await _resolve_advisor_runner(request, page=page)
    if runner is None:
        return None, {}
    try:
        out = await _call_with_tolerant_signatures(
            runner,
            timeout_seconds=_resolver_timeout("runner", page=page),
            kwargs={
                "request": request, "body": payload, "payload": payload, "page": page, "sheet": page, "sheet_name": page,
                "mode": _strip(payload.get("mode")) or "live", "symbols": payload.get("symbols"), "tickers": payload.get("tickers"),
                "top_n": payload.get("top_n"), "limit": payload.get("limit"), "offset": payload.get("offset"),
            },
        )
        result = _normalize_result_payload(out, page=page)
        if _has_usable_payload(result, page=page):
            return result, resolver_meta
        return None, resolver_meta
    except Exception as exc:
        logger.warning("Advisor runner failed; page=%s error=%s", page, exc, exc_info=True)
        return None, resolver_meta or {}


def _prefer_direct_bridge(page: str, *, operation: str) -> bool:
    if page in DERIVED_PAGES:
        return True
    if operation == "sheet_rows" and page in BASE_PAGES:
        return True
    return False


def _prefer_top10_builder(page: str, *, operation: str) -> bool:
    return page == "Top_10_Investments" and operation in {"sheet_rows", "recommendations", "run"}


def _prefer_runner(page: str, *, operation: str) -> bool:
    if page in DERIVED_PAGES:
        return False
    return operation in {"recommendations", "run"}


async def _run_advisor_logic(*, request: Request, payload: Dict[str, Any], token: Optional[str], x_app_token: Optional[str], x_api_key: Optional[str] = None, authorization: Optional[str], x_request_id: Optional[str], operation: str = "sheet_rows") -> Dict[str, Any]:
    """v6.8.1: accepts x_api_key. v6.8.0 was missing this parameter even
    though every route handler passes it — every request raised TypeError."""
    started_at = time.perf_counter()
    _require_auth_or_401(token_query=token, x_app_token=x_app_token, x_api_key=x_api_key, authorization=authorization)
    request_id = _request_id(request, x_request_id)
    payload = _prepare_payload_for_downstream(payload, request_id=request_id, operation=operation)
    page = _extract_page(payload, default_page=DEFAULT_ADVISOR_PAGE)
    payload.update({"page": page, "sheet": page, "sheet_name": page})

    if _prefer_top10_builder(page, operation=operation):
        top10_result, top10_meta = await _run_top10_builder(request=request, payload=payload, page=page)
        if top10_result is not None:
            return _envelope_from_payload_result(result=top10_result, page=page, request_id=request_id, started_at=started_at, resolver_meta=top10_meta or {"source": "top10_builder", "kind": "direct"}, operation=operation)

    if _prefer_direct_bridge(page, operation=operation):
        analysis_result = await _delegate_to_analysis_bridge(request=request, payload=payload, token=token, x_app_token=x_app_token, x_api_key=x_api_key, authorization=authorization, x_request_id=request_id)
        if analysis_result is not None:
            return _envelope_from_payload_result(result=analysis_result, page=page, request_id=request_id, started_at=started_at, resolver_meta=_extract_meta_mapping(analysis_result).get("resolver"), operation=operation)
        advanced_result = await _delegate_to_advanced_bridge(request=request, payload=payload, token=token, x_app_token=x_app_token, x_api_key=x_api_key, authorization=authorization, x_request_id=request_id)
        if advanced_result is not None:
            return _envelope_from_payload_result(result=advanced_result, page=page, request_id=request_id, started_at=started_at, resolver_meta=_extract_meta_mapping(advanced_result).get("resolver"), operation=operation)
        engine_result = await _run_engine_sheet_rows_fallback(request=request, payload=payload, request_id=request_id)
        if engine_result is not None:
            return _envelope_from_payload_result(result=engine_result, page=page, request_id=request_id, started_at=started_at, resolver_meta=_extract_meta_mapping(engine_result).get("resolver"), operation=operation)

    if _prefer_runner(page, operation=operation):
        runner_result, runner_meta = await _run_advisor_runner(request=request, payload=payload, page=page)
        if runner_result is not None:
            return _envelope_from_payload_result(result=runner_result, page=page, request_id=request_id, started_at=started_at, resolver_meta=runner_meta, operation=operation)

    analysis_result = await _delegate_to_analysis_bridge(request=request, payload=payload, token=token, x_app_token=x_app_token, x_api_key=x_api_key, authorization=authorization, x_request_id=request_id)
    if analysis_result is not None:
        return _envelope_from_payload_result(result=analysis_result, page=page, request_id=request_id, started_at=started_at, resolver_meta=_extract_meta_mapping(analysis_result).get("resolver"), operation=operation)
    advanced_result = await _delegate_to_advanced_bridge(request=request, payload=payload, token=token, x_app_token=x_app_token, x_api_key=x_api_key, authorization=authorization, x_request_id=request_id)
    if advanced_result is not None:
        return _envelope_from_payload_result(result=advanced_result, page=page, request_id=request_id, started_at=started_at, resolver_meta=_extract_meta_mapping(advanced_result).get("resolver"), operation=operation)
    engine_result = await _run_engine_sheet_rows_fallback(request=request, payload=payload, request_id=request_id)
    if engine_result is not None:
        return _envelope_from_payload_result(result=engine_result, page=page, request_id=request_id, started_at=started_at, resolver_meta=_extract_meta_mapping(engine_result).get("resolver"), operation=operation)

    raise HTTPException(status_code=503, detail="Advisor router could not resolve a usable bridge, builder, or engine provider")


@router.get("/health")
@router.get("/healthz")
@router.get("/ping")
async def advisor_health(request: Request) -> Dict[str, Any]:
    engine = await _get_engine(request)
    analysis_impl, analysis_meta = await _resolve_analysis_bridge_impl()
    advanced_impl, advanced_meta = await _resolve_advanced_bridge_impl()
    runner, runner_meta = await _resolve_advisor_runner(request, page=DEFAULT_ADVISOR_PAGE)
    top10_builder, top10_meta = await _resolve_top10_builder(request)
    settings_summary: Dict[str, Any] = {}
    try:
        settings = get_settings_cached()
        if isinstance(settings, Mapping):
            for key in ("APP_VERSION", "ENV", "OPEN_MODE", "REQUIRE_AUTH"):
                value = settings.get(key)
                if value not in (None, ""):
                    settings_summary[key] = value
    except Exception:
        settings_summary = {}
    contract_counts = {k: _contract_header_count(k) for k in KNOWN_CANONICAL_HEADER_COUNTS.keys()}
    return _json_safe({
        "status": "ok" if (engine is not None or analysis_impl is not None or advanced_impl is not None) else "degraded",
        "version": ADVISOR_VERSION,
        "short_family": True,
        "engine_available": bool(engine),
        "engine_type": _safe_engine_type(engine),
        "analysis_bridge_available": bool(analysis_impl),
        "analysis_bridge": analysis_meta or None,
        "advanced_bridge_available": bool(advanced_impl),
        "advanced_bridge": advanced_meta or None,
        "advisor_runner_available": bool(runner),
        "advisor_runner": runner_meta or None,
        "top10_builder_available": bool(top10_builder),
        "top10_builder": top10_meta or None,
        "default_page": DEFAULT_ADVISOR_PAGE,
        "base_pages": sorted(BASE_PAGES),
        "derived_pages": sorted(DERIVED_PAGES),
        "contract_header_counts": contract_counts,
        "open_mode": _is_open_mode_enabled(),
        "settings": settings_summary or None,
        "timestamp_utc": _now_utc(),
        "request_id": _strip(getattr(request.state, "request_id", "")) or None,
    })


@router.get("/meta")
async def advisor_meta(request: Request) -> Dict[str, Any]:
    engine = await _get_engine(request)
    analysis_impl, analysis_meta = await _resolve_analysis_bridge_impl()
    advanced_impl, advanced_meta = await _resolve_advanced_bridge_impl()
    runner, runner_meta = await _resolve_advisor_runner(request, page=DEFAULT_ADVISOR_PAGE)
    top10_builder, top10_meta = await _resolve_top10_builder(request)
    return _json_safe({
        "status": "success",
        "version": ADVISOR_VERSION,
        "route_family": "advisor_short",
        "default_page": DEFAULT_ADVISOR_PAGE,
        "engine_present": bool(engine),
        "engine_type": _safe_engine_type(engine),
        "analysis_bridge_available": bool(analysis_impl),
        "analysis_bridge": analysis_meta or None,
        "advanced_bridge_available": bool(advanced_impl),
        "advanced_bridge": advanced_meta or None,
        "advisor_runner_available": bool(runner),
        "advisor_runner": runner_meta or None,
        "top10_builder_available": bool(top10_builder),
        "top10_builder": top10_meta or None,
        "contract_header_counts": {k: _contract_header_count(k) for k in KNOWN_CANONICAL_HEADER_COUNTS.keys()},
        "timestamp_utc": _now_utc(),
        "request_id": _strip(getattr(request.state, "request_id", "")) or None,
    })


@router.get("/metrics")
async def advisor_metrics() -> Response:
    if not PROMETHEUS_AVAILABLE or generate_latest is None:
        return Response(content="Metrics not available", media_type="text/plain", status_code=503)
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.get("/sheet-rows")
@router.get("/sheet_rows")
async def advisor_sheet_rows_get(request: Request, page: Optional[str] = Query(default=None), sheet: Optional[str] = Query(default=None), sheet_name: Optional[str] = Query(default=None), name: Optional[str] = Query(default=None), tab: Optional[str] = Query(default=None), symbol: Optional[str] = Query(default=None), ticker: Optional[str] = Query(default=None), symbols: Optional[str] = Query(default=None), tickers: Optional[str] = Query(default=None), mode: str = Query(default="live"), include_matrix: Optional[bool] = Query(default=None), schema_only: Optional[bool] = Query(default=None), headers_only: Optional[bool] = Query(default=None), top_n: Optional[int] = Query(default=None, ge=1, le=500), limit: int = Query(default=200, ge=1, le=5000), offset: int = Query(default=0, ge=0), token: Optional[str] = Query(default=None), x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"), x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"), authorization: Optional[str] = Header(default=None, alias="Authorization"), x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID")) -> Dict[str, Any]:
    payload = _payload_from_get(page=page, sheet=sheet, sheet_name=sheet_name, name=name, tab=tab, symbol=symbol, ticker=ticker, symbols=symbols, tickers=tickers, mode=mode, include_matrix=include_matrix, schema_only=schema_only, headers_only=headers_only, top_n=top_n, limit=limit, offset=offset)
    return await _run_advisor_logic(request=request, payload=payload, token=token, x_app_token=x_app_token, x_api_key=x_api_key, authorization=authorization, x_request_id=x_request_id, operation="sheet_rows")


@router.post("/sheet-rows")
@router.post("/sheet_rows")
async def advisor_sheet_rows_post(request: Request, body: Dict[str, Any] = Body(default_factory=dict), token: Optional[str] = Query(default=None), x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"), x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"), authorization: Optional[str] = Header(default=None, alias="Authorization"), x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID")) -> Dict[str, Any]:
    return await _run_advisor_logic(request=request, payload=_safe_dict(body), token=token, x_app_token=x_app_token, x_api_key=x_api_key, authorization=authorization, x_request_id=x_request_id, operation="sheet_rows")


@router.get("/recommendations")
async def advisor_recommendations_get(request: Request, symbol: Optional[str] = Query(default=None), ticker: Optional[str] = Query(default=None), symbols: Optional[str] = Query(default=None), tickers: Optional[str] = Query(default=None), page: Optional[str] = Query(default=DEFAULT_ADVISOR_PAGE), mode: str = Query(default="live"), top_n: int = Query(default=10, ge=1, le=100), limit: int = Query(default=200, ge=1, le=5000), offset: int = Query(default=0, ge=0), include_matrix: Optional[bool] = Query(default=None), token: Optional[str] = Query(default=None), x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"), x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"), authorization: Optional[str] = Header(default=None, alias="Authorization"), x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID")) -> Dict[str, Any]:
    payload = _payload_from_get(page=page, sheet=None, sheet_name=None, name=None, tab=None, symbol=symbol, ticker=ticker, symbols=symbols, tickers=tickers, mode=mode, include_matrix=include_matrix, schema_only=None, headers_only=None, top_n=top_n, limit=limit, offset=offset)
    return await _run_advisor_logic(request=request, payload=payload, token=token, x_app_token=x_app_token, x_api_key=x_api_key, authorization=authorization, x_request_id=x_request_id, operation="recommendations")


@router.post("/recommendations")
async def advisor_recommendations_post(request: Request, body: Dict[str, Any] = Body(default_factory=dict), token: Optional[str] = Query(default=None), x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"), x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"), authorization: Optional[str] = Header(default=None, alias="Authorization"), x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID")) -> Dict[str, Any]:
    payload = _safe_dict(body)
    payload.setdefault("page", payload.get("sheet") or payload.get("sheet_name") or DEFAULT_ADVISOR_PAGE)
    return await _run_advisor_logic(request=request, payload=payload, token=token, x_app_token=x_app_token, x_api_key=x_api_key, authorization=authorization, x_request_id=x_request_id, operation="recommendations")


@router.get("/run")
async def advisor_run_get(request: Request, page: Optional[str] = Query(default=DEFAULT_ADVISOR_PAGE), symbols: Optional[str] = Query(default=None), tickers: Optional[str] = Query(default=None), mode: str = Query(default="live"), top_n: int = Query(default=10, ge=1, le=100), limit: int = Query(default=200, ge=1, le=5000), offset: int = Query(default=0, ge=0), token: Optional[str] = Query(default=None), x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"), x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"), authorization: Optional[str] = Header(default=None, alias="Authorization"), x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID")) -> Dict[str, Any]:
    payload = _payload_from_get(page=page, sheet=None, sheet_name=None, name=None, tab=None, symbol=None, ticker=None, symbols=symbols, tickers=tickers, mode=mode, include_matrix=None, schema_only=None, headers_only=None, top_n=top_n, limit=limit, offset=offset)
    return await _run_advisor_logic(request=request, payload=payload, token=token, x_app_token=x_app_token, x_api_key=x_api_key, authorization=authorization, x_request_id=x_request_id, operation="run")


@router.post("/run")
async def advisor_run_post(request: Request, body: Dict[str, Any] = Body(default_factory=dict), token: Optional[str] = Query(default=None), x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"), x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"), authorization: Optional[str] = Header(default=None, alias="Authorization"), x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID")) -> Dict[str, Any]:
    return await _run_advisor_logic(request=request, payload=_safe_dict(body), token=token, x_app_token=x_app_token, x_api_key=x_api_key, authorization=authorization, x_request_id=x_request_id, operation="run")


__all__ = ["router", "ADVISOR_VERSION"]
