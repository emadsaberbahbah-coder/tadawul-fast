#!/usr/bin/env python3
"""
routes/investment_advisor.py
================================================================================
ADVANCED INVESTMENT ADVISOR ROUTER — v2.15.0
================================================================================
BRIDGE-FIRST • ROOT-OWNER ALIGNED • TOP10 FAIL-SOFT • STARTUP-SAFE
AUTH-TOLERANT • GET+POST CANONICAL ALIASES • JSON-SAFE • SCHEMA v2.6.0
DIAGNOSTIC-VISIBLE • ENGINE-V2-PREFERRED • EXCEPTION-MESSAGE-CAPTURE

WHY v2.15.0 — diagnostic visibility + bridge exception capture
--------------------------------------------------------------

This router is the /v1/advanced entry point. Its job is to delegate
to the canonical bridge in routes.advanced_analysis (where the actual
engine call happens) and normalize the result back. v2.15.0 makes that
delegation chain visible in the response so we can debug it without
guessing.

The motivating bug: routes/advanced_analysis.py v4.3.4 added rich
diagnostic fields (upstream_call_summary, upstream_call_outcome) and
core/data_engine_v2.py v5.51.0 added structured error envelopes.
Those fields survive _normalize_payload_from_bridge correctly (it uses
setdefault, not update) — but when the bridge call ITSELF raised an
exception, v2.14.0's catch block at line 1054 reduced the diagnostic
to "bridge exception: {ClassName}" — losing the actual error message
and any per-attempt summary _call_candidate had collected.

v2.15.0 changes (from v2.14.0)
------------------------------

[FIX-1] Engine binding cascade with source tracking. _get_engine() now
    explicitly probes core.data_engine_v2.get_engine() FIRST (the v5.51.0
    engine), tracks which binding succeeded in the module-level
    CORE_ENGINE_SOURCE constant, and surfaces it via meta.engine_source
    on every response. Mirrors the pattern from advanced_analysis v4.3.1
    and enriched_quote v8.4.0.

[FIX-2] _call_candidate now returns (result, call_summary, outcome_label)
    — a per-attempt list with kwargs_keys + outcome (success | typeerror
    | error). Previously it just returned the result and dropped all
    information about which signature variants were tried. This makes
    "all 9 variants raised TypeError" distinguishable from "matched
    variant 3 successfully".

[FIX-3 — CRITICAL] Bridge exception capture preserves the full error
    message, not just the class name. v2.14.0's catch block produced
    `warning="bridge exception: ValueError"` regardless of what the
    actual exception said. v2.15.0 produces `bridge_error: "ValueError:
    <full message truncated to 500 chars>"` AND surfaces
    bridge_call_summary so we can see which signature variant failed
    and how. This is the single highest-value diagnostic improvement.

[FIX-4] Top-level try/except on every handler. advanced_root_get,
    advanced_root_post, advanced_request_post, advanced_request_get
    all wrap their entire body. Uncaught exceptions return a structured
    partial envelope with status="error", _engine_error,
    _engine_error_class instead of bubbling to FastAPI as 500.

[FIX-5] /health and /meta surface engine_source explicitly so
    operators can confirm at a glance which engine binding the router
    is actually using right now.

[FIX-6] Diagnostic logging. [investment_advisor v2.15.0] log prefix
    on warnings. INVESTMENT_ADVISOR_DEBUG=1 enables DEBUG level.

NO BUSINESS LOGIC CHANGED. v2.14.0 callers continue to work unchanged.
v2.14.0's schema-alignment fixes (90/93/7/9 column counts, view +
insights columns) are preserved verbatim.

Verification after deploy
-------------------------
1. /v1/advanced/health and /v1/advanced/meta should report
   version "2.15.0" and engine_source one of:
   - "request.app.state.engine" (typical)
   - "core.data_engine_v2.get_engine().result" (v2 factory)
   - "core.data_engine.get_engine" (legacy fallback — bug indicator)
2. /v1/advanced/sheet-rows?sheet=Market_Leaders&limit=1 — when the
   bridge fails, the response should now contain meta.bridge_error
   (full message), meta.bridge_call_summary (per-attempt outcomes),
   and meta.bridge_call_outcome.
3. When the bridge succeeds, advanced_analysis v4.3.4's
   meta.upstream_call_summary / meta.upstream_call_outcome continue
   to flow through unchanged — _normalize_payload_from_bridge still
   uses setdefault for its own keys.

WHY v2.14.0 (preserved verbatim)
--------------------------------
v2.14.0 brought this router into alignment with the v2.6.0 schema family.
The v2.13.1 build was running with stale column counts and a fallback
schema list missing 9 columns, which surfaced as silent contract drift
whenever schema_registry import failed.

- 🔑 FIX [CRITICAL]: KNOWN_CANONICAL_HEADER_COUNTS updated from
     v2.5.0 numbers (85 / 88) to v2.6.0 numbers (90 / 93).
- 🔑 FIX [HIGH]: _CANONICAL_TOP10_SCHEMA_FALLBACK rewritten to the
     full v2.6.0 layout (93 entries) — 4 view columns + 5 insights
     columns + Upside %.
- FIX: _CANONICAL_INSTRUMENT_SCHEMA_FALLBACK derived as
     _CANONICAL_TOP10_SCHEMA_FALLBACK[:-3] (90 entries).
- FIX: import-time consistency assertions catch schema drift at boot.

WHY v2.13.1 (preserved verbatim)
--------------------------------
- FIX: keeps the canonical root-owner bridge first while preserving bounded
       partial payloads instead of bubbling 502/5xx on bridge failure.
- FIX: stops masking non-TypeError bridge exceptions during signature probing.
- FIX: downgrades empty bridge payloads for Top_10_Investments,
       Insights_Analysis, and Data_Dictionary to partial with warnings.
- FIX: aligns fallback schema keys with the canonical engine contract
       (for example open_price, week_52_high, pb_ratio, ps_ratio, peg_ratio).
- SAFE: no import-time network work and no hard dependency on optional modules.

Co-deployment matrix (Wave 2A)
------------------------------
  Module                                Version    Notes
  -------                               -------    -----
  core/sheets/schema_registry.py        2.6.0      90/93/7/9 column layout
  core/scoring.py                       5.1.0      View + Insights producer
  core/reco_normalize.py                7.1.0      conviction-floor gating
  core/insights_builder.py              1.0.0      pure-function module
  core/investment_advisor.py            5.2.0      v2.6.0 fallback schemas
  routes/investment_advisor.py          2.15.0     this file
  routes/advanced_analysis.py           4.3.4      diagnostic-emitting bridge
  routes/enriched_quote.py              8.4.0      v2-binding cascade
  core/data_engine_v2.py                5.51.0     hardened structured errors
  scripts/run_dashboard_sync.py         6.6.0      passive
================================================================================
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import math
import os
import re
import uuid
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal
from enum import Enum
from importlib import import_module
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, Response, status
from fastapi.encoders import jsonable_encoder

logger = logging.getLogger("routes.investment_advisor")
logger.addHandler(logging.NullHandler())

INVESTMENT_ADVISOR_VERSION = "2.15.0"
ROUTE_FAMILY_NAME = "advanced"
ROUTE_OWNER_NAME = "investment_advisor"

TOP10_PAGE_NAME = "Top_10_Investments"
INSIGHTS_PAGE_NAME = "Insights_Analysis"
DATA_DICTIONARY_PAGE_NAME = "Data_Dictionary"

BASE_SOURCE_PAGES: Tuple[str, ...] = (
    "Market_Leaders",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
    "My_Portfolio",
)
SOURCE_PAGES_SET = set(BASE_SOURCE_PAGES)

KNOWN_CANONICAL_HEADER_COUNTS: Dict[str, int] = {
    "Market_Leaders": 90,
    "Global_Markets": 90,
    "Commodities_FX": 90,
    "Mutual_Funds": 90,
    "My_Portfolio": 90,
    "Insights_Analysis": 7,
    "Top_10_Investments": 93,
    "Data_Dictionary": 9,
}

TOP10_SPECIAL_FIELDS: Tuple[str, ...] = (
    "top10_rank",
    "selection_reason",
    "criteria_snapshot",
)

# v2.14.0: keys for the column groups added in schema v2.3.0 / v2.6.0.
# Exposed for any downstream code that wants to project / select them.
VIEW_COLUMN_KEYS: Tuple[str, ...] = (
    "fundamental_view",
    "technical_view",
    "risk_view",
    "value_view",
)
INSIGHTS_COLUMN_KEYS: Tuple[str, ...] = (
    "sector_relative_score",
    "conviction_score",
    "top_factors",
    "top_risks",
    "position_size_hint",
)

PAGE_ALIAS_MAP: Dict[str, str] = {
    "top10": TOP10_PAGE_NAME,
    "top_10": TOP10_PAGE_NAME,
    "top-10": TOP10_PAGE_NAME,
    "top10investments": TOP10_PAGE_NAME,
    "top_10_investments": TOP10_PAGE_NAME,
    "top-10-investments": TOP10_PAGE_NAME,
    "top 10 investments": TOP10_PAGE_NAME,
    "investment_advisor": TOP10_PAGE_NAME,
    "investment-advisor": TOP10_PAGE_NAME,
    "investment advisor": TOP10_PAGE_NAME,
    "advisor": TOP10_PAGE_NAME,
    "insights": INSIGHTS_PAGE_NAME,
    "insight": INSIGHTS_PAGE_NAME,
    "insights_analysis": INSIGHTS_PAGE_NAME,
    "insights-analysis": INSIGHTS_PAGE_NAME,
    "insights analysis": INSIGHTS_PAGE_NAME,
    "data_dictionary": DATA_DICTIONARY_PAGE_NAME,
    "data-dictionary": DATA_DICTIONARY_PAGE_NAME,
    "data dictionary": DATA_DICTIONARY_PAGE_NAME,
    "dictionary": DATA_DICTIONARY_PAGE_NAME,
    "market_leaders": "Market_Leaders",
    "market-leaders": "Market_Leaders",
    "market leaders": "Market_Leaders",
    "global_markets": "Global_Markets",
    "global-markets": "Global_Markets",
    "global markets": "Global_Markets",
    "commodities_fx": "Commodities_FX",
    "commodities-fx": "Commodities_FX",
    "commodities fx": "Commodities_FX",
    "mutual_funds": "Mutual_Funds",
    "mutual-funds": "Mutual_Funds",
    "mutual funds": "Mutual_Funds",
    "my_portfolio": "My_Portfolio",
    "my-portfolio": "My_Portfolio",
    "my portfolio": "My_Portfolio",
}

BRIDGE_MODULE_CANDIDATES: Tuple[str, ...] = (
    "routes.advanced_analysis",
    "routes.analysis_sheet_rows",
)
BRIDGE_FUNCTION_CANDIDATES: Tuple[str, ...] = (
    "_run_advanced_sheet_rows_impl",
    "run_advanced_sheet_rows_impl",
    "_run_sheet_rows_impl",
    "run_sheet_rows_impl",
    "_sheet_rows_impl",
)

router = APIRouter(prefix="/v1/advanced", tags=["advanced"])

try:
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest  # type: ignore

    PROMETHEUS_AVAILABLE = True
except Exception:  # pragma: no cover
    CONTENT_TYPE_LATEST = "text/plain"
    generate_latest = None  # type: ignore
    PROMETHEUS_AVAILABLE = False

try:
    from core.config import auth_ok, get_settings_cached, is_open_mode  # type: ignore
except Exception:  # pragma: no cover
    auth_ok = None  # type: ignore
    is_open_mode = None  # type: ignore

    def get_settings_cached(*args: Any, **kwargs: Any) -> Any:  # type: ignore
        return None


_CANONICAL_TOP10_SCHEMA_FALLBACK: List[Tuple[str, str]] = [
    ("symbol", "Symbol"),
    ("name", "Name"),
    ("asset_class", "Asset Class"),
    ("exchange", "Exchange"),
    ("currency", "Currency"),
    ("country", "Country"),
    ("sector", "Sector"),
    ("industry", "Industry"),
    ("current_price", "Current Price"),
    ("previous_close", "Previous Close"),
    ("open_price", "Open"),
    ("day_high", "Day High"),
    ("day_low", "Day Low"),
    ("week_52_high", "52W High"),
    ("week_52_low", "52W Low"),
    ("price_change", "Price Change"),
    ("percent_change", "Percent Change"),
    ("week_52_position_pct", "52W Position %"),
    ("volume", "Volume"),
    ("avg_volume_10d", "Avg Volume 10D"),
    ("avg_volume_30d", "Avg Volume 30D"),
    ("market_cap", "Market Cap"),
    ("float_shares", "Float Shares"),
    ("beta_5y", "Beta (5Y)"),
    ("pe_ttm", "P/E (TTM)"),
    ("pe_forward", "P/E (Forward)"),
    ("eps_ttm", "EPS (TTM)"),
    ("dividend_yield", "Dividend Yield"),
    ("payout_ratio", "Payout Ratio"),
    ("revenue_ttm", "Revenue (TTM)"),
    ("revenue_growth_yoy", "Revenue Growth YoY"),
    ("gross_margin", "Gross Margin"),
    ("operating_margin", "Operating Margin"),
    ("profit_margin", "Profit Margin"),
    ("debt_to_equity", "Debt/Equity"),
    ("free_cash_flow_ttm", "Free Cash Flow (TTM)"),
    ("rsi_14", "RSI (14)"),
    ("volatility_30d", "Volatility 30D"),
    ("volatility_90d", "Volatility 90D"),
    ("max_drawdown_1y", "Max Drawdown 1Y"),
    ("var_95_1d", "VaR 95% (1D)"),
    ("sharpe_1y", "Sharpe (1Y)"),
    ("risk_score", "Risk Score"),
    ("risk_bucket", "Risk Bucket"),
    ("pb_ratio", "P/B"),
    ("ps_ratio", "P/S"),
    ("ev_ebitda", "EV/EBITDA"),
    ("peg_ratio", "PEG"),
    ("intrinsic_value", "Intrinsic Value"),
    ("upside_pct", "Upside %"),
    ("valuation_score", "Valuation Score"),
    ("forecast_price_1m", "Forecast Price 1M"),
    ("forecast_price_3m", "Forecast Price 3M"),
    ("forecast_price_12m", "Forecast Price 12M"),
    ("expected_roi_1m", "Expected ROI 1M"),
    ("expected_roi_3m", "Expected ROI 3M"),
    ("expected_roi_12m", "Expected ROI 12M"),
    ("forecast_confidence", "Forecast Confidence"),
    ("confidence_score", "Confidence Score"),
    ("confidence_bucket", "Confidence Bucket"),
    ("value_score", "Value Score"),
    ("quality_score", "Quality Score"),
    ("momentum_score", "Momentum Score"),
    ("growth_score", "Growth Score"),
    ("overall_score", "Overall Score"),
    ("opportunity_score", "Opportunity Score"),
    ("rank_overall", "Rank (Overall)"),
    # Views (4) — added schema v2.3.0; consumed by reco_normalize.from_views
    ("fundamental_view", "Fundamental View"),
    ("technical_view", "Technical View"),
    ("risk_view", "Risk View"),
    ("value_view", "Value View"),
    ("recommendation", "Recommendation"),
    ("recommendation_reason", "Recommendation Reason"),
    ("horizon_days", "Horizon Days"),
    ("invest_period_label", "Invest Period Label"),
    ("position_qty", "Position Qty"),
    ("avg_cost", "Avg Cost"),
    ("position_cost", "Position Cost"),
    ("position_value", "Position Value"),
    ("unrealized_pl", "Unrealized P/L"),
    ("unrealized_pl_pct", "Unrealized P/L %"),
    ("data_provider", "Data Provider"),
    ("last_updated_utc", "Last Updated (UTC)"),
    ("last_updated_riyadh", "Last Updated (Riyadh)"),
    ("warnings", "Warnings"),
    # Insights (5) — added schema v2.6.0; produced by core.insights_builder
    ("sector_relative_score", "Sector-Adj Score"),
    ("conviction_score", "Conviction Score"),
    ("top_factors", "Top Factors"),
    ("top_risks", "Top Risks"),
    ("position_size_hint", "Position Size Hint"),
    ("top10_rank", "Top10 Rank"),
    ("selection_reason", "Selection Reason"),
    ("criteria_snapshot", "Criteria Snapshot"),
]
_CANONICAL_INSTRUMENT_SCHEMA_FALLBACK: List[Tuple[str, str]] = _CANONICAL_TOP10_SCHEMA_FALLBACK[:-3]
_CANONICAL_INSIGHTS_SCHEMA_FALLBACK: List[Tuple[str, str]] = [
    ("section", "Section"),
    ("item", "Item"),
    ("symbol", "Symbol"),
    ("metric", "Metric"),
    ("value", "Value"),
    ("notes", "Notes"),
    ("last_updated_riyadh", "Last Updated (Riyadh)"),
]
_CANONICAL_DATA_DICTIONARY_SCHEMA_FALLBACK: List[Tuple[str, str]] = [
    ("sheet", "Sheet"),
    ("group", "Group"),
    ("header", "Header"),
    ("key", "Key"),
    ("dtype", "DType"),
    ("fmt", "Format"),
    ("required", "Required"),
    ("source", "Source"),
    ("notes", "Notes"),
]

_SCHEMA_CACHE: Dict[str, Tuple[List[str], List[str]]] = {}


# v2.15.0 [FIX-1]: tracks which engine binding actually loaded. Updated
# at request time inside _get_engine(). Surfaced in meta.engine_source on
# every response and in /health and /meta. Mirrors v4.3.1's
# CORE_GET_SHEET_ROWS_SOURCE pattern.
CORE_ENGINE_SOURCE: str = "unresolved"


def _investment_advisor_debug_enabled() -> bool:
    """v2.15.0 [FIX-6]: cheap env-flag check for diagnostic logging."""
    raw = (os.getenv("INVESTMENT_ADVISOR_DEBUG", "") or "").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


# v2.14.0: import-time consistency assertion. If anyone hand-edits the
# fallback list and forgets to keep it in sync with KNOWN_CANONICAL_HEADER_COUNTS,
# we want startup to surface that immediately rather than at row time.
_FALLBACK_INSTRUMENT_LEN = len(_CANONICAL_INSTRUMENT_SCHEMA_FALLBACK)
_FALLBACK_TOP10_LEN = len(_CANONICAL_TOP10_SCHEMA_FALLBACK)
assert _FALLBACK_INSTRUMENT_LEN == KNOWN_CANONICAL_HEADER_COUNTS["Market_Leaders"], (
    "Fallback instrument schema has {} entries, but KNOWN_CANONICAL_HEADER_COUNTS "
    "says {}. Update both together.".format(
        _FALLBACK_INSTRUMENT_LEN, KNOWN_CANONICAL_HEADER_COUNTS["Market_Leaders"]
    )
)
assert _FALLBACK_TOP10_LEN == KNOWN_CANONICAL_HEADER_COUNTS["Top_10_Investments"], (
    "Fallback Top10 schema has {} entries, but KNOWN_CANONICAL_HEADER_COUNTS "
    "says {}. Update both together.".format(
        _FALLBACK_TOP10_LEN, KNOWN_CANONICAL_HEADER_COUNTS["Top_10_Investments"]
    )
)


def _s(v: Any) -> str:
    if v is None:
        return ""
    try:
        out = str(v).strip()
    except Exception:
        return ""
    return "" if out.lower() in {"none", "null", "nil", "undefined"} else out


def _boolish(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    s = _s(v).lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _safe_int(v: Any, default: int) -> int:
    try:
        if v is None or isinstance(v, bool):
            return default
        return int(float(v))
    except Exception:
        return default


def _dedupe_keep_order(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in values:
        s = _s(item)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _split_csv(text: str) -> List[str]:
    raw = (text or "").replace(";", ",").replace("\n", ",").replace("\t", ",")
    return _dedupe_keep_order(part for part in raw.split(","))


def _normalize_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return _split_csv(value)
    if isinstance(value, (list, tuple, set)):
        return _dedupe_keep_order(_s(item) for item in value)
    s = _s(value)
    return [s] if s else []


def _normalize_symbol_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return _dedupe_keep_order(x for x in re.split(r"[\s,;]+", value) if _s(x))
    if isinstance(value, (list, tuple, set)):
        out: List[str] = []
        seen = set()
        for item in value:
            part = _s(item)
            if part and part not in seen:
                seen.add(part)
                out.append(part)
        return out
    s = _s(value)
    return [s] if s else []


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _request_id(request: Optional[Request], x_request_id: Optional[str]) -> str:
    rid = _s(x_request_id)
    if rid:
        return rid
    try:
        if request is not None:
            state_rid = _s(getattr(request.state, "request_id", ""))
            if state_rid:
                return state_rid
            hdr = _s(request.headers.get("X-Request-ID"))
            if hdr:
                return hdr
    except Exception:
        pass
    return uuid.uuid4().hex[:12]


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
                return _s(obj)
        if isinstance(obj, (datetime, date, dt_time)):
            try:
                return obj.isoformat()
            except Exception:
                return _s(obj)
        if isinstance(obj, Enum):
            return _clean(obj.value)
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
            return _s(obj)

    return _clean(value)


def _page_family(page: str) -> str:
    normalized = _normalize_page_name(page)
    if normalized == TOP10_PAGE_NAME:
        return "top10"
    if normalized == INSIGHTS_PAGE_NAME:
        return "insights"
    if normalized == DATA_DICTIONARY_PAGE_NAME:
        return "data_dictionary"
    if normalized in SOURCE_PAGES_SET:
        return "source"
    return ROUTE_FAMILY_NAME


def _canonicalize_name(raw: str) -> str:
    text = _s(raw).lower()
    text = re.sub(r"[\s\-]+", "_", text)
    text = re.sub(r"_+", "_", text)
    return text.strip("_")


def _normalize_page_name(value: Any) -> str:
    raw = _s(value)
    if not raw:
        return TOP10_PAGE_NAME

    try:
        from core.sheets.page_catalog import normalize_page_name as normalize_from_catalog  # type: ignore

        normalized = _s(normalize_from_catalog(raw))
        if normalized:
            return normalized
    except Exception:
        pass

    compact = _canonicalize_name(raw)
    return PAGE_ALIAS_MAP.get(compact, PAGE_ALIAS_MAP.get(raw.strip().lower(), raw))


def _source_pages_only(values: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in values:
        page = _normalize_page_name(item)
        if page in SOURCE_PAGES_SET and page not in seen:
            seen.add(page)
            out.append(page)
    return out


def _schema_fallback_for_page(page: str) -> Tuple[List[str], List[str]]:
    family = _page_family(page)
    if family == "top10":
        schema = _CANONICAL_TOP10_SCHEMA_FALLBACK
    elif family == "insights":
        schema = _CANONICAL_INSIGHTS_SCHEMA_FALLBACK
    elif family == "data_dictionary":
        schema = _CANONICAL_DATA_DICTIONARY_SCHEMA_FALLBACK
    else:
        schema = _CANONICAL_INSTRUMENT_SCHEMA_FALLBACK
    return [h for _, h in schema], [k for k, _ in schema]


def _extract_schema_headers_keys_from_spec(spec: Any) -> Tuple[List[str], List[str]]:
    if spec is None:
        return [], []

    if isinstance(spec, Mapping):
        headers = spec.get("headers") or spec.get("display_headers") or spec.get("sheet_headers") or []
        keys = spec.get("keys") or spec.get("fields") or []
        if isinstance(headers, list) and isinstance(keys, list) and headers and keys:
            return [_s(x) for x in headers if _s(x)], [_s(x) for x in keys if _s(x)]

        columns = spec.get("columns")
        if isinstance(columns, list) and columns:
            headers_out: List[str] = []
            keys_out: List[str] = []
            for idx, col in enumerate(columns):
                if isinstance(col, Mapping):
                    key = _s(col.get("key") or col.get("field") or col.get("name"))
                    header = _s(col.get("header") or col.get("label") or col.get("title"))
                else:
                    key = ""
                    header = ""
                if not key and not header:
                    continue
                if not key:
                    key = f"column_{idx + 1}"
                if not header:
                    header = key.replace("_", " ").title()
                keys_out.append(key)
                headers_out.append(header)
            if headers_out and keys_out:
                return headers_out, keys_out

        for nested_key in ("sheet", "spec", "schema", "contract", "definition"):
            nested = spec.get(nested_key)
            headers2, keys2 = _extract_schema_headers_keys_from_spec(nested)
            if headers2 and keys2:
                return headers2, keys2

    return [], []


def _load_schema_defaults(page: str) -> Tuple[List[str], List[str]]:
    page = _normalize_page_name(page)
    if page in _SCHEMA_CACHE:
        headers, keys = _SCHEMA_CACHE[page]
        return list(headers), list(keys)

    try:
        from core.sheets.schema_registry import get_sheet_spec  # type: ignore

        spec = get_sheet_spec(page)
        headers, keys = _extract_schema_headers_keys_from_spec(spec)
        if headers and keys:
            _SCHEMA_CACHE[page] = (list(headers), list(keys))
            return headers, keys
    except Exception:
        pass

    headers, keys = _schema_fallback_for_page(page)
    _SCHEMA_CACHE[page] = (list(headers), list(keys))
    return headers, keys


def _rows_present(payload: Any) -> bool:
    if not isinstance(payload, Mapping):
        return False

    for key in ("rows", "row_objects", "records", "results", "items", "quotes", "data", "rows_matrix"):
        value = payload.get(key)
        if isinstance(value, list) and len(value) > 0:
            return True
    return False


def _make_meta(
    *,
    request_id: str,
    page: str,
    status_out: str,
    bridge_source: str,
    bridge_name: str,
    warnings: List[str],
    timeout_sec: float,
) -> Dict[str, Any]:
    return jsonable_encoder(
        {
            "request_id": request_id,
            "timestamp_utc": _now_utc(),
            "version": INVESTMENT_ADVISOR_VERSION,
            "route_owner": ROUTE_OWNER_NAME,
            "route_family": ROUTE_FAMILY_NAME,
            "page_family": _page_family(page),
            "bridge_source": bridge_source,
            "bridge_name": bridge_name,
            "bridge_timeout_sec": timeout_sec,
            "warnings": warnings,
            "status": status_out,
            "contract_header_count": KNOWN_CANONICAL_HEADER_COUNTS.get(page),
        }
    )


def _make_schema_only_response(
    page: str,
    *,
    include_matrix: bool,
    request_id: str,
    bridge_source: str = "",
    bridge_name: str = "",
    warnings: Optional[List[str]] = None,
) -> Dict[str, Any]:
    headers, keys = _load_schema_defaults(page)
    rows: List[Dict[str, Any]] = []
    matrix: Optional[List[List[Any]]] = [] if include_matrix else None

    return jsonable_encoder(
        {
            "status": "success",
            "page": page,
            "sheet": page,
            "sheet_name": page,
            "route_family": _page_family(page),
            "headers": headers,
            "display_headers": headers,
            "sheet_headers": headers,
            "column_headers": headers,
            "keys": keys,
            "fields": keys,
            "columns": keys,
            "rows": rows,
            "row_objects": rows,
            "records": rows,
            "results": rows,
            "data": rows,
            "items": rows,
            "quotes": rows,
            "rows_matrix": matrix,
            "version": INVESTMENT_ADVISOR_VERSION,
            "request_id": request_id,
            "meta": _make_meta(
                request_id=request_id,
                page=page,
                status_out="success",
                bridge_source=bridge_source,
                bridge_name=bridge_name,
                warnings=warnings or [],
                timeout_sec=0.0,
            ),
        }
    )


def _make_partial_response(
    page: str,
    *,
    include_matrix: bool,
    request_id: str,
    bridge_source: str,
    bridge_name: str,
    warning: str,
    extra_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    headers, keys = _load_schema_defaults(page)
    rows: List[Dict[str, Any]] = []
    matrix: Optional[List[List[Any]]] = [] if include_matrix else None

    meta = _make_meta(
        request_id=request_id,
        page=page,
        status_out="partial",
        bridge_source=bridge_source,
        bridge_name=bridge_name,
        warnings=[warning],
        timeout_sec=0.0,
    )
    if isinstance(extra_meta, Mapping):
        meta.update(jsonable_encoder(extra_meta))

    return jsonable_encoder(
        {
            "status": "partial",
            "page": page,
            "sheet": page,
            "sheet_name": page,
            "route_family": _page_family(page),
            "headers": headers,
            "display_headers": headers,
            "sheet_headers": headers,
            "column_headers": headers,
            "keys": keys,
            "fields": keys,
            "columns": keys,
            "rows": rows,
            "row_objects": rows,
            "records": rows,
            "results": rows,
            "data": rows,
            "items": rows,
            "quotes": rows,
            "rows_matrix": matrix,
            "version": INVESTMENT_ADVISOR_VERSION,
            "request_id": request_id,
            "meta": meta,
        }
    )


def _is_public_path(path: str) -> bool:
    p = _s(path)
    if not p:
        return False
    if p in {"/v1/advanced", "/v1/advanced/health", "/v1/advanced/meta", "/v1/advanced/metrics"}:
        return True
    env_paths = os.getenv("PUBLIC_PATHS", "") or os.getenv("AUTH_PUBLIC_PATHS", "")
    for raw in env_paths.split(","):
        candidate = raw.strip()
        if not candidate:
            continue
        if candidate.endswith("*") and p.startswith(candidate[:-1]):
            return True
        if p == candidate:
            return True
    return False


def _extract_auth_token(*, token_query: Optional[str], x_app_token: Optional[str], authorization: Optional[str]) -> str:
    token = _s(x_app_token)
    authz = _s(authorization)
    if authz.lower().startswith("bearer "):
        token = authz.split(" ", 1)[1].strip()
    if token_query and not token:
        allow_query = False
        try:
            settings = get_settings_cached()
            allow_query = bool(getattr(settings, "allow_query_token", False))
        except Exception:
            allow_query = False
        if allow_query:
            token = _s(token_query)
    return token


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
        env_v = _s(os.getenv(name))
        if env_v:
            return _boolish(env_v, False)
    return False


def _auth_passed(*, request: Request, token_query: Optional[str], x_app_token: Optional[str], authorization: Optional[str]) -> bool:
    if _is_open_mode_enabled():
        return True

    try:
        path = str(getattr(getattr(request, "url", None), "path", "") or "")
    except Exception:
        path = ""

    if _is_public_path(path):
        return True

    if auth_ok is None:
        return True

    auth_token = _extract_auth_token(token_query=token_query, x_app_token=x_app_token, authorization=authorization)
    headers_dict = dict(request.headers)

    settings = None
    try:
        settings = get_settings_cached()
    except Exception:
        settings = None

    attempts = [
        {
            "token": auth_token or None,
            "authorization": authorization,
            "headers": headers_dict,
            "path": path,
            "request": request,
            "settings": settings,
        },
        {"token": auth_token or None, "authorization": authorization, "headers": headers_dict, "path": path, "request": request},
        {"token": auth_token or None, "authorization": authorization, "headers": headers_dict, "path": path},
        {"token": auth_token or None, "authorization": authorization, "headers": headers_dict},
        {"token": auth_token or None, "authorization": authorization},
        {"token": auth_token or None},
        {},
    ]

    for kwargs in attempts:
        try:
            return bool(auth_ok(**kwargs))
        except TypeError:
            continue
        except Exception:
            return False

    return False


def _require_auth_or_401(*, request: Request, token_query: Optional[str], x_app_token: Optional[str], authorization: Optional[str]) -> None:
    if not _auth_passed(request=request, token_query=token_query, x_app_token=x_app_token, authorization=authorization):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")


async def _get_engine(request: Request) -> Optional[Any]:
    """v2.15.0 [FIX-1]: explicit engine binding with source tracking.

    v2.14.0 walked through bindings and returned the first non-None.
    v2.15.0 explicitly probes core.data_engine_v2 FIRST (the v5.51.0
    engine with hardening), tracks which binding succeeded in the
    module-level CORE_ENGINE_SOURCE constant, and surfaces it via
    meta.engine_source on every response. This is the same observability
    pattern that v4.3.1 introduced for routes/advanced_analysis.py and
    v8.4.0 introduced for routes/enriched_quote.py.

    Cascade order (most-preferred first):
      1. request.app.state.engine|data_engine
         — already-instantiated engine on the FastAPI app state
         (set by main.py at startup; SAME instance shared across requests)
      2. core.data_engine_v2.get_engine() — async factory for v5.51.0
      3. core.data_engine_v2.get_engine_if_ready() — sync ready-check
      4. core.data_engine.get_engine() — legacy fallback (BUG INDICATOR)
      5. core.investment_advisor_engine.get_engine() — last-resort
    """
    global CORE_ENGINE_SOURCE

    # Path 1: app.state (instance shared across requests)
    try:
        st = getattr(request.app, "state", None)
        if st is not None:
            for attr in ("engine", "data_engine"):
                value = getattr(st, attr, None)
                if value is not None:
                    CORE_ENGINE_SOURCE = "request.app.state." + attr
                    return value
    except Exception:
        pass

    # Path 2: core.data_engine_v2.get_engine() — preferred async factory
    try:
        mod = import_module("core.data_engine_v2")
        get_engine = getattr(mod, "get_engine", None)
        if callable(get_engine):
            eng = get_engine()
            if inspect.isawaitable(eng):
                eng = await eng
            if eng is not None:
                CORE_ENGINE_SOURCE = "core.data_engine_v2.get_engine().result"
                return eng
    except Exception as v2_err:
        try:
            logger.info(
                "[investment_advisor v%s] core.data_engine_v2.get_engine() unavailable: %s: %s",
                INVESTMENT_ADVISOR_VERSION, v2_err.__class__.__name__, v2_err,
            )
        except Exception:
            pass

    # Path 3: core.data_engine_v2.get_engine_if_ready() — sync ready-check
    try:
        mod = import_module("core.data_engine_v2")
        get_ready = getattr(mod, "get_engine_if_ready", None)
        if callable(get_ready):
            eng = get_ready()
            if eng is not None:
                CORE_ENGINE_SOURCE = "core.data_engine_v2.get_engine_if_ready().result"
                return eng
    except Exception as ready_err:
        try:
            logger.info(
                "[investment_advisor v%s] core.data_engine_v2.get_engine_if_ready() unavailable: %s: %s",
                INVESTMENT_ADVISOR_VERSION, ready_err.__class__.__name__, ready_err,
            )
        except Exception:
            pass

    # Path 4: legacy fallback — bug indicator if reached
    try:
        mod = import_module("core.data_engine")
        get_engine = getattr(mod, "get_engine", None)
        if callable(get_engine):
            eng = get_engine()
            if inspect.isawaitable(eng):
                eng = await eng
            if eng is not None:
                CORE_ENGINE_SOURCE = "core.data_engine.get_engine"
                try:
                    logger.warning(
                        "[investment_advisor v%s] all v2 binding patterns failed; using legacy "
                        "core.data_engine (this loses v5.51.0 enrichment — investigate v2 exports)",
                        INVESTMENT_ADVISOR_VERSION,
                    )
                except Exception:
                    pass
                return eng
    except Exception:
        pass

    # Path 5: investment_advisor_engine — last-resort
    try:
        mod = import_module("core.investment_advisor_engine")
        for attr in ("get_engine", "engine", "ENGINE"):
            candidate = getattr(mod, attr, None)
            if candidate is None:
                continue
            try:
                if callable(candidate):
                    eng = candidate()
                    if inspect.isawaitable(eng):
                        eng = await eng
                    if eng is not None:
                        CORE_ENGINE_SOURCE = "core.investment_advisor_engine." + attr + "()"
                        return eng
                elif candidate is not None:
                    CORE_ENGINE_SOURCE = "core.investment_advisor_engine." + attr
                    return candidate
            except Exception:
                continue
    except Exception:
        pass

    CORE_ENGINE_SOURCE = "unavailable"
    return None


async def _resolve_bridge_impl() -> Tuple[Optional[Any], str, str]:
    for module_name in BRIDGE_MODULE_CANDIDATES:
        try:
            module = import_module(module_name)
        except Exception:
            continue
        for fn_name in BRIDGE_FUNCTION_CANDIDATES:
            fn = getattr(module, fn_name, None)
            if callable(fn):
                return fn, module_name, fn_name
    return None, "", ""


async def _call_candidate(
    fn: Any,
    *,
    body: Dict[str, Any],
    request: Request,
    page: str,
    limit: int,
    offset: int,
    schema_only: bool,
) -> Tuple[Any, List[Dict[str, Any]], str]:
    """v2.15.0 [FIX-2]: returns (result, call_summary, outcome_label).

    call_summary is a per-attempt list of dicts with keys:
      - attempt_idx (int)
      - kwargs_keys (list[str])     — what we passed
      - outcome (str)                — one of: success | typeerror | error
      - error_class (str, optional)
      - error_message (str, truncated, optional)

    outcome_label is one of:
      - "success"                       — result returned (may still be None)
      - "all_signatures_typed_mismatch" — every variant raised TypeError
      - "raised"                        — non-TypeError raised; raise propagated
                                          to caller
      - "no_attempts_executed"          — fn was None
      - "fn_not_callable"               — fn provided but not callable

    The outcome distinguishes "didn't match any signature" (typed_mismatch)
    from "matched a signature but the call body raised" (raised) — the
    same distinction used in advanced_analysis v4.3.4 and enriched_quote
    v8.4.0's tolerant call wrappers.

    Note: when outcome is "raised", _execute_via_bridge's catch block will
    receive the exception; call_summary is captured separately on the
    last_call_summary module-level slot so the caller can pull it after
    the raise. We expose this via a function-attribute storage pattern.
    """
    if fn is None:
        return None, [], "no_attempts_executed"
    if not callable(fn):
        return None, [], "fn_not_callable"

    kwargs_variants = [
        {"request": request, "body": body, "page": page, "limit": limit, "offset": offset, "schema_only": schema_only},
        {"request": request, "payload": body, "page": page, "limit": limit, "offset": offset, "schema_only": schema_only},
        {"request": request, "page": page, "limit": limit, "offset": offset, "schema_only": schema_only, **body},
        {"page": page, "limit": limit, "offset": offset, "schema_only": schema_only, **body},
        {"request": request, "body": body},
        {"payload": body},
        {"request": request, **body},
        body,
        {},
    ]

    call_summary: List[Dict[str, Any]] = []
    last_type_error: Optional[Exception] = None

    for attempt_idx, kwargs in enumerate(kwargs_variants):
        kwargs_keys = sorted(list(kwargs.keys()))[:15]
        try:
            result = fn(**kwargs)
            if inspect.isawaitable(result):
                result = await result
            call_summary.append({
                "attempt_idx": attempt_idx,
                "kwargs_keys": kwargs_keys,
                "outcome": "success",
            })
            return result, call_summary, "success"
        except TypeError as exc:
            call_summary.append({
                "attempt_idx": attempt_idx,
                "kwargs_keys": kwargs_keys,
                "outcome": "typeerror",
                "error_class": "TypeError",
                "error_message": str(exc)[:200],
            })
            last_type_error = exc
            continue
        except Exception as exc:
            call_summary.append({
                "attempt_idx": attempt_idx,
                "kwargs_keys": kwargs_keys,
                "outcome": "error",
                "error_class": exc.__class__.__name__,
                "error_message": str(exc)[:200],
            })
            # Surface the call_summary on the function attribute so the
            # caller's except block can retrieve it after the raise.
            try:
                _call_candidate._last_call_summary = call_summary  # type: ignore[attr-defined]
            except Exception:
                pass
            raise

    # All attempts raised TypeError
    if last_type_error is not None:
        try:
            _call_candidate._last_call_summary = call_summary  # type: ignore[attr-defined]
        except Exception:
            pass
        return None, call_summary, "all_signatures_typed_mismatch"
    return None, call_summary, "no_attempts_executed"


def _bridge_timeout_for_page(page: str) -> float:
    page = _normalize_page_name(page)
    if page == TOP10_PAGE_NAME:
        default = 20.0
    elif page == DATA_DICTIONARY_PAGE_NAME:
        default = 12.0
    else:
        default = 15.0

    try:
        env_value = float(os.getenv("TFB_ADV_BRIDGE_TIMEOUT_SEC", str(default)))
        return max(0.5, env_value)
    except Exception:
        return default


def _append_warning(meta: Dict[str, Any], warning: str) -> None:
    warnings = _normalize_list(meta.get("warnings"))
    if warning not in warnings:
        warnings.append(warning)
    meta["warnings"] = warnings


def _normalize_payload_from_bridge(
    payload: Any,
    *,
    page: str,
    include_matrix: bool,
    request_id: str,
    bridge_source: str,
    bridge_name: str,
    timeout_sec: float,
) -> Dict[str, Any]:
    page = _normalize_page_name(page)

    if not isinstance(payload, Mapping):
        return _make_partial_response(
            page,
            include_matrix=include_matrix,
            request_id=request_id,
            bridge_source=bridge_source,
            bridge_name=bridge_name,
            warning="bridge returned non-mapping payload",
            extra_meta={"bridge_timeout_sec": timeout_sec},
        )

    out = dict(jsonable_encoder(payload))
    out["page"] = out.get("page") or page
    out["sheet"] = out.get("sheet") or page
    out["sheet_name"] = out.get("sheet_name") or page
    out["route_family"] = out.get("route_family") or _page_family(page)
    out["version"] = out.get("version") or INVESTMENT_ADVISOR_VERSION
    out["request_id"] = out.get("request_id") or request_id

    meta = dict(out.get("meta") or {})
    meta.setdefault("request_id", out["request_id"])
    meta.setdefault("timestamp_utc", _now_utc())
    meta.setdefault("version", out["version"])
    meta.setdefault("route_owner", ROUTE_OWNER_NAME)
    meta.setdefault("route_family", ROUTE_FAMILY_NAME)
    meta.setdefault("bridge_source", bridge_source)
    meta.setdefault("bridge_name", bridge_name)
    meta.setdefault("bridge_timeout_sec", timeout_sec)
    out["meta"] = meta

    headers = out.get("headers") or out.get("display_headers") or []
    keys = out.get("keys") or out.get("fields") or []

    if not isinstance(headers, list) or not headers:
        headers, keys_from_schema = _load_schema_defaults(page)
        if not isinstance(keys, list) or not keys:
            keys = keys_from_schema

    if not isinstance(keys, list) or not keys:
        _, keys = _load_schema_defaults(page)

    out["headers"] = headers
    out["display_headers"] = out.get("display_headers") or headers
    out["sheet_headers"] = out.get("sheet_headers") or headers
    out["column_headers"] = out.get("column_headers") or headers
    out["keys"] = keys
    out["fields"] = out.get("fields") or keys
    out["columns"] = out.get("columns") or keys

    if include_matrix and out.get("rows_matrix") is None:
        rows = out.get("rows") or out.get("row_objects") or out.get("records") or out.get("results") or []
        if isinstance(rows, list) and rows and isinstance(rows[0], Mapping):
            matrix = []
            for row in rows:
                matrix.append([row.get(k) for k in keys])
            out["rows_matrix"] = matrix
        elif out.get("rows_matrix") is None:
            out["rows_matrix"] = []

    has_rows = _rows_present(out)
    if not has_rows:
        out["status"] = "partial"
        if page == TOP10_PAGE_NAME:
            _append_warning(out["meta"], "Top10 bridge returned no rows.")
        elif page == INSIGHTS_PAGE_NAME:
            _append_warning(out["meta"], "Insights bridge returned no rows.")
        elif page == DATA_DICTIONARY_PAGE_NAME:
            _append_warning(out["meta"], "Data Dictionary bridge returned no rows.")
        else:
            _append_warning(out["meta"], "Bridge returned no rows.")
    elif not out.get("status"):
        out["status"] = "success"

    return out


async def _execute_via_bridge(
    *,
    request: Request,
    body: Dict[str, Any],
    include_matrix: Optional[bool],
    limit: Optional[int],
    offset: Optional[int],
    schema_only: Optional[bool],
    headers_only: Optional[bool],
    x_request_id: Optional[str],
) -> Dict[str, Any]:
    page = _normalize_page_name(body.get("page") or body.get("sheet") or body.get("sheet_name") or TOP10_PAGE_NAME)
    body = dict(body or {})
    body["page"] = page
    body["sheet"] = page
    body["sheet_name"] = page

    include_matrix_final = _boolish(include_matrix if include_matrix is not None else body.get("include_matrix"), False)
    schema_only_final = _boolish(schema_only if schema_only is not None else body.get("schema_only"), False)
    headers_only_final = _boolish(headers_only if headers_only is not None else body.get("headers_only"), False)
    limit_final = _safe_int(limit if limit is not None else body.get("limit") or body.get("top_n"), 20)
    offset_final = _safe_int(offset if offset is not None else body.get("offset"), 0)

    body["limit"] = limit_final
    body["top_n"] = limit_final
    body["offset"] = offset_final
    body["include_matrix"] = include_matrix_final
    body["schema_only"] = schema_only_final
    body["headers_only"] = headers_only_final

    request_id = _request_id(request, x_request_id)

    bridge_impl, bridge_source, bridge_name = await _resolve_bridge_impl()

    # v2.15.0 [FIX-1]: ensure CORE_ENGINE_SOURCE is fresh for this request,
    # surfaced in any partial responses below.
    try:
        await _get_engine(request)
    except Exception:
        pass

    if schema_only_final or headers_only_final:
        resp = _make_schema_only_response(
            page,
            include_matrix=include_matrix_final,
            request_id=request_id,
            bridge_source=bridge_source,
            bridge_name=bridge_name,
            warnings=(["headers_only"] if headers_only_final else None),
        )
        if isinstance(resp.get("meta"), dict):
            resp["meta"]["engine_source"] = CORE_ENGINE_SOURCE
        return resp

    if bridge_impl is None:
        return _make_partial_response(
            page,
            include_matrix=include_matrix_final,
            request_id=request_id,
            bridge_source="",
            bridge_name="",
            warning="canonical root bridge unavailable",
            extra_meta={"engine_source": CORE_ENGINE_SOURCE},
        )

    timeout_sec = _bridge_timeout_for_page(page)

    # v2.15.0 [FIX-2 + FIX-3]: capture call_summary and outcome. Errors
    # captured with FULL message, not just class name.
    bridge_call_summary: List[Dict[str, Any]] = []
    bridge_call_outcome: str = "unknown"
    bridge_error_class: Optional[str] = None
    bridge_error_message: Optional[str] = None
    payload: Any = None

    try:
        payload, bridge_call_summary, bridge_call_outcome = await asyncio.wait_for(
            _call_candidate(
                bridge_impl,
                body=body,
                request=request,
                page=page,
                limit=limit_final,
                offset=offset_final,
                schema_only=schema_only_final,
            ),
            timeout=timeout_sec,
        )
    except asyncio.TimeoutError:
        # Try to recover whatever call_summary _call_candidate captured
        # before the timeout fired.
        try:
            bridge_call_summary = list(getattr(_call_candidate, "_last_call_summary", []) or [])
        except Exception:
            bridge_call_summary = []
        bridge_call_outcome = "timeout"
        return _make_partial_response(
            page,
            include_matrix=include_matrix_final,
            request_id=request_id,
            bridge_source=bridge_source,
            bridge_name=bridge_name,
            warning="bridge timeout after {:.1f}s".format(timeout_sec),
            extra_meta={
                "bridge_timeout_sec": timeout_sec,
                "bridge_call_outcome": bridge_call_outcome,
                "bridge_call_summary": bridge_call_summary[:5],
                "engine_source": CORE_ENGINE_SOURCE,
            },
        )
    except Exception as exc:
        # v2.15.0 [FIX-3 — CRITICAL]: capture full error message AND
        # the call_summary _call_candidate accumulated before raising.
        # v2.14.0 only kept the class name. v2.15.0 surfaces the full
        # message (truncated) plus per-attempt diagnostic.
        bridge_error_class = exc.__class__.__name__
        bridge_error_message = "{}: {}".format(bridge_error_class, str(exc)[:500])
        bridge_call_outcome = "raised"
        try:
            bridge_call_summary = list(getattr(_call_candidate, "_last_call_summary", []) or [])
        except Exception:
            bridge_call_summary = []

        try:
            logger.warning(
                "[investment_advisor v%s] bridge raised: source=%r name=%r error=%s",
                INVESTMENT_ADVISOR_VERSION, bridge_source, bridge_name, bridge_error_message,
            )
        except Exception:
            pass

        return _make_partial_response(
            page,
            include_matrix=include_matrix_final,
            request_id=request_id,
            bridge_source=bridge_source,
            bridge_name=bridge_name,
            warning="bridge exception: {}".format(bridge_error_class),
            extra_meta={
                "bridge_timeout_sec": timeout_sec,
                "bridge_error": bridge_error_message,
                "bridge_error_class": bridge_error_class,
                "bridge_call_outcome": bridge_call_outcome,
                "bridge_call_summary": bridge_call_summary[:5],
                "engine_source": CORE_ENGINE_SOURCE,
            },
        )

    # Success path: payload is what the bridge returned. Forward through
    # _normalize_payload_from_bridge then attach call_summary + engine_source.
    normalized = _normalize_payload_from_bridge(
        payload,
        page=page,
        include_matrix=include_matrix_final,
        request_id=request_id,
        bridge_source=bridge_source,
        bridge_name=bridge_name,
        timeout_sec=timeout_sec,
    )
    if isinstance(normalized.get("meta"), dict):
        meta_ref = normalized["meta"]
        # setdefault so v4.3.4's upstream_call_summary etc. aren't clobbered
        meta_ref.setdefault("bridge_call_outcome", bridge_call_outcome)
        if bridge_call_summary:
            meta_ref.setdefault("bridge_call_summary", bridge_call_summary[:5])
        meta_ref.setdefault("engine_source", CORE_ENGINE_SOURCE)
    return normalized


def _advanced_get_body(
    *,
    page: Optional[str],
    sheet: Optional[str],
    sheet_name: Optional[str],
    name: Optional[str],
    tab: Optional[str],
    symbols: Optional[str],
    tickers: Optional[str],
    pages: Optional[str],
    sources: Optional[str],
    risk_level: Optional[str],
    risk_profile: Optional[str],
    confidence_level: Optional[str],
    confidence_bucket: Optional[str],
    investment_period_days: Optional[int],
    horizon_days: Optional[int],
    min_expected_roi: Optional[float],
    min_roi: Optional[float],
    min_confidence: Optional[float],
    top_n: Optional[int],
    limit: Optional[int],
    offset: Optional[int],
    include_matrix: Optional[bool],
    schema_only: Optional[bool],
    headers_only: Optional[bool],
) -> Dict[str, Any]:
    target_page = _normalize_page_name(page or sheet or sheet_name or name or tab or TOP10_PAGE_NAME)
    direct_symbols = _normalize_symbol_list(symbols) or _normalize_symbol_list(tickers)
    selected_pages = _normalize_list(pages) or _normalize_list(sources)

    return {
        "page": target_page,
        "sheet": target_page,
        "sheet_name": target_page,
        "direct_symbols": direct_symbols,
        "symbols": direct_symbols,
        "tickers": direct_symbols,
        "pages_selected": selected_pages,
        "source_pages": _source_pages_only(selected_pages),
        "risk_level": _s(risk_level) or _s(risk_profile),
        "risk_profile": _s(risk_profile) or _s(risk_level),
        "confidence_level": _s(confidence_level),
        "confidence_bucket": _s(confidence_bucket),
        "investment_period_days": investment_period_days,
        "horizon_days": horizon_days,
        "min_expected_roi": min_expected_roi,
        "min_roi": min_roi if min_roi is not None else min_expected_roi,
        "min_confidence": min_confidence,
        "top_n": top_n if top_n is not None else limit,
        "limit": limit if limit is not None else top_n,
        "offset": offset,
        "include_matrix": include_matrix,
        "schema_only": schema_only,
        "headers_only": headers_only,
    }


def _advanced_root_has_request_filters(
    *,
    page: Optional[str],
    sheet: Optional[str],
    sheet_name: Optional[str],
    name: Optional[str],
    tab: Optional[str],
    symbols: Optional[str],
    tickers: Optional[str],
    pages: Optional[str],
    sources: Optional[str],
    risk_level: Optional[str],
    risk_profile: Optional[str],
    confidence_level: Optional[str],
    confidence_bucket: Optional[str],
    investment_period_days: Optional[int],
    horizon_days: Optional[int],
    min_expected_roi: Optional[float],
    min_roi: Optional[float],
    min_confidence: Optional[float],
    top_n: Optional[int],
    limit: Optional[int],
    offset: Optional[int],
    mode: str,
    include_matrix: Optional[bool],
    schema_only: Optional[bool],
    headers_only: Optional[bool],
) -> bool:
    values = [
        page,
        sheet,
        sheet_name,
        name,
        tab,
        symbols,
        tickers,
        pages,
        sources,
        risk_level,
        risk_profile,
        confidence_level,
        confidence_bucket,
        investment_period_days,
        horizon_days,
        min_expected_roi,
        min_roi,
        min_confidence,
        top_n,
        limit,
        offset,
        mode,
        include_matrix,
        schema_only,
        headers_only,
    ]
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return True
    return False


def _make_handler_exception_response(
    *,
    page_hint: str,
    request_id: str,
    handler_name: str,
    exc: BaseException,
) -> Dict[str, Any]:
    """v2.15.0 [FIX-4]: structured envelope for unhandled handler exceptions.

    Returns a payload with status="error" plus _engine_error / _engine_error_class
    in meta, instead of letting the exception bubble to FastAPI as a 500.
    Same shape as a partial response so client code that handles
    `partial`/`degraded` continues to work.
    """
    page = _normalize_page_name(page_hint or TOP10_PAGE_NAME)
    error_class = exc.__class__.__name__
    error_repr = "{}: {}".format(error_class, str(exc)[:500])
    headers, keys = _load_schema_defaults(page)
    meta = _make_meta(
        request_id=request_id,
        page=page,
        status_out="error",
        bridge_source="",
        bridge_name="",
        warnings=["{} top-level exception: {}".format(handler_name, error_class)],
        timeout_sec=0.0,
    )
    meta["_engine_error"] = error_repr
    meta["_engine_error_class"] = error_class
    meta["engine_source"] = CORE_ENGINE_SOURCE
    meta["handler"] = handler_name

    try:
        logger.error(
            "[investment_advisor v%s] %s top-level exception: %s",
            INVESTMENT_ADVISOR_VERSION, handler_name, error_repr,
            exc_info=True,
        )
    except Exception:
        pass

    return jsonable_encoder(
        {
            "status": "error",
            "page": page,
            "sheet": page,
            "sheet_name": page,
            "route_family": _page_family(page),
            "headers": headers,
            "display_headers": headers,
            "sheet_headers": headers,
            "column_headers": headers,
            "keys": keys,
            "fields": keys,
            "columns": keys,
            "rows": [],
            "row_objects": [],
            "records": [],
            "results": [],
            "data": [],
            "items": [],
            "quotes": [],
            "rows_matrix": [],
            "version": INVESTMENT_ADVISOR_VERSION,
            "request_id": request_id,
            "error": error_repr,
            "detail": error_repr,
            "meta": meta,
        }
    )


async def _advanced_root_summary(request: Request) -> Dict[str, Any]:
    engine = await _get_engine(request)
    bridge_impl, bridge_source, bridge_name = await _resolve_bridge_impl()

    return jsonable_encoder(
        {
            "status": "success" if bridge_impl else "degraded",
            "service": "advanced_investment_advisor",
            "version": INVESTMENT_ADVISOR_VERSION,
            "route_owner": ROUTE_OWNER_NAME,
            "route_family": ROUTE_FAMILY_NAME,
            "root_path": "/v1/advanced",
            "engine_present": bool(engine),
            "engine_type": type(engine).__name__ if engine else "none",
            "engine_source": CORE_ENGINE_SOURCE,
            "bridge_available": bool(bridge_impl),
            "bridge_source": bridge_source,
            "bridge_name": bridge_name,
            "contract_header_counts": dict(KNOWN_CANONICAL_HEADER_COUNTS),
            "view_columns": list(VIEW_COLUMN_KEYS),
            "insights_columns": list(INSIGHTS_COLUMN_KEYS),
            "supported_aliases": [
                "/v1/advanced",
                "/v1/advanced/sheet-rows",
                "/v1/advanced/recommendations",
                "/v1/advanced/top10",
                "/v1/advanced/top10-investments",
                "/v1/advanced/investment-advisor",
                "/v1/advanced/advisor",
                "/v1/advanced/run",
                "/v1/advanced/health",
                "/v1/advanced/meta",
                "/v1/advanced/metrics",
            ],
            "timestamp_utc": _now_utc(),
        }
    )


@router.get("")
async def advanced_root_get(
    request: Request,
    response: Response,
    page: Optional[str] = Query(default=None),
    sheet: Optional[str] = Query(default=None),
    sheet_name: Optional[str] = Query(default=None),
    name: Optional[str] = Query(default=None),
    tab: Optional[str] = Query(default=None),
    symbols: Optional[str] = Query(default=None),
    tickers: Optional[str] = Query(default=None),
    pages: Optional[str] = Query(default=None),
    sources: Optional[str] = Query(default=None),
    risk_level: Optional[str] = Query(default=None),
    risk_profile: Optional[str] = Query(default=None),
    confidence_level: Optional[str] = Query(default=None),
    confidence_bucket: Optional[str] = Query(default=None),
    investment_period_days: Optional[int] = Query(default=None, ge=1, le=3650),
    horizon_days: Optional[int] = Query(default=None, ge=1, le=3650),
    min_expected_roi: Optional[float] = Query(default=None),
    min_roi: Optional[float] = Query(default=None),
    min_confidence: Optional[float] = Query(default=None),
    top_n: Optional[int] = Query(default=None, ge=1, le=200),
    limit: Optional[int] = Query(default=None, ge=1, le=200),
    offset: Optional[int] = Query(default=None, ge=0, le=50000),
    mode: str = Query(default=""),
    include_matrix: Optional[bool] = Query(default=None),
    schema_only: Optional[bool] = Query(default=None),
    headers_only: Optional[bool] = Query(default=None),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    request_id = _request_id(request, x_request_id)
    try:
        _require_auth_or_401(request=request, token_query=token, x_app_token=x_app_token, authorization=authorization)

        has_filters = _advanced_root_has_request_filters(
            page=page,
            sheet=sheet,
            sheet_name=sheet_name,
            name=name,
            tab=tab,
            symbols=symbols,
            tickers=tickers,
            pages=pages,
            sources=sources,
            risk_level=risk_level,
            risk_profile=risk_profile,
            confidence_level=confidence_level,
            confidence_bucket=confidence_bucket,
            investment_period_days=investment_period_days,
            horizon_days=horizon_days,
            min_expected_roi=min_expected_roi,
            min_roi=min_roi,
            min_confidence=min_confidence,
            top_n=top_n,
            limit=limit,
            offset=offset,
            mode=mode,
            include_matrix=include_matrix,
            schema_only=schema_only,
            headers_only=headers_only,
        )

        if not has_filters:
            payload = await _advanced_root_summary(request)
            response.headers["X-Request-ID"] = request_id
            return payload

        body = _advanced_get_body(
            page=page,
            sheet=sheet,
            sheet_name=sheet_name,
            name=name,
            tab=tab,
            symbols=symbols,
            tickers=tickers,
            pages=pages,
            sources=sources,
            risk_level=risk_level,
            risk_profile=risk_profile,
            confidence_level=confidence_level,
            confidence_bucket=confidence_bucket,
            investment_period_days=investment_period_days,
            horizon_days=horizon_days,
            min_expected_roi=min_expected_roi,
            min_roi=min_roi,
            min_confidence=min_confidence,
            top_n=top_n,
            limit=limit,
            offset=offset,
            include_matrix=include_matrix,
            schema_only=schema_only,
            headers_only=headers_only,
        )

        payload = await _execute_via_bridge(
            request=request,
            body=body,
            include_matrix=include_matrix,
            limit=limit if limit is not None else top_n,
            offset=offset,
            schema_only=schema_only,
            headers_only=headers_only,
            x_request_id=x_request_id,
        )
        response.headers["X-Request-ID"] = payload.get("request_id") or request_id
        return payload
    except HTTPException:
        # Auth failures and other 4xx — re-raise as-is
        raise
    except Exception as handler_err:
        # v2.15.0 [FIX-4]: top-level catch — return structured envelope
        page_hint = page or sheet or sheet_name or name or tab or TOP10_PAGE_NAME
        return _make_handler_exception_response(
            page_hint=page_hint,
            request_id=request_id,
            handler_name="advanced_root_get",
            exc=handler_err,
        )


@router.post("")
async def advanced_root_post(
    request: Request,
    response: Response,
    body: Dict[str, Any] = Body(default_factory=dict),
    mode: str = Query(default=""),
    include_matrix: Optional[bool] = Query(default=None),
    limit: Optional[int] = Query(default=None, ge=1, le=200),
    offset: Optional[int] = Query(default=None, ge=0, le=50000),
    schema_only: Optional[bool] = Query(default=None),
    headers_only: Optional[bool] = Query(default=None),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    request_id = _request_id(request, x_request_id)
    try:
        _require_auth_or_401(request=request, token_query=token, x_app_token=x_app_token, authorization=authorization)
        payload = await _execute_via_bridge(
            request=request,
            body=dict(body or {}),
            include_matrix=include_matrix,
            limit=limit,
            offset=offset,
            schema_only=schema_only,
            headers_only=headers_only,
            x_request_id=x_request_id,
        )
        response.headers["X-Request-ID"] = payload.get("request_id") or request_id
        return payload
    except HTTPException:
        raise
    except Exception as handler_err:
        page_hint = TOP10_PAGE_NAME
        try:
            if isinstance(body, Mapping):
                page_hint = _s(body.get("page") or body.get("sheet") or body.get("sheet_name")) or page_hint
        except Exception:
            pass
        return _make_handler_exception_response(
            page_hint=page_hint,
            request_id=request_id,
            handler_name="advanced_root_post",
            exc=handler_err,
        )


@router.get("/health")
async def advanced_health(request: Request) -> Dict[str, Any]:
    engine = await _get_engine(request)
    bridge_impl, bridge_source, bridge_name = await _resolve_bridge_impl()
    return jsonable_encoder(
        {
            "status": "ok" if bridge_impl else "degraded",
            "service": "advanced_investment_advisor",
            "version": INVESTMENT_ADVISOR_VERSION,
            "route_owner": ROUTE_OWNER_NAME,
            "route_family": ROUTE_FAMILY_NAME,
            "engine_available": bool(engine),
            "engine_type": type(engine).__name__ if engine else "none",
            "engine_source": CORE_ENGINE_SOURCE,
            "bridge_available": bool(bridge_impl),
            "bridge_source": bridge_source,
            "bridge_name": bridge_name,
            "contract_header_counts": dict(KNOWN_CANONICAL_HEADER_COUNTS),
            "timestamp_utc": _now_utc(),
        }
    )


@router.get("/meta")
async def advanced_meta(request: Request) -> Dict[str, Any]:
    engine = await _get_engine(request)
    bridge_impl, bridge_source, bridge_name = await _resolve_bridge_impl()
    return jsonable_encoder(
        {
            "status": "success",
            "version": INVESTMENT_ADVISOR_VERSION,
            "route_owner": ROUTE_OWNER_NAME,
            "route_family": ROUTE_FAMILY_NAME,
            "engine_present": bool(engine),
            "engine_type": type(engine).__name__ if engine else "none",
            "engine_source": CORE_ENGINE_SOURCE,
            "bridge_available": bool(bridge_impl),
            "bridge_source": bridge_source,
            "bridge_name": bridge_name,
            "contract_header_counts": dict(KNOWN_CANONICAL_HEADER_COUNTS),
            "view_columns": list(VIEW_COLUMN_KEYS),
            "insights_columns": list(INSIGHTS_COLUMN_KEYS),
            "timestamp_utc": _now_utc(),
        }
    )


@router.get("/metrics")
async def advanced_metrics() -> Response:
    if not PROMETHEUS_AVAILABLE or generate_latest is None:
        return Response(content="Metrics not available", media_type="text/plain", status_code=503)
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.post("/top10-investments")
@router.post("/top10")
@router.post("/investment-advisor")
@router.post("/advisor")
@router.post("/run")
@router.post("/sheet-rows")
async def advanced_request_post(
    request: Request,
    response: Response,
    body: Dict[str, Any] = Body(default_factory=dict),
    mode: str = Query(default=""),
    include_matrix: Optional[bool] = Query(default=None),
    limit: Optional[int] = Query(default=None, ge=1, le=200),
    offset: Optional[int] = Query(default=None, ge=0, le=50000),
    schema_only: Optional[bool] = Query(default=None),
    headers_only: Optional[bool] = Query(default=None),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    request_id = _request_id(request, x_request_id)
    try:
        _require_auth_or_401(request=request, token_query=token, x_app_token=x_app_token, authorization=authorization)
        body = dict(body or {})
        if mode and not body.get("mode"):
            body["mode"] = mode

        payload = await _execute_via_bridge(
            request=request,
            body=body,
            include_matrix=include_matrix,
            limit=limit,
            offset=offset,
            schema_only=schema_only,
            headers_only=headers_only,
            x_request_id=x_request_id,
        )
        response.headers["X-Request-ID"] = payload.get("request_id") or request_id
        return payload
    except HTTPException:
        raise
    except Exception as handler_err:
        page_hint = TOP10_PAGE_NAME
        try:
            if isinstance(body, Mapping):
                page_hint = _s(body.get("page") or body.get("sheet") or body.get("sheet_name")) or page_hint
        except Exception:
            pass
        return _make_handler_exception_response(
            page_hint=page_hint,
            request_id=request_id,
            handler_name="advanced_request_post",
            exc=handler_err,
        )


@router.get("/recommendations")
@router.get("/sheet-rows")
async def advanced_request_get(
    request: Request,
    response: Response,
    page: Optional[str] = Query(default=None),
    sheet: Optional[str] = Query(default=None),
    sheet_name: Optional[str] = Query(default=None),
    name: Optional[str] = Query(default=None),
    tab: Optional[str] = Query(default=None),
    symbols: Optional[str] = Query(default=None),
    tickers: Optional[str] = Query(default=None),
    pages: Optional[str] = Query(default=None),
    sources: Optional[str] = Query(default=None),
    risk_level: Optional[str] = Query(default=None),
    risk_profile: Optional[str] = Query(default=None),
    confidence_level: Optional[str] = Query(default=None),
    confidence_bucket: Optional[str] = Query(default=None),
    investment_period_days: Optional[int] = Query(default=None, ge=1, le=3650),
    horizon_days: Optional[int] = Query(default=None, ge=1, le=3650),
    min_expected_roi: Optional[float] = Query(default=None),
    min_roi: Optional[float] = Query(default=None),
    min_confidence: Optional[float] = Query(default=None),
    top_n: Optional[int] = Query(default=None, ge=1, le=200),
    limit: Optional[int] = Query(default=None, ge=1, le=200),
    offset: Optional[int] = Query(default=None, ge=0, le=50000),
    mode: str = Query(default=""),
    include_matrix: Optional[bool] = Query(default=None),
    schema_only: Optional[bool] = Query(default=None),
    headers_only: Optional[bool] = Query(default=None),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    request_id = _request_id(request, x_request_id)
    try:
        _require_auth_or_401(request=request, token_query=token, x_app_token=x_app_token, authorization=authorization)

        body = _advanced_get_body(
            page=page,
            sheet=sheet,
            sheet_name=sheet_name,
            name=name,
            tab=tab,
            symbols=symbols,
            tickers=tickers,
            pages=pages,
            sources=sources,
            risk_level=risk_level,
            risk_profile=risk_profile,
            confidence_level=confidence_level,
            confidence_bucket=confidence_bucket,
            investment_period_days=investment_period_days,
            horizon_days=horizon_days,
            min_expected_roi=min_expected_roi,
            min_roi=min_roi,
            min_confidence=min_confidence,
            top_n=top_n,
            limit=limit,
            offset=offset,
            include_matrix=include_matrix,
            schema_only=schema_only,
            headers_only=headers_only,
        )
        if mode:
            body["mode"] = mode

        payload = await _execute_via_bridge(
            request=request,
            body=body,
            include_matrix=include_matrix,
            limit=limit if limit is not None else top_n,
            offset=offset,
            schema_only=schema_only,
            headers_only=headers_only,
            x_request_id=x_request_id,
        )
        response.headers["X-Request-ID"] = payload.get("request_id") or request_id
        return payload
    except HTTPException:
        raise
    except Exception as handler_err:
        page_hint = page or sheet or sheet_name or name or tab or TOP10_PAGE_NAME
        return _make_handler_exception_response(
            page_hint=page_hint,
            request_id=request_id,
            handler_name="advanced_request_get",
            exc=handler_err,
        )


__all__ = [
    "router",
    "INVESTMENT_ADVISOR_VERSION",
    "KNOWN_CANONICAL_HEADER_COUNTS",
    "VIEW_COLUMN_KEYS",
    "INSIGHTS_COLUMN_KEYS",
]
